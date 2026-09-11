from __future__ import annotations

import json
import logging
import math
import threading
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from fastapi.encoders import jsonable_encoder

from app.config import settings
from app.db import get_conn
from app.db_clickhouse import query_dict
from app.services.etl_orchestrator import (
    TENANT_TRACK_LOCK_NAMESPACE,
    advisory_lock_is_available,
    inspect_running_etl_state,
)

SNAPSHOT_TABLE = "app.snapshot_cache"

# Bump when snapshot scope identity changes. v2 distinguishes empty [] from
# legacy unscoped None (both previously serialized as branch_ids=[]).
SCOPE_CONTEXT_VERSION = 2
COMPATIBLE_SNAPSHOT_SCAN_LIMIT = 50

# Only these keys may differ between request and reused compatible snapshot.
# All other scope_context fields (branches, filters, contract version, …)
# must match exactly after normalization — no silent widen/omit.
TEMPORAL_COMPATIBLE_CONTEXT_KEYS = frozenset({"dt_ini", "dt_fim", "dt_ref"})

logger = logging.getLogger(__name__)

_refresh_lock = threading.Lock()
_refresh_keys: set[tuple[str, int, str]] = set()

HOT_ROUTE_REFRESH_AFTER_SECONDS: dict[str, int] = {
    "dashboard_home": 300,
    "sales_overview": 300,
    "cash_overview": 300,
    "fraud_overview": 300,
    "customers_overview": 300,
    "finance_overview": 300,
    "pricing_competitor_overview": 300,
    "goals_overview": 300,
}
ROUTE_SNAPSHOT_BYPASS_KEYS = frozenset({"pricing_competitor_overview", "customers_overview"})
REALTIME_TRUTH_BYPASS_KEYS = frozenset({
    "dashboard_home",
    "sales_overview",
    "cash_overview",
    "fraud_overview",
    "finance_overview",
    "goals_overview",
})
SYNC_SNAPSHOT_KEYS = tuple(HOT_ROUTE_REFRESH_AFTER_SECONDS.keys())
DB_BUSY_LONG_QUERY_SECONDS = 15
DB_BUSY_LOCK_WAITER_THRESHOLD = 1
DB_BUSY_LONG_QUERY_THRESHOLD = 2


def _date_key_to_iso(value: Any) -> Optional[str]:
    try:
        key = int(value or 0)
    except (TypeError, ValueError):
        return None
    if key <= 0:
        return None
    raw = f"{key:08d}"
    return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"


def _clickhouse_publication_status() -> Optional[Dict[str, Any]]:
    if not getattr(settings, "use_clickhouse", False):
        return None
    try:
        rows = query_dict(
            """
            SELECT
              formatDateTime(last_success_at, '%Y-%m-%dT%H:%M:%S', 'America/Sao_Paulo') AS last_success_at,
              mode,
              dt_fim_key,
              message
            FROM torqmind_ops.sync_state
            WHERE name = 'mart_publication'
              AND status = 'ok'
            ORDER BY updated_at DESC
            LIMIT 1
            """
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("ClickHouse publication status unavailable: %s", exc)
        return None
    if not rows:
        return None
    row = rows[0]
    last_success_at = row.get("last_success_at")
    if last_success_at and len(str(last_success_at)) == 19:
        last_success_at = f"{last_success_at}-03:00"
    return {
        "available": bool(last_success_at),
        "last_success_at": last_success_at,
        "last_sync_at": last_success_at,
        "data_through_dt": _date_key_to_iso(row.get("dt_fim_key")),
        "source": "clickhouse_mart",
        "mode": row.get("mode") or "incremental",
        "message": "Atualizado.",
    }


def build_scope_signature(context: Dict[str, Any]) -> str:
    return json.dumps(context, sort_keys=True, default=str)


def branch_scope_kind(branch_scope: Any) -> str:
    """Classify effective filial scope for snapshot identity.

    ``empty`` — authorized set is empty (must never serve/reuse snapshots).
    ``unscoped`` — explicit None (legacy tenant-wide marker; not empty).
    ``single`` / ``multi`` — concrete authorized ids.
    """
    if isinstance(branch_scope, list) and len(branch_scope) == 0:
        return "empty"
    if branch_scope is None:
        return "unscoped"
    if isinstance(branch_scope, list):
        return "multi"
    return "single"


def is_empty_branch_scope(branch_scope: Any) -> bool:
    return branch_scope_kind(branch_scope) == "empty"


def normalize_branch_ids(branch_scope: Any) -> list[int]:
    if isinstance(branch_scope, list):
        return sorted({int(value) for value in branch_scope if value is not None})
    if branch_scope is None:
        return []
    return [int(branch_scope)]


def branch_fields_for_context(branch_scope: Any) -> Dict[str, Any]:
    kind = branch_scope_kind(branch_scope)
    return {
        "scope_v": SCOPE_CONTEXT_VERSION,
        "branch_scope_kind": kind,
        "branch_ids": normalize_branch_ids(branch_scope),
    }


def extract_branch_ids_from_context(context: Any) -> Optional[list[int]]:
    """Return concrete branch ids when the stored context is unambiguous.

    Legacy rows with ``branch_ids: []`` and no ``branch_scope_kind`` are
    ambiguous (historical None and empty shared that shape) → ``None``.
    """
    if not isinstance(context, dict):
        return None
    kind = context.get("branch_scope_kind")
    raw = context.get("branch_ids")
    if kind == "empty":
        return []
    if kind == "unscoped":
        return None
    if not isinstance(raw, list):
        return None
    ids = sorted({int(value) for value in raw if value is not None})
    if kind in {"single", "multi"}:
        return ids
    # Legacy without kind: only trust non-empty explicit id lists.
    if kind is None and ids:
        return ids
    return None


def contexts_share_effective_branches(expected: Dict[str, Any], stored: Any) -> bool:
    """Exact authorized branch-set equality — never widen or shrink aggregates."""
    if not isinstance(expected, dict):
        return False
    if expected.get("branch_scope_kind") == "empty":
        return False
    if isinstance(stored, dict) and stored.get("branch_scope_kind") == "empty":
        return False
    left = extract_branch_ids_from_context(expected)
    right = extract_branch_ids_from_context(stored)
    if left is None or right is None:
        return False
    return left == right


def _normalize_filter_value(value: Any) -> Any:
    """Stable compare for business filters (lists of ids, nested dicts, scalars)."""
    if isinstance(value, dict):
        return {str(key): _normalize_filter_value(item) for key, item in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        items = [_normalize_filter_value(item) for item in value]
        try:
            return sorted(items, key=lambda item: json.dumps(item, sort_keys=True, default=str))
        except TypeError:
            return items
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int) and not isinstance(value, bool):
        return int(value)
    if isinstance(value, float):
        return float(value)
    return value


def fallback_identity_context(context: Any) -> Optional[Dict[str, Any]]:
    """Non-temporal identity required to reuse a compatible snapshot.

    Inventory of cached-route context fields (beyond base dates/branches):
    - sales_overview: ``module``, ``id_grupos``
    - fraud_overview: ``module``, ``contract_version``, ``sections``, optional
      ``credito_risco`` / ``credito_q`` / ``troca_only_suspeita`` / ``troca_forma_nova``
    - customers_overview: ``feature``
    - finance_overview: ``include_series`` / ``include_payments`` / ``include_operational``
    - cash_overview: ``module``, ``contract_version``
    - goals_overview: ``goal_date``
    - dashboard_home: base fields only

    Rejects missing ``scope_v``, empty/ambiguous branch sets, and any context
    that cannot prove equality of the non-temporal identity.
    """
    if not isinstance(context, dict):
        return None
    if context.get("scope_v") != SCOPE_CONTEXT_VERSION:
        return None
    if context.get("branch_scope_kind") == "empty":
        return None
    if extract_branch_ids_from_context(context) is None:
        return None
    identity: Dict[str, Any] = {}
    for key, value in context.items():
        if key in TEMPORAL_COMPATIBLE_CONTEXT_KEYS:
            continue
        identity[str(key)] = _normalize_filter_value(value)
    return identity


def contexts_are_compatible_for_fallback(expected: Dict[str, Any], stored: Any) -> bool:
    """Compatible stale fallback: same branches + business filters; dates may differ."""
    left = fallback_identity_context(expected)
    right = fallback_identity_context(stored)
    if left is None or right is None:
        return False
    return left == right


def record_matches_effective_branch_scope(record: Optional[Dict[str, Any]], expected_context: Dict[str, Any]) -> bool:
    """Backward-compatible name — full fallback compatibility (branches + filters)."""
    return record_is_compatible_for_fallback(record, expected_context)


def record_is_compatible_for_fallback(
    record: Optional[Dict[str, Any]], expected_context: Dict[str, Any]
) -> bool:
    if not record:
        return False
    return contexts_are_compatible_for_fallback(expected_context, record.get("scope_context"))


def route_snapshot_is_bypassed(snapshot_key: str) -> bool:
    if snapshot_key in ROUTE_SNAPSHOT_BYPASS_KEYS:
        return True
    return bool(getattr(settings, "use_realtime_marts", False)) and snapshot_key in REALTIME_TRUTH_BYPASS_KEYS


def _normalize_jsonb(value: Any) -> Any:
    return dict(value) if isinstance(value, dict) else value


def _normalize_non_finite_json(value: Any) -> Any:
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {key: _normalize_non_finite_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_non_finite_json(item) for item in value]
    if isinstance(value, tuple):
        return [_normalize_non_finite_json(item) for item in value]
    return value


def _serialize_jsonb(value: Any) -> str:
    encoded = jsonable_encoder(value)
    return json.dumps(_normalize_non_finite_json(encoded), ensure_ascii=False, allow_nan=False)



def _snapshot_record_from_row(row_map: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "snapshot_data": _normalize_jsonb(row_map.get("snapshot_data")),
        "scope_context": _normalize_jsonb(row_map.get("scope_context")),
        "updated_at": row_map.get("updated_at"),
        "scope_signature": row_map.get("scope_signature"),
        "branch_id": row_map.get("id_filial"),
    }


def read_snapshot_record(
    role: str,
    tenant_id: int,
    branch_id: Optional[int],
    snapshot_key: str,
    scope_signature: str,
) -> Optional[Dict[str, Any]]:
    sql = f"""
      SELECT snapshot_data, scope_context, updated_at, scope_signature, id_filial
      FROM {SNAPSHOT_TABLE}
      WHERE snapshot_key = %s
        AND id_empresa = %s
        AND scope_signature = %s
    """
    params = [snapshot_key, tenant_id, scope_signature]
    with get_conn(role=role, tenant_id=tenant_id, branch_id=branch_id) as conn:
        row = conn.execute(sql, params).fetchone()
    if not row:
        return None
    row_map = dict(row) if not isinstance(row, dict) else row
    return _snapshot_record_from_row(row_map)


def read_latest_compatible_snapshot_record(
    role: str,
    tenant_id: int,
    branch_id: Optional[int],
    snapshot_key: str,
    expected_context: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    """Latest snapshot for the same tenant with compatible non-temporal identity.

    Does **not** treat ``id_filial IS NULL`` as proof of compatibility. Candidates
    must match authorized branches **and** business filters / ``scope_v`` in
    ``scope_context``. Only ``dt_ini`` / ``dt_fim`` / ``dt_ref`` may differ
    (protected stale window). Empty / ambiguous / under-specified contexts never match.
    """
    if fallback_identity_context(expected_context) is None:
        return None

    sql = f"""
      SELECT snapshot_data, scope_context, updated_at, scope_signature, id_filial
      FROM {SNAPSHOT_TABLE}
      WHERE snapshot_key = %s
        AND id_empresa = %s
      ORDER BY updated_at DESC, scope_signature DESC
      LIMIT %s
    """
    params = [snapshot_key, tenant_id, COMPATIBLE_SNAPSHOT_SCAN_LIMIT]
    with get_conn(role=role, tenant_id=tenant_id, branch_id=branch_id) as conn:
        rows = conn.execute(sql, params).fetchall() or []
    for row in rows:
        row_map = dict(row) if not isinstance(row, dict) else row
        record = _snapshot_record_from_row(row_map)
        if record_is_compatible_for_fallback(record, expected_context):
            return record
    return None


def read_snapshot(
    role: str,
    tenant_id: int,
    branch_id: Optional[int],
    snapshot_key: str,
    scope_signature: str,
) -> Optional[Dict[str, Any]]:
    record = read_snapshot_record(role, tenant_id, branch_id, snapshot_key, scope_signature)
    if record is None:
        return None
    snapshot = record["snapshot_data"]
    return dict(snapshot) if isinstance(snapshot, dict) else snapshot


def write_snapshot(
    role: str,
    tenant_id: int,
    branch_id: Optional[int],
    snapshot_key: str,
    scope_signature: str,
    context: Dict[str, Any],
    payload: Dict[str, Any],
) -> Optional[datetime]:
    if isinstance(payload.get("_fallback_meta"), dict):
        logger.info(
            "Skipping placeholder snapshot write for %s tenant=%s signature=%s",
            snapshot_key,
            tenant_id,
            scope_signature,
        )
        return None
    cache_meta = payload.get("_snapshot_cache")
    if isinstance(cache_meta, dict) and str(cache_meta.get("source") or "").lower() == "fallback":
        logger.info(
            "Skipping fallback snapshot write for %s tenant=%s signature=%s",
            snapshot_key,
            tenant_id,
            scope_signature,
        )
        return None
    sql = f"""
      INSERT INTO {SNAPSHOT_TABLE}
        (snapshot_key, id_empresa, id_filial, scope_signature, scope_context, snapshot_data, updated_at)
      VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, now())
      ON CONFLICT (snapshot_key, id_empresa, scope_signature)
      DO UPDATE
        SET scope_context = EXCLUDED.scope_context,
            snapshot_data = EXCLUDED.snapshot_data,
            updated_at = now()
      RETURNING updated_at
    """
    params = [
        snapshot_key,
        tenant_id,
        branch_id,
        scope_signature,
        _serialize_jsonb(context),
        _serialize_jsonb(payload),
    ]
    with get_conn(role=role, tenant_id=tenant_id, branch_id=branch_id) as conn:
        row = conn.execute(sql, params).fetchone() or {}
        conn.commit()
    updated_at = row.get("updated_at")
    if isinstance(updated_at, datetime):
        return updated_at
    return datetime.now(timezone.utc)


def snapshot_age_seconds(updated_at: Any) -> Optional[float]:
    if not isinstance(updated_at, datetime):
        return None
    now = datetime.now(timezone.utc)
    reference = updated_at if updated_at.tzinfo else updated_at.replace(tzinfo=timezone.utc)
    return max(0.0, (now - reference).total_seconds())


def snapshot_refresh_after_seconds(snapshot_key: str) -> int:
    return int(HOT_ROUTE_REFRESH_AFTER_SECONDS.get(snapshot_key, 300))


def snapshot_is_fresh(updated_at: Any, snapshot_key: str) -> bool:
    age_seconds = snapshot_age_seconds(updated_at)
    if age_seconds is None:
        return False
    return age_seconds < float(snapshot_refresh_after_seconds(snapshot_key))


def is_tenant_etl_running(tenant_id: int) -> bool:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        state = inspect_running_etl_state(conn, tenant_id=int(tenant_id))
    return bool(state["live_rows"])


def get_hot_route_guard(tenant_id: int) -> Dict[str, Any]:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        etl_state = inspect_running_etl_state(conn, tenant_id=int(tenant_id))
        db_row = conn.execute(
            """
            SELECT
              COUNT(*) FILTER (
                WHERE state = 'active'
                  AND wait_event_type = 'Lock'
              )::int AS lock_waiters,
              COUNT(*) FILTER (
                WHERE state = 'active'
                  AND query_start IS NOT NULL
                  AND now() - query_start >= (%s * interval '1 second')
              )::int AS long_running_queries
            FROM pg_stat_activity
            WHERE datname = current_database()
              AND pid <> pg_backend_pid()
              AND backend_type = 'client backend'
            """,
            (DB_BUSY_LONG_QUERY_SECONDS,),
        ).fetchone() or {}
        tenant_lock_available = advisory_lock_is_available(conn, TENANT_TRACK_LOCK_NAMESPACE, int(tenant_id))

    etl_running = bool(etl_state["live_rows"])
    lock_waiters = int(db_row.get("lock_waiters") or 0)
    long_running_queries = int(db_row.get("long_running_queries") or 0)
    protect_reads = (
        etl_running
        or not tenant_lock_available
        or lock_waiters >= DB_BUSY_LOCK_WAITER_THRESHOLD
        or long_running_queries >= DB_BUSY_LONG_QUERY_THRESHOLD
    )
    reasons: list[str] = []
    if etl_running:
        reasons.append("etl_running")
    if not tenant_lock_available:
        reasons.append("tenant_lock_busy")
    if lock_waiters >= DB_BUSY_LOCK_WAITER_THRESHOLD:
        reasons.append("lock_waiters")
    if long_running_queries >= DB_BUSY_LONG_QUERY_THRESHOLD:
        reasons.append("long_running_queries")
    if etl_state["stale_rows"]:
        reasons.append("stale_run_log_reconciled")
    if not reasons and protect_reads:
        reasons.append("guarded")

    return {
        "protect_reads": protect_reads,
        "etl_running": etl_running,
        "tenant_lock_available": tenant_lock_available,
        "lock_waiters": lock_waiters,
        "long_running_queries": long_running_queries,
        "stale_rows_reconciled": len(etl_state["stale_rows"]),
        "reasons": reasons,
    }


def last_consolidated_sync(tenant_id: int, branch_id: Optional[int] = None) -> Dict[str, Any]:
    clickhouse_status = _clickhouse_publication_status()
    branch_sql = "AND id_filial IS NULL" if branch_id is None else "AND id_filial = %s"
    snapshot_params: list[Any] = [tenant_id, list(SYNC_SNAPSHOT_KEYS)]
    if branch_id is not None:
        snapshot_params.append(branch_id)

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        snapshot_row: Dict[str, Any] = {}
        if SYNC_SNAPSHOT_KEYS:
            snapshot_row = conn.execute(
                f"""
                SELECT MAX(updated_at) AS updated_at
                FROM {SNAPSHOT_TABLE}
                WHERE id_empresa = %s
                  AND snapshot_key = ANY(%s)
                  {branch_sql}
                """,
                snapshot_params,
            ).fetchone() or {}
        phase_row = conn.execute(
            """
            SELECT
              finished_at,
              step_name,
              COALESCE(NULLIF(meta->>'track', ''), 'full') AS track
            FROM etl.run_log
            WHERE id_empresa = %s
              AND status = 'ok'
              AND finished_at IS NOT NULL
              AND step_name IN ('run_tenant_phase', 'run_all')
              AND COALESCE(NULLIF(meta->>'track', ''), 'full') IN ('operational', 'full')
            ORDER BY finished_at DESC
            LIMIT 1
            """,
            (tenant_id,),
        ).fetchone() or {}
        analytics_row = conn.execute(
            """
            SELECT
              finished_at,
              step_name,
              COALESCE(NULLIF(meta->>'track', ''), 'full') AS track,
              COALESCE(NULLIF(meta->>'publication_mode', ''), 'legacy') AS publication_mode
            FROM etl.run_log
            WHERE id_empresa = %s
              AND status = 'ok'
              AND finished_at IS NOT NULL
              AND step_name IN ('run_tenant_post_refresh', 'refresh_marts', 'run_all')
              AND COALESCE(NULLIF(meta->>'track', ''), 'full') IN ('operational', 'risk', 'full')
            ORDER BY finished_at DESC
            LIMIT 1
            """,
            (tenant_id,),
        ).fetchone() or {}

    snapshot_updated_at = snapshot_row.get("updated_at")
    phase_finished_at = phase_row.get("finished_at")
    analytics_finished_at = analytics_row.get("finished_at")

    operational = {
        "available": isinstance(phase_finished_at, datetime),
        "last_sync_at": phase_finished_at.isoformat() if isinstance(phase_finished_at, datetime) else None,
        "source": "etl_phase" if isinstance(phase_finished_at, datetime) else "unavailable",
    }
    analytics_source = "unavailable"
    if analytics_row.get("step_name") == "run_tenant_post_refresh":
        analytics_source = (
            "etl_publication_fast_path"
            if analytics_row.get("publication_mode") == "fast_path"
            else "etl_post_refresh"
        )
    elif analytics_row.get("step_name") == "refresh_marts":
        analytics_source = "etl_refresh_marts"
    elif analytics_row.get("step_name") == "run_all":
        analytics_source = "etl_run"
    analytics = {
        "available": isinstance(analytics_finished_at, datetime),
        "last_sync_at": analytics_finished_at.isoformat() if isinstance(analytics_finished_at, datetime) else None,
        "source": analytics_source,
        "mode": analytics_row.get("publication_mode") if isinstance(analytics_finished_at, datetime) else None,
    }
    publication = {
        "available": isinstance(snapshot_updated_at, datetime),
        "last_sync_at": snapshot_updated_at.isoformat() if isinstance(snapshot_updated_at, datetime) else None,
        "source": "snapshot_cache" if isinstance(snapshot_updated_at, datetime) else "unavailable",
    }

    if isinstance(phase_finished_at, datetime):
        if clickhouse_status and clickhouse_status.get("available"):
            last_success_at = clickhouse_status.get("last_success_at")
            return {
                "available": True,
                "last_success_at": last_success_at,
                "last_sync_at": last_success_at,
                "data_through_dt": clickhouse_status.get("data_through_dt"),
                "source": "clickhouse_mart",
                "mode": clickhouse_status.get("mode") or "incremental",
                "message": "Atualizado.",
                "operational": operational,
                "analytics": {
                    "available": True,
                    "last_sync_at": last_success_at,
                    "last_success_at": last_success_at,
                    "source": "clickhouse_mart",
                    "mode": clickhouse_status.get("mode") or "incremental",
                },
                "publication": {
                    "available": True,
                    "last_sync_at": last_success_at,
                    "last_success_at": last_success_at,
                    "source": "clickhouse_mart",
                    "mode": clickhouse_status.get("mode") or "incremental",
                },
            }
        message = f"Atualizado em {phase_finished_at.isoformat()}."
        if isinstance(analytics_finished_at, datetime):
            if analytics_source == "etl_publication_fast_path":
                message = (
                    f"{message} BI concluído em {analytics_finished_at.isoformat()}."
                )
            else:
                message = (
                    f"{message} BI concluído em {analytics_finished_at.isoformat()}."
                )
        elif isinstance(snapshot_updated_at, datetime):
            message = f"{message} Última leitura salva em {snapshot_updated_at.isoformat()}."
        return {
            "available": True,
            "last_success_at": phase_finished_at.isoformat(),
            "last_sync_at": phase_finished_at.isoformat(),
            "source": "operational_phase",
            "mode": "incremental",
            "message": message,
            "operational": operational,
            "analytics": analytics,
            "publication": publication,
        }
    if isinstance(snapshot_updated_at, datetime) and (
        not isinstance(analytics_finished_at, datetime) or snapshot_updated_at >= analytics_finished_at
    ):
        return {
            "available": True,
            "last_sync_at": snapshot_updated_at.isoformat(),
            "source": "snapshot_cache",
            "message": "Base publicada pronta para este escopo.",
            "operational": operational,
            "analytics": analytics,
            "publication": publication,
        }
    if isinstance(analytics_finished_at, datetime):
        analytics_message = f"Base atualizada em {analytics_finished_at.isoformat()}."
        return {
            "available": True,
            "last_sync_at": analytics_finished_at.isoformat(),
            "source": analytics_source,
            "message": analytics_message,
            "operational": operational,
            "analytics": analytics,
            "publication": publication,
        }
    return {
        "available": False,
        "last_success_at": None,
        "data_through_dt": None,
        "last_sync_at": None,
        "source": "unavailable",
        "message": "Estamos atualizando os dados.",
        "operational": operational,
        "analytics": analytics,
        "publication": publication,
    }


def refresh_snapshot_async(
    snapshot_key: str,
    tenant_id: int,
    scope_signature: str,
    refresh_fn: Callable[[], None],
) -> bool:
    refresh_key = (snapshot_key, tenant_id, scope_signature)
    with _refresh_lock:
        if refresh_key in _refresh_keys:
            return False
        _refresh_keys.add(refresh_key)

    def runner() -> None:
        try:
            refresh_fn()
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Background snapshot refresh failed for %s tenant=%s: %s",
                snapshot_key,
                tenant_id,
                exc,
                exc_info=exc,
            )
        finally:
            with _refresh_lock:
                _refresh_keys.discard(refresh_key)

    threading.Thread(
        target=runner,
        name=f"snapshot-refresh-{snapshot_key}-{tenant_id}",
        daemon=True,
    ).start()
    return True
