from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from app.business_time import business_today, resolve_business_date
from app.cash_operational_truth import (
    cash_open_source_sql,
    ensure_cash_operational_truth_schema,
    missing_relation_columns,
    CASH_OPEN_RELATION,
    CASH_OPEN_REQUIRED_COLUMNS,
)
from app.db import get_conn
from app.services.etl_orchestrator import (
    TRACK_OPERATIONAL,
    TRACK_RISK,
    inspect_track_locks,
    run_incremental_cycle,
)


PURGE_SCOPES = frozenset({"cash", "cash-fraud"})
DEFAULT_REBUILD_WITH_RISK = True


class OperationalTruthBusyError(RuntimeError):
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        super().__init__(str(payload.get("message") or "Operational truth rebuild is busy."))


def _parse_date(raw: str | None, *, default: date | None = None) -> date | None:
    if raw is None or str(raw).strip() == "":
        return default
    return date.fromisoformat(str(raw))


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


def _emit(payload: dict[str, Any]) -> None:
    print(json.dumps(_json_ready(payload), ensure_ascii=False, default=str))


def _branch_filters(alias: str, branch_id: int | None) -> tuple[str, list[Any]]:
    if branch_id is None:
        return "", []
    return f"AND {alias}.id_filial = %s", [branch_id]


def _cash_eligible_sql(
    cash_column: str,
    data_column: str,
    data_conta_column: str,
    id_turno_column: str,
) -> str:
    return f"etl.resolve_cash_eligible({cash_column}, {data_column}, {data_conta_column}, {id_turno_column})"


def _tenant_branch_label(tenant_id: int, branch_id: int | None) -> dict[str, Any]:
    return {
        "tenant_id": int(tenant_id),
        "branch_id": None if branch_id is None else int(branch_id),
    }


def diagnose_operational_truth(
    tenant_id: int,
    *,
    branch_id: int | None = None,
    dt_ini: date | None = None,
    dt_fim: date | None = None,
) -> dict[str, Any]:
    effective_dt_fim = dt_fim or business_today(tenant_id)
    effective_dt_ini = dt_ini or (effective_dt_fim - timedelta(days=29))
    data_key_ini = int(effective_dt_ini.strftime("%Y%m%d"))
    data_key_fim = int(effective_dt_fim.strftime("%Y%m%d"))

    where_stg_turnos, stg_turnos_params = _branch_filters("t", branch_id)
    where_stg_usuarios, stg_users_params = _branch_filters("u", branch_id)
    where_dw_turnos, dw_turnos_params = _branch_filters("t", branch_id)
    mart_turnos_params = [] if branch_id is None else [branch_id]
    where_comp, comp_params = _branch_filters("c", branch_id)
    where_events, event_params = _branch_filters("e", branch_id)

    with get_conn(role="MASTER", tenant_id=tenant_id, branch_id=branch_id) as conn:
        cash_from_sql, schema_mode = cash_open_source_sql(
            conn,
            id_empresa=tenant_id,
            id_filial=branch_id,
            alias="a",
        )
        missing_cash_columns = missing_relation_columns(conn, *CASH_OPEN_RELATION, CASH_OPEN_REQUIRED_COLUMNS)
        where_mart_turnos = "" if branch_id is None else "AND a.id_filial = %s"
        counts = dict(
            conn.execute(
                f"""
                WITH stg_turnos AS (
                  SELECT
                    COUNT(*)::int AS total_turnos,
                    COUNT(*) FILTER (
                      WHERE etl.safe_int(t.payload->>'ENCERRANTEFECHAMENTO') = 0
                    )::int AS turnos_abertos_fonte
                  FROM stg.turnos t
                  WHERE t.id_empresa = %s
                  {where_stg_turnos}
                ),
                stg_usuarios AS (
                  SELECT COUNT(*)::int AS usuarios_stg
                  FROM stg.usuarios u
                  WHERE u.id_empresa = %s
                  {where_stg_usuarios}
                ),
                dw_usuarios AS (
                  SELECT COUNT(*)::int AS usuarios_dw
                  FROM dw.dim_usuario_caixa u
                  WHERE u.id_empresa = %s
                  {where_stg_usuarios}
                ),
                dw_turnos AS (
                  SELECT
                    COUNT(*)::int AS turnos_dw,
                    COUNT(*) FILTER (WHERE t.is_aberto = true)::int AS turnos_abertos_dw
                  FROM dw.fact_caixa_turno t
                  WHERE t.id_empresa = %s
                  {where_dw_turnos}
                ),
                mart_open AS (
                  SELECT
                    COUNT(*) FILTER (WHERE a.is_operational_live)::int AS caixas_abertos_live,
                    COUNT(*) FILTER (WHERE a.is_stale)::int AS caixas_stale
                  FROM {cash_from_sql}
                  WHERE a.id_empresa = %s
                  {where_mart_turnos}
                )
                SELECT *
                FROM stg_turnos
                CROSS JOIN stg_usuarios
                CROSS JOIN dw_usuarios
                CROSS JOIN dw_turnos
                CROSS JOIN mart_open
                """,
                [tenant_id] + stg_turnos_params + [tenant_id] + stg_users_params + [tenant_id] + stg_users_params + [tenant_id] + dw_turnos_params + [tenant_id] + mart_turnos_params,
            ).fetchone()
            or {}
        )

        alignment = dict(
            conn.execute(
                f"""
                WITH cash_period AS (
                  SELECT
                    COUNT(*)::int AS cancelamentos_cash,
                    COALESCE(SUM(c.valor_total), 0)::numeric(18,2) AS valor_cancelado_cash
                  FROM dw.fact_comprovante c
                  WHERE c.id_empresa = %s
                    AND c.data_key BETWEEN %s AND %s
                    {where_comp}
                    AND COALESCE(c.cancelado, false) = true
                    AND {_cash_eligible_sql('c.cash_eligible', 'c.data', 'c.data_conta', 'c.id_turno')}
                    AND etl.safe_int(NULLIF(regexp_replace(COALESCE(c.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) > 5000
                ),
                fraud_period AS (
                  SELECT
                    COUNT(*)::int AS cancelamentos_fraud,
                    COALESCE(SUM(e.valor_total), 0)::numeric(18,2) AS valor_cancelado_fraud,
                    COUNT(*) FILTER (WHERE e.usuario_source = 'turno')::int AS cancelamentos_resolvidos_turno,
                    COUNT(*) FILTER (WHERE e.usuario_source = 'comprovante')::int AS cancelamentos_fallback_comprovante
                  FROM mart.fraude_cancelamentos_eventos e
                  WHERE e.id_empresa = %s
                    AND e.data_key BETWEEN %s AND %s
                    {where_events}
                ),
                divergencias AS (
                  SELECT
                    COUNT(*) FILTER (
                      WHERE c.id_usuario IS DISTINCT FROM t.id_usuario
                    )::int AS divergencias_usuario,
                    COUNT(*) FILTER (
                      WHERE t.id_usuario IS NOT NULL
                    )::int AS cancelamentos_com_turno_resolvido
                  FROM dw.fact_comprovante c
                  LEFT JOIN dw.fact_caixa_turno t
                    ON t.id_empresa = c.id_empresa
                   AND t.id_filial = c.id_filial
                   AND t.id_turno = c.id_turno
                  WHERE c.id_empresa = %s
                    AND c.data_key BETWEEN %s AND %s
                    {where_comp}
                    AND COALESCE(c.cancelado, false) = true
                    AND {_cash_eligible_sql('c.cash_eligible', 'c.data', 'c.data_conta', 'c.id_turno')}
                    AND etl.safe_int(NULLIF(regexp_replace(COALESCE(c.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) > 5000
                )
                SELECT *
                FROM cash_period
                CROSS JOIN fraud_period
                CROSS JOIN divergencias
                """,
                [
                    tenant_id,
                    data_key_ini,
                    data_key_fim,
                    *comp_params,
                    tenant_id,
                    data_key_ini,
                    data_key_fim,
                    *event_params,
                    tenant_id,
                    data_key_ini,
                    data_key_fim,
                    *comp_params,
                ],
            ).fetchone()
            or {}
        )

        stale_turns = [
            dict(row)
            for row in conn.execute(
                f"""
                SELECT
                  a.id_filial,
                  a.filial_nome,
                  a.id_turno,
                  a.id_usuario,
                  a.usuario_nome,
                  a.last_activity_ts,
                  a.horas_aberto,
                  a.horas_sem_movimento,
                  a.total_vendas,
                  a.total_cancelamentos
                FROM {cash_from_sql}
                WHERE a.id_empresa = %s
                  {where_mart_turnos}
                  AND a.is_stale = true
                ORDER BY a.last_activity_ts DESC NULLS LAST, a.id_turno DESC
                LIMIT 10
                """,
                [tenant_id] + mart_turnos_params,
            ).fetchall()
        ]

        divergent_events = [
            dict(row)
            for row in conn.execute(
                f"""
                SELECT
                  c.id_filial,
                  c.id_comprovante,
                  c.id_turno,
                  c.id_usuario AS id_usuario_documento,
                  t.id_usuario AS id_usuario_turno,
                  e.usuario_nome,
                  e.usuario_source,
                  c.valor_total,
                  c.data
                FROM dw.fact_comprovante c
                JOIN mart.fraude_cancelamentos_eventos e
                  ON e.id_empresa = c.id_empresa
                 AND e.id_filial = c.id_filial
                 AND e.id_db = c.id_db
                 AND e.id_comprovante = c.id_comprovante
                LEFT JOIN dw.fact_caixa_turno t
                  ON t.id_empresa = c.id_empresa
                 AND t.id_filial = c.id_filial
                 AND t.id_turno = c.id_turno
                WHERE c.id_empresa = %s
                  AND c.data_key BETWEEN %s AND %s
                  {where_comp}
                  AND COALESCE(c.cancelado, false) = true
                  AND {_cash_eligible_sql('c.cash_eligible', 'c.data', 'c.data_conta', 'c.id_turno')}
                  AND etl.safe_int(NULLIF(regexp_replace(COALESCE(c.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')) > 5000
                  AND c.id_usuario IS DISTINCT FROM t.id_usuario
                ORDER BY c.data DESC NULLS LAST, c.id_comprovante DESC
                LIMIT 10
                """,
                [tenant_id, data_key_ini, data_key_fim] + comp_params,
            ).fetchall()
        ]

    cash_cancel = int(alignment.get("cancelamentos_cash") or 0)
    fraud_cancel = int(alignment.get("cancelamentos_fraud") or 0)
    cash_value = float(alignment.get("valor_cancelado_cash") or 0)
    fraud_value = float(alignment.get("valor_cancelado_fraud") or 0)
    source_open = int(counts.get("turnos_abertos_fonte") or 0)
    live_open = int(counts.get("caixas_abertos_live") or 0)
    stale_open = int(counts.get("caixas_stale") or 0)

    return {
        **_tenant_branch_label(tenant_id, branch_id),
        "window": {"dt_ini": effective_dt_ini, "dt_fim": effective_dt_fim},
        "schema": {
            "cash_open_mode": schema_mode,
            "missing_columns": missing_cash_columns,
        },
        "counts": {
            "turnos_stg": int(counts.get("total_turnos") or 0),
            "turnos_abertos_fonte": source_open,
            "usuarios_stg": int(counts.get("usuarios_stg") or 0),
            "usuarios_dw": int(counts.get("usuarios_dw") or 0),
            "turnos_dw": int(counts.get("turnos_dw") or 0),
            "turnos_abertos_dw": int(counts.get("turnos_abertos_dw") or 0),
            "caixas_abertos_live": live_open,
            "caixas_stale": stale_open,
        },
        "alignment": {
            "cancelamentos_cash": cash_cancel,
            "cancelamentos_fraud": fraud_cancel,
            "valor_cancelado_cash": round(cash_value, 2),
            "valor_cancelado_fraud": round(fraud_value, 2),
            "cancelamentos_resolvidos_turno": int(alignment.get("cancelamentos_resolvidos_turno") or 0),
            "cancelamentos_fallback_comprovante": int(alignment.get("cancelamentos_fallback_comprovante") or 0),
            "divergencias_usuario_documento_vs_turno": int(alignment.get("divergencias_usuario") or 0),
            "cancelamentos_com_turno_resolvido": int(alignment.get("cancelamentos_com_turno_resolvido") or 0),
            "gap_cancelamentos": cash_cancel - fraud_cancel,
            "gap_valor_cancelado": round(cash_value - fraud_value, 2),
            "gap_turnos_abertos_fonte_vs_live_plus_stale": source_open - (live_open + stale_open),
        },
        "samples": {
            "stale_turns": stale_turns,
            "divergent_cancel_events": divergent_events,
        },
    }


def purge_operational_truth(
    tenant_id: int,
    *,
    branch_id: int | None = None,
    scope: str = "cash-fraud",
    include_staging: bool = False,
    ref_date: date | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    normalized_scope = str(scope).strip().lower()
    if normalized_scope not in PURGE_SCOPES:
        raise ValueError(f"Unsupported purge scope: {scope}")

    effective_ref_date = resolve_business_date(ref_date, tenant_id)
    branch_where = "" if branch_id is None else "AND id_filial = %s"
    branch_params = [] if branch_id is None else [branch_id]
    changed: dict[str, Any] = {"track": "operational", "dim_usuario_caixa": 1, "fact_caixa_turno": 1}
    tables: list[tuple[str, str, tuple[Any, ...]]] = [
        ("dw.dim_usuario_caixa", f"DELETE FROM dw.dim_usuario_caixa WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
        ("dw.fact_caixa_turno", f"DELETE FROM dw.fact_caixa_turno WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
    ]
    watermark_keys = {"usuarios", "turnos"}

    if normalized_scope == "cash-fraud":
        changed.update(
            {
                "fact_comprovante": 1,
                "fact_pagamento_comprovante": 1,
                "fact_venda": 1,
                "fact_venda_item": 1,
                "risk_events": 1,
            }
        )
        watermark_keys.update({"comprovantes", "formas_pgto_comprovantes", "movprodutos", "itensmovprodutos"})
        tables.extend(
            [
                ("dw.fact_pagamento_comprovante", f"DELETE FROM dw.fact_pagamento_comprovante WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                ("dw.fact_venda_item", f"DELETE FROM dw.fact_venda_item WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                ("dw.fact_venda", f"DELETE FROM dw.fact_venda WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                ("dw.fact_comprovante", f"DELETE FROM dw.fact_comprovante WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                ("dw.fact_risco_evento", f"DELETE FROM dw.fact_risco_evento WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
            ]
        )

    if include_staging:
        changed["force_full"] = True
        if normalized_scope == "cash":
            tables.extend(
                [
                    ("stg.usuarios", f"DELETE FROM stg.usuarios WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                    ("stg.turnos", f"DELETE FROM stg.turnos WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                ]
            )
        else:
            tables.extend(
                [
                    ("stg.usuarios", f"DELETE FROM stg.usuarios WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                    ("stg.turnos", f"DELETE FROM stg.turnos WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                    ("stg.comprovantes", f"DELETE FROM stg.comprovantes WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                    ("stg.movprodutos", f"DELETE FROM stg.movprodutos WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                    ("stg.itensmovprodutos", f"DELETE FROM stg.itensmovprodutos WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                    ("stg.formas_pgto_comprovantes", f"DELETE FROM stg.formas_pgto_comprovantes WHERE id_empresa = %s {branch_where}", tuple([tenant_id] + branch_params)),
                ]
            )

    deleted: dict[str, int] = {}
    with get_conn(role="MASTER", tenant_id=tenant_id, branch_id=branch_id) as conn:
        for table_name, sql_text, params in tables:
            if dry_run:
                total = conn.execute(
                    sql_text.replace("DELETE FROM", "SELECT COUNT(*) AS total FROM", 1),
                    params,
                ).fetchone()
                deleted[table_name] = int((total or {}).get("total") or 0)
                continue
            result = conn.execute(sql_text, params)
            deleted[table_name] = int(result.rowcount or 0)

        if not dry_run:
            conn.execute(
                "DELETE FROM etl.watermark WHERE id_empresa = %s AND dataset = ANY(%s)",
                (tenant_id, sorted(watermark_keys)),
            )
            refresh_meta = conn.execute(
                "SELECT etl.refresh_marts(%s::jsonb, %s::date) AS meta",
                (json.dumps(changed), effective_ref_date),
            ).fetchone()
            conn.commit()
        else:
            refresh_meta = {"meta": {"dry_run": True}}

    return {
        **_tenant_branch_label(tenant_id, branch_id),
        "scope": normalized_scope,
        "include_staging": include_staging,
        "dry_run": dry_run,
        "deleted_rows": deleted,
        "watermarks_reset": [] if dry_run else sorted(watermark_keys),
        "refresh_meta": (refresh_meta or {}).get("meta") if isinstance(refresh_meta, dict) else refresh_meta,
    }


def rebuild_operational_truth(
    tenant_id: int,
    *,
    ref_date: date | None = None,
    with_risk: bool = DEFAULT_REBUILD_WITH_RISK,
) -> dict[str, Any]:
    effective_ref_date = resolve_business_date(ref_date, tenant_id)
    schema_repaired = False
    preflight = preflight_operational_truth_rebuild(tenant_id, with_risk=with_risk)
    if not preflight.get("ok"):
        raise OperationalTruthBusyError(preflight)

    with get_conn(role="MASTER", tenant_id=tenant_id, branch_id=None) as conn:
        schema_repaired = ensure_cash_operational_truth_schema(conn)
    operational = run_incremental_cycle(
        [tenant_id],
        ref_date=effective_ref_date,
        refresh_mart=True,
        force_full=False,
        fail_fast=True,
        track=TRACK_OPERATIONAL,
        skip_busy_tenants=False,
        tenant_rows=[{"id_empresa": tenant_id}],
        db_role="MASTER",
        db_tenant_scope=None,
        acquire_lock=True,
        progress_callback=None,
    )
    result: dict[str, Any] = {
        "tenant_id": tenant_id,
        "ref_date": effective_ref_date,
        "schema_repaired": schema_repaired,
        "operational": operational,
    }
    if with_risk:
        risk = run_incremental_cycle(
            [tenant_id],
            ref_date=effective_ref_date,
            refresh_mart=True,
            force_full=True,
            fail_fast=True,
            track=TRACK_RISK,
            skip_busy_tenants=False,
            tenant_rows=[{"id_empresa": tenant_id}],
            db_role="MASTER",
            db_tenant_scope=None,
            acquire_lock=True,
            progress_callback=None,
        )
        result["risk"] = risk
        result["ok"] = bool(operational.get("ok")) and bool(risk.get("ok"))
    else:
        result["ok"] = bool(operational.get("ok"))
    return result


def preflight_operational_truth_rebuild(
    tenant_id: int,
    *,
    with_risk: bool = DEFAULT_REBUILD_WITH_RISK,
) -> dict[str, Any]:
    requested_tracks = [TRACK_OPERATIONAL]
    if with_risk:
        requested_tracks.append(TRACK_RISK)

    with get_conn(role="MASTER", tenant_id=tenant_id, branch_id=None) as conn:
        lock_checks = [inspect_track_locks(conn, track, tenant_id=tenant_id) for track in requested_tracks]

    blocking_reasons: list[str] = []
    for item in lock_checks:
        if not bool(item.get("cycle_available")):
            blocking_reasons.append(f"ciclo_{item['track']}_ocupado")
        if item.get("tenant_available") is False:
            blocking_reasons.append(f"tenant_busy_{item['track']}")

    ok = not blocking_reasons
    if ok:
        message = (
            "Preflight liberado. Nenhum ETL concorrente está segurando o ciclo ou o tenant; "
            "o rebuild pode começar com segurança."
        )
    else:
        message = (
            "Rebuild recusado porque o tenant ou a esteira ETL ainda estão ocupados. "
            "Aguarde o cron terminar e rode primeiro `make operational-truth-preflight TENANT_ID=...`."
        )

    return {
        "tenant_id": int(tenant_id),
        "with_risk": bool(with_risk),
        "tracks": requested_tracks,
        "ok": ok,
        "error": None if ok else "etl_busy",
        "message": message,
        "lock_checks": lock_checks,
        "blocking_reasons": blocking_reasons,
        "operator_hint": (
            "Use `make operational-truth-preflight TENANT_ID=...` até receber `ok=true`. "
            "Depois execute `make operational-truth-rebuild TENANT_ID=...` "
            "ou acrescente `OPERATIONAL_ONLY=1` se quiser pular o trilho de risco."
        ),
    }


def validate_operational_truth(
    tenant_id: int,
    *,
    branch_id: int | None = None,
    dt_ini: date | None = None,
    dt_fim: date | None = None,
) -> dict[str, Any]:
    diagnostic = diagnose_operational_truth(
        tenant_id,
        branch_id=branch_id,
        dt_ini=dt_ini,
        dt_fim=dt_fim,
    )
    counts = diagnostic["counts"]
    alignment = diagnostic["alignment"]

    checks = {
        "source_open_turns_match_live_plus_stale": int(alignment["gap_turnos_abertos_fonte_vs_live_plus_stale"]) == 0,
        "cash_and_fraud_cancel_counts_match": int(alignment["gap_cancelamentos"]) == 0,
        "cash_and_fraud_cancel_values_match": abs(float(alignment["gap_valor_cancelado"])) < 0.01,
        "usuarios_dimension_loaded_when_staging_exists": not (
            int(counts["usuarios_stg"]) > 0 and int(counts["usuarios_dw"]) == 0
        ),
        "dw_open_turns_match_source_open_turns": int(counts["turnos_abertos_dw"]) == int(counts["turnos_abertos_fonte"]),
    }
    return {
        **_tenant_branch_label(tenant_id, branch_id),
        "window": diagnostic["window"],
        "ok": all(checks.values()),
        "checks": checks,
        "diagnostic": diagnostic,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diagnose, purge, rebuild and validate cash/fraud operational truth by tenant.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    diagnose = subparsers.add_parser("diagnose", help="Inspect current cash/fraud operational truth and divergence signals.")
    diagnose.add_argument("--tenant-id", required=True, type=int)
    diagnose.add_argument("--branch-id", type=int, default=None)
    diagnose.add_argument("--dt-ini", default=None)
    diagnose.add_argument("--dt-fim", default=None)

    purge = subparsers.add_parser("purge", help="Delete only the cash/fraud domain for one tenant before rebuild.")
    purge.add_argument("--tenant-id", required=True, type=int)
    purge.add_argument("--branch-id", type=int, default=None)
    purge.add_argument("--scope", choices=sorted(PURGE_SCOPES), default="cash-fraud")
    purge.add_argument("--include-staging", action="store_true")
    purge.add_argument("--ref-date", default=None)
    purge.add_argument("--dry-run", action="store_true")

    rebuild = subparsers.add_parser("rebuild", help="Reprocess one tenant through the canonical ETL lanes.")
    rebuild.add_argument("--tenant-id", required=True, type=int)
    rebuild.add_argument("--ref-date", default=None)
    rebuild.add_argument("--with-risk", action="store_true", help="Mantido por compatibilidade; risco já roda por padrão.")
    rebuild.add_argument("--operational-only", action="store_true", help="Executa apenas o trilho operacional, sem risk.")

    preflight = subparsers.add_parser("preflight", help="Refuse rebuild while another ETL lane still holds the tenant or cycle lock.")
    preflight.add_argument("--tenant-id", required=True, type=int)
    preflight.add_argument("--with-risk", action="store_true", help="Mantido por compatibilidade; risco já entra no preflight por padrão.")
    preflight.add_argument("--operational-only", action="store_true", help="Valida apenas o trilho operacional.")

    validate = subparsers.add_parser("validate", help="Assert cash/fraud alignment invariants for one tenant.")
    validate.add_argument("--tenant-id", required=True, type=int)
    validate.add_argument("--branch-id", type=int, default=None)
    validate.add_argument("--dt-ini", default=None)
    validate.add_argument("--dt-fim", default=None)
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        if args.command == "diagnose":
            result = diagnose_operational_truth(
                args.tenant_id,
                branch_id=args.branch_id,
                dt_ini=_parse_date(args.dt_ini),
                dt_fim=_parse_date(args.dt_fim),
            )
            _emit(result)
            return

        if args.command == "purge":
            result = purge_operational_truth(
                args.tenant_id,
                branch_id=args.branch_id,
                scope=args.scope,
                include_staging=bool(args.include_staging),
                ref_date=_parse_date(args.ref_date, default=business_today(args.tenant_id)),
                dry_run=bool(args.dry_run),
            )
            _emit(result)
            return

        if args.command == "preflight":
            if args.with_risk and args.operational_only:
                parser.error("--with-risk and --operational-only cannot be used together.")
            result = preflight_operational_truth_rebuild(
                args.tenant_id,
                with_risk=not bool(args.operational_only),
            )
            _emit(result)
            if not result.get("ok"):
                sys.exit(1)
            return

        if args.command == "rebuild":
            if args.with_risk and args.operational_only:
                parser.error("--with-risk and --operational-only cannot be used together.")
            result = rebuild_operational_truth(
                args.tenant_id,
                ref_date=_parse_date(args.ref_date, default=business_today(args.tenant_id)),
                with_risk=not bool(args.operational_only),
            )
            _emit(result)
            if not result.get("ok"):
                sys.exit(1)
            return

        if args.command == "validate":
            result = validate_operational_truth(
                args.tenant_id,
                branch_id=args.branch_id,
                dt_ini=_parse_date(args.dt_ini),
                dt_fim=_parse_date(args.dt_fim),
            )
            _emit(result)
            if not result.get("ok"):
                sys.exit(1)
            return
    except OperationalTruthBusyError as exc:
        _emit(exc.payload)
        sys.exit(1)

    parser.error(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
