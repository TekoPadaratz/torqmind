from __future__ import annotations

"""High-throughput ingestion endpoint (NDJSON).

PT-BR:
- O cliente (agent/extractor) faz POST de NDJSON (1 JSON por linha).
- A API faz UPSERT em `stg.*` (raw JSONB) com PK composta.
- Opcional: dispara ETL (STG->DW->MART) logo após ingest.

EN:
- Client posts NDJSON (1 JSON per line).
- API upserts into `stg.*` raw tables with composite PK.
- Optional: trigger ETL after ingest.

Why NDJSON?
- Streaming-friendly
- Simple to generate from SQL Server
"""

import gzip
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Header, HTTPException, Query, Request

from app.config import settings
from app.db import get_conn
from app.services.etl_orchestrator import EtlCycleBusyError, run_incremental_cycle
from app.services.telegram import notify_cancelled_comprovantes

router = APIRouter(prefix="/ingest", tags=["ingest"])

SALES_RETENTION_DATASETS = {"comprovantes", "movprodutos"}


def _get_any(d: Dict[str, Any], keys: List[str]) -> Any:
    for k in keys:
        if k in d and d[k] is not None:
            return d[k]
    return None


def _to_int(x: Any) -> Optional[int]:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        try:
            # handle numeric strings with spaces
            return int(str(x).strip())
        except Exception:
            return None


def _parse_ts(x: Any) -> Optional[datetime]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
    raw = str(x).strip()
    if not raw:
        return None
    candidates = [
        raw,
        raw.replace(" ", "T"),
    ]
    for c in candidates:
        try:
            dt = datetime.fromisoformat(c.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return None


def _infer_dt_evento(obj: Dict[str, Any]) -> Optional[datetime]:
    keys = [
        "TORQMIND_DT_EVENTO",
        "DT_EVENTO",
        "DATA",
        "DATAMOV",
        "DTMOV",
        "DTALTERACAO",
        "DTCADASTRO",
        "VENCIMENTO",
    ]
    for k in keys:
        if k in obj:
            dt = _parse_ts(obj.get(k))
            if dt is not None:
                return dt
    return None


def _infer_id_db_shadow(obj: Dict[str, Any]) -> Optional[int]:
    return _to_int(_get_any(obj, ["ID_DB", "id_db", "ID", "id"]))


def _infer_natural_key(obj: Dict[str, Any], pk_values: Dict[str, Any]) -> str:
    if "ID_CHAVE_NATURAL" in obj and obj["ID_CHAVE_NATURAL"] not in (None, ""):
        return str(obj["ID_CHAVE_NATURAL"])
    parts = [f"{k}={pk_values.get(k)}" for k in sorted(pk_values.keys()) if pk_values.get(k) is not None]
    if parts:
        return "|".join(parts)
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)[:1000]


class DatasetSpec:
    def __init__(
        self,
        table: str,
        pk_cols: List[str],
        pk_extractors: List[Tuple[str, List[str]]],
    ) -> None:
        self.table = table
        self.pk_cols = pk_cols
        # list of tuples: (dest_col_in_table, [possible keys in payload])
        self.pk_extractors = pk_extractors


DATASETS: Dict[str, DatasetSpec] = {
    # Dimension-like
    "filiais": DatasetSpec(
        table="stg.filiais",
        pk_cols=["id_empresa", "id_filial"],
        pk_extractors=[("id_filial", ["ID_FILIAL", "id_filial"])],
    ),
    "funcionarios": DatasetSpec(
        table="stg.funcionarios",
        pk_cols=["id_empresa", "id_filial", "id_funcionario"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_funcionario", ["ID_FUNCIONARIOS", "ID_FUNCIONARIO", "id_funcionario"]),
        ],
    ),
    "usuarios": DatasetSpec(
        table="stg.usuarios",
        pk_cols=["id_empresa", "id_filial", "id_usuario"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_usuario", ["ID_USUARIOS", "ID_USUARIO", "id_usuario"]),
        ],
    ),
    "entidades": DatasetSpec(
        table="stg.entidades",
        pk_cols=["id_empresa", "id_filial", "id_entidade"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_entidade", ["ID_ENTIDADE", "id_entidade", "ID_CLIENTE"]),
        ],
    ),
    "clientes": DatasetSpec(
        table="stg.entidades",
        pk_cols=["id_empresa", "id_filial", "id_entidade"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_entidade", ["ID_ENTIDADE", "id_entidade", "ID_CLIENTE"]),
        ],
    ),
    "grupoprodutos": DatasetSpec(
        table="stg.grupoprodutos",
        pk_cols=["id_empresa", "id_filial", "id_grupoprodutos"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_grupoprodutos", ["ID_GRUPOPRODUTOS", "id_grupoprodutos"]),
        ],
    ),
    "localvendas": DatasetSpec(
        table="stg.localvendas",
        pk_cols=["id_empresa", "id_filial", "id_localvendas"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_localvendas", ["ID_LOCALVENDAS", "id_localvendas"]),
        ],
    ),
    "produtos": DatasetSpec(
        table="stg.produtos",
        pk_cols=["id_empresa", "id_filial", "id_produto"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_produto", ["ID_PRODUTOS", "ID_PRODUTO", "id_produto"]),
        ],
    ),
    "turnos": DatasetSpec(
        table="stg.turnos",
        pk_cols=["id_empresa", "id_filial", "id_turno"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_turno", ["ID_TURNOS", "ID_TURNO", "id_turno"]),
        ],
    ),

    # Facts raw
    "comprovantes": DatasetSpec(
        table="stg.comprovantes",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_comprovante"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_comprovante", ["ID_COMPROVANTE", "id_comprovante"]),
        ],
    ),
    "movprodutos": DatasetSpec(
        table="stg.movprodutos",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_movprodutos"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_movprodutos", ["ID_MOVPRODUTOS", "id_movprodutos"]),
        ],
    ),
    "movlctos": DatasetSpec(
        table="stg.movlctos",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_movlctos"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_movlctos", ["ID_MOVLCTOS", "ID_MOVLCTO", "id_movlctos", "id_movlcto"]),
        ],
    ),
    "itensmovprodutos": DatasetSpec(
        table="stg.itensmovprodutos",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_movprodutos", "id_itensmovprodutos"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_movprodutos", ["ID_MOVPRODUTOS", "id_movprodutos"]),
            ("id_itensmovprodutos", ["ID_ITENSMOVPRODUTOS", "id_itensmovprodutos"]),
        ],
    ),
    "formas_pgto_comprovantes": DatasetSpec(
        table="stg.formas_pgto_comprovantes",
        pk_cols=["id_empresa", "id_filial", "id_referencia", "tipo_forma"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_referencia", ["ID_REFERENCIA", "id_referencia", "REFERENCIA", "referencia"]),
            ("tipo_forma", ["TIPO_FORMA", "tipo_forma"]),
        ],
    ),

    # Finance
    "contaspagar": DatasetSpec(
        table="stg.contaspagar",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_contaspagar"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_contaspagar", ["ID_CONTASPAGAR", "id_contaspagar"]),
        ],
    ),
    "contasreceber": DatasetSpec(
        table="stg.contasreceber",
        pk_cols=["id_empresa", "id_filial", "id_db", "id_contasreceber"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("id_contasreceber", ["ID_CONTASRECEBER", "id_contasreceber"]),
        ],
    ),
    "financeiro": DatasetSpec(
        table="stg.financeiro",
        pk_cols=["id_empresa", "id_filial", "id_db", "tipo_titulo", "id_titulo"],
        pk_extractors=[
            ("id_filial", ["ID_FILIAL", "id_filial"]),
            ("id_db", ["ID_DB", "id_db"]),
            ("tipo_titulo", ["TIPO_TITULO", "tipo_titulo"]),
            ("id_titulo", ["ID_TITULO", "id_titulo"]),
        ],
    ),
}


def _resolve_id_empresa(x_ingest_key: Optional[str], x_empresa_id: Optional[str]) -> int:
    """Resolve tenant id from X-Ingest-Key, fallback to X-Empresa-Id (dev only)."""

    if x_ingest_key:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                "SELECT id_empresa FROM app.tenants WHERE ingest_key = %s AND is_active = true",
                (x_ingest_key,),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=401, detail="Invalid X-Ingest-Key")
            return int(row["id_empresa"])

    if settings.ingest_require_key:
        raise HTTPException(status_code=401, detail="Missing X-Ingest-Key")

    # Dev fallback
    if x_empresa_id:
        try:
            return int(x_empresa_id)
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid X-Empresa-Id")

    return 1


def _load_tenant_ingest_policy(id_empresa: int) -> Dict[str, Any]:
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            """
            SELECT
              id_empresa,
              sales_history_days,
              default_product_scope_days,
              CURRENT_DATE AS ref_date
            FROM app.tenants
            WHERE id_empresa = %s
            """,
            (id_empresa,),
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail={"error": "tenant_not_found", "message": "Empresa não encontrada."})
    return dict(row)


def _sales_retention_cutoff(ref_date: date, days: int) -> date:
    window_days = max(int(days or 365), 1)
    return ref_date - timedelta(days=window_days - 1)


def _retention_policy_response(dataset_key: str, tenant_policy: Dict[str, Any]) -> Dict[str, Any]:
    enforced = dataset_key in SALES_RETENTION_DATASETS
    cutoff = None
    days = None
    if enforced:
        days = int(tenant_policy.get("sales_history_days") or 365)
        cutoff = _sales_retention_cutoff(tenant_policy["ref_date"], days)
    return {
        "name": "sales_history_days" if enforced else "none",
        "enforced": enforced,
        "days": days,
        "cutoff": cutoff.isoformat() if cutoff else None,
        "business_date_field": "dt_evento",
        "datasets": sorted(SALES_RETENTION_DATASETS) if enforced else [],
    }


def _parse_ndjson_body(raw: bytes, is_gzip: bool) -> List[Dict[str, Any]]:
    if is_gzip:
        raw = gzip.decompress(raw)

    # Split by lines (supports both \n and \r\n)
    lines = raw.splitlines()
    out: List[Dict[str, Any]] = []
    for idx, line in enumerate(lines, start=1):
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValueError("NDJSON line is not an object")
            out.append(obj)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid NDJSON at line {idx}: {e}")
    return out


def _bulk_upsert_with_stats(conn, table: str, pk_cols: List[str], rows: List[Tuple[Any, ...]]) -> Tuple[int, int]:
    if not rows:
        return 0, 0

    cols = pk_cols + ["id_db_shadow", "id_chave_natural", "dt_evento", "payload"]
    placeholders_row = "(" + ",".join(["%s"] * (len(cols) - 1) + ["%s::jsonb"]) + ")"
    values_sql = ",".join([placeholders_row] * len(rows))
    update_assignments = ",".join(
        [
            "id_db_shadow = EXCLUDED.id_db_shadow",
            "id_chave_natural = EXCLUDED.id_chave_natural",
            "dt_evento = EXCLUDED.dt_evento",
            "payload = EXCLUDED.payload",
            "ingested_at = now()",
            "received_at = now()",
        ]
    )
    sql = f"""
      INSERT INTO {table} ({",".join(cols)})
      VALUES {values_sql}
      ON CONFLICT ({",".join(pk_cols)})
      DO UPDATE SET {update_assignments}
      RETURNING (xmax = 0) AS inserted_flag
    """

    flat_params: List[Any] = []
    for row in rows:
        flat_params.extend(row)

    with conn.cursor() as cur:
        cur.execute(sql, flat_params)
        flags = cur.fetchall()
    inserted = sum(1 for f in flags if f["inserted_flag"])
    updated = len(flags) - inserted
    return inserted, updated


def _dedupe_rows_by_pk(pk_cols: List[str], rows: List[Tuple[Any, ...]]) -> Tuple[List[Tuple[Any, ...]], int]:
    """Deduplicate rows in-memory by PK tuple, preserving the last occurrence.

    This avoids Postgres error:
    'ON CONFLICT DO UPDATE command cannot affect row a second time'
    when a single VALUES batch contains duplicate PKs.
    """
    if not rows:
        return rows, 0

    pk_size = len(pk_cols)
    by_pk: Dict[Tuple[Any, ...], Tuple[Any, ...]] = {}
    for row in rows:
        by_pk[tuple(row[:pk_size])] = row
    deduped = list(by_pk.values())
    duplicates = len(rows) - len(deduped)
    return deduped, duplicates


@router.get("/health")
def ingest_health(
    x_ingest_key: Optional[str] = Header(None, alias="X-Ingest-Key"),
    x_empresa_id: Optional[str] = Header(None, alias="X-Empresa-Id"),
):
    id_empresa = _resolve_id_empresa(x_ingest_key=x_ingest_key, x_empresa_id=x_empresa_id)
    out: List[Dict[str, Any]] = []
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        for dataset, spec in sorted(DATASETS.items()):
            row = conn.execute(
                f"""
                SELECT
                  COUNT(*)::bigint AS rows_total,
                  MAX(ingested_at) AS max_ingested_at,
                  MAX(received_at) AS max_received_at,
                  MAX(dt_evento) AS max_dt_evento
                FROM {spec.table}
                WHERE id_empresa = %s
                """,
                (id_empresa,),
            ).fetchone()
            out.append(
                {
                    "dataset": dataset,
                    "table": spec.table,
                    "rows_total": int(row["rows_total"] or 0),
                    "max_ingested_at": row["max_ingested_at"],
                    "max_received_at": row["max_received_at"],
                    "max_dt_evento": row["max_dt_evento"],
                }
            )
    return {"ok": True, "id_empresa": id_empresa, "datasets": out}


@router.post("/{dataset}")
async def ingest_dataset(
    dataset: str,
    request: Request,
    run_etl: bool = Query(False, description="Run ETL right after ingest"),
    refresh_mart: bool = Query(True, description="If run_etl, also refresh materialized views"),
    x_ingest_key: Optional[str] = Header(None, alias="X-Ingest-Key"),
    x_empresa_id: Optional[str] = Header(None, alias="X-Empresa-Id"),
):
    dataset_key = dataset.strip().lower()
    if dataset_key not in DATASETS:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "Unknown dataset",
                "dataset": dataset,
                "allowed": sorted(DATASETS.keys()),
            },
        )

    id_empresa = _resolve_id_empresa(x_ingest_key=x_ingest_key, x_empresa_id=x_empresa_id)
    spec = DATASETS[dataset_key]
    tenant_policy = _load_tenant_ingest_policy(id_empresa)
    retention_policy = _retention_policy_response(dataset_key, tenant_policy)
    retention_cutoff = (
        date.fromisoformat(retention_policy["cutoff"])
        if retention_policy.get("cutoff")
        else None
    )

    raw = await request.body()
    is_gzip = (request.headers.get("content-encoding") or "").lower() == "gzip"
    rows = _parse_ndjson_body(raw, is_gzip=is_gzip)

    # Build values list
    values: List[Tuple[Any, ...]] = []
    rejected_invalid: List[Dict[str, Any]] = []
    rejected_by_retention: List[Dict[str, Any]] = []

    for obj in rows:
        pk: Dict[str, Any] = {"id_empresa": id_empresa}

        ok = True
        for dest_col, keys in spec.pk_extractors:
            v = _get_any(obj, keys)
            iv = _to_int(v)
            if iv is None:
                ok = False
                break
            pk[dest_col] = iv

        if not ok:
            rejected_invalid.append({"row": obj, "reason": "Missing/invalid PK fields"})
            continue

        # Ensure id_filial exists when table needs it
        if "id_filial" in spec.pk_cols and "id_filial" not in pk:
            rejected_invalid.append({"row": obj, "reason": "Missing id_filial"})
            continue

        payload_json = json.dumps(obj, ensure_ascii=False)
        dt_evento = _infer_dt_evento(obj)
        if retention_cutoff is not None and dt_evento is not None and dt_evento.date() < retention_cutoff:
            rejected_by_retention.append(
                {
                    "row": obj,
                    "reason": "Rejected by sales_history_days retention window",
                    "dt_evento": dt_evento.isoformat(),
                    "cutoff": retention_cutoff.isoformat(),
                }
            )
            continue
        id_db_shadow = _infer_id_db_shadow(obj)
        natural_key = _infer_natural_key(obj, pk)

        # Compose tuple in table column order: pk_cols + shadow + payload
        tuple_values = [pk.get(col) for col in spec.pk_cols]
        tuple_values.append(id_db_shadow)
        tuple_values.append(natural_key)
        tuple_values.append(dt_evento)
        tuple_values.append(payload_json)
        values.append(tuple(tuple_values))

    if not values:
        rejected = rejected_invalid + rejected_by_retention
        return {
            "ok": True,
            "dataset": dataset_key,
            "id_empresa": id_empresa,
            "inserted_or_updated": 0,
            "rejected": len(rejected),
            "rejected_invalid": len(rejected_invalid),
            "rejected_by_retention": len(rejected_by_retention),
            "retention_cutoff": retention_policy.get("cutoff"),
            "retention_policy": retention_policy,
            "details": rejected[:5],
        }

    # Deduplicate by PK within the same batch to avoid ON CONFLICT re-hit errors.
    values, duplicates_in_batch = _dedupe_rows_by_pk(spec.pk_cols, values)

    # Execute batch with inserted/updated stats
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        with conn.transaction():
            inserted, updated = _bulk_upsert_with_stats(conn, spec.table, spec.pk_cols, values)
        conn.commit()

    # Optional: send telegram notifications when there are cancelled comprovantes
    if dataset_key == "comprovantes":
        try:
            await notify_cancelled_comprovantes(id_empresa=id_empresa, raw_rows=rows)
        except Exception:
            # Never fail ingestion due to notification issues.
            pass

    etl_result = None
    if run_etl:
        try:
            summary = run_incremental_cycle(
                [id_empresa],
                ref_date=datetime.now(tz=timezone.utc).date(),
                refresh_mart=refresh_mart,
                force_full=False,
                fail_fast=True,
                db_role="MASTER",
                db_tenant_scope=id_empresa,
                tenant_rows=[{"id_empresa": id_empresa}],
                acquire_lock=True,
            )
            item = (summary.get("items") or [None])[0] or {}
            etl_result = item.get("result")
            if item.get("ok") is False and not etl_result:
                etl_result = {
                    "ok": False,
                    "error": "etl_failed",
                    "message": str(item.get("error") or "Falha ao executar ETL após ingestão."),
                }
        except EtlCycleBusyError as exc:
            etl_result = {
                "ok": False,
                "error": "etl_busy",
                "message": str(exc),
            }

    rejected = rejected_invalid + rejected_by_retention
    return {
        "ok": True,
        "dataset": dataset_key,
        "id_empresa": id_empresa,
        "inserted_or_updated": len(values),
        "inserted": inserted,
        "updated": updated,
        "duplicates_in_batch": duplicates_in_batch,
        "rejected": len(rejected),
        "rejected_invalid": len(rejected_invalid),
        "rejected_by_retention": len(rejected_by_retention),
        "retention_cutoff": retention_policy.get("cutoff"),
        "retention_policy": retention_policy,
        "etl": etl_result,
        "sample_rejections": rejected[:5],
    }
