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
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, Header, HTTPException, Query, Request

from app.config import settings
from app.db import get_conn
from app.services.telegram import notify_cancelled_comprovantes

router = APIRouter(prefix="/ingest", tags=["ingest"])


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

    raw = await request.body()
    is_gzip = (request.headers.get("content-encoding") or "").lower() == "gzip"
    rows = _parse_ndjson_body(raw, is_gzip=is_gzip)

    # Build values list
    values: List[Tuple[Any, ...]] = []
    rejected: List[Dict[str, Any]] = []

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
            rejected.append({"row": obj, "reason": "Missing/invalid PK fields"})
            continue

        # Ensure id_filial exists when table needs it
        if "id_filial" in spec.pk_cols and "id_filial" not in pk:
            rejected.append({"row": obj, "reason": "Missing id_filial"})
            continue

        payload_json = json.dumps(obj, ensure_ascii=False)

        # Compose tuple in table column order: pk_cols + payload
        tuple_values = [pk.get(col) for col in spec.pk_cols]
        tuple_values.append(payload_json)
        values.append(tuple(tuple_values))

    if not values:
        return {
            "ok": True,
            "dataset": dataset_key,
            "id_empresa": id_empresa,
            "inserted_or_updated": 0,
            "rejected": len(rejected),
            "details": rejected[:5],
        }

    cols_sql = ",".join(spec.pk_cols + ["payload"])
    placeholders = ",".join(["%s"] * len(spec.pk_cols) + ["%s::jsonb"])
    conflict_cols = ",".join(spec.pk_cols)

    # Always update payload/ingested_at on conflict
    sql = f"""
      INSERT INTO {spec.table} ({cols_sql})
      VALUES ({placeholders})
      ON CONFLICT ({conflict_cols})
      DO UPDATE SET payload = EXCLUDED.payload, ingested_at = now()
    """

    # Execute batch
    with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
        with conn.transaction():
            conn.executemany(sql, values)

    # Optional: send telegram notifications when there are cancelled comprovantes
    if dataset_key == "comprovantes":
        try:
            await notify_cancelled_comprovantes(id_empresa=id_empresa, raw_rows=rows)
        except Exception:
            # Never fail ingestion due to notification issues.
            pass

    etl_result = None
    if run_etl:
        with get_conn(role="MASTER", tenant_id=id_empresa, branch_id=None) as conn:
            etl_result = conn.execute(
                "SELECT etl.run_all(%s, %s) AS result", (id_empresa, refresh_mart)
            ).fetchone()["result"]

    return {
        "ok": True,
        "dataset": dataset_key,
        "id_empresa": id_empresa,
        "inserted_or_updated": len(values),
        "rejected": len(rejected),
        "etl": etl_result,
        "sample_rejections": rejected[:5],
    }
