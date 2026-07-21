#!/usr/bin/env python3
"""Apply CREATE-only ClickHouse DDL for cliente preço fixo (safe; no DROP)."""
from __future__ import annotations

import pathlib
import re
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
DDL_FILES = [
    ROOT / "sql/clickhouse/streaming/054_cliente_preco_fixo.sql",
]
# Also ensure current table exists (CREATE IF NOT EXISTS snippet).
CURRENT_SNIPPET = """
CREATE TABLE IF NOT EXISTS torqmind_current.stg_descontos_entidades_itens (
    id_empresa                    Int32 NOT NULL,
    id_filial                     Int32 NOT NULL,
    id_descontoentidadesitens     Int32 NOT NULL,
    payload                       String NOT NULL DEFAULT '{}',
    ingested_at                   Nullable(DateTime64(6, 'UTC')),
    dt_evento                     Nullable(DateTime64(6, 'UTC')),
    id_db_shadow                  Nullable(Int64),
    id_chave_natural              Nullable(String),
    received_at                   Nullable(DateTime64(6, 'UTC')),
    is_deleted                    UInt8 NOT NULL DEFAULT 0,
    source_ts_ms                  Int64 NOT NULL
) ENGINE = ReplacingMergeTree(source_ts_ms)
ORDER BY (id_empresa, id_filial, id_descontoentidadesitens)
SETTINGS index_granularity = 8192
"""
ENV = pathlib.Path("/etc/torqmind/prod.analytics.env")


def load_env(path: pathlib.Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k] = v.strip().strip('"').strip("'")
    return out


def split_statements(sql: str) -> list[str]:
    cleaned = re.sub(r"(?m)^\s*--.*?$", "", sql)
    parts = []
    for chunk in cleaned.split(";"):
        stmt = chunk.strip()
        if stmt:
            parts.append(stmt)
    return parts


def main() -> None:
    env = load_env(ENV)
    user = env.get("CLICKHOUSE_USER", "torqmind")
    password = env.get("CLICKHOUSE_PASSWORD", "")
    host = env.get("CLICKHOUSE_HOST", "172.30.0.9")
    port = env.get("CLICKHOUSE_PORT", "8123")
    qs = urllib.parse.urlencode({"user": user, "password": password})
    url = f"http://{host}:{port}/?{qs}"

    stmts = [CURRENT_SNIPPET.strip()]
    for ddl in DDL_FILES:
        stmts.extend(split_statements(ddl.read_text()))

    for i, stmt in enumerate(stmts, start=1):
        req = urllib.request.Request(url, data=stmt.encode("utf-8"), method="POST")
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode().strip()
        print(f"stmt{i} OK", body or "(empty)")

    verify = (
        "SELECT database, name FROM system.tables "
        "WHERE database IN ('torqmind_current','torqmind_mart_rt') "
        "AND name IN ("
        "'stg_descontos_entidades_itens',"
        "'mart_preco_bomba_dia',"
        "'mart_cliente_preco_fixo_cadastro',"
        "'mart_cliente_preco_fixo_item'"
        ") ORDER BY database, name"
    )
    req2 = urllib.request.Request(url, data=verify.encode("utf-8"), method="POST")
    with urllib.request.urlopen(req2, timeout=30) as resp:
        print("VERIFY:")
        print(resp.read().decode())


if __name__ == "__main__":
    main()
