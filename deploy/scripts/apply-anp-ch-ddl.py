#!/usr/bin/env python3
"""Apply CREATE-only ClickHouse DDL for ANP compliance (safe; no DROP).

ClickHouse HTTP rejects multi-statements by default — apply one statement at a time.
"""
from __future__ import annotations

import pathlib
import re
import urllib.parse
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[2]
DDL = ROOT / "sql/clickhouse/streaming/050_anp_compliance.sql"
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
    # Strip line comments then split on ';'
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

    for i, stmt in enumerate(split_statements(DDL.read_text()), start=1):
        req = urllib.request.Request(url, data=stmt.encode("utf-8"), method="POST")
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read().decode().strip()
        print(f"stmt{i} OK", body or "(empty)")

    verify = (
        "SELECT database, name FROM system.tables "
        "WHERE database IN ('torqmind_current','torqmind_mart_rt') "
        "AND name IN ('fact_nfe_entrada','fact_preco_bomba','mart_anp_compliance') "
        "ORDER BY database, name"
    )
    req2 = urllib.request.Request(url, data=verify.encode("utf-8"), method="POST")
    with urllib.request.urlopen(req2, timeout=30) as resp:
        print("VERIFY:")
        print(resp.read().decode())


if __name__ == "__main__":
    main()
