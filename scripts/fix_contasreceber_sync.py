#!/usr/bin/env python3
"""Reconcile CONTASRECEBER / CONTASRECEBERBAIXA between Xpert and TorqMind.

Root cause this heals
---------------------
A title PAID directly in ``CONTASRECEBER`` (DTAPGTO/VLRPAGO set on the row) does
NOT bump ``DATAREPL`` in the ERP, and the agent's composite watermark for
``contasreceber`` can be poisoned to a future date by a dirty row. The agent's
incremental therefore misses the payment, and its ``revisit_open_clause`` only
re-reads STILL-OPEN titles (``DTAPGTO IS NULL``). So once a title is paid it
freezes as "open" in ``stg.contasreceber`` -> ``dw.fact_financeiro`` keeps
``data_pagamento IS NULL`` -> the delinquency mart shows it as overdue.

The canonical fix is in the agent (``apps/agent/agent/config.py`` revisit clause
now also re-reads recently-paid titles). This script is the server-side
reconciliation / safety net that heals already-stale data and can run on the App
VM (which can reach the Xpert SQL Server) on a schedule.

What it does (idempotent, hot-window, NOT a full table scan)
------------------------------------------------------------
1. Pulls from Xpert only the titles that are OPEN or RECENTLY PAID (last
   ``--paid-days`` days by DTAPGTO), plus recent baixas.
2. Upserts ``stg.contasreceber`` / ``stg.contasreceberbaixa`` by PK, bumping
   ``received_at`` ONLY when the payment fields actually changed (minimal churn).
3. Calls the CANONICAL ETL functions ``etl.load_fact_financeiro`` and
   ``etl.refresh_customer_delinquency_summary`` (no TRUNCATE, no inline mart SQL).

Security
--------
No hardcoded credentials. SQL Server comes from ``SQLSERVER_*`` env vars (or
``--sqlserver-env-file``, e.g. ``config/source-explorer.env``); PostgreSQL from
``POSTGRES_*`` / ``PG_*`` env vars (e.g. ``/etc/torqmind/prod.app.env``).

Usage
-----
    set -a; source /etc/torqmind/prod.app.env; set +a
    python scripts/fix_contasreceber_sync.py \
        --sqlserver-env-file config/source-explorer.env --id-empresa 1
    # preview only:
    python scripts/fix_contasreceber_sync.py ... --dry-run
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("reconcile_contasreceber")


# --------------------------------------------------------------------------- #
# Config from environment (NO hardcoded secrets)
# --------------------------------------------------------------------------- #
def _load_env_file(path: str) -> None:
    """Load KEY=VALUE pairs from a file into os.environ (setdefault)."""
    if not path or not os.path.isfile(path):
        return
    with open(path, "r", encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ.setdefault(key, val)


def _mssql_config() -> dict[str, Any]:
    host = os.getenv("SQLSERVER_HOST", "")
    user = os.getenv("SQLSERVER_USER", "")
    password = os.getenv("SQLSERVER_PASSWORD", "")
    database = os.getenv("SQLSERVER_DATABASE", "")
    if not (host and user and database):
        log.error(
            "Missing SQL Server env (SQLSERVER_HOST/USER/PASSWORD/DATABASE). "
            "Pass --sqlserver-env-file config/source-explorer.env or export them."
        )
        sys.exit(2)
    return {
        "server": host,
        "port": int(os.getenv("SQLSERVER_PORT", "1433")),
        "user": user,
        "password": password,
        "database": database,
        "timeout": int(os.getenv("SQLSERVER_TIMEOUT_SECONDS", "30")),
        "login_timeout": int(os.getenv("SQLSERVER_TIMEOUT_SECONDS", "30")),
    }


def _pg_dsn() -> dict[str, Any]:
    host = os.getenv("PG_HOST") or os.getenv("POSTGRES_HOST", "")
    if not host:
        log.error("Missing PG_HOST/POSTGRES_HOST env (source /etc/torqmind/prod.app.env).")
        sys.exit(2)
    return {
        "host": host,
        "port": int(os.getenv("PG_PORT") or os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("PG_DATABASE") or os.getenv("POSTGRES_DB", "torqmind"),
        "user": os.getenv("PG_USER") or os.getenv("POSTGRES_USER", "torqmind"),
        "password": os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD", ""),
    }


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    raise TypeError(f"Type {type(obj)} not serializable")


# --------------------------------------------------------------------------- #
# Xpert extract (hot window only)
# --------------------------------------------------------------------------- #
def _mssql_connect(cfg: dict[str, Any]):
    import pymssql

    return pymssql.connect(
        server=cfg["server"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        database=cfg["database"],
        timeout=cfg["timeout"],
        login_timeout=cfg["login_timeout"],
    )


def extract_contasreceber(cfg: dict[str, Any], paid_days: int) -> list[dict[str, Any]]:
    """Open titles + titles paid in the last ``paid_days`` days."""
    sql = (
        "SELECT c.* FROM dbo.CONTASRECEBER c "
        "WHERE c.DTAPGTO IS NULL "
        "   OR CAST(c.DTAPGTO AS date) >= CAST(DATEADD(day, %d, GETDATE()) AS date)"
        % (-abs(paid_days),)
    )
    conn = _mssql_connect(cfg)
    try:
        cur = conn.cursor(as_dict=True)
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        conn.close()
    log.info("Xpert CONTASRECEBER hot-window rows: %d", len(rows))
    return rows


def extract_baixas(cfg: dict[str, Any], paid_days: int) -> list[dict[str, Any]]:
    sql = (
        "SELECT c.* FROM dbo.CONTASRECEBERBAIXA c "
        "WHERE CAST(c.DATABAIXA AS date) >= CAST(DATEADD(day, %d, GETDATE()) AS date)"
        % (-abs(paid_days),)
    )
    conn = _mssql_connect(cfg)
    try:
        cur = conn.cursor(as_dict=True)
        cur.execute(sql)
        rows = cur.fetchall()
    finally:
        conn.close()
    log.info("Xpert CONTASRECEBERBAIXA hot-window rows: %d", len(rows))
    return rows


def _fetch_titles_by_keys(cfg: dict[str, Any], keys: set[tuple[int, int]]) -> list[dict[str, Any]]:
    """Batch-fetch CONTASRECEBER rows by (ID_DB, ID_CONTASRECEBER) — the true
    unique key (ID_CONTASRECEBER is numbered per ID_DB)."""
    if not keys:
        return []
    rows: list[dict[str, Any]] = []
    conn = _mssql_connect(cfg)
    try:
        cur = conn.cursor(as_dict=True)
        klist = list(keys)
        for i in range(0, len(klist), 400):
            chunk = klist[i:i + 400]
            ors = " OR ".join(
                f"(c.ID_DB={int(db)} AND c.ID_CONTASRECEBER={int(idc)})" for db, idc in chunk
            )
            cur.execute(f"SELECT c.* FROM dbo.CONTASRECEBER c WHERE {ors}")
            rows.extend(cur.fetchall())
    finally:
        conn.close()
    return rows


def _key_db_idc(row: dict[str, Any]) -> tuple[int, int]:
    db = row.get("ID_DB")
    if db is None:
        db = row.get("ID_FILIAL")
    return (int(db), int(row["ID_CONTASRECEBER"]))


def close_phantoms(pg, cfg: dict[str, Any], cr_rows: list[dict[str, Any]], id_empresa: int, dry_run: bool) -> tuple[int, int]:
    """Close STG-open titles that are no longer OPEN in Xpert (the deletion side
    a pure UPSERT misses).

    A title can disappear from "open" in Xpert by being (a) paid longer ago than
    the reconcile window, or (b) deleted/renumbered. The reconcile already pulls
    the COMPLETE current open set (``DTAPGTO IS NULL`` has no date filter), so any
    STG-open title whose (ID_DB, ID_CONTASRECEBER) is absent from it is stale. We
    re-fetch those exact titles from Xpert and:
      * upsert whatever Xpert returns (paid stragglers, truthful real data); and
      * tombstone the rest (deleted/renumbered) with a ``TORQMIND_RECONCILED_ABSENT``
        payload marker so the delinquency mart excludes them (migration 094).
    Returns (reupserted, tombstoned).
    """
    xpert_open = {_key_db_idc(r) for r in cr_rows if r.get("DTAPGTO") is None}

    cur = pg.cursor()
    cur.execute(
        """
        SELECT id_filial, id_db, id_contasreceber
        FROM stg.contasreceber
        WHERE id_empresa = %s
          AND NOT (payload ? 'TORQMIND_RECONCILED_ABSENT')
          AND (payload->>'DTAPGTO' IS NULL OR payload->>'DTAPGTO' = '')
        """,
        (id_empresa,),
    )
    stg_open = cur.fetchall()
    phantoms = [(f, db, idc) for (f, db, idc) in stg_open if (int(db), int(idc)) not in xpert_open]
    if not phantoms:
        log.info("phantom closure: 0 phantoms (all STG-open titles still open in Xpert).")
        return 0, 0

    keys = {(int(db), int(idc)) for (_f, db, idc) in phantoms}
    found = _fetch_titles_by_keys(cfg, keys)
    found_keys = {_key_db_idc(r) for r in found}
    deleted = [(f, db, idc) for (f, db, idc) in phantoms if (int(db), int(idc)) not in found_keys]

    if dry_run:
        log.info("[dry-run] phantom closure: %d STG-open not open in Xpert -> %d re-upsert (paid), %d tombstone (deleted).",
                 len(phantoms), len(found), len(deleted))
        return len(found), len(deleted)

    reupserted = 0
    if found:
        reupserted = upsert_stg(
            pg, found, "contasreceber", "id_contasreceber", id_empresa,
            change_keys=("DTAPGTO", "VLRPAGO"), dt_evento_key="DTACONTA", dry_run=False,
        )

    if deleted:
        now = datetime.now(timezone.utc)
        for (f, db, idc) in deleted:
            cur.execute(
                """
                UPDATE stg.contasreceber
                SET payload = jsonb_set(payload, '{TORQMIND_RECONCILED_ABSENT}', to_jsonb(%s::text), true),
                    received_at = %s
                WHERE id_empresa = %s AND id_filial = %s AND id_db = %s AND id_contasreceber = %s
                """,
                (now.isoformat(), now, id_empresa, f, db, idc),
            )
        pg.commit()

    log.info("phantom closure: %d phantoms -> %d re-upserted (paid stragglers), %d tombstoned (deleted).",
             len(phantoms), reupserted, len(deleted))
    return reupserted, len(deleted)


# --------------------------------------------------------------------------- #
# STG upsert (only bump received_at when payment fields changed)
# --------------------------------------------------------------------------- #
def upsert_stg(
    pg,
    rows: list[dict[str, Any]],
    table: str,
    id_col: str,
    id_empresa: int,
    change_keys: tuple[str, ...],
    dt_evento_key: str,
    dry_run: bool,
) -> int:
    if not rows:
        return 0
    import psycopg2.extras

    now = datetime.now(timezone.utc)
    values = []
    for row in rows:
        id_filial = int(row["ID_FILIAL"])
        id_db = int(row.get("ID_DB") or id_filial)
        id_record = int(row[id_col.upper()])
        dt_evento = row.get(dt_evento_key)
        payload = json.dumps(row, default=_json_default)
        values.append((id_empresa, id_filial, id_db, id_record, payload, now, now, dt_evento))

    # Only update (and bump received_at) when a payment-relevant field changed,
    # so the DW loader (received_at > watermark) reprocesses ONLY healed titles.
    change_pred = " OR ".join(
        f"(stg.{table}.payload->>'{k}') IS DISTINCT FROM (EXCLUDED.payload->>'{k}')"
        for k in change_keys
    )
    sql = f"""
        INSERT INTO stg.{table}
            (id_empresa, id_filial, id_db, id_{table}, payload, ingested_at, received_at, dt_evento)
        VALUES %s
        ON CONFLICT (id_empresa, id_filial, id_db, id_{table})
        DO UPDATE SET
            payload = EXCLUDED.payload,
            ingested_at = EXCLUDED.ingested_at,
            received_at = EXCLUDED.received_at,
            dt_evento = EXCLUDED.dt_evento
        WHERE {change_pred}
    """
    cur = pg.cursor()
    if dry_run:
        # Count how many would actually change (INSERT or payment-field UPDATE).
        cur.execute(
            f"CREATE TEMP TABLE _recon_{table} "
            f"(id_empresa int, id_filial int, id_db int, id_{table} int, payload jsonb) ON COMMIT DROP"
        )
        psycopg2.extras.execute_values(
            cur,
            f"INSERT INTO _recon_{table} VALUES %s",
            [(v[0], v[1], v[2], v[3], v[4]) for v in values],
            template="(%s,%s,%s,%s,%s::jsonb)",
            page_size=1000,
        )
        diff_pred = " OR ".join(
            f"(s.payload->>'{k}') IS DISTINCT FROM (t.payload->>'{k}')" for k in change_keys
        )
        cur.execute(
            f"""
            SELECT count(*) FROM _recon_{table} t
            LEFT JOIN stg.{table} s
              ON s.id_empresa=t.id_empresa AND s.id_filial=t.id_filial
             AND s.id_db=t.id_db AND s.id_{table}=t.id_{table}
            WHERE s.id_{table} IS NULL OR ({diff_pred})
            """
        )
        n = int(cur.fetchone()[0])
        pg.rollback()
        log.info("[dry-run] stg.%s would change %d/%d rows", table, n, len(rows))
        return n

    psycopg2.extras.execute_values(
        cur,
        sql,
        values,
        template="(%s,%s,%s,%s,%s::jsonb,%s,%s,%s)",
        page_size=1000,
    )
    changed = cur.rowcount
    pg.commit()
    log.info("stg.%s upserted; %d rows changed (received_at bumped)", table, changed)
    return changed


# --------------------------------------------------------------------------- #
# Canonical DW + mart refresh
# --------------------------------------------------------------------------- #
def refresh_dw_and_mart(pg, id_empresa: int) -> None:
    cur = pg.cursor()
    # Force a full re-scan of the finance STG for THIS transaction only. The
    # shared 'financeiro' watermark is advanced by the */2 ETL orchestrator,
    # which races us: a healed/tombstoned title written just now can have
    # received_at <= watermark by the time we load, so an incremental load would
    # skip it. force_full_scan re-reads every stg.contasreceber row (upsert is
    # still guarded by "payload IS DISTINCT", so only changed titles are
    # rewritten). SET LOCAL must share the transaction with the load, so we do
    # NOT commit between them. (psycopg2 with autocommit off already holds an
    # open transaction here.)
    cur.execute("SET LOCAL etl.force_full_scan = 'true'")
    cur.execute("SELECT etl.load_fact_financeiro(%s) AS rows", (id_empresa,))
    dw_rows = cur.fetchone()[0]
    pg.commit()
    log.info("etl.load_fact_financeiro(%s) [full-scan] -> %s rows", id_empresa, dw_rows)

    # Belt-and-suspenders: directly sync dw.fact_financeiro <- stg.contasreceber
    # for any receivable whose payload still diverges. id_titulo (ID_CONTASRECEBER)
    # is NOT globally unique (numbered per ID_DB), and the shared watermark race
    # can leave the incremental loader skipping a just-healed title; this targeted
    # sync is the same canonical payload copy the loader performs, so the DW can
    # never stay stale after STG was healed.
    cur.execute(
        """
        UPDATE dw.fact_financeiro f
        SET payload = s.payload,
            data_pagamento = (etl.safe_timestamp(s.payload->>'DTAPGTO'))::date,
            data_key_pgto = etl.date_key(etl.safe_timestamp(s.payload->>'DTAPGTO')),
            valor = etl.safe_numeric(s.payload->>'VALOR')::numeric(18,2),
            valor_pago = etl.safe_numeric(s.payload->>'VLRPAGO')::numeric(18,2),
            updated_at = now()
        FROM stg.contasreceber s
        WHERE f.id_empresa = s.id_empresa
          AND f.id_filial = s.id_filial
          AND f.id_db = s.id_db
          AND f.tipo_titulo = 1
          AND f.id_titulo = s.id_contasreceber
          AND f.id_empresa = %s
          AND f.payload IS DISTINCT FROM s.payload
        """,
        (id_empresa,),
    )
    synced = cur.rowcount
    pg.commit()
    if synced:
        log.info("DW payload sync (race-proof): %s receivables reconciled DW<-STG", synced)

    # The mart refresh is serialized server-side by a per-empresa advisory lock
    # (migration 093), so concurrent callers wait instead of racing. We still
    # retry a couple of times as defense-in-depth: STG/DW are already healed, so
    # the mart MUST end up refreshed — never leave it stale after a transient
    # error.
    last_err: Exception | None = None
    for attempt in range(1, 4):
        try:
            cur.execute("SELECT etl.refresh_customer_delinquency_summary(%s) AS rows", (id_empresa,))
            mart_rows = cur.fetchone()[0]
            pg.commit()
            log.info("etl.refresh_customer_delinquency_summary(%s) -> %s rows", id_empresa, mart_rows)
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            pg.rollback()
            log.warning("mart refresh attempt %d/3 failed: %s", attempt, exc)
            time.sleep(2 * attempt)
    raise RuntimeError(f"mart refresh failed after retries: {last_err}")


# --------------------------------------------------------------------------- #
def main() -> None:
    ap = argparse.ArgumentParser(description="Reconcile CONTASRECEBER Xpert->TorqMind (hot window).")
    ap.add_argument("--id-empresa", type=int, default=int(os.getenv("RECON_ID_EMPRESA", "1")))
    ap.add_argument("--paid-days", type=int, default=180, help="Re-read titles paid in the last N days.")
    ap.add_argument("--sqlserver-env-file", default=None, help="Optional KEY=VALUE file with SQLSERVER_* vars.")
    ap.add_argument("--dry-run", action="store_true", help="Preview changes; do not write.")
    ap.add_argument("--no-refresh", action="store_true", help="Skip DW/mart refresh.")
    args = ap.parse_args()

    if args.sqlserver_env_file:
        _load_env_file(args.sqlserver_env_file)

    import psycopg2

    mssql = _mssql_config()
    pg_dsn = _pg_dsn()

    log.info("=== Reconcile CONTASRECEBER (id_empresa=%s, paid_days=%s, dry_run=%s) ===",
             args.id_empresa, args.paid_days, args.dry_run)

    cr_rows = extract_contasreceber(mssql, args.paid_days)
    bx_rows = extract_baixas(mssql, args.paid_days)

    pg = psycopg2.connect(**pg_dsn)
    try:
        cr_changed = upsert_stg(
            pg, cr_rows, "contasreceber", "id_contasreceber", args.id_empresa,
            change_keys=("DTAPGTO", "VLRPAGO"), dt_evento_key="DTACONTA", dry_run=args.dry_run,
        )
        bx_changed = upsert_stg(
            pg, bx_rows, "contasreceberbaixa", "id_contasreceberbaixa", args.id_empresa,
            change_keys=("VALORBAIXA", "DATABAIXA"), dt_evento_key="DATABAIXA", dry_run=args.dry_run,
        )

        # Close the deletion side a pure UPSERT misses: STG-open titles no longer
        # open in Xpert (paid stragglers re-upserted; deleted ones tombstoned).
        ph_up, ph_tomb = close_phantoms(pg, mssql, cr_rows, args.id_empresa, dry_run=args.dry_run)

        if args.dry_run:
            log.info("[dry-run] DONE. contasreceber=%d contasreceberbaixa=%d phantoms(reupsert=%d,tombstone=%d) would change.",
                     cr_changed, bx_changed, ph_up, ph_tomb)
            return

        if not args.no_refresh and (cr_changed or bx_changed or ph_up or ph_tomb):
            refresh_dw_and_mart(pg, args.id_empresa)
        elif args.no_refresh:
            log.info("Skipping DW/mart refresh (--no-refresh).")
        else:
            log.info("Nothing changed; skipping DW/mart refresh.")
    finally:
        pg.close()

    log.info("=== DONE. contasreceber changed=%d, contasreceberbaixa changed=%d, phantoms reupsert=%d tombstone=%d ===",
             cr_changed, bx_changed, ph_up, ph_tomb)


if __name__ == "__main__":
    main()
