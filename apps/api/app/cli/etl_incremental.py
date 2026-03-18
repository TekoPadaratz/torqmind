from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from typing import Any

from app.db import get_conn


def _parse_date(value: str | None) -> date:
    if value:
        return date.fromisoformat(value)
    return date.today()


def _list_target_tenants(tenant_id: int | None) -> list[dict[str, Any]]:
    if tenant_id is not None:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            rows = conn.execute(
                """
                SELECT id_empresa, nome, status, is_active
                FROM app.tenants
                WHERE id_empresa = %s
                ORDER BY id_empresa
                """,
                (tenant_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        rows = conn.execute(
            """
            SELECT id_empresa, nome, status, is_active
            FROM app.tenants
            WHERE is_active = true
            ORDER BY id_empresa
            """
        ).fetchall()
    return [dict(row) for row in rows]


def _run_incremental_for_tenant(tenant_id: int, ref_date: date) -> dict[str, Any]:
    started = time.perf_counter()
    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            "SELECT etl.run_all(%s, %s, %s, %s) AS result",
            (tenant_id, False, True, ref_date),
        ).fetchone()
        conn.commit()

    elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
    result = row["result"] if row else None
    meta = result.get("meta") if isinstance(result, dict) else None
    return {
        "tenant_id": tenant_id,
        "elapsed_ms": elapsed_ms,
        "result": result,
        "payment_notifications": (meta or {}).get("payment_notifications"),
        "cash_notifications": (meta or {}).get("cash_notifications"),
        "mart_refreshed": (meta or {}).get("mart_refreshed"),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the canonical incremental ETL for active tenants.")
    parser.add_argument("--tenant-id", dest="tenant_id", type=int, default=None, help="Run only for one tenant.")
    parser.add_argument("--ref-date", dest="ref_date", default=None, help="Reference date YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--fail-fast", dest="fail_fast", action="store_true", help="Stop on the first tenant failure.")
    args = parser.parse_args()

    ref_date = _parse_date(args.ref_date)
    tenants = _list_target_tenants(args.tenant_id)
    if not tenants:
        print(
            json.dumps(
                {
                    "ok": True,
                    "reference_date": ref_date.isoformat(),
                    "processed": 0,
                    "items": [],
                    "message": "No target tenants found.",
                },
                ensure_ascii=False,
                default=str,
            )
        )
        return

    started_at = time.time()
    items: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    for tenant in tenants:
        tenant_id = int(tenant["id_empresa"])
        try:
            result = _run_incremental_for_tenant(tenant_id, ref_date)
            items.append(
                {
                    **result,
                    "tenant_name": tenant.get("nome"),
                    "tenant_status": tenant.get("status"),
                    "is_active": bool(tenant.get("is_active", True)),
                }
            )
        except Exception as exc:  # noqa: BLE001 - CLI summary must include tenant failures.
            failure = {
                "tenant_id": tenant_id,
                "tenant_name": tenant.get("nome"),
                "tenant_status": tenant.get("status"),
                "error": str(exc),
            }
            failures.append(failure)
            items.append({**failure, "ok": False})
            if args.fail_fast:
                break

    summary = {
        "ok": not failures,
        "reference_date": ref_date.isoformat(),
        "processed": len(items),
        "failed": len(failures),
        "duration_ms": round((time.time() - started_at) * 1000, 2),
        "items": items,
    }
    print(json.dumps(summary, ensure_ascii=False, default=str))

    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
