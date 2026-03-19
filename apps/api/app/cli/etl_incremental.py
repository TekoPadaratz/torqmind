from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from app.services.etl_orchestrator import EtlCycleBusyError, list_target_tenants, run_incremental_cycle


def _parse_date(value: str | None) -> date:
    if value:
        return date.fromisoformat(value)
    return date.today()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the canonical incremental ETL for active tenants.")
    parser.add_argument("--tenant-id", dest="tenant_id", type=int, default=None, help="Run only for one tenant.")
    parser.add_argument("--ref-date", dest="ref_date", default=None, help="Reference date YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--fail-fast", dest="fail_fast", action="store_true", help="Stop on the first tenant failure.")
    args = parser.parse_args()

    ref_date = _parse_date(args.ref_date)
    tenants = list_target_tenants(args.tenant_id)
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

    try:
        summary = run_incremental_cycle(
            [int(tenant["id_empresa"]) for tenant in tenants],
            ref_date=ref_date,
            refresh_mart=True,
            force_full=False,
            fail_fast=args.fail_fast,
            tenant_rows=tenants,
            db_role="MASTER",
            db_tenant_scope=None,
            acquire_lock=True,
        )
    except EtlCycleBusyError as exc:
        print(
            json.dumps(
                {
                    "ok": False,
                    "reference_date": ref_date.isoformat(),
                    "processed": 0,
                    "failed": 1,
                    "error": "etl_busy",
                    "message": str(exc),
                    "items": [],
                },
                ensure_ascii=False,
                default=str,
            )
        )
        sys.exit(1)

    print(json.dumps(summary, ensure_ascii=False, default=str))

    if summary.get("failed"):
        sys.exit(1)


if __name__ == "__main__":
    main()
