from __future__ import annotations

import argparse
import json
import sys
from datetime import date

from app.db import get_conn


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Purge retained sales history for the short commercial trail and refresh dependent marts."
    )
    parser.add_argument("--tenant-id", dest="tenant_id", type=int, default=None, help="Run only for one tenant.")
    parser.add_argument("--ref-date", dest="ref_date", default=None, help="Reference date YYYY-MM-DD. Defaults to CURRENT_DATE.")
    args = parser.parse_args()

    ref_date = _parse_date(args.ref_date)

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            "SELECT etl.purge_sales_history(%s, %s) AS result",
            (args.tenant_id, ref_date),
        ).fetchone()
        conn.commit()

    result = dict((row or {}).get("result") or {})
    print(json.dumps(result, ensure_ascii=False, default=str))

    if not result.get("ok", True):
        sys.exit(1)


if __name__ == "__main__":
    main()
