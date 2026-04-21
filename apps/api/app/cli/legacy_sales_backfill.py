from __future__ import annotations

import argparse
import json
from typing import Any

from app.db import get_conn


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Run the explicit one-shot legacy sales transition backfill. "
            "This is administrative and must not be part of the operational ETL hot path."
        )
    )
    parser.add_argument(
        "--tenant-id",
        dest="tenant_id",
        type=int,
        default=None,
        help="Restrict the backfill to one tenant. Defaults to all tenants.",
    )
    args = parser.parse_args()

    with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
        row = conn.execute(
            "SELECT etl.run_legacy_sales_transition_backfill(%s) AS result",
            (args.tenant_id,),
        ).fetchone()
        conn.commit()

    payload = dict((row or {}).get("result") or {})
    print(json.dumps(_json_ready(payload), ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
