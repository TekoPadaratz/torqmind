from __future__ import annotations

import argparse
import json
from datetime import date

from app import repos_platform


SYSTEM_CLAIMS = {"sub": None, "user_role": "platform_master"}


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    return date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="TorqMind platform billing jobs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    daily = subparsers.add_parser("daily", help="Generate receivables and refresh overdue statuses")
    daily.add_argument("--as-of", dest="as_of", default=None, help="Reference date YYYY-MM-DD")
    daily.add_argument("--competence-month", dest="competence_month", default=None, help="Competence month YYYY-MM-DD")
    daily.add_argument("--months-ahead", dest="months_ahead", type=int, default=0)
    daily.add_argument("--tenant-id", dest="tenant_id", type=int, default=None)

    generate = subparsers.add_parser("generate", help="Generate receivables only")
    generate.add_argument("--as-of", dest="as_of", default=None, help="Reference date YYYY-MM-DD")
    generate.add_argument("--competence-month", dest="competence_month", default=None, help="Competence month YYYY-MM-DD")
    generate.add_argument("--months-ahead", dest="months_ahead", type=int, default=0)
    generate.add_argument("--tenant-id", dest="tenant_id", type=int, default=None)

    args = parser.parse_args()

    result = repos_platform.generate_receivables(
        SYSTEM_CLAIMS,
        ip="cli",
        competence_month=_parse_date(args.competence_month),
        as_of=_parse_date(args.as_of),
        months_ahead=int(args.months_ahead or 0),
        tenant_id=args.tenant_id,
    )

    print(json.dumps({"ok": True, "command": args.command, "result": result}, default=str, ensure_ascii=False))


if __name__ == "__main__":
    main()
