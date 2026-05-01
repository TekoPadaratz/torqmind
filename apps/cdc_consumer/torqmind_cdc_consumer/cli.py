"""CLI entry point for TorqMind CDC Consumer mart builder operations.

Usage:
  python -m torqmind_cdc_consumer.cli backfill --from-date 2025-01-01 --id-empresa 1
  python -m torqmind_cdc_consumer.cli status
"""

from __future__ import annotations

import argparse
import sys

from .config import settings
from .logging import setup_logging, get_logger
from .mart_builder import MartBuilder

logger = get_logger("cli")


def cmd_backfill(args: argparse.Namespace) -> None:
    """Run mart_rt backfill from torqmind_current."""
    setup_logging(settings.log_level)
    logger.info(
        "backfill_start",
        from_date=args.from_date,
        to_date=args.to_date,
        id_empresa=args.id_empresa,
        id_filial=args.id_filial,
    )

    builder = MartBuilder(
        clickhouse_host=settings.clickhouse_host,
        clickhouse_port=settings.clickhouse_port,
        clickhouse_user=settings.clickhouse_user,
        clickhouse_password=settings.clickhouse_password,
    )

    results = builder.backfill(
        from_date=args.from_date,
        to_date=args.to_date,
        id_empresa=args.id_empresa,
        id_filial=args.id_filial,
    )

    total_rows = sum(r.rows_written for r in results)
    errors = [r for r in results if r.error]

    logger.info("backfill_complete", total_rows=total_rows, mart_count=len(results), errors=len(errors))

    if errors:
        for e in errors:
            logger.error("backfill_error", mart=e.mart_name, error=e.error)
        sys.exit(1)


def cmd_status(args: argparse.Namespace) -> None:
    """Check mart builder status."""
    setup_logging(settings.log_level)

    builder = MartBuilder(
        clickhouse_host=settings.clickhouse_host,
        clickhouse_port=settings.clickhouse_port,
        clickhouse_user=settings.clickhouse_user,
        clickhouse_password=settings.clickhouse_password,
    )

    client = builder._get_client()
    try:
        rows = client.query(
            "SELECT mart_name, max(published_at) AS last, sum(rows_written) AS total "
            "FROM torqmind_mart_rt.mart_publication_log GROUP BY mart_name ORDER BY last DESC"
        )
        print(f"{'Mart':<30} {'Last Published':<25} {'Total Rows':<15}")
        print("-" * 70)
        for row in rows.result_rows or []:
            print(f"{row[0]:<30} {str(row[1]):<25} {row[2]:<15}")
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="TorqMind CDC Consumer CLI")
    subparsers = parser.add_subparsers(dest="command")

    # backfill
    bp = subparsers.add_parser("backfill", help="Backfill mart_rt from current data")
    bp.add_argument("--from-date", default="2025-01-01", help="Start date YYYY-MM-DD")
    bp.add_argument("--to-date", default=None, help="End date YYYY-MM-DD (optional)")
    bp.add_argument("--id-empresa", type=int, default=1, help="Tenant ID")
    bp.add_argument("--id-filial", type=int, default=None, help="Branch ID (optional, all if omitted)")

    # status
    subparsers.add_parser("status", help="Show mart builder status")

    args = parser.parse_args()
    if args.command == "backfill":
        cmd_backfill(args)
    elif args.command == "status":
        cmd_status(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
