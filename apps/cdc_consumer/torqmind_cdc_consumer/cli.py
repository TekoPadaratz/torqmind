"""CLI entry point for TorqMind CDC Consumer mart builder operations.

Usage:
  python -m torqmind_cdc_consumer.cli backfill-stg --from-date 2025-01-01 --id-empresa 1
  python -m torqmind_cdc_consumer.cli backfill-stg --mart-only --from-date 2025-01-01 --id-empresa 1
  python -m torqmind_cdc_consumer.cli backfill --source dw --from-date 2025-01-01 --id-empresa 1
  python -m torqmind_cdc_consumer.cli validate --from-date 2025-01-01 --id-empresa 1
  python -m torqmind_cdc_consumer.cli status
"""

from __future__ import annotations

import argparse
import os
import sys

from .config import settings
from .logging import setup_logging, get_logger
from .mart_builder import MartBuilder, _DEFAULT_BATCH_SIZE, _DEFAULT_MAX_THREADS, _DEFAULT_MAX_MEMORY

logger = get_logger("cli")

# Environment variable overrides for performance tuning
_ENV_BATCH_SIZE = int(os.environ.get("TORQMIND_BACKFILL_BATCH_SIZE", str(_DEFAULT_BATCH_SIZE)))
_ENV_MAX_THREADS = int(os.environ.get("TORQMIND_BACKFILL_MAX_THREADS", str(_DEFAULT_MAX_THREADS)))
_ENV_MAX_MEMORY_GB = os.environ.get("TORQMIND_BACKFILL_MAX_MEMORY_GB", "")


def _resolve_max_memory(args: argparse.Namespace) -> int:
    """Resolve max_memory_usage from --max-memory-gb, env, or default."""
    if hasattr(args, "max_memory_gb") and args.max_memory_gb is not None:
        return int(args.max_memory_gb * 1_000_000_000)
    if _ENV_MAX_MEMORY_GB:
        return int(float(_ENV_MAX_MEMORY_GB) * 1_000_000_000)
    return _DEFAULT_MAX_MEMORY


def cmd_backfill(args: argparse.Namespace) -> None:
    """Run mart_rt backfill from torqmind_current."""
    setup_logging(settings.log_level)

    mart_only = getattr(args, "mart_only", False) or getattr(args, "skip_slim", False)
    skip_batch_deletes = getattr(args, "skip_batch_deletes", False)
    batch_size = getattr(args, "batch_size", None) or _ENV_BATCH_SIZE
    max_threads = getattr(args, "max_threads", None) or _ENV_MAX_THREADS
    max_memory = _resolve_max_memory(args)

    if mart_only and args.source != "stg":
        print("ERROR: --mart-only / --skip-slim requires --source stg", file=sys.stderr)
        sys.exit(2)

    logger.info(
        "backfill_start",
        from_date=args.from_date,
        to_date=args.to_date,
        id_empresa=args.id_empresa,
        id_filial=args.id_filial,
        source=args.source,
        mart_only=mart_only,
        batch_size=batch_size,
        max_threads=max_threads,
        max_memory_gb=max_memory / 1_000_000_000,
        skip_batch_deletes=skip_batch_deletes,
    )

    builder = MartBuilder(
        clickhouse_host=settings.clickhouse_host,
        clickhouse_port=settings.clickhouse_port,
        clickhouse_user=settings.clickhouse_user,
        clickhouse_password=settings.clickhouse_password,
        source=args.source,
        batch_size=batch_size,
        max_threads=max_threads,
        max_memory_usage=max_memory,
    )

    results = builder.backfill(
        from_date=args.from_date,
        to_date=args.to_date,
        id_empresa=args.id_empresa,
        id_filial=args.id_filial,
        mart_only=mart_only,
        skip_batch_deletes=skip_batch_deletes,
    )

    total_rows = sum(r.rows_written for r in results)
    errors = [r for r in results if r.error]

    logger.info("backfill_complete", total_rows=total_rows, mart_count=len(results), errors=len(errors))

    for r in results:
        status = "OK" if not r.error else f"ERROR: {r.error}"
        logger.info(
            "backfill_mart_result",
            mart=r.mart_name,
            rows=r.rows_written,
            duration_ms=r.duration_ms,
            status=status,
        )

    if errors:
        for e in errors:
            logger.error("backfill_error", mart=e.mart_name, error=e.error)
        sys.exit(1)


def cmd_validate(args: argparse.Namespace) -> None:
    """Validate slim→mart completeness for sales marts."""
    setup_logging(settings.log_level)
    logger.info(
        "validate_start",
        from_date=args.from_date,
        to_date=args.to_date,
        id_empresa=args.id_empresa,
    )

    builder = MartBuilder(
        clickhouse_host=settings.clickhouse_host,
        clickhouse_port=settings.clickhouse_port,
        clickhouse_user=settings.clickhouse_user,
        clickhouse_password=settings.clickhouse_password,
        source="stg",
    )

    result = builder.validate_completeness(
        id_empresa=args.id_empresa,
        from_date=args.from_date,
        to_date=args.to_date,
    )

    print(f"Slim data_keys: {result['slim_data_keys_count']}")
    print(f"Missing from marts: {len(result['missing'])}")
    print(f"data_key=0 violations: {len(result['data_key_zero'])}")

    if result['missing']:
        print("\nMISSING DATA_KEYS:")
        for m in result['missing'][:50]:
            print(f"  {m['mart']}: data_key={m['data_key']}")

    if result['data_key_zero']:
        print("\nDATA_KEY=0 VIOLATIONS:")
        for v in result['data_key_zero']:
            print(f"  {v['mart']}: {v['rows_with_zero']} rows")

    if result['pass']:
        print("\nRESULT: PASS")
        sys.exit(0)
    else:
        print("\nRESULT: FAIL")
        sys.exit(1)


def cmd_status(args: argparse.Namespace) -> None:
    """Check mart builder status."""
    setup_logging(settings.log_level)

    builder = MartBuilder(
        clickhouse_host=settings.clickhouse_host,
        clickhouse_port=settings.clickhouse_port,
        clickhouse_user=settings.clickhouse_user,
        clickhouse_password=settings.clickhouse_password,
        source=settings.realtime_marts_source,
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


def _add_backfill_perf_args(parser: argparse.ArgumentParser) -> None:
    """Add performance tuning arguments to a backfill parser."""
    parser.add_argument(
        "--batch-size", type=int, default=None,
        help=f"Data_keys per batch (default: {_DEFAULT_BATCH_SIZE}, env: TORQMIND_BACKFILL_BATCH_SIZE)",
    )
    parser.add_argument(
        "--max-threads", type=int, default=None,
        help=f"ClickHouse max_threads per query (default: {_DEFAULT_MAX_THREADS}, env: TORQMIND_BACKFILL_MAX_THREADS)",
    )
    parser.add_argument(
        "--max-memory-gb", type=float, default=None,
        help=f"ClickHouse max_memory_usage in GB (default: {_DEFAULT_MAX_MEMORY / 1e9:.0f}, env: TORQMIND_BACKFILL_MAX_MEMORY_GB)",
    )
    parser.add_argument(
        "--skip-batch-deletes", action="store_true", default=False,
        help="Skip DELETE mutations before INSERT (use after mart TRUNCATE/DROP-RECREATE)",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="TorqMind CDC Consumer CLI")
    subparsers = parser.add_subparsers(dest="command")

    # backfill (generic)
    bp = subparsers.add_parser("backfill", help="Backfill mart_rt from current data")
    bp.add_argument("--from-date", default="2025-01-01", help="Start date YYYY-MM-DD")
    bp.add_argument("--to-date", default=None, help="End date YYYY-MM-DD (optional)")
    bp.add_argument("--id-empresa", type=int, default=1, help="Tenant ID")
    bp.add_argument("--id-filial", type=int, default=None, help="Branch ID (optional, all if omitted)")
    bp.add_argument("--source", choices=("stg", "dw"), default=settings.realtime_marts_source, help="Realtime source")
    bp.add_argument("--mart-only", action="store_true", default=False,
                     help="Rebuild only mart_rt from existing slim tables (no payload/STG reads)")
    bp.add_argument("--skip-slim", action="store_true", default=False,
                     help="Alias for --mart-only")
    _add_backfill_perf_args(bp)

    # backfill-stg (STG-specific shortcut)
    stg_bp = subparsers.add_parser("backfill-stg", help="Backfill mart_rt from STG CDC current tables")
    stg_bp.add_argument("--from-date", default="2025-01-01", help="Start date YYYY-MM-DD")
    stg_bp.add_argument("--to-date", default=None, help="End date YYYY-MM-DD (optional)")
    stg_bp.add_argument("--id-empresa", type=int, default=1, help="Tenant ID")
    stg_bp.add_argument("--id-filial", type=int, default=None, help="Branch ID (optional, all if omitted)")
    stg_bp.add_argument("--mart-only", action="store_true", default=False,
                         help="Rebuild only mart_rt from existing slim tables (no payload/STG reads)")
    stg_bp.add_argument("--skip-slim", action="store_true", default=False,
                         help="Alias for --mart-only")
    _add_backfill_perf_args(stg_bp)
    stg_bp.set_defaults(source="stg")

    # status
    subparsers.add_parser("status", help="Show mart builder status")

    # validate
    vp = subparsers.add_parser("validate", help="Validate slim→mart completeness")
    vp.add_argument("--from-date", default="2025-01-01", help="Start date YYYY-MM-DD")
    vp.add_argument("--to-date", default=None, help="End date YYYY-MM-DD (optional)")
    vp.add_argument("--id-empresa", type=int, default=1, help="Tenant ID")

    args = parser.parse_args()
    if args.command in {"backfill", "backfill-stg"}:
        cmd_backfill(args)
    elif args.command == "status":
        cmd_status(args)
    elif args.command == "validate":
        cmd_validate(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
