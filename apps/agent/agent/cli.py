from __future__ import annotations

import argparse
from datetime import datetime
import logging
import json
from pathlib import Path

from agent.config import load_config
from agent.runner import AgentRunner
from agent.state.watermark import WatermarkStore
from agent.utils.log import build_logger


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m agent", description="TorqMind Extractor Agent")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARN/ERROR")

    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run extractor and send to TorqMind")
    run.add_argument("--config", dest="command_config", default=None, help="Path to config YAML")
    run.add_argument("--once", action="store_true", help="Execute one cycle and exit")
    run.add_argument("--loop", action="store_true", help="Run forever")
    run.add_argument("--interval", type=int, default=60, help="Loop interval in seconds")
    run.add_argument("--reset-watermark", default=None, help="Reset watermark for dataset before running")
    run.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue processing other datasets when one dataset fails",
    )

    backfill = sub.add_parser("backfill", help="Run backfill for one dataset")
    backfill.add_argument("--config", dest="command_config", default=None, help="Path to config YAML")
    backfill.add_argument("--dataset", required=True)
    backfill.add_argument("--from", dest="from_date", required=True, help="YYYY-MM-DD")
    backfill.add_argument("--to", dest="to_date", required=True, help="YYYY-MM-DD")

    check = sub.add_parser("check", help="Check SQL Server + API + ingest credentials")
    check.add_argument("--config", dest="command_config", default=None, help="Path to config YAML")

    reset = sub.add_parser("reset", help="Reset watermark for a dataset")
    reset.add_argument("--config", dest="command_config", default=None, help="Path to config YAML")
    reset.add_argument("--dataset", required=True)

    reset2 = sub.add_parser("reset-watermark", help="Reset watermark for a dataset")
    reset2.add_argument("--config", dest="command_config", default=None, help="Path to config YAML")
    reset2.add_argument("--dataset", required=True)

    scan = sub.add_parser("schema-scan", help="Scan SQL Server schema candidates for AR/AP datasets")
    scan.add_argument("--config", dest="command_config", default=None, help="Path to config YAML")
    scan.add_argument(
        "--keywords",
        default="PAGAR,RECEBER,TITULO,DUPLICATA,FINANC",
        help="Comma separated keywords",
    )
    scan.add_argument("--output", default="docs/xpert_schema_report.json", help="Output JSON report path")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    logger = build_logger(level=level)

    config_path = getattr(args, "command_config", None) or args.config
    cfg = load_config(config_path)
    runner = AgentRunner(cfg, logger)

    # One-shot migration of legacy state.json if present
    tenant_key = f"empresa_{cfg.api.empresa_id or cfg.id_empresa or 'unknown'}"
    store = WatermarkStore(root_dir=cfg.runtime.state_dir, tenant_key=tenant_key)
    migrated = WatermarkStore.migrate_legacy_state("state.json", store, scope=f"db:{cfg.id_db or 1}")
    if migrated:
        logger.info("phase=state_migration migrated=%s source=state.json", migrated)

    try:
        if args.command == "check":
            runner.check()
            return

        if args.command == "run":
            if args.reset_watermark:
                runner.reset_watermark(args.reset_watermark)
            if args.once or not args.loop:
                runner.run_once(continue_on_error=bool(args.continue_on_error))
                return
            runner.run_loop(interval_seconds=args.interval)
            return

        if args.command == "backfill":
            dt_from = _parse_date(args.from_date)
            dt_to = _parse_date(args.to_date)
            if dt_to < dt_from:
                raise ValueError("--to must be >= --from")
            runner.backfill(dataset=args.dataset, from_date=dt_from, to_date=dt_to)
            return

        if args.command in {"reset", "reset-watermark"}:
            runner.reset_watermark(args.dataset)
            return

        if args.command == "schema-scan":
            keywords = [k.strip() for k in str(args.keywords).split(",") if k.strip()]
            report = runner.schema_scan(keywords=keywords)
            out = Path(args.output)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("action=schema_scan result=ok output=%s tables=%s", str(out), len(report.get("candidates", [])))
            return
    except Exception as exc:  # noqa: PERF203
        logger.error("command=%s status=failed error=%s", args.command, str(exc))
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
