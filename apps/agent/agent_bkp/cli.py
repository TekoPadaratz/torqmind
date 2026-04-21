from __future__ import annotations

import argparse
from datetime import datetime
import getpass
import json
import logging
from pathlib import Path
from typing import Any, Dict

from agent_bkp.config import (
    AgentConfigError,
    build_default_raw_config,
    derive_encrypted_config_path,
    load_config,
    load_raw_config,
    migrate_yaml_to_encrypted_config,
    save_encrypted_config,
)
from agent_bkp.utils.log import build_logger


SAFE_MASK_KEYS = {"password", "ingest_key", "token", "secret"}


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def _mask_value(key: str, value: Any) -> str:
    if value in {None, ""}:
        return ""
    if any(token in key.lower() for token in SAFE_MASK_KEYS):
        return "********"
    return str(value)


def _set_nested(raw: Dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = [p for p in dotted_key.split(".") if p]
    if not parts:
        raise AgentConfigError("Config key must not be empty.")

    cursor = raw
    for part in parts[:-1]:
        next_value = cursor.get(part)
        if not isinstance(next_value, dict):
            next_value = {}
            cursor[part] = next_value
        cursor = next_value
    cursor[parts[-1]] = value


def _get_nested(raw: Dict[str, Any], dotted_key: str, default: Any = None) -> Any:
    cursor: Any = raw
    for part in dotted_key.split("."):
        if not isinstance(cursor, dict) or part not in cursor:
            return default
        cursor = cursor[part]
    return cursor


def _parse_dynamic_values(items: list[str]) -> Dict[str, Any]:
    parsed: Dict[str, Any] = {}
    for item in items:
        if "=" not in str(item):
            raise AgentConfigError(f"Invalid --set value `{item}`. Use key=value.")
        key, value = str(item).split("=", 1)
        key = key.strip()
        if not key:
            raise AgentConfigError("Config key must not be empty.")
        parsed[key] = value
    return parsed


def _coerce_known_fields(values: Dict[str, Any]) -> Dict[str, Any]:
    coerced = dict(values)
    int_fields = {
        "sqlserver.port",
        "sqlserver.login_timeout_seconds",
        "runtime.batch_size",
        "runtime.fetch_size",
        "runtime.max_retries",
        "runtime.timeout_seconds",
        "runtime.spool_flush_max_files",
        "runtime.interval_seconds",
        "id_empresa",
        "id_db",
        "api.empresa_id",
    }
    bool_fields = {
        "sqlserver.encrypt",
        "sqlserver.trust_server_certificate",
        "runtime.gzip_enabled",
    }
    for key, value in list(coerced.items()):
        if value in {None, ""}:
            continue
        if key in int_fields:
            coerced[key] = int(value)
        elif key in bool_fields:
            coerced[key] = str(value).strip().lower() in {"1", "true", "t", "yes", "y", "on"}
    return coerced


def _load_editable_raw(config_path: str) -> Dict[str, Any]:
    path = Path(config_path)
    if path.exists():
        raw, _ = load_raw_config(path)
        return raw
    return build_default_raw_config()


def _apply_updates(raw: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    merged = json.loads(json.dumps(raw))
    for key, value in _coerce_known_fields(updates).items():
        _set_nested(merged, key, value)
    return merged


def _prompt(field_label: str, current: Any = None, *, secret: bool = False, required: bool = False) -> str:
    suffix = ""
    if current not in {None, ""}:
        suffix = f" [{_mask_value(field_label, current) if not secret else '********'}]"
    while True:
        value = getpass.getpass(f"{field_label}{suffix}: ") if secret else input(f"{field_label}{suffix}: ")
        value = value.strip()
        if value:
            return value
        if current not in {None, ""}:
            return str(current)
        if not required:
            return ""


def _interactive_config_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    updates = {
        "api.base_url": _prompt("API base_url", _get_nested(raw, "api.base_url"), required=True),
        "api.ingest_key": _prompt("API ingest_key", _get_nested(raw, "api.ingest_key"), secret=True, required=True),
        "sqlserver.server": _prompt("SQL host", _get_nested(raw, "sqlserver.server"), required=True),
        "sqlserver.port": _prompt("SQL port", _get_nested(raw, "sqlserver.port", 1433), required=True),
        "sqlserver.database": _prompt("SQL database", _get_nested(raw, "sqlserver.database"), required=True),
        "sqlserver.user": _prompt("SQL username", _get_nested(raw, "sqlserver.user"), required=True),
        "sqlserver.password": _prompt("SQL password", _get_nested(raw, "sqlserver.password"), secret=True, required=True),
        "runtime.interval_seconds": _prompt(
            "Intervalo de sync (segundos)",
            _get_nested(raw, "runtime.interval_seconds", 60),
            required=True,
        ),
    }
    return _apply_updates(raw, updates)


def _collect_config_updates(args) -> Dict[str, Any]:
    updates = _parse_dynamic_values(list(args.set_values or []))
    direct_map = {
        "api.base_url": args.api_base_url,
        "api.ingest_key": args.ingest_key,
        "sqlserver.server": args.sql_host,
        "sqlserver.port": args.sql_port,
        "sqlserver.database": args.sql_database,
        "sqlserver.user": args.sql_username,
        "sqlserver.password": args.sql_password,
        "runtime.interval_seconds": args.interval_seconds,
    }
    for key, value in direct_map.items():
        if value is not None:
            updates[key] = value
    return updates


def _show_safe_config(raw: Dict[str, Any], logger) -> None:
    lines = [
        ("sqlserver.host", _get_nested(raw, "sqlserver.server")),
        ("sqlserver.port", _get_nested(raw, "sqlserver.port")),
        ("sqlserver.database", _get_nested(raw, "sqlserver.database")),
        ("sqlserver.username", _get_nested(raw, "sqlserver.user")),
        ("sqlserver.password", _get_nested(raw, "sqlserver.password")),
        ("api.base_url", _get_nested(raw, "api.base_url")),
        ("api.ingest_key", _get_nested(raw, "api.ingest_key")),
        ("runtime.interval_seconds", _get_nested(raw, "runtime.interval_seconds")),
    ]
    for key, value in lines:
        logger.info("%s=%s", key, _mask_value(key, value))


def _handle_config_command(args, logger) -> None:
    config_path = getattr(args, "command_config", None) or args.config
    if not str(config_path).lower().endswith(".enc"):
        config_path = str(derive_encrypted_config_path(config_path))

    if args.config_command == "migrate-from-yaml":
        source = args.source or "config.local.yaml"
        result = migrate_yaml_to_encrypted_config(source, encrypted_path=config_path)
        logger.info("command=config action=migrate status=ok source=%s target=%s", result["source"], result["target"])
        return

    if args.config_command == "show-safe":
        raw, _ = load_raw_config(config_path)
        _show_safe_config(raw, logger)
        return

    if args.config_command == "test":
        from agent_bkp.runner import AgentRunner
        from agent_bkp.state.watermark import WatermarkStore

        cfg = load_config(config_path)
        runner = AgentRunner(cfg, logger)
        tenant_key = f"empresa_{cfg.api.empresa_id or cfg.id_empresa or 'unknown'}"
        store = WatermarkStore(root_dir=cfg.runtime.state_dir, tenant_key=tenant_key)
        WatermarkStore.migrate_legacy_state("state.json", store, scope=f"db:{cfg.id_db or 1}")
        runner.check()
        logger.info("command=config action=test status=ok config=%s", config_path)
        return

    raw = _load_editable_raw(config_path)
    if args.config_command in {"init", "edit"} and args.interactive:
        final_raw = _interactive_config_payload(raw)
    else:
        updates = _collect_config_updates(args)
        if args.config_command == "init" and not updates and not args.interactive:
            raise AgentConfigError("Provide configuration fields or use --interactive.")
        if args.config_command == "set" and not updates and not args.interactive:
            raise AgentConfigError("Provide at least one field to update.")
        final_raw = _apply_updates(raw, updates)

    save_encrypted_config(config_path, final_raw)
    logger.info("command=config action=%s status=ok config=%s", args.config_command, config_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m agent", description="TorqMind Extractor Agent")
    parser.add_argument("--config", default="config.local.yaml", help="Path to config file (.yaml or .enc)")
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARN/ERROR")

    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run extractor and send to TorqMind")
    run.add_argument("--config", dest="command_config", default=None, help="Path to config file")
    run.add_argument("--once", action="store_true", help="Execute one cycle and exit")
    run.add_argument("--loop", action="store_true", help="Run forever")
    run.add_argument("--interval", type=int, default=60, help="Loop interval in seconds")
    run.add_argument("--reset-watermark", default=None, help="Reset watermark for dataset before running")
    run.add_argument("--continue-on-error", action="store_true", help="Continue processing other datasets when one fails")

    backfill = sub.add_parser("backfill", help="Run backfill for one dataset")
    backfill.add_argument("--config", dest="command_config", default=None, help="Path to config file")
    backfill.add_argument("--dataset", required=True)
    backfill.add_argument("--from", dest="from_date", required=True, help="YYYY-MM-DD")
    backfill.add_argument("--to", dest="to_date", required=True, help="YYYY-MM-DD")

    check = sub.add_parser("check", help="Check SQL Server + API + ingest credentials")
    check.add_argument("--config", dest="command_config", default=None, help="Path to config file")

    test_config = sub.add_parser("test-config", help="Validate config, SQL Server and API")
    test_config.add_argument("--config", dest="command_config", default=None, help="Path to config file")

    reset = sub.add_parser("reset", help="Reset watermark for a dataset")
    reset.add_argument("--config", dest="command_config", default=None, help="Path to config file")
    reset.add_argument("--dataset", required=True)

    reset2 = sub.add_parser("reset-watermark", help="Reset watermark for a dataset")
    reset2.add_argument("--config", dest="command_config", default=None, help="Path to config file")
    reset2.add_argument("--dataset", required=True)

    scan = sub.add_parser("schema-scan", help="Scan SQL Server schema candidates for AR/AP datasets")
    scan.add_argument("--config", dest="command_config", default=None, help="Path to config file")
    scan.add_argument("--keywords", default="PAGAR,RECEBER,TITULO,DUPLICATA,FINANC", help="Comma separated keywords")
    scan.add_argument("--output", default="docs/xpert_schema_report.json", help="Output JSON report path")

    config_cmd = sub.add_parser("config", help="Manage encrypted config.enc")
    config_sub = config_cmd.add_subparsers(dest="config_command", required=True)
    for name, help_text in (
        ("init", "Create config.enc"),
        ("set", "Update one or more config fields"),
        ("edit", "Interactively edit config.enc"),
    ):
        current = config_sub.add_parser(name, help=help_text)
        current.add_argument("--config", dest="command_config", default=None, help="Path to config.enc")
        current.add_argument("--interactive", action="store_true", help="Prompt for fields interactively")
        current.add_argument("--api-base-url", default=None)
        current.add_argument("--ingest-key", default=None)
        current.add_argument("--sql-host", default=None)
        current.add_argument("--sql-port", default=None)
        current.add_argument("--sql-database", default=None)
        current.add_argument("--sql-username", default=None)
        current.add_argument("--sql-password", default=None)
        current.add_argument("--interval-seconds", default=None)
        current.add_argument("--set", dest="set_values", action="append", default=[], help="Additional key=value update")

    show_safe = config_sub.add_parser("show-safe", help="Show safe masked config summary")
    show_safe.add_argument("--config", dest="command_config", default=None, help="Path to config.enc")

    cfg_test = config_sub.add_parser("test", help="Validate config.enc, SQL Server and API")
    cfg_test.add_argument("--config", dest="command_config", default=None, help="Path to config.enc")

    migrate = config_sub.add_parser("migrate-from-yaml", help="Migrate plaintext YAML into config.enc")
    migrate.add_argument("--config", dest="command_config", default=None, help="Target config.enc path")
    migrate.add_argument("--source", default="config.local.yaml", help="Source YAML path")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    level = getattr(logging, str(args.log_level).upper(), logging.INFO)
    logger = build_logger(level=level)

    config_path = getattr(args, "command_config", None) or args.config
    if args.command == "config":
        try:
            _handle_config_command(args, logger)
            return
        except Exception as exc:  # noqa: PERF203
            logger.error("command=config status=failed error=%s", str(exc))
            raise SystemExit(2) from exc

    from agent_bkp.runner import AgentRunner
    from agent_bkp.state.watermark import WatermarkStore

    cfg = load_config(config_path)
    runner = AgentRunner(cfg, logger)
    tenant_key = f"empresa_{cfg.api.empresa_id or cfg.id_empresa or 'unknown'}"
    store = WatermarkStore(root_dir=cfg.runtime.state_dir, tenant_key=tenant_key)
    migrated = WatermarkStore.migrate_legacy_state("state.json", store, scope=f"db:{cfg.id_db or 1}")
    if migrated:
        logger.info("phase=state_migration migrated=%s source=state.json", migrated)

    try:
        if args.command in {"check", "test-config"}:
            runner.check()
            if args.command == "test-config":
                logger.info("command=test-config status=ok")
            return

        if args.command == "run":
            if args.reset_watermark:
                runner.reset_watermark(args.reset_watermark)
            if args.once or not args.loop:
                runner.run_once(continue_on_error=bool(args.continue_on_error))
                return
            interval_seconds = cfg.runtime.interval_seconds
            if args.interval != 60:
                interval_seconds = args.interval
            runner.run_loop(interval_seconds=interval_seconds)
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
