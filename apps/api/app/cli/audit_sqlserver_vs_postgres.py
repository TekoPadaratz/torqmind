from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Any

from app.cross_db_audit import parse_branch_ids, parse_date, resolve_audit_config, run_cross_db_audit


def _json_ready(value: Any) -> Any:
    if isinstance(value, (date,)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audita SQL Server/Xpert ↔ PostgreSQL (STG/DW/MART) em modo read-only."
    )
    parser.add_argument("--tenant-id", type=int, default=None, help="Tenant alvo. Fallback: AUDIT_TENANT_ID.")
    parser.add_argument(
        "--branch-ids",
        default=None,
        help="Lista CSV de filiais. Fallback: AUDIT_BRANCH_IDS. Se omitido, usa todas as filiais ativas do tenant.",
    )
    parser.add_argument("--date-start", default=None, help="Data inicial YYYY-MM-DD. Fallback: AUDIT_DATE_START.")
    parser.add_argument("--date-end", default=None, help="Data final YYYY-MM-DD. Fallback: AUDIT_DATE_END.")
    parser.add_argument("--sample-days", type=int, default=None, help="Quantidade de dias amostrados.")
    parser.add_argument("--focused-day", default=None, help="Dia específico YYYY-MM-DD para drilldown prioritário.")
    parser.add_argument("--output-dir", default=None, help="Diretório dos artefatos.")
    parser.add_argument("--pg-dsn", default=None, help="DSN read-only do PostgreSQL. Fallback: AUDIT_PG_DSN.")
    parser.add_argument(
        "--sqlserver-dsn",
        default=None,
        help="DSN read-only do SQL Server. Fallback: AUDIT_SQLSERVER_DSN ou apps/agent/config.local.yaml.",
    )
    parser.add_argument(
        "--agent-config",
        default=None,
        help="Caminho alternativo do config YAML do Agent para fallback de SQL Server/tabelas.",
    )
    args = parser.parse_args()

    config = resolve_audit_config(
        tenant_id=args.tenant_id,
        branch_ids=parse_branch_ids(args.branch_ids),
        date_start=parse_date(args.date_start),
        date_end=parse_date(args.date_end),
        sample_days=args.sample_days,
        output_dir=args.output_dir,
        pg_dsn=args.pg_dsn,
        sqlserver_dsn=args.sqlserver_dsn,
        focused_day=parse_date(args.focused_day),
        agent_config_path=args.agent_config,
    )
    result = run_cross_db_audit(config)
    summary = {
        "scope": result.get("effective_config"),
        "findings": result.get("findings"),
        "hypotheses": result.get("hypotheses"),
        "artifacts": result.get("artifacts"),
        "query_errors": result.get("query_errors"),
    }
    print(json.dumps(_json_ready(summary), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
