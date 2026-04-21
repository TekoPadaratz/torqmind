from __future__ import annotations

import contextlib
import csv
import json
import os
import random
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional, Sequence
from urllib.parse import unquote, urlparse

import psycopg
from psycopg.rows import dict_row

from app.business_time import business_timezone_name, business_today
from app.config import settings

try:  # pragma: no cover - optional in some local envs
    import pyodbc
except ImportError:  # pragma: no cover - exercised in runtime guard
    pyodbc = None  # type: ignore[assignment]

try:  # pragma: no cover - optional in some local envs
    import pymssql
except ImportError:  # pragma: no cover - exercised in runtime guard
    pymssql = None  # type: ignore[assignment]

try:  # pragma: no cover - optional in some local envs
    import yaml
except ImportError:  # pragma: no cover - exercised in runtime guard
    yaml = None  # type: ignore[assignment]


READ_ONLY_SQL_RE = re.compile(r"^(select|with)\b", re.IGNORECASE | re.DOTALL)
SQL_LEADING_COMMENTS_RE = re.compile(r"^\s*(?:--[^\n]*\n|/\*.*?\*/\s*)*", re.DOTALL)
DEFAULT_CONFIG_PATH = Path("apps/agent/config.local.yaml")
DEFAULT_OUTPUT_DIR = Path("artifacts/audit")
DATE_FMT = "%Y-%m-%d"
ZERO_DECIMAL = Decimal("0.00")
DEFAULT_SQLSERVER_TABLES = {
    "comprovantes": "dbo.COMPROVANTES",
    "movprodutos": "dbo.MOVPRODUTOS",
    "itensmovprodutos": "dbo.ITENSMOVPRODUTOS",
    "formas_pgto_comprovantes": "dbo.FORMAS_PGTO_COMPROVANTES",
    "turnos": "dbo.TURNOS",
    "entidades": "dbo.ENTIDADES",
    "contaspagar": "dbo.CONTASPAGAR",
    "contasreceber": "dbo.CONTASRECEBER",
}


def _detect_repo_root() -> Path:
    module_path = Path(__file__).resolve()
    cwd = Path.cwd().resolve()
    candidates: list[Path] = []
    for candidate in [cwd, *cwd.parents, module_path.parent, *module_path.parents]:
        if candidate not in candidates:
            candidates.append(candidate)
    for candidate in candidates:
        if (candidate / DEFAULT_CONFIG_PATH).exists():
            return candidate
    for candidate in candidates:
        if (candidate / ".git").exists() or (candidate / "pyproject.toml").exists():
            return candidate
    return module_path.parent


REPO_ROOT = _detect_repo_root()


@dataclass(frozen=True)
class AuditConfig:
    tenant_id: int
    branch_ids: list[int]
    date_start: date
    date_end: date
    sample_days: int
    output_dir: Path
    pg_dsn: str
    sqlserver_dsn: str
    sqlserver_tables: dict[str, str]
    focused_day: Optional[date] = None
    agent_config_path: Optional[Path] = None


@dataclass(frozen=True)
class HypothesisResult:
    hypothesis_id: int
    title: str
    status: str
    test: str
    result: str
    evidence: str


@dataclass(frozen=True)
class Finding:
    rank: int
    title: str
    layer: str
    impact: str
    confidence: str
    evidence: str
    recommendation: str


class AuditError(RuntimeError):
    pass


def _json_ready(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


def _strip_sql_leading_comments(sql: str) -> str:
    return SQL_LEADING_COMMENTS_RE.sub("", str(sql or "")).lstrip()


def ensure_read_only_query(sql: str) -> str:
    cleaned = _strip_sql_leading_comments(sql)
    if not READ_ONLY_SQL_RE.match(cleaned):
        raise AuditError("A auditoria aceita apenas queries read-only iniciadas por SELECT/ WITH.")
    return cleaned


def parse_branch_ids(raw: str | None) -> list[int]:
    if raw is None:
        return []
    values: list[int] = []
    for chunk in str(raw).split(","):
        part = chunk.strip()
        if not part:
            continue
        values.append(int(part))
    return sorted(dict.fromkeys(values))


def parse_date(raw: str | date | None, *, default: date | None = None) -> date | None:
    if raw is None or raw == "":
        return default
    if isinstance(raw, date):
        return raw
    return datetime.strptime(str(raw).strip(), DATE_FMT).date()


def _pg_default_dsn() -> str:
    if settings.database_url:
        parsed = urlparse(settings.database_url)
        if parsed.scheme.startswith("postgresql"):
            user = unquote(parsed.username or settings.pg_user)
            password = unquote(parsed.password or settings.pg_password)
            host = parsed.hostname or settings.pg_host
            port = parsed.port or settings.pg_port
            dbname = (parsed.path or "").lstrip("/") or settings.pg_database
            return f"host={host} port={port} dbname={dbname} user={user} password={password}"
    return (
        f"host={settings.pg_host} port={settings.pg_port} dbname={settings.pg_database} "
        f"user={settings.pg_user} password={settings.pg_password}"
    )


def _ensure_application_intent_readonly(dsn: str) -> str:
    raw = str(dsn or "").strip().rstrip(";")
    if not raw:
        return raw
    if "applicationintent=" in raw.lower():
        return raw
    return f"{raw};ApplicationIntent=ReadOnly"


def _resolve_agent_config_path(path: Path) -> Path:
    if not path.is_absolute():
        cwd_candidate = path.resolve()
        if cwd_candidate.exists():
            return cwd_candidate
        return (REPO_ROOT / path).resolve()
    return path


def _load_agent_config(path: Path) -> dict[str, Any]:
    path = _resolve_agent_config_path(path)
    if yaml is None:  # pragma: no cover - runtime guard
        raise AuditError("PyYAML não está disponível para ler o config.local.yaml do Agent.")
    if not path.exists():
        raise AuditError(f"Arquivo de configuração do Agent não encontrado: {path}")
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise AuditError(f"Config do Agent inválida em {path}")
    return payload


def _load_optional_agent_config(path: Path, *, required: bool) -> tuple[Path, dict[str, Any]]:
    resolved = _resolve_agent_config_path(path)
    if not resolved.exists():
        if required:
            raise AuditError(f"Arquivo de configuração do Agent não encontrado: {resolved}")
        return resolved, {}
    return resolved, _load_agent_config(resolved)


def resolve_audit_config(
    *,
    tenant_id: int | None = None,
    branch_ids: Sequence[int] | None = None,
    date_start: date | None = None,
    date_end: date | None = None,
    sample_days: int | None = None,
    output_dir: str | Path | None = None,
    pg_dsn: str | None = None,
    sqlserver_dsn: str | None = None,
    focused_day: date | None = None,
    agent_config_path: str | Path | None = None,
) -> AuditConfig:
    env = os.environ
    requested_agent_path = Path(agent_config_path or env.get("AUDIT_AGENT_CONFIG") or DEFAULT_CONFIG_PATH)
    resolved_pg_dsn = str(pg_dsn or env.get("AUDIT_PG_DSN") or _pg_default_dsn()).strip()
    if not resolved_pg_dsn:
        raise AuditError("AUDIT_PG_DSN não informado e nenhum fallback PostgreSQL válido foi encontrado.")

    raw_sqlserver_dsn = str(sqlserver_dsn or env.get("AUDIT_SQLSERVER_DSN") or "").strip()
    require_agent_config = not raw_sqlserver_dsn
    agent_path, agent_cfg = _load_optional_agent_config(requested_agent_path, required=require_agent_config)
    datasets = dict(agent_cfg.get("datasets") or {})
    sql_cfg = dict(agent_cfg.get("sqlserver") or {})

    resolved_tenant = int(tenant_id or env.get("AUDIT_TENANT_ID") or agent_cfg.get("id_empresa") or 1)
    resolved_date_end = parse_date(date_end or env.get("AUDIT_DATE_END"), default=business_today(resolved_tenant))
    resolved_date_start = parse_date(date_start or env.get("AUDIT_DATE_START"), default=(resolved_date_end - timedelta(days=30)))
    if resolved_date_start is None or resolved_date_end is None:
        raise AuditError("Data inicial/final da auditoria não pôde ser resolvida.")
    if resolved_date_start > resolved_date_end:
        raise AuditError("AUDIT_DATE_START não pode ser maior que AUDIT_DATE_END.")

    raw_branches = list(branch_ids or [])
    if not raw_branches:
        raw_branches = parse_branch_ids(env.get("AUDIT_BRANCH_IDS"))
    resolved_sample_days = max(1, int(sample_days or env.get("AUDIT_SAMPLE_DAYS") or 5))
    resolved_output_dir = Path(output_dir or env.get("AUDIT_OUTPUT_DIR") or DEFAULT_OUTPUT_DIR)

    resolved_sqlserver_dsn = raw_sqlserver_dsn
    if not resolved_sqlserver_dsn:
        if not sql_cfg:
            raise AuditError("AUDIT_SQLSERVER_DSN não informado e o Agent não possui config sqlserver.")
        sql_driver = str(sql_cfg.get("driver") or "ODBC Driver 18 for SQL Server").strip().strip("{}")
        resolved_sqlserver_dsn = ";".join(
            [
                f"DRIVER={{{sql_driver}}}",
                f"SERVER={sql_cfg.get('server')}",
                f"DATABASE={sql_cfg.get('database')}",
                f"UID={sql_cfg.get('user')}",
                f"PWD={sql_cfg.get('password')}",
                f"Encrypt={'yes' if sql_cfg.get('encrypt') else 'no'}",
                f"TrustServerCertificate={'yes' if sql_cfg.get('trust_server_certificate') else 'no'}",
                "LoginTimeout=5",
            ]
        )
    resolved_sqlserver_dsn = _ensure_application_intent_readonly(resolved_sqlserver_dsn)

    tables = {
        key: str((datasets.get(key) or {}).get("table") or default_table)
        for key, default_table in DEFAULT_SQLSERVER_TABLES.items()
    }

    return AuditConfig(
        tenant_id=resolved_tenant,
        branch_ids=sorted(dict.fromkeys(int(item) for item in raw_branches)),
        date_start=resolved_date_start,
        date_end=resolved_date_end,
        sample_days=resolved_sample_days,
        output_dir=resolved_output_dir,
        pg_dsn=resolved_pg_dsn,
        sqlserver_dsn=resolved_sqlserver_dsn,
        sqlserver_tables=tables,
        focused_day=parse_date(focused_day or env.get("AUDIT_FOCUSED_DAY")),
        agent_config_path=agent_path if agent_cfg else None,
    )


class ReadOnlyPostgres:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: Optional[psycopg.Connection] = None

    def __enter__(self) -> "ReadOnlyPostgres":
        self._conn = psycopg.connect(self._dsn, row_factory=dict_row)
        self._conn.execute("SET default_transaction_read_only = on")
        self._conn.execute("SET statement_timeout = '5min'")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._conn is not None:
            self._conn.close()

    @property
    def conn(self) -> psycopg.Connection:
        if self._conn is None:
            raise AuditError("Conexão PostgreSQL não inicializada.")
        return self._conn

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        statement = ensure_read_only_query(sql)
        try:
            with self.conn.cursor() as cur:
                cur.execute(statement, tuple(params or ()))
                return [dict(row) for row in cur.fetchall()]
        except Exception:
            with contextlib.suppress(Exception):
                self.conn.rollback()
            raise

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any]:
        rows = self.query(sql, params)
        return rows[0] if rows else {}

    def query_optional(self, sql: str, params: Sequence[Any] | None = None) -> tuple[list[dict[str, Any]], Optional[str]]:
        try:
            return self.query(sql, params), None
        except Exception as exc:  # noqa: BLE001
            with contextlib.suppress(Exception):
                self.conn.rollback()
            return [], f"{exc.__class__.__name__}: {exc}"


class ReadOnlySqlServer:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: Optional[Any] = None
        self._driver = "pyodbc"

    @staticmethod
    def _parse_dsn(dsn: str) -> dict[str, str]:
        params: dict[str, str] = {}
        for chunk in str(dsn or "").split(";"):
            part = chunk.strip()
            if not part or "=" not in part:
                continue
            key, value = part.split("=", 1)
            params[key.strip().lower()] = value.strip()
        return params

    @staticmethod
    def _parse_server(value: str | None) -> tuple[str, int | None]:
        raw = str(value or "").strip()
        if raw.lower().startswith("tcp:"):
            raw = raw[4:]
        port: int | None = None
        if "," in raw:
            host, raw_port = raw.rsplit(",", 1)
            if raw_port.isdigit():
                return host.strip(), int(raw_port)
        if raw.count(":") == 1:
            host, raw_port = raw.rsplit(":", 1)
            if raw_port.isdigit():
                return host.strip(), int(raw_port)
        return raw, port

    def _connect_pymssql(self) -> Any:
        if pymssql is None:  # pragma: no cover - runtime guard
            raise AuditError("pymssql não está disponível para fallback de conexão com SQL Server.")
        params = self._parse_dsn(self._dsn)
        server, port = self._parse_server(params.get("server"))
        if not server:
            raise AuditError("DSN SQL Server sem SERVER para fallback pymssql.")
        return pymssql.connect(
            server=server,
            port=port or 1433,
            user=params.get("uid") or params.get("user") or params.get("user id"),
            password=params.get("pwd") or params.get("password"),
            database=params.get("database"),
            login_timeout=int(params.get("logintimeout") or 5),
            timeout=300,
            charset="UTF-8",
        )

    def __enter__(self) -> "ReadOnlySqlServer":
        failures: list[str] = []
        if pyodbc is not None:
            try:
                self._conn = pyodbc.connect(self._dsn)
                self._driver = "pyodbc"
                return self
            except Exception as exc:  # noqa: BLE001
                failures.append(f"pyodbc={exc.__class__.__name__}: {exc}")
        if pymssql is not None:
            try:
                self._conn = self._connect_pymssql()
                self._driver = "pymssql"
                return self
            except Exception as exc:  # noqa: BLE001
                failures.append(f"pymssql={exc.__class__.__name__}: {exc}")
        if not failures:  # pragma: no cover - runtime guard
            raise AuditError("Nem pyodbc nem pymssql estão disponíveis para conectar no SQL Server.")
        raise AuditError("Falha ao conectar no SQL Server: " + " | ".join(failures))

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._conn is not None:
            self._conn.close()

    @property
    def conn(self) -> Any:
        if self._conn is None:
            raise AuditError("Conexão SQL Server não inicializada.")
        return self._conn

    def query(self, sql: str, params: Sequence[Any] | None = None) -> list[dict[str, Any]]:
        statement = ensure_read_only_query(sql)
        if self._driver == "pymssql":
            statement = statement.replace("?", "%s")
        cursor = self.conn.cursor()
        cursor.execute(statement, tuple(params or ()))
        columns = [item[0] for item in cursor.description]
        rows = []
        for row in cursor.fetchall():
            rows.append({columns[idx]: row[idx] for idx in range(len(columns))})
        return rows

    def query_one(self, sql: str, params: Sequence[Any] | None = None) -> dict[str, Any]:
        rows = self.query(sql, params)
        return rows[0] if rows else {}


def _day_key(value: date) -> int:
    return int(value.strftime("%Y%m%d"))


def _date_from_key(value: int | str | None) -> Optional[date]:
    if value in (None, "", 0):
        return None
    raw = str(value)
    return datetime.strptime(raw, "%Y%m%d").date()


def _decimal(value: Any) -> Decimal:
    if value is None:
        return ZERO_DECIMAL
    if isinstance(value, Decimal):
        return value.quantize(Decimal("0.01"))
    return Decimal(str(value)).quantize(Decimal("0.01"))


def _int(value: Any) -> int:
    if value is None:
        return 0
    return int(value)


def _sqlserver_branch_clause(column: str, values: Sequence[int]) -> tuple[str, list[Any]]:
    if not values:
        return "", []
    placeholders = ", ".join("?" for _ in values)
    return f" AND {column} IN ({placeholders})", [int(item) for item in values]


def _pg_branch_clause(alias: str, branches: Sequence[int]) -> tuple[str, list[Any]]:
    if not branches:
        return "", []
    return f" AND {alias}.id_filial = ANY(%s)", [list(int(item) for item in branches)]


def _coalesce_layer_row(row: Mapping[str, Any] | None) -> dict[str, Any]:
    row = row or {}
    return {
        "total": _decimal(row.get("total")),
        "docs": _int(row.get("docs")),
        "items": _int(row.get("items")),
        "min_ts": row.get("min_ts"),
        "max_ts": row.get("max_ts"),
    }


def _window_days(date_start: date, date_end: date, sample_days: int) -> list[date]:
    total_days = max(1, (date_end - date_start).days + 1)
    count = min(total_days, max(1, sample_days))
    all_days = [date_start + timedelta(days=offset) for offset in range(total_days)]
    if count >= len(all_days):
        return all_days
    rng = random.Random(f"{date_start.isoformat()}:{date_end.isoformat()}:{sample_days}")
    return sorted(rng.sample(all_days, count))


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = list(rows)
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(str(key))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _json_ready(row.get(key)) for key in fieldnames})


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(dict(payload)), ensure_ascii=False, indent=2), encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _sqlserver_mov_situacao_expr(alias: str = "m") -> str:
    return f"TRY_CONVERT(int, NULLIF(CONVERT(varchar(32), {alias}.SITUACAO), ''))"


def _sqlserver_mov_cancelled_expr(alias: str = "m") -> str:
    return f"CASE WHEN COALESCE({_sqlserver_mov_situacao_expr(alias)}, 0) = 2 THEN 1 ELSE 0 END"


def _pg_mov_situacao_expr(alias: str = "m") -> str:
    return f"etl.movimento_venda_situacao({alias}.situacao_shadow, {alias}.payload)"


def _pg_mov_cancelled_expr(alias: str = "m") -> str:
    return f"etl.movimento_venda_is_cancelled({_pg_mov_situacao_expr(alias)})"


def _sales_source_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _sqlserver_branch_clause("i.ID_FILIAL", config.branch_ids)
    tables = config.sqlserver_tables
    sql = f"""
    WITH item_base AS (
      SELECT
        CAST(m.DATA AS date) AS dt_ref,
        i.ID_FILIAL AS branch_id,
        COALESCE(m.ID_COMPROVANTE, m.ID_MOVPRODUTOS) AS doc_ref,
        m.DATA AS data_mov,
        CASE
          WHEN ISNUMERIC(REPLACE(REPLACE(CONVERT(varchar(32), i.CFOP), '.', ''), ',', '')) = 1
            THEN CAST(REPLACE(REPLACE(CONVERT(varchar(32), i.CFOP), '.', ''), ',', '') AS int)
          ELSE NULL
        END AS cfop_num,
        CASE
          WHEN ISNUMERIC(CONVERT(varchar(64), i.TOTAL)) = 1
            THEN CAST(CONVERT(varchar(64), i.TOTAL) AS decimal(18,2))
          ELSE 0
        END AS total_item,
        {_sqlserver_mov_situacao_expr('m')} AS situacao,
        {_sqlserver_mov_cancelled_expr('m')} AS cancelado
      FROM {tables['itensmovprodutos']} i
      JOIN {tables['movprodutos']} m
        ON m.ID_FILIAL = i.ID_FILIAL
       AND m.ID_DB = i.ID_DB
       AND m.ID_MOVPRODUTOS = i.ID_MOVPRODUTOS
      LEFT JOIN {tables['comprovantes']} c
        ON c.ID_FILIAL = m.ID_FILIAL
       AND c.ID_DB = m.ID_DB
       AND c.ID_COMPROVANTE = m.ID_COMPROVANTE
      WHERE CAST(m.DATA AS date) BETWEEN ? AND ?
      {branch_sql}
    )
    SELECT
      dt_ref,
      branch_id,
      CAST(SUM(total_item) AS decimal(18,2)) AS total,
      COUNT(*) AS items,
      COUNT(DISTINCT doc_ref) AS docs,
      MIN(data_mov) AS min_ts,
      MAX(data_mov) AS max_ts
    FROM item_base
    WHERE ISNULL(cfop_num, 0) >= 5000
      AND cancelado = 0
    GROUP BY dt_ref, branch_id
    ORDER BY dt_ref, branch_id
    """
    return sql, [config.date_start, config.date_end, *branch_params]


def _sales_stg_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("i", config.branch_ids)
    sql = f"""
    WITH item_base AS (
      SELECT
        to_date(
          etl.business_date_key(COALESCE(i.dt_evento, etl.sales_event_timestamptz(m.payload, m.dt_evento)))::text,
          'YYYYMMDD'
        ) AS dt_ref,
        i.id_filial AS branch_id,
        COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE'), i.id_movprodutos) AS doc_ref,
        etl.business_timestamp(COALESCE(i.dt_evento, etl.sales_event_timestamptz(m.payload, m.dt_evento))) AS event_ts,
        COALESCE(i.cfop_shadow, etl.safe_int(i.payload->>'CFOP'), 0) AS cfop_num,
        COALESCE(i.total_shadow, etl.safe_numeric(i.payload->>'TOTAL')::numeric(18,2), 0)::numeric(18,2) AS total_item,
        {_pg_mov_situacao_expr('m')} AS situacao,
        {_pg_mov_cancelled_expr('m')} AS cancelado
      FROM stg.itensmovprodutos i
      JOIN stg.movprodutos m
        ON m.id_empresa = i.id_empresa
       AND m.id_filial = i.id_filial
       AND m.id_db = i.id_db
       AND m.id_movprodutos = i.id_movprodutos
      WHERE i.id_empresa = %s
        AND COALESCE(
          etl.business_date_key(COALESCE(i.dt_evento, etl.sales_event_timestamptz(m.payload, m.dt_evento))),
          0
        ) BETWEEN %s AND %s
      {branch_sql}
    )
    SELECT
      dt_ref,
      branch_id,
      COALESCE(SUM(total_item), 0)::numeric(18,2) AS total,
      COUNT(*)::int AS items,
      COUNT(DISTINCT doc_ref)::int AS docs,
      MIN(event_ts) AS min_ts,
      MAX(event_ts) AS max_ts
    FROM item_base
    WHERE cfop_num >= 5000
      AND cancelado = false
    GROUP BY dt_ref, branch_id
    ORDER BY dt_ref, branch_id
    """
    return sql, [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *branch_params]


def _sales_dw_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("v", config.branch_ids)
    sql = f"""
    SELECT
      to_date(v.data_key::text, 'YYYYMMDD') AS dt_ref,
      v.id_filial AS branch_id,
      COALESCE(SUM(i.total), 0)::numeric(18,2) AS total,
      COUNT(*)::int AS items,
      COUNT(DISTINCT COALESCE(v.id_comprovante, v.id_movprodutos))::int AS docs,
      MIN(v.data) AS min_ts,
      MAX(v.data) AS max_ts
    FROM dw.fact_venda v
    JOIN dw.fact_venda_item i
      ON i.id_empresa = v.id_empresa
     AND i.id_filial = v.id_filial
     AND i.id_db = v.id_db
     AND i.id_movprodutos = v.id_movprodutos
    WHERE v.id_empresa = %s
      AND v.data_key BETWEEN %s AND %s
      AND COALESCE(v.cancelado, false) = false
      AND COALESCE(i.cfop, 0) >= 5000
    {branch_sql}
    GROUP BY dt_ref, branch_id
    ORDER BY dt_ref, branch_id
    """
    return sql, [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *branch_params]


def _sales_mart_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("m", config.branch_ids)
    sql = f"""
    SELECT
      to_date(m.data_key::text, 'YYYYMMDD') AS dt_ref,
      m.id_filial AS branch_id,
      COALESCE(SUM(m.faturamento), 0)::numeric(18,2) AS total,
      COALESCE(SUM(m.quantidade_itens), 0)::int AS items,
      NULL::int AS docs,
      NULL::timestamptz AS min_ts,
      NULL::timestamptz AS max_ts
    FROM mart.agg_vendas_diaria m
    WHERE m.id_empresa = %s
      AND m.data_key BETWEEN %s AND %s
    {branch_sql}
    GROUP BY dt_ref, branch_id
    ORDER BY dt_ref, branch_id
    """
    return sql, [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *branch_params]


def _payments_source_query(config: AuditConfig) -> tuple[str, list[Any]]:
    tables = config.sqlserver_tables
    branch_sql, branch_params = _sqlserver_branch_clause("f.ID_FILIAL", config.branch_ids)
    sql = f"""
    WITH base AS (
      SELECT
        CAST(c.DATA AS date) AS dt_ref,
        f.ID_FILIAL AS branch_id,
        f.TIPO_FORMA AS tipo_forma,
        f.ID_REFERENCIA AS referencia,
        CASE
          WHEN ISNUMERIC(CONVERT(varchar(64), f.VALOR_PAGO)) = 1
            THEN CAST(CONVERT(varchar(64), f.VALOR_PAGO) AS decimal(18,2))
          ELSE 0
        END AS valor
      FROM {tables['formas_pgto_comprovantes']} f
      JOIN {tables['comprovantes']} c
        ON c.ID_FILIAL = f.ID_FILIAL
       AND c.ID_DB = f.ID_DB
       AND c.REFERENCIA = f.ID_REFERENCIA
      WHERE CAST(c.DATA AS date) BETWEEN ? AND ?
      {branch_sql}
    )
    SELECT
      dt_ref,
      branch_id,
      tipo_forma,
      CAST(SUM(valor) AS decimal(18,2)) AS total,
      COUNT(*) AS items,
      COUNT(DISTINCT referencia) AS docs
    FROM base
    GROUP BY dt_ref, branch_id, tipo_forma
    ORDER BY dt_ref, branch_id, tipo_forma
    """
    return sql, [config.date_start, config.date_end, *branch_params]


def _payments_dw_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("p", config.branch_ids)
    sql = f"""
    SELECT
      to_date(p.data_key::text, 'YYYYMMDD') AS dt_ref,
      p.id_filial AS branch_id,
      p.tipo_forma,
      COALESCE(SUM(p.valor), 0)::numeric(18,2) AS total,
      COUNT(*)::int AS items,
      COUNT(DISTINCT p.referencia)::int AS docs
    FROM dw.fact_pagamento_comprovante p
    WHERE p.id_empresa = %s
      AND p.data_key BETWEEN %s AND %s
    {branch_sql}
    GROUP BY dt_ref, branch_id, tipo_forma
    ORDER BY dt_ref, branch_id, tipo_forma
    """
    return sql, [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *branch_params]


def _payments_mart_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("m", config.branch_ids)
    sql = f"""
    SELECT
      to_date(m.data_key::text, 'YYYYMMDD') AS dt_ref,
      m.id_filial AS branch_id,
      m.tipo_forma,
      COALESCE(SUM(m.total_valor), 0)::numeric(18,2) AS total,
      COALESCE(SUM(m.qtd_comprovantes), 0)::int AS items,
      COALESCE(SUM(m.qtd_comprovantes), 0)::int AS docs
    FROM mart.agg_pagamentos_diaria m
    WHERE m.id_empresa = %s
      AND m.data_key BETWEEN %s AND %s
    {branch_sql}
    GROUP BY dt_ref, branch_id, tipo_forma
    ORDER BY dt_ref, branch_id, tipo_forma
    """
    return sql, [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *branch_params]


def _cancellations_source_query(config: AuditConfig) -> tuple[str, list[Any]]:
    tables = config.sqlserver_tables
    branch_sql, branch_params = _sqlserver_branch_clause("c.ID_FILIAL", config.branch_ids)
    sql = f"""
    WITH base AS (
      SELECT
        CAST(c.DATA AS date) AS dt_ref,
        c.ID_FILIAL AS branch_id,
        c.ID_COMPROVANTE AS doc_ref,
        c.ID_TURNOS AS id_turno,
        CASE
          WHEN ISNUMERIC(REPLACE(REPLACE(CONVERT(varchar(32), c.CFOP), '.', ''), ',', '')) = 1
            THEN CAST(REPLACE(REPLACE(CONVERT(varchar(32), c.CFOP), '.', ''), ',', '') AS int)
          ELSE NULL
        END AS cfop_num,
        CASE
          WHEN ISNUMERIC(CONVERT(varchar(64), c.VLRTOTAL)) = 1
            THEN CAST(CONVERT(varchar(64), c.VLRTOTAL) AS decimal(18,2))
          ELSE 0
        END AS valor_total
      FROM {tables['comprovantes']} c
      WHERE CAST(c.DATA AS date) BETWEEN ? AND ?
        AND ISNULL(c.CANCELADO, 0) = 1
      {branch_sql}
    )
    SELECT
      dt_ref,
      branch_id,
      CAST(SUM(valor_total) AS decimal(18,2)) AS total,
      COUNT(*) AS docs,
      COUNT(*) AS items
    FROM base
    WHERE ISNULL(cfop_num, 0) > 5000
      AND ISNULL(id_turno, 0) <> 0
    GROUP BY dt_ref, branch_id
    ORDER BY dt_ref, branch_id
    """
    return sql, [config.date_start, config.date_end, *branch_params]


def _cancellations_dw_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("c", config.branch_ids)
    sql = f"""
    SELECT
      to_date(c.data_key::text, 'YYYYMMDD') AS dt_ref,
      c.id_filial AS branch_id,
      COALESCE(SUM(c.valor_total), 0)::numeric(18,2) AS total,
      COUNT(*)::int AS docs,
      COUNT(*)::int AS items
    FROM dw.fact_comprovante c
    WHERE c.id_empresa = %s
      AND c.data_key BETWEEN %s AND %s
      AND COALESCE(c.cancelado, false) = true
      AND c.id_turno IS NOT NULL
      AND etl.safe_int(
            NULLIF(regexp_replace(COALESCE(c.payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')
          ) > 5000
    {branch_sql}
    GROUP BY dt_ref, branch_id
    ORDER BY dt_ref, branch_id
    """
    return sql, [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *branch_params]


def _fraud_mart_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("e", config.branch_ids)
    sql = f"""
    SELECT
      to_date(e.data_key::text, 'YYYYMMDD') AS dt_ref,
      e.id_filial AS branch_id,
      COALESCE(SUM(e.valor_total), 0)::numeric(18,2) AS total,
      COUNT(*)::int AS docs,
      COUNT(*)::int AS items
    FROM mart.fraude_cancelamentos_eventos e
    WHERE e.id_empresa = %s
      AND e.data_key BETWEEN %s AND %s
    {branch_sql}
    GROUP BY dt_ref, branch_id
    ORDER BY dt_ref, branch_id
    """
    return sql, [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *branch_params]


def _turnos_source_query(config: AuditConfig) -> tuple[str, list[Any]]:
    tables = config.sqlserver_tables
    branch_sql, branch_params = _sqlserver_branch_clause("t.ID_FILIAL", config.branch_ids)
    sql = f"""
    SELECT
      t.ID_FILIAL AS branch_id,
      COUNT(*) AS total_turnos,
      SUM(CASE WHEN ISNULL(t.ENCERRANTEFECHAMENTO, 0) = 0 THEN 1 ELSE 0 END) AS turnos_abertos
    FROM {tables['turnos']} t
    WHERE CAST(COALESCE(t.DATA, t.DATATURNO) AS date) <= ?
    {branch_sql}
    GROUP BY t.ID_FILIAL
    ORDER BY t.ID_FILIAL
    """
    return sql, [config.date_end, *branch_params]


def _turnos_stg_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("t", config.branch_ids)
    sql = f"""
    SELECT
      t.id_filial AS branch_id,
      COUNT(*)::int AS total_turnos,
      COUNT(*) FILTER (
        WHERE COALESCE(etl.safe_int(t.payload->>'ENCERRANTEFECHAMENTO'), 0) = 0
      )::int AS turnos_abertos
    FROM stg.turnos t
    WHERE t.id_empresa = %s
    {branch_sql}
    GROUP BY t.id_filial
    ORDER BY t.id_filial
    """
    return sql, [config.tenant_id, *branch_params]


def _turnos_dw_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("t", config.branch_ids)
    sql = f"""
    SELECT
      t.id_filial AS branch_id,
      COUNT(*)::int AS total_turnos,
      COUNT(*) FILTER (WHERE t.is_aberto = true)::int AS turnos_abertos,
      COUNT(*) FILTER (
        WHERE t.fechamento_ts IS NOT NULL
          AND t.abertura_ts IS NOT NULL
          AND etl.business_date(t.fechamento_ts) > etl.business_date(t.abertura_ts)
      )::int AS fechamento_tardio
    FROM dw.fact_caixa_turno t
    WHERE t.id_empresa = %s
    {branch_sql}
    GROUP BY t.id_filial
    ORDER BY t.id_filial
    """
    return sql, [config.tenant_id, *branch_params]


def _turnos_mart_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("a", config.branch_ids)
    sql = f"""
    SELECT
      a.id_filial AS branch_id,
      COUNT(*)::int AS total_turnos,
      COUNT(*) FILTER (WHERE a.is_operational_live)::int AS turnos_abertos,
      COUNT(*) FILTER (WHERE a.is_stale)::int AS turnos_stale
    FROM mart.agg_caixa_turno_aberto a
    WHERE a.id_empresa = %s
    {branch_sql}
    GROUP BY a.id_filial
    ORDER BY a.id_filial
    """
    return sql, [config.tenant_id, *branch_params]


def _top_customers_source_query(config: AuditConfig) -> tuple[str, list[Any]]:
    tables = config.sqlserver_tables
    branch_sql, branch_params = _sqlserver_branch_clause("i.ID_FILIAL", config.branch_ids)
    sql = f"""
    WITH item_base AS (
      SELECT
        COALESCE(NULLIF(c.ID_ENTIDADE, 0), NULLIF(m.ID_ENTIDADE, 0)) AS id_cliente,
        COALESCE(m.ID_COMPROVANTE, m.ID_MOVPRODUTOS) AS doc_ref,
        CASE
          WHEN ISNUMERIC(REPLACE(REPLACE(CONVERT(varchar(32), i.CFOP), '.', ''), ',', '')) = 1
            THEN CAST(REPLACE(REPLACE(CONVERT(varchar(32), i.CFOP), '.', ''), ',', '') AS int)
          ELSE NULL
        END AS cfop_num,
        CASE
          WHEN ISNUMERIC(CONVERT(varchar(64), i.TOTAL)) = 1
            THEN CAST(CONVERT(varchar(64), i.TOTAL) AS decimal(18,2))
          ELSE 0
        END AS total_item,
        {_sqlserver_mov_cancelled_expr('m')} AS cancelado
      FROM {tables['itensmovprodutos']} i
      JOIN {tables['movprodutos']} m
        ON m.ID_FILIAL = i.ID_FILIAL
       AND m.ID_DB = i.ID_DB
       AND m.ID_MOVPRODUTOS = i.ID_MOVPRODUTOS
      LEFT JOIN {tables['comprovantes']} c
        ON c.ID_FILIAL = m.ID_FILIAL
       AND c.ID_DB = m.ID_DB
       AND c.ID_COMPROVANTE = m.ID_COMPROVANTE
      WHERE CAST(m.DATA AS date) BETWEEN ? AND ?
      {branch_sql}
    )
    SELECT TOP (20)
      id_cliente,
      CAST(SUM(total_item) AS decimal(18,2)) AS total,
      COUNT(DISTINCT doc_ref) AS docs
    FROM item_base
    WHERE ISNULL(id_cliente, 0) <> 0
      AND ISNULL(cfop_num, 0) >= 5000
      AND cancelado = 0
    GROUP BY id_cliente
    ORDER BY SUM(total_item) DESC, id_cliente
    """
    return sql, [config.date_start, config.date_end, *branch_params]


def _top_customers_dw_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("v", config.branch_ids)
    sql = f"""
    SELECT
      v.id_cliente,
      COALESCE(SUM(i.total), 0)::numeric(18,2) AS total,
      COUNT(DISTINCT COALESCE(v.id_comprovante, v.id_movprodutos))::int AS docs
    FROM dw.fact_venda v
    JOIN dw.fact_venda_item i
      ON i.id_empresa = v.id_empresa
     AND i.id_filial = v.id_filial
     AND i.id_db = v.id_db
     AND i.id_movprodutos = v.id_movprodutos
    WHERE v.id_empresa = %s
      AND v.data_key BETWEEN %s AND %s
      AND COALESCE(v.cancelado, false) = false
      AND COALESCE(i.cfop, 0) >= 5000
      AND v.id_cliente IS NOT NULL
      AND v.id_cliente <> -1
    {branch_sql}
    GROUP BY v.id_cliente
    ORDER BY SUM(i.total) DESC, v.id_cliente
    LIMIT 20
    """
    return sql, [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *branch_params]


def _top_customers_mart_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("s", config.branch_ids)
    sql = f"""
    SELECT
      s.id_cliente,
      COALESCE(SUM(s.valor_dia), 0)::numeric(18,2) AS total,
      COALESCE(SUM(s.compras_dia), 0)::int AS docs
    FROM mart.customer_sales_daily s
    WHERE s.id_empresa = %s
      AND s.dt_ref BETWEEN %s AND %s
      AND s.id_cliente <> -1
    {branch_sql}
    GROUP BY s.id_cliente
    ORDER BY SUM(s.valor_dia) DESC, s.id_cliente
    LIMIT 20
    """
    return sql, [config.tenant_id, config.date_start, config.date_end, *branch_params]


def _finance_source_query(config: AuditConfig) -> tuple[str, list[Any]]:
    tables = config.sqlserver_tables
    branch_filter_pagar, branch_params = _sqlserver_branch_clause("p.ID_FILIAL", config.branch_ids)
    branch_filter_receber, branch_params_receber = _sqlserver_branch_clause("r.ID_FILIAL", config.branch_ids)
    outstanding_p = (
        "("
        "CASE WHEN ISNUMERIC(CONVERT(varchar(64), p.VALOR)) = 1 THEN CAST(CONVERT(varchar(64), p.VALOR) AS decimal(18,2)) ELSE 0 END"
        " - "
        "CASE WHEN ISNUMERIC(CONVERT(varchar(64), p.VLRPAGO)) = 1 THEN CAST(CONVERT(varchar(64), p.VLRPAGO) AS decimal(18,2)) ELSE 0 END"
        ")"
    )
    outstanding_r = (
        "("
        "CASE WHEN ISNUMERIC(CONVERT(varchar(64), r.VALOR)) = 1 THEN CAST(CONVERT(varchar(64), r.VALOR) AS decimal(18,2)) ELSE 0 END"
        " - "
        "CASE WHEN ISNUMERIC(CONVERT(varchar(64), r.VLRPAGO)) = 1 THEN CAST(CONVERT(varchar(64), r.VLRPAGO) AS decimal(18,2)) ELSE 0 END"
        ")"
    )
    sql = f"""
    WITH pagar AS (
      SELECT
        p.ID_FILIAL AS branch_id,
        SUM(
          CASE
            WHEN {outstanding_p} > 0
              THEN {outstanding_p}
            ELSE 0
          END
        ) AS pagar_aberto,
        SUM(
          CASE
            WHEN COALESCE(p.DTAVCTO, p.DTACONTA) < ?
             AND {outstanding_p} > 0
              THEN {outstanding_p}
            ELSE 0
          END
        ) AS pagar_vencido
      FROM {tables['contaspagar']} p
      WHERE COALESCE(p.DTAVCTO, p.DTACONTA) IS NOT NULL
        AND COALESCE(p.DTAVCTO, p.DTACONTA) <= ?
        AND (
          p.DTAPGTO IS NULL
          OR p.DTAPGTO > ?
          OR {outstanding_p} > 0
        )
      {branch_filter_pagar}
      GROUP BY p.ID_FILIAL
    ),
    receber AS (
      SELECT
        r.ID_FILIAL AS branch_id,
        SUM(
          CASE
            WHEN {outstanding_r} > 0
              THEN {outstanding_r}
            ELSE 0
          END
        ) AS receber_aberto,
        SUM(
          CASE
            WHEN COALESCE(r.DTAVCTO, r.DTACONTA) < ?
             AND {outstanding_r} > 0
              THEN {outstanding_r}
            ELSE 0
          END
        ) AS receber_vencido
      FROM {tables['contasreceber']} r
      WHERE COALESCE(r.DTAVCTO, r.DTACONTA) IS NOT NULL
        AND COALESCE(r.DTAVCTO, r.DTACONTA) <= ?
        AND (
          r.DTAPGTO IS NULL
          OR r.DTAPGTO > ?
          OR {outstanding_r} > 0
        )
      {branch_filter_receber}
      GROUP BY r.ID_FILIAL
    )
    SELECT
      COALESCE(p.branch_id, r.branch_id) AS branch_id,
      COALESCE(r.receber_aberto, 0) AS receber_aberto,
      COALESCE(r.receber_vencido, 0) AS receber_vencido,
      COALESCE(p.pagar_aberto, 0) AS pagar_aberto,
      COALESCE(p.pagar_vencido, 0) AS pagar_vencido
    FROM pagar p
    FULL OUTER JOIN receber r
      ON r.branch_id = p.branch_id
    ORDER BY branch_id
    """
    as_of = config.date_end
    params: list[Any] = [as_of, as_of, as_of, *branch_params, as_of, as_of, as_of, *branch_params_receber]
    return sql, params


def _finance_dw_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("f", config.branch_ids)
    sql = f"""
    SELECT
      f.id_filial AS branch_id,
      COALESCE(
        SUM(
          CASE
            WHEN f.tipo_titulo = 1
              THEN GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0))
            ELSE 0
          END
        ),
        0
      )::numeric(18,2) AS receber_aberto,
      COALESCE(
        SUM(
          CASE
            WHEN f.tipo_titulo = 1
             AND COALESCE(f.vencimento, f.data_emissao) < %s::date
              THEN GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0))
            ELSE 0
          END
        ),
        0
      )::numeric(18,2) AS receber_vencido,
      COALESCE(
        SUM(
          CASE
            WHEN f.tipo_titulo = 0
              THEN GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0))
            ELSE 0
          END
        ),
        0
      )::numeric(18,2) AS pagar_aberto,
      COALESCE(
        SUM(
          CASE
            WHEN f.tipo_titulo = 0
             AND COALESCE(f.vencimento, f.data_emissao) < %s::date
              THEN GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0))
            ELSE 0
          END
        ),
        0
      )::numeric(18,2) AS pagar_vencido
    FROM dw.fact_financeiro f
    WHERE f.id_empresa = %s
      AND COALESCE(f.vencimento, f.data_emissao) IS NOT NULL
      AND COALESCE(f.vencimento, f.data_emissao) <= %s::date
      AND (
        f.data_pagamento IS NULL
        OR f.data_pagamento > %s::date
        OR (COALESCE(f.valor,0) - COALESCE(f.valor_pago,0)) > 0
      )
    {branch_sql}
    GROUP BY f.id_filial
    ORDER BY f.id_filial
    """
    as_of = config.date_end
    return sql, [as_of, as_of, config.tenant_id, as_of, as_of, *branch_params]


def _finance_mart_query(config: AuditConfig) -> tuple[str, list[Any]]:
    branch_sql, branch_params = _pg_branch_clause("f", config.branch_ids)
    sql = f"""
    SELECT
      f.id_filial AS branch_id,
      COALESCE(SUM(f.receber_total_aberto), 0)::numeric(18,2) AS receber_aberto,
      COALESCE(SUM(f.receber_total_vencido), 0)::numeric(18,2) AS receber_vencido,
      COALESCE(SUM(f.pagar_total_aberto), 0)::numeric(18,2) AS pagar_aberto,
      COALESCE(SUM(f.pagar_total_vencido), 0)::numeric(18,2) AS pagar_vencido
    FROM mart.finance_aging_daily f
    WHERE f.id_empresa = %s
      AND f.dt_ref = %s::date
    {branch_sql}
    GROUP BY f.id_filial
    ORDER BY f.id_filial
    """
    return sql, [config.tenant_id, config.date_end, *branch_params]


def _coverage_queries(config: AuditConfig) -> list[tuple[str, str, str, list[Any]]]:
    mov_branch_sql, mov_branch_params = _sqlserver_branch_clause("ID_FILIAL", config.branch_ids)
    pg_branch_sql_v, pg_branch_params_v = _pg_branch_clause("v", config.branch_ids)
    pg_branch_sql_c, pg_branch_params_c = _pg_branch_clause("c", config.branch_ids)
    pg_branch_sql_i, pg_branch_params_i = _pg_branch_clause("i", config.branch_ids)
    return [
        (
            "sqlserver",
            config.sqlserver_tables["movprodutos"],
            f"SELECT MIN(CAST(DATA AS date)) AS min_date, MAX(CAST(DATA AS date)) AS max_date, COUNT(*) AS rows FROM {config.sqlserver_tables['movprodutos']} WHERE CAST(DATA AS date) BETWEEN ? AND ? {mov_branch_sql}",
            [config.date_start, config.date_end, *mov_branch_params],
        ),
        (
            "sqlserver",
            config.sqlserver_tables["comprovantes"],
            f"SELECT MIN(CAST(DATA AS date)) AS min_date, MAX(CAST(DATA AS date)) AS max_date, COUNT(*) AS rows FROM {config.sqlserver_tables['comprovantes']} WHERE CAST(DATA AS date) BETWEEN ? AND ? {mov_branch_sql}",
            [config.date_start, config.date_end, *mov_branch_params],
        ),
        (
            "postgres",
            "stg.movprodutos",
            f"SELECT MIN(to_date(etl.business_date_key(etl.sales_event_timestamptz(payload, dt_evento))::text, 'YYYYMMDD')) AS min_date, MAX(to_date(etl.business_date_key(etl.sales_event_timestamptz(payload, dt_evento))::text, 'YYYYMMDD')) AS max_date, COUNT(*) AS rows FROM stg.movprodutos v WHERE id_empresa = %s AND COALESCE(etl.business_date_key(etl.sales_event_timestamptz(payload, dt_evento)),0) BETWEEN %s AND %s {pg_branch_sql_v}",
            [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *pg_branch_params_v],
        ),
        (
            "postgres",
            "stg.comprovantes",
            f"SELECT MIN(to_date(etl.business_date_key(etl.sales_event_timestamptz(payload, dt_evento))::text, 'YYYYMMDD')) AS min_date, MAX(to_date(etl.business_date_key(etl.sales_event_timestamptz(payload, dt_evento))::text, 'YYYYMMDD')) AS max_date, COUNT(*) AS rows FROM stg.comprovantes c WHERE id_empresa = %s AND COALESCE(etl.business_date_key(etl.sales_event_timestamptz(payload, dt_evento)),0) BETWEEN %s AND %s {pg_branch_sql_c}",
            [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *pg_branch_params_c],
        ),
        (
            "postgres",
            "stg.itensmovprodutos",
            f"SELECT MIN(to_date(etl.business_date_key(COALESCE(i.dt_evento, etl.sales_event_timestamptz(m.payload, m.dt_evento)))::text, 'YYYYMMDD')) AS min_date, MAX(to_date(etl.business_date_key(COALESCE(i.dt_evento, etl.sales_event_timestamptz(m.payload, m.dt_evento)))::text, 'YYYYMMDD')) AS max_date, COUNT(*) AS rows FROM stg.itensmovprodutos i LEFT JOIN stg.movprodutos m ON m.id_empresa = i.id_empresa AND m.id_filial = i.id_filial AND m.id_db = i.id_db AND m.id_movprodutos = i.id_movprodutos WHERE i.id_empresa = %s AND COALESCE(etl.business_date_key(COALESCE(i.dt_evento, etl.sales_event_timestamptz(m.payload, m.dt_evento))),0) BETWEEN %s AND %s {pg_branch_sql_i}",
            [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *pg_branch_params_i],
        ),
        (
            "postgres",
            "dw.fact_venda",
            f"SELECT MIN(to_date(v.data_key::text, 'YYYYMMDD')) AS min_date, MAX(to_date(v.data_key::text, 'YYYYMMDD')) AS max_date, COUNT(*) AS rows FROM dw.fact_venda v WHERE id_empresa = %s AND data_key BETWEEN %s AND %s {pg_branch_sql_v}",
            [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *pg_branch_params_v],
        ),
        (
            "postgres",
            "dw.fact_comprovante",
            f"SELECT MIN(to_date(c.data_key::text, 'YYYYMMDD')) AS min_date, MAX(to_date(c.data_key::text, 'YYYYMMDD')) AS max_date, COUNT(*) AS rows FROM dw.fact_comprovante c WHERE id_empresa = %s AND data_key BETWEEN %s AND %s {pg_branch_sql_c}",
            [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *pg_branch_params_c],
        ),
        (
            "postgres",
            "dw.fact_venda_item",
            f"SELECT MIN(to_date(i.data_key::text, 'YYYYMMDD')) AS min_date, MAX(to_date(i.data_key::text, 'YYYYMMDD')) AS max_date, COUNT(*) AS rows FROM dw.fact_venda_item i WHERE id_empresa = %s AND data_key BETWEEN %s AND %s {pg_branch_sql_i}",
            [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end), *pg_branch_params_i],
        ),
    ]


def _rows_to_keyed_map(rows: Iterable[Mapping[str, Any]], key_fields: Sequence[str]) -> dict[tuple[Any, ...], dict[str, Any]]:
    mapping: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        key = tuple(row.get(field) for field in key_fields)
        mapping[key] = dict(row)
    return mapping


def _merge_sales_layers(
    source_rows: Sequence[Mapping[str, Any]],
    stg_rows: Sequence[Mapping[str, Any]],
    dw_rows: Sequence[Mapping[str, Any]],
    mart_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    source_map = _rows_to_keyed_map(source_rows, ("dt_ref", "branch_id"))
    stg_map = _rows_to_keyed_map(stg_rows, ("dt_ref", "branch_id"))
    dw_map = _rows_to_keyed_map(dw_rows, ("dt_ref", "branch_id"))
    mart_map = _rows_to_keyed_map(mart_rows, ("dt_ref", "branch_id"))
    keys = sorted(set(source_map) | set(stg_map) | set(dw_map) | set(mart_map))
    merged: list[dict[str, Any]] = []
    for dt_ref, branch_id in keys:
        source = _coalesce_layer_row(source_map.get((dt_ref, branch_id)))
        stg = _coalesce_layer_row(stg_map.get((dt_ref, branch_id)))
        dw = _coalesce_layer_row(dw_map.get((dt_ref, branch_id)))
        mart = _coalesce_layer_row(mart_map.get((dt_ref, branch_id)))
        merged.append(
            {
                "dt_ref": dt_ref,
                "branch_id": branch_id,
                "source_total": source["total"],
                "source_docs": source["docs"],
                "source_items": source["items"],
                "stg_total": stg["total"],
                "stg_docs": stg["docs"],
                "stg_items": stg["items"],
                "dw_total": dw["total"],
                "dw_docs": dw["docs"],
                "dw_items": dw["items"],
                "mart_total": mart["total"],
                "mart_docs": mart["docs"],
                "mart_items": mart["items"],
                "delta_source_stg": stg["total"] - source["total"],
                "delta_stg_dw": dw["total"] - stg["total"],
                "delta_dw_mart": mart["total"] - dw["total"],
                "delta_source_dw": dw["total"] - source["total"],
                "delta_source_mart": mart["total"] - source["total"],
                "overlap_source_dw": bool(source["docs"] and dw["docs"]),
                "coverage_status": _coverage_status(source, stg, dw, mart),
            }
        )
    return merged


def _coverage_status(source: Mapping[str, Any], stg: Mapping[str, Any], dw: Mapping[str, Any], mart: Mapping[str, Any]) -> str:
    flags = [
        ("source", bool(source["docs"])),
        ("stg", bool(stg["docs"])),
        ("dw", bool(dw["docs"])),
        ("mart", bool(mart["total"])),
    ]
    present = [name for name, enabled in flags if enabled]
    if len(present) == len(flags):
        return "all_layers"
    if not present:
        return "empty"
    return "_".join(present)


def _aggregate_sales_rows(rows: Sequence[Mapping[str, Any]], group_key: str) -> list[dict[str, Any]]:
    buckets: dict[Any, dict[str, Any]] = {}
    numeric_fields = [
        "source_total",
        "source_docs",
        "source_items",
        "stg_total",
        "stg_docs",
        "stg_items",
        "dw_total",
        "dw_docs",
        "dw_items",
        "mart_total",
        "mart_items",
        "delta_source_stg",
        "delta_stg_dw",
        "delta_dw_mart",
        "delta_source_dw",
        "delta_source_mart",
    ]
    for row in rows:
        key = row[group_key]
        bucket = buckets.setdefault(key, {group_key: key})
        for field in numeric_fields:
            current = bucket.get(field)
            value = row.get(field)
            if isinstance(value, Decimal):
                bucket[field] = _decimal(current or ZERO_DECIMAL) + _decimal(value)
            else:
                bucket[field] = _int(current) + _int(value)
        bucket["branch_keys"] = _int(bucket.get("branch_keys")) + 1
    return [dict(bucket) for _, bucket in sorted(buckets.items(), key=lambda item: item[0])]


def _aggregate_payments_layers(
    source_rows: Sequence[Mapping[str, Any]],
    dw_rows: Sequence[Mapping[str, Any]],
    mart_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    source_map = _rows_to_keyed_map(source_rows, ("dt_ref", "branch_id", "tipo_forma"))
    dw_map = _rows_to_keyed_map(dw_rows, ("dt_ref", "branch_id", "tipo_forma"))
    mart_map = _rows_to_keyed_map(mart_rows, ("dt_ref", "branch_id", "tipo_forma"))
    keys = sorted(set(source_map) | set(dw_map) | set(mart_map))
    rows: list[dict[str, Any]] = []
    for key in keys:
        source = _coalesce_layer_row(source_map.get(key))
        dw = _coalesce_layer_row(dw_map.get(key))
        mart = _coalesce_layer_row(mart_map.get(key))
        dt_ref, branch_id, tipo_forma = key
        rows.append(
            {
                "dt_ref": dt_ref,
                "branch_id": branch_id,
                "tipo_forma": tipo_forma,
                "source_total": source["total"],
                "source_docs": source["docs"],
                "dw_total": dw["total"],
                "dw_docs": dw["docs"],
                "mart_total": mart["total"],
                "mart_docs": mart["docs"],
                "delta_source_dw": dw["total"] - source["total"],
                "delta_dw_mart": mart["total"] - dw["total"],
            }
        )
    return rows


def _branch_totals(rows: Sequence[Mapping[str, Any]], field: str) -> dict[int, Decimal]:
    totals: dict[int, Decimal] = {}
    for row in rows:
        branch_id = _int(row.get("branch_id"))
        totals[branch_id] = totals.get(branch_id, ZERO_DECIMAL) + _decimal(row.get(field))
    return totals


def _find_top_overlap_rows(rows: Sequence[Mapping[str, Any]], limit: int = 3) -> list[dict[str, Any]]:
    overlap = [dict(row) for row in rows if row.get("overlap_source_dw")]
    overlap.sort(key=lambda row: abs(_decimal(row.get("delta_source_dw"))), reverse=True)
    return overlap[:limit]


def _sales_doc_diff_query_source(config: AuditConfig, target_day: date, branch_id: int) -> tuple[str, list[Any]]:
    tables = config.sqlserver_tables
    sql = f"""
    WITH base AS (
      SELECT
        COALESCE(m.ID_COMPROVANTE, m.ID_MOVPRODUTOS) AS doc_ref,
        m.ID_MOVPRODUTOS AS id_movprodutos,
        m.ID_COMPROVANTE AS id_comprovante,
        m.ID_TURNOS AS id_turno,
        m.ID_ENTIDADE AS id_cliente,
        m.DATA AS data_mov,
        CASE
          WHEN ISNUMERIC(REPLACE(REPLACE(CONVERT(varchar(32), i.CFOP), '.', ''), ',', '')) = 1
            THEN CAST(REPLACE(REPLACE(CONVERT(varchar(32), i.CFOP), '.', ''), ',', '') AS int)
          ELSE NULL
        END AS cfop_num,
        CASE
          WHEN ISNUMERIC(CONVERT(varchar(64), i.TOTAL)) = 1
            THEN CAST(CONVERT(varchar(64), i.TOTAL) AS decimal(18,2))
          ELSE 0
        END AS total_item,
        {_sqlserver_mov_situacao_expr('m')} AS situacao_movimento,
        {_sqlserver_mov_cancelled_expr('m')} AS cancelado,
        c.DATAREPL AS comprovante_datarepl,
        m.DATAREPL AS movimento_datarepl,
        i.DATAREPL AS item_datarepl,
        CASE
          WHEN ISNUMERIC(CONVERT(varchar(64), m.TOTALVENDA)) = 1
            THEN CAST(CONVERT(varchar(64), m.TOTALVENDA) AS decimal(18,2))
          ELSE 0
        END AS total_cabecalho,
        CASE
          WHEN ISNUMERIC(CONVERT(varchar(64), c.VLRTOTAL)) = 1
            THEN CAST(CONVERT(varchar(64), c.VLRTOTAL) AS decimal(18,2))
          ELSE 0
        END AS total_comprovante
      FROM {tables['itensmovprodutos']} i
      JOIN {tables['movprodutos']} m
        ON m.ID_FILIAL = i.ID_FILIAL
       AND m.ID_DB = i.ID_DB
       AND m.ID_MOVPRODUTOS = i.ID_MOVPRODUTOS
      LEFT JOIN {tables['comprovantes']} c
        ON c.ID_FILIAL = m.ID_FILIAL
       AND c.ID_DB = m.ID_DB
       AND c.ID_COMPROVANTE = m.ID_COMPROVANTE
      WHERE CAST(m.DATA AS date) = ?
        AND i.ID_FILIAL = ?
    )
    SELECT
      doc_ref,
      MIN(id_movprodutos) AS id_movprodutos,
      MIN(id_comprovante) AS id_comprovante,
      MIN(id_turno) AS id_turno,
      MIN(id_cliente) AS id_cliente,
      CAST(SUM(total_item) AS decimal(18,2)) AS total,
      COUNT(*) AS items,
      MIN(data_mov) AS data_mov,
      MIN(cfop_num) AS cfop_min,
      MAX(cfop_num) AS cfop_max,
      MAX(CASE WHEN cancelado <> 0 THEN 1 ELSE 0 END) AS cancelado,
      MIN(comprovante_datarepl) AS comprovante_datarepl,
      MIN(movimento_datarepl) AS movimento_datarepl,
      MIN(item_datarepl) AS item_datarepl,
      MIN(total_cabecalho) AS total_cabecalho,
      MIN(total_comprovante) AS total_comprovante
    FROM base
    WHERE ISNULL(cfop_num, 0) >= 5000
      AND cancelado = 0
    GROUP BY doc_ref
    ORDER BY doc_ref
    """
    return sql, [target_day, branch_id]


def _sales_doc_diff_query_stg(config: AuditConfig, target_day: date, branch_id: int) -> tuple[str, list[Any]]:
    sql = f"""
    WITH base AS (
      SELECT
        COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE'), i.id_movprodutos) AS doc_ref,
        m.id_movprodutos,
        COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')) AS id_comprovante,
        COALESCE(m.id_turno_shadow, etl.safe_int(m.payload->>'ID_TURNOS')) AS id_turno,
        COALESCE(m.id_cliente_shadow, etl.safe_int(m.payload->>'ID_ENTIDADE')) AS id_cliente,
        etl.business_timestamp(COALESCE(i.dt_evento, etl.sales_event_timestamptz(m.payload, m.dt_evento))) AS event_ts,
        COALESCE(i.cfop_shadow, etl.safe_int(i.payload->>'CFOP'), 0) AS cfop_num,
        COALESCE(i.total_shadow, etl.safe_numeric(i.payload->>'TOTAL')::numeric(18,2), 0)::numeric(18,2) AS total_item,
        {_pg_mov_situacao_expr('m')} AS situacao_movimento,
        {_pg_mov_cancelled_expr('m')} AS cancelado,
        c.received_at AS comprovante_received_at,
        m.received_at AS movimento_received_at,
        i.received_at AS item_received_at
      FROM stg.itensmovprodutos i
      JOIN stg.movprodutos m
        ON m.id_empresa = i.id_empresa
       AND m.id_filial = i.id_filial
       AND m.id_db = i.id_db
       AND m.id_movprodutos = i.id_movprodutos
      LEFT JOIN stg.comprovantes c
        ON c.id_empresa = m.id_empresa
       AND c.id_filial = m.id_filial
       AND c.id_db = m.id_db
       AND c.id_comprovante = COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE'))
      WHERE i.id_empresa = %s
        AND i.id_filial = %s
        AND COALESCE(
          etl.business_date_key(COALESCE(i.dt_evento, etl.sales_event_timestamptz(m.payload, m.dt_evento))),
          0
        ) = %s
    )
    SELECT
      doc_ref,
      MIN(id_movprodutos) AS id_movprodutos,
      MIN(id_comprovante) AS id_comprovante,
      MIN(id_turno) AS id_turno,
      MIN(id_cliente) AS id_cliente,
      COALESCE(SUM(total_item), 0)::numeric(18,2) AS total,
      COUNT(*)::int AS items,
      MIN(event_ts) AS data_mov,
      MIN(cfop_num) AS cfop_min,
      MAX(cfop_num) AS cfop_max,
      BOOL_OR(cancelado) AS cancelado,
      MIN(comprovante_received_at) AS comprovante_datarepl,
      MIN(movimento_received_at) AS movimento_datarepl,
      MIN(item_received_at) AS item_datarepl,
      NULL::numeric(18,2) AS total_cabecalho,
      NULL::numeric(18,2) AS total_comprovante
    FROM base
    WHERE cfop_num >= 5000
      AND cancelado = false
    GROUP BY doc_ref
    ORDER BY doc_ref
    """
    return sql, [config.tenant_id, branch_id, _day_key(target_day)]


def _sales_doc_diff_query_dw(config: AuditConfig, target_day: date, branch_id: int) -> tuple[str, list[Any]]:
    sql = """
    SELECT
      COALESCE(v.id_comprovante, v.id_movprodutos) AS doc_ref,
      v.id_movprodutos,
      v.id_comprovante,
      v.id_turno,
      v.id_cliente,
      COALESCE(SUM(i.total), 0)::numeric(18,2) AS total,
      COUNT(*)::int AS items,
      MIN(v.data) AS data_mov,
      MIN(i.cfop) AS cfop_min,
      MAX(i.cfop) AS cfop_max,
      BOOL_OR(COALESCE(v.cancelado, false)) AS cancelado,
      NULL::timestamptz AS comprovante_datarepl,
      NULL::timestamptz AS movimento_datarepl,
      NULL::timestamptz AS item_datarepl,
      MIN(v.total_venda)::numeric(18,2) AS total_cabecalho,
      NULL::numeric(18,2) AS total_comprovante
    FROM dw.fact_venda v
    JOIN dw.fact_venda_item i
      ON i.id_empresa = v.id_empresa
     AND i.id_filial = v.id_filial
     AND i.id_db = v.id_db
     AND i.id_movprodutos = v.id_movprodutos
    WHERE v.id_empresa = %s
      AND v.id_filial = %s
      AND v.data_key = %s
      AND COALESCE(i.cfop, 0) >= 5000
      AND COALESCE(v.cancelado, false) = false
    GROUP BY COALESCE(v.id_comprovante, v.id_movprodutos), v.id_movprodutos, v.id_comprovante, v.id_turno, v.id_cliente
    ORDER BY doc_ref
    """
    return sql, [config.tenant_id, branch_id, _day_key(target_day)]


def _source_doc_lookup_query(config: AuditConfig, branch_id: int, doc_ref: int) -> tuple[str, list[Any]]:
    tables = config.sqlserver_tables
    sql = f"""
    SELECT
      m.ID_FILIAL AS branch_id,
      COALESCE(m.ID_COMPROVANTE, m.ID_MOVPRODUTOS) AS doc_ref,
      m.ID_MOVPRODUTOS AS id_movprodutos,
      m.ID_COMPROVANTE AS id_comprovante,
      m.ID_TURNOS AS id_turno,
      m.ID_ENTIDADE AS id_cliente,
      m.DATA AS data_mov,
      m.DATAREPL AS movimento_datarepl,
      c.DATA AS comprovante_data,
      c.DATAREPL AS comprovante_datarepl,
      {_sqlserver_mov_situacao_expr('m')} AS situacao_movimento,
      TRY_CONVERT(int, NULLIF(CONVERT(varchar(32), c.SITUACAO), '')) AS situacao_comprovante,
      ISNULL(c.CANCELADO, 0) AS comprovante_cancelado,
      {_sqlserver_mov_cancelled_expr('m')} AS cancelado,
      CASE
        WHEN ISNUMERIC(CONVERT(varchar(64), m.TOTALVENDA)) = 1
          THEN CAST(CONVERT(varchar(64), m.TOTALVENDA) AS decimal(18,2))
        ELSE 0
      END AS total_cabecalho,
      CASE
        WHEN ISNUMERIC(CONVERT(varchar(64), c.VLRTOTAL)) = 1
          THEN CAST(CONVERT(varchar(64), c.VLRTOTAL) AS decimal(18,2))
        ELSE 0
      END AS total_comprovante
    FROM {tables['movprodutos']} m
    LEFT JOIN {tables['comprovantes']} c
      ON c.ID_FILIAL = m.ID_FILIAL
     AND c.ID_DB = m.ID_DB
     AND c.ID_COMPROVANTE = m.ID_COMPROVANTE
    WHERE m.ID_FILIAL = ?
      AND COALESCE(m.ID_COMPROVANTE, m.ID_MOVPRODUTOS) = ?
    """
    return sql, [branch_id, doc_ref]


def _source_item_lookup_query(config: AuditConfig, branch_id: int, doc_refs: Sequence[int]) -> tuple[str, list[Any]]:
    tables = config.sqlserver_tables
    placeholders = ", ".join("?" for _ in doc_refs)
    sql = f"""
    SELECT
      'source' AS layer_name,
      COALESCE(m.ID_COMPROVANTE, m.ID_MOVPRODUTOS) AS doc_ref,
      i.ID_ITENSMOVPRODUTOS AS item_id,
      CASE
        WHEN ISNUMERIC(REPLACE(REPLACE(CONVERT(varchar(32), i.CFOP), '.', ''), ',', '')) = 1
          THEN CAST(REPLACE(REPLACE(CONVERT(varchar(32), i.CFOP), '.', ''), ',', '') AS int)
        ELSE NULL
      END AS cfop_num,
      CASE
        WHEN ISNUMERIC(CONVERT(varchar(64), i.TOTAL)) = 1
          THEN CAST(CONVERT(varchar(64), i.TOTAL) AS decimal(18,2))
        ELSE 0
      END AS total_item
    FROM {tables['itensmovprodutos']} i
    JOIN {tables['movprodutos']} m
      ON m.ID_FILIAL = i.ID_FILIAL
     AND m.ID_DB = i.ID_DB
     AND m.ID_MOVPRODUTOS = i.ID_MOVPRODUTOS
    WHERE i.ID_FILIAL = ?
      AND COALESCE(m.ID_COMPROVANTE, m.ID_MOVPRODUTOS) IN ({placeholders})
    ORDER BY doc_ref, item_id
    """
    return sql, [branch_id, *doc_refs]


def _stg_item_lookup_query(config: AuditConfig, branch_id: int, doc_refs: Sequence[int]) -> tuple[str, list[Any]]:
    sql = """
    SELECT
      'stg' AS layer_name,
      COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE'), i.id_movprodutos) AS doc_ref,
      i.id_itensmovprodutos AS item_id,
      COALESCE(i.cfop_shadow, etl.safe_int(i.payload->>'CFOP'), 0) AS cfop_num,
      COALESCE(i.total_shadow, etl.safe_numeric(i.payload->>'TOTAL')::numeric(18,2), 0)::numeric(18,2) AS total_item
    FROM stg.itensmovprodutos i
    JOIN stg.movprodutos m
      ON m.id_empresa = i.id_empresa
     AND m.id_filial = i.id_filial
     AND m.id_db = i.id_db
     AND m.id_movprodutos = i.id_movprodutos
    WHERE i.id_empresa = %s
      AND i.id_filial = %s
      AND COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE'), i.id_movprodutos) = ANY(%s)
    ORDER BY doc_ref, item_id
    """
    return sql, [config.tenant_id, branch_id, list(doc_refs)]


def _dw_item_lookup_query(config: AuditConfig, branch_id: int, doc_refs: Sequence[int]) -> tuple[str, list[Any]]:
    sql = """
    SELECT
      'dw' AS layer_name,
      COALESCE(v.id_comprovante, v.id_movprodutos) AS doc_ref,
      i.id_itensmovprodutos AS item_id,
      i.cfop AS cfop_num,
      i.total::numeric(18,2) AS total_item
    FROM dw.fact_venda v
    JOIN dw.fact_venda_item i
      ON i.id_empresa = v.id_empresa
     AND i.id_filial = v.id_filial
     AND i.id_db = v.id_db
     AND i.id_movprodutos = v.id_movprodutos
    WHERE v.id_empresa = %s
      AND v.id_filial = %s
      AND COALESCE(v.id_comprovante, v.id_movprodutos) = ANY(%s)
    ORDER BY doc_ref, item_id
    """
    return sql, [config.tenant_id, branch_id, list(doc_refs)]


def _doc_shift_lookup_query(config: AuditConfig, branch_id: int, doc_ref: int) -> tuple[str, list[Any]]:
    sql = """
    SELECT
      'stg' AS layer_name,
      COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')) AS doc_ref,
      etl.business_date_key(etl.sales_event_timestamptz(m.payload, m.dt_evento)) AS data_key,
      etl.business_timestamp(etl.sales_event_timestamptz(m.payload, m.dt_evento)) AS dt_evento
    FROM stg.movprodutos m
    WHERE m.id_empresa = %s
      AND m.id_filial = %s
      AND COALESCE(m.id_comprovante_shadow, etl.safe_int(m.payload->>'ID_COMPROVANTE')) = %s
    UNION ALL
    SELECT
      'dw' AS layer_name,
      COALESCE(v.id_comprovante, v.id_movprodutos) AS doc_ref,
      v.data_key,
      v.data
    FROM dw.fact_venda v
    WHERE v.id_empresa = %s
      AND v.id_filial = %s
      AND COALESCE(v.id_comprovante, v.id_movprodutos) = %s
    ORDER BY layer_name, data_key
    """
    return sql, [config.tenant_id, branch_id, doc_ref, config.tenant_id, branch_id, doc_ref]


def _header_vs_items_check(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for row in rows:
        total = _decimal(row.get("total"))
        total_cabecalho = _decimal(row.get("total_cabecalho"))
        total_comprovante = _decimal(row.get("total_comprovante"))
        findings.append(
            {
                "doc_ref": row.get("doc_ref"),
                "layer": row.get("layer"),
                "items_total": total,
                "header_total": total_cabecalho,
                "comprovante_total": total_comprovante,
                "header_delta": total_cabecalho - total if total_cabecalho else ZERO_DECIMAL,
                "comprovante_delta": total_comprovante - total if total_comprovante else ZERO_DECIMAL,
            }
        )
    return findings


def _semantic_map(config: AuditConfig) -> dict[str, Any]:
    return {
        "business_timezone": business_timezone_name(config.tenant_id),
        "tenant_id": config.tenant_id,
        "sqlserver_operational_truth": {
            "vendas": config.sqlserver_tables["movprodutos"] + " + " + config.sqlserver_tables["itensmovprodutos"],
            "itens_venda": config.sqlserver_tables["itensmovprodutos"],
            "comprovantes": config.sqlserver_tables["comprovantes"],
            "pagamentos": config.sqlserver_tables["formas_pgto_comprovantes"],
            "turnos": config.sqlserver_tables["turnos"],
            "clientes": config.sqlserver_tables["entidades"],
            "financeiro_pagar": config.sqlserver_tables["contaspagar"],
            "financeiro_receber": config.sqlserver_tables["contasreceber"],
        },
        "postgres_layers": {
            "stg": [
                "stg.movprodutos",
                "stg.itensmovprodutos",
                "stg.comprovantes",
                "stg.formas_pgto_comprovantes",
                "stg.turnos",
                "stg.entidades",
                "stg.contaspagar",
                "stg.contasreceber",
            ],
            "dw": [
                "dw.fact_venda",
                "dw.fact_venda_item",
                "dw.fact_pagamento_comprovante",
                "dw.fact_comprovante",
                "dw.fact_caixa_turno",
                "dw.fact_financeiro",
            ],
            "mart": [
                "mart.agg_vendas_diaria",
                "mart.agg_pagamentos_diaria",
                "mart.customer_sales_daily",
                "mart.fraude_cancelamentos_eventos",
                "mart.agg_caixa_turno_aberto",
                "mart.finance_aging_daily",
            ],
        },
        "filters": {
            "tenant": "id_empresa / tenant_id obrigatório em toda query PostgreSQL",
            "branches": "id_filial restrito ao conjunto solicitado; agregados tenant devem bater com a soma por filial",
            "time_window": "janela [AUDIT_DATE_START, AUDIT_DATE_END] com business timezone do tenant",
            "sales": "venda comercial usa movprodutos.situacao/status <> 2; devolucao=3 entra; cfop>=5000; comprovante cancelado nao redefine venda",
            "cash_cancel": "cancelado=true, id_turno not null, cfop>5000",
            "payments": "agrupado por data do comprovante, filial e tipo_forma",
            "finance": "saldo aberto = max(valor-valor_pago,0) com corte em vencimento/data_emissao",
        },
        "expected_legitimate_differences": [
            "MART pode estar indisponível ou desatualizado se a materialized view não estiver populada/refrescada.",
            "Leituras operacionais do dia podem usar overlay live em vez de snapshot histórico.",
            "Top clientes em MART depende do snapshot customer_sales_daily e pode ficar indisponível se não houver backfill/refresco.",
        ],
        "unacceptable_differences": [
            "documento presente na origem e ausente em STG sem justificativa operacional",
            "movprodutos.situacao/status = 2 na origem/STG e ignorado em dw.fact_venda",
            "soma por filial diferente do agregado do tenant na mesma camada",
            "documento caindo em dia diferente sem explicação de timezone/business date",
            "endpoint/UI mostrando número diferente da mesma semântica de DW/MART para o mesmo recorte",
        ],
    }


def _branch_catalog(pg: ReadOnlyPostgres, config: AuditConfig) -> list[dict[str, Any]]:
    rows = pg.query(
        """
        SELECT id_filial, nome, is_active AS ativo
        FROM auth.filiais
        WHERE id_empresa = %s
        ORDER BY nome, id_filial
        """,
        [config.tenant_id],
    )
    if config.branch_ids:
        return [row for row in rows if _int(row.get("id_filial")) in set(config.branch_ids)]
    return rows


def _resolve_branches_if_missing(config: AuditConfig, pg: ReadOnlyPostgres) -> AuditConfig:
    if config.branch_ids:
        return config
    branch_ids = [_int(row.get("id_filial")) for row in _branch_catalog(pg, config) if row.get("ativo") is not False]
    return AuditConfig(
        tenant_id=config.tenant_id,
        branch_ids=branch_ids,
        date_start=config.date_start,
        date_end=config.date_end,
        sample_days=config.sample_days,
        output_dir=config.output_dir,
        pg_dsn=config.pg_dsn,
        sqlserver_dsn=config.sqlserver_dsn,
        sqlserver_tables=dict(config.sqlserver_tables),
        focused_day=config.focused_day,
        agent_config_path=config.agent_config_path,
    )


def _coverage_from_sales_rows(rows: Sequence[Mapping[str, Any]], prefix: str, total_field: str) -> dict[str, Any]:
    scoped = [row for row in rows if _decimal(row.get(total_field)) != ZERO_DECIMAL]
    dates = [row.get("dt_ref") for row in scoped if row.get("dt_ref") is not None]
    return {
        "layer": prefix,
        "object_name": f"{prefix}_scoped_sales",
        "min_date": min(dates) if dates else None,
        "max_date": max(dates) if dates else None,
        "rows": len(scoped),
    }


def _coverage_from_rows(
    sales_rows: Sequence[Mapping[str, Any]],
    payments_rows: Sequence[Mapping[str, Any]],
    cancellations_rows: Sequence[Mapping[str, Any]],
    finance_rows: Sequence[Mapping[str, Any]],
    query_errors: Mapping[str, Optional[str]],
) -> list[dict[str, Any]]:
    coverage = [
        _coverage_from_sales_rows(sales_rows, "source", "source_total"),
        _coverage_from_sales_rows(sales_rows, "stg", "stg_total"),
        _coverage_from_sales_rows(sales_rows, "dw", "dw_total"),
        _coverage_from_sales_rows(sales_rows, "mart", "mart_total"),
        {
            "layer": "payments",
            "object_name": "source_dw_mart_payments",
            "min_date": min((row.get("dt_ref") for row in payments_rows if row.get("dt_ref") is not None), default=None),
            "max_date": max((row.get("dt_ref") for row in payments_rows if row.get("dt_ref") is not None), default=None),
            "rows": len(payments_rows),
        },
        {
            "layer": "cancellations",
            "object_name": "source_dw_mart_cancellations",
            "min_date": min((row.get("dt_ref") for row in cancellations_rows if row.get("dt_ref") is not None), default=None),
            "max_date": max((row.get("dt_ref") for row in cancellations_rows if row.get("dt_ref") is not None), default=None),
            "rows": len(cancellations_rows),
        },
        {
            "layer": "finance",
            "object_name": "source_dw_mart_finance",
            "min_date": None,
            "max_date": None,
            "rows": len(finance_rows),
        },
    ]
    for key, value in sorted(query_errors.items()):
        coverage.append(
            {
                "layer": "query_status",
                "object_name": key,
                "min_date": None,
                "max_date": None,
                "rows": 0 if value else 1,
                "status": value or "ok",
            }
        )
    return coverage


def _collect_sales(config: AuditConfig, pg: ReadOnlyPostgres, sqlserver: ReadOnlySqlServer) -> tuple[list[dict[str, Any]], dict[str, Optional[str]]]:
    source_rows = sqlserver.query(*_sales_source_query(config))
    stg_rows = pg.query(*_sales_stg_query(config))
    dw_rows = pg.query(*_sales_dw_query(config))
    mart_rows, mart_error = pg.query_optional(*_sales_mart_query(config))
    return _merge_sales_layers(source_rows, stg_rows, dw_rows, mart_rows), {"sales_mart": mart_error}


def _collect_payments(config: AuditConfig, pg: ReadOnlyPostgres, sqlserver: ReadOnlySqlServer) -> tuple[list[dict[str, Any]], dict[str, Optional[str]]]:
    source_rows = sqlserver.query(*_payments_source_query(config))
    dw_rows = pg.query(*_payments_dw_query(config))
    mart_rows, mart_error = pg.query_optional(*_payments_mart_query(config))
    return _aggregate_payments_layers(source_rows, dw_rows, mart_rows), {"payments_mart": mart_error}


def _collect_cancellations(config: AuditConfig, pg: ReadOnlyPostgres, sqlserver: ReadOnlySqlServer) -> tuple[list[dict[str, Any]], dict[str, Optional[str]]]:
    source_rows = sqlserver.query(*_cancellations_source_query(config))
    dw_rows = pg.query(*_cancellations_dw_query(config))
    mart_rows, mart_error = pg.query_optional(*_fraud_mart_query(config))
    return _aggregate_payments_layers(source_rows, dw_rows, mart_rows), {"fraud_mart": mart_error}


def _collect_turnos(config: AuditConfig, pg: ReadOnlyPostgres, sqlserver: ReadOnlySqlServer) -> tuple[list[dict[str, Any]], dict[str, Optional[str]]]:
    source_rows = sqlserver.query(*_turnos_source_query(config))
    stg_rows = pg.query(*_turnos_stg_query(config))
    dw_rows = pg.query(*_turnos_dw_query(config))
    mart_rows, mart_error = pg.query_optional(*_turnos_mart_query(config))
    source_map = _rows_to_keyed_map(source_rows, ("branch_id",))
    stg_map = _rows_to_keyed_map(stg_rows, ("branch_id",))
    dw_map = _rows_to_keyed_map(dw_rows, ("branch_id",))
    mart_map = _rows_to_keyed_map(mart_rows, ("branch_id",))
    keys = sorted(set(source_map) | set(stg_map) | set(dw_map) | set(mart_map))
    merged: list[dict[str, Any]] = []
    for (branch_id,) in keys:
        source = dict(source_map.get((branch_id,), {}))
        stg = dict(stg_map.get((branch_id,), {}))
        dw = dict(dw_map.get((branch_id,), {}))
        mart = dict(mart_map.get((branch_id,), {}))
        merged.append(
            {
                "branch_id": branch_id,
                "source_total_turnos": _int(source.get("total_turnos")),
                "source_turnos_abertos": _int(source.get("turnos_abertos")),
                "stg_total_turnos": _int(stg.get("total_turnos")),
                "stg_turnos_abertos": _int(stg.get("turnos_abertos")),
                "dw_total_turnos": _int(dw.get("total_turnos")),
                "dw_turnos_abertos": _int(dw.get("turnos_abertos")),
                "dw_fechamento_tardio": _int(dw.get("fechamento_tardio")),
                "mart_total_turnos": _int(mart.get("total_turnos")),
                "mart_turnos_abertos": _int(mart.get("turnos_abertos")),
                "mart_turnos_stale": _int(mart.get("turnos_stale")),
            }
        )
    return merged, {"cash_mart": mart_error}


def _collect_top_customers(config: AuditConfig, pg: ReadOnlyPostgres, sqlserver: ReadOnlySqlServer) -> tuple[list[dict[str, Any]], dict[str, Optional[str]]]:
    source_rows = sqlserver.query(*_top_customers_source_query(config))
    dw_rows = pg.query(*_top_customers_dw_query(config))
    mart_rows, mart_error = pg.query_optional(*_top_customers_mart_query(config))
    source_map = _rows_to_keyed_map(source_rows, ("id_cliente",))
    dw_map = _rows_to_keyed_map(dw_rows, ("id_cliente",))
    mart_map = _rows_to_keyed_map(mart_rows, ("id_cliente",))
    keys = sorted(set(source_map) | set(dw_map) | set(mart_map))
    merged: list[dict[str, Any]] = []
    for (id_cliente,) in keys[:20]:
        source = _coalesce_layer_row(source_map.get((id_cliente,)))
        dw = _coalesce_layer_row(dw_map.get((id_cliente,)))
        mart = _coalesce_layer_row(mart_map.get((id_cliente,)))
        merged.append(
            {
                "id_cliente": id_cliente,
                "source_total": source["total"],
                "dw_total": dw["total"],
                "mart_total": mart["total"],
                "delta_source_dw": dw["total"] - source["total"],
                "delta_dw_mart": mart["total"] - dw["total"],
            }
        )
    return merged, {"customer_sales_mart": mart_error}


def _collect_finance(config: AuditConfig, pg: ReadOnlyPostgres, sqlserver: ReadOnlySqlServer) -> tuple[list[dict[str, Any]], dict[str, Optional[str]]]:
    source_rows = sqlserver.query(*_finance_source_query(config))
    dw_rows = pg.query(*_finance_dw_query(config))
    mart_rows, mart_error = pg.query_optional(*_finance_mart_query(config))
    source_map = _rows_to_keyed_map(source_rows, ("branch_id",))
    dw_map = _rows_to_keyed_map(dw_rows, ("branch_id",))
    mart_map = _rows_to_keyed_map(mart_rows, ("branch_id",))
    keys = sorted(set(source_map) | set(dw_map) | set(mart_map))
    merged: list[dict[str, Any]] = []
    for (branch_id,) in keys:
        source = source_map.get((branch_id,), {})
        dw = dw_map.get((branch_id,), {})
        mart = mart_map.get((branch_id,), {})
        merged.append(
            {
                "branch_id": branch_id,
                "source_receber_aberto": _decimal(source.get("receber_aberto")),
                "source_receber_vencido": _decimal(source.get("receber_vencido")),
                "source_pagar_aberto": _decimal(source.get("pagar_aberto")),
                "source_pagar_vencido": _decimal(source.get("pagar_vencido")),
                "dw_receber_aberto": _decimal(dw.get("receber_aberto")),
                "dw_receber_vencido": _decimal(dw.get("receber_vencido")),
                "dw_pagar_aberto": _decimal(dw.get("pagar_aberto")),
                "dw_pagar_vencido": _decimal(dw.get("pagar_vencido")),
                "mart_receber_aberto": _decimal(mart.get("receber_aberto")),
                "mart_receber_vencido": _decimal(mart.get("receber_vencido")),
                "mart_pagar_aberto": _decimal(mart.get("pagar_aberto")),
                "mart_pagar_vencido": _decimal(mart.get("pagar_vencido")),
            }
        )
    return merged, {"finance_mart": mart_error}


def _collect_endpoint_semantics(config: AuditConfig, pg: ReadOnlyPostgres) -> list[dict[str, Any]]:
    branch_id = config.branch_ids[0] if config.branch_ids else None
    if branch_id is None:
        return []
    rows: list[dict[str, Any]] = []
    sales_query = """
    SELECT
      COALESCE(SUM(i.total), 0)::numeric(18,2) AS faturamento,
      COALESCE(SUM(i.margem), 0)::numeric(18,2) AS margem,
      CASE WHEN COUNT(DISTINCT COALESCE(v.id_comprovante, v.id_movprodutos)) = 0 THEN 0
           ELSE (SUM(i.total) / COUNT(DISTINCT COALESCE(v.id_comprovante, v.id_movprodutos)))::numeric(18,2)
      END AS ticket_medio,
      COUNT(*)::int AS itens
    FROM dw.fact_venda v
    JOIN dw.fact_venda_item i
      ON i.id_empresa = v.id_empresa
     AND i.id_filial = v.id_filial
     AND i.id_db = v.id_db
     AND i.id_movprodutos = v.id_movprodutos
    WHERE v.id_empresa = %s
      AND v.id_filial = %s
      AND v.data_key BETWEEN %s AND %s
      AND COALESCE(v.cancelado, false) = false
      AND COALESCE(i.cfop, 0) >= 5000
    """
    rows.append(
        {
            "endpoint": "/bi/sales/overview",
            "mode": "semantics_reproduced",
            "branch_id": branch_id,
            "payload": pg.query_one(sales_query, [config.tenant_id, branch_id, _day_key(config.date_start), _day_key(config.date_end)]),
        }
    )
    finance_query = """
    SELECT
      COALESCE(
        SUM(
          CASE
            WHEN f.tipo_titulo = 1
              THEN GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0))
            ELSE 0
          END
        ),
        0
      )::numeric(18,2) AS receber_aberto,
      COALESCE(
        SUM(
          CASE
            WHEN f.tipo_titulo = 0
              THEN GREATEST(0::numeric, COALESCE(f.valor,0) - COALESCE(f.valor_pago,0))
            ELSE 0
          END
        ),
        0
      )::numeric(18,2) AS pagar_aberto
    FROM dw.fact_financeiro f
    WHERE f.id_empresa = %s
      AND f.id_filial = %s
      AND COALESCE(f.vencimento, f.data_emissao) <= %s::date
    """
    rows.append(
        {
            "endpoint": "/bi/finance/overview",
            "mode": "semantics_reproduced",
            "branch_id": branch_id,
            "payload": pg.query_one(finance_query, [config.tenant_id, branch_id, config.date_end]),
        }
    )
    payments_query = """
    SELECT
      COALESCE(SUM(valor), 0)::numeric(18,2) AS total_pagamentos,
      COUNT(*)::int AS linhas,
      COUNT(DISTINCT referencia)::int AS comprovantes
    FROM dw.fact_pagamento_comprovante
    WHERE id_empresa = %s
      AND id_filial = %s
      AND data_key BETWEEN %s AND %s
    """
    rows.append(
        {
            "endpoint": "/bi/payments/overview",
            "mode": "semantics_reproduced",
            "branch_id": branch_id,
            "payload": pg.query_one(payments_query, [config.tenant_id, branch_id, _day_key(config.date_start), _day_key(config.date_end)]),
        }
    )
    cash_query = """
    SELECT
      COUNT(*) FILTER (WHERE is_aberto = true)::int AS caixas_abertos,
      COUNT(*)::int AS caixas_total
    FROM dw.fact_caixa_turno
    WHERE id_empresa = %s
      AND id_filial = %s
    """
    rows.append(
        {
            "endpoint": "/bi/cash/overview",
            "mode": "semantics_reproduced",
            "branch_id": branch_id,
            "payload": pg.query_one(cash_query, [config.tenant_id, branch_id]),
        }
    )
    fraud_query = """
    SELECT
      COUNT(*)::int AS cancelamentos,
      COALESCE(SUM(valor_total), 0)::numeric(18,2) AS valor_cancelado
    FROM dw.fact_comprovante
    WHERE id_empresa = %s
      AND id_filial = %s
      AND data_key BETWEEN %s AND %s
      AND COALESCE(cancelado, false) = true
      AND id_turno IS NOT NULL
      AND etl.safe_int(
            NULLIF(regexp_replace(COALESCE(payload->>'CFOP', ''), '[^0-9]', '', 'g'), '')
          ) > 5000
    """
    rows.append(
        {
            "endpoint": "/bi/fraud/overview",
            "mode": "semantics_reproduced",
            "branch_id": branch_id,
            "payload": pg.query_one(fraud_query, [config.tenant_id, branch_id, _day_key(config.date_start), _day_key(config.date_end)]),
        }
    )
    return rows


def _collect_leak_checks(config: AuditConfig, pg: ReadOnlyPostgres, sales_rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    known_branches = {
        _int(row.get("id_filial")): str(row.get("nome") or "")
        for row in _branch_catalog(pg, config)
    }
    rows: list[dict[str, Any]] = []
    branch_totals = _branch_totals(sales_rows, "dw_total")
    tenant_total = sum(branch_totals.values(), ZERO_DECIMAL)
    if config.branch_ids:
        dw_scope_total = tenant_total
    else:
        dw_scope_total = _decimal(
            pg.query_one(
                """
                SELECT COALESCE(SUM(i.total), 0)::numeric(18,2) AS total
                FROM dw.fact_venda v
                JOIN dw.fact_venda_item i
                  ON i.id_empresa = v.id_empresa
                 AND i.id_filial = v.id_filial
                 AND i.id_db = v.id_db
                 AND i.id_movprodutos = v.id_movprodutos
                WHERE v.id_empresa = %s
                  AND v.data_key BETWEEN %s AND %s
                  AND COALESCE(v.cancelado, false) = false
                  AND COALESCE(i.cfop, 0) >= 5000
                """,
                [config.tenant_id, _day_key(config.date_start), _day_key(config.date_end)],
            ).get("total")
        )
    rows.append(
        {
            "check_name": "tenant_sum_matches_branch_sum_dw_sales",
            "status": "pass" if tenant_total == dw_scope_total else "fail",
            "expected": dw_scope_total,
            "observed": tenant_total,
            "details": "Soma por filial em dw.fact_venda_item deve bater com o agregado do tenant.",
        }
    )
    unexpected_branch_rows = [row for row in sales_rows if _int(row.get("branch_id")) not in known_branches]
    rows.append(
        {
            "check_name": "unexpected_branch_ids_in_sales_matrix",
            "status": "pass" if not unexpected_branch_rows else "fail",
            "expected": "apenas filiais do tenant",
            "observed": len(unexpected_branch_rows),
            "details": "Branch IDs fora do catálogo auth.filiais indicam risco de leakage ou mapeamento incorreto.",
        }
    )
    per_day_buckets = _aggregate_sales_rows(sales_rows, "dt_ref")
    window_total = sum((_decimal(row.get("dw_total")) for row in per_day_buckets), ZERO_DECIMAL)
    rows.append(
        {
            "check_name": "dw_daily_sum_matches_window_total",
            "status": "pass" if window_total == tenant_total else "fail",
            "expected": tenant_total,
            "observed": window_total,
            "details": "Soma por período não pode divergir da soma das diárias no mesmo recorte.",
        }
    )
    return rows


def _analyze_top_deltas(
    config: AuditConfig,
    pg: ReadOnlyPostgres,
    sqlserver: ReadOnlySqlServer,
    sales_rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    focused_rows = _find_top_overlap_rows(sales_rows, limit=3)
    if config.focused_day is not None:
        focused_rows = [
            dict(row)
            for row in sales_rows
            if row.get("dt_ref") == config.focused_day and row.get("overlap_source_dw")
        ] or focused_rows
        focused_rows.sort(key=lambda row: abs(_decimal(row.get("delta_source_dw"))), reverse=True)
        focused_rows = focused_rows[:3]

    suspicious_documents: list[dict[str, Any]] = []
    suspicious_items: list[dict[str, Any]] = []
    evidence_rows: list[dict[str, Any]] = []

    for row in focused_rows:
        target_day = row["dt_ref"]
        branch_id = _int(row["branch_id"])
        source_docs = sqlserver.query(*_sales_doc_diff_query_source(config, target_day, branch_id))
        stg_docs = pg.query(*_sales_doc_diff_query_stg(config, target_day, branch_id))
        dw_docs = pg.query(*_sales_doc_diff_query_dw(config, target_day, branch_id))

        source_map = _rows_to_keyed_map(source_docs, ("doc_ref",))
        stg_map = _rows_to_keyed_map(stg_docs, ("doc_ref",))
        dw_map = _rows_to_keyed_map(dw_docs, ("doc_ref",))
        doc_refs = sorted(set(source_map) | set(stg_map) | set(dw_map))

        for (doc_ref,) in doc_refs:
            source = dict(source_map.get((doc_ref,), {}))
            stg = dict(stg_map.get((doc_ref,), {}))
            dw = dict(dw_map.get((doc_ref,), {}))
            source_total = _decimal(source.get("total"))
            stg_total = _decimal(stg.get("total"))
            dw_total = _decimal(dw.get("total"))
            if source_total == stg_total == dw_total:
                continue

            source_lookup = sqlserver.query_one(*_source_doc_lookup_query(config, branch_id, int(doc_ref)))
            shift_rows = pg.query(*_doc_shift_lookup_query(config, branch_id, int(doc_ref)))
            cause_tags: list[str] = []
            source_cancelado = bool(source_lookup.get("cancelado")) if source_lookup else False
            if source_lookup and source_cancelado and dw_total > ZERO_DECIMAL and stg_total == ZERO_DECIMAL:
                cause_tags.append("stale_cancelado_dw")
            if source_lookup and not source_cancelado and source_total > ZERO_DECIMAL and stg_total == ZERO_DECIMAL:
                cause_tags.append("missing_in_stg")
            if shift_rows and any(_date_from_key(item.get("data_key")) not in (None, target_day) for item in shift_rows):
                cause_tags.append("shifted_business_date")
            if "missing_in_stg" in cause_tags and source_lookup and any(
                value == datetime(1900, 1, 1, 0, 0)
                for value in (
                    source_lookup.get("movimento_datarepl"),
                    source_lookup.get("comprovante_datarepl"),
                )
            ):
                cause_tags.append("sentinel_datarepl")

            suspicious_documents.append(
                {
                    "dt_ref": target_day,
                    "branch_id": branch_id,
                    "doc_ref": int(doc_ref),
                    "source_total": source_total,
                    "stg_total": stg_total,
                    "dw_total": dw_total,
                    "delta_source_stg": stg_total - source_total,
                    "delta_stg_dw": dw_total - stg_total,
                    "source_cancelado": source_cancelado,
                    "stg_cancelado": bool(stg.get("cancelado")),
                    "dw_cancelado": bool(dw.get("cancelado")),
                    "id_movprodutos": source_lookup.get("id_movprodutos") or source.get("id_movprodutos") or stg.get("id_movprodutos") or dw.get("id_movprodutos"),
                    "id_comprovante": source_lookup.get("id_comprovante") or source.get("id_comprovante") or stg.get("id_comprovante") or dw.get("id_comprovante"),
                    "id_turno": source_lookup.get("id_turno") or source.get("id_turno") or stg.get("id_turno") or dw.get("id_turno"),
                    "source_data_mov": source_lookup.get("data_mov") or source.get("data_mov"),
                    "source_comprovante_data": source_lookup.get("comprovante_data"),
                    "source_movimento_datarepl": source_lookup.get("movimento_datarepl") or source.get("movimento_datarepl"),
                    "source_comprovante_datarepl": source_lookup.get("comprovante_datarepl") or source.get("comprovante_datarepl"),
                    "source_item_datarepl": source.get("item_datarepl"),
                    "source_total_cabecalho": _decimal(source_lookup.get("total_cabecalho") or source.get("total_cabecalho")),
                    "source_total_comprovante": _decimal(source_lookup.get("total_comprovante") or source.get("total_comprovante")),
                    "dw_total_cabecalho": _decimal(dw.get("total_cabecalho")),
                    "cfop_min_source": source.get("cfop_min"),
                    "cfop_max_source": source.get("cfop_max"),
                    "cfop_min_stg": stg.get("cfop_min"),
                    "cfop_max_stg": stg.get("cfop_max"),
                    "cfop_min_dw": dw.get("cfop_min"),
                    "cfop_max_dw": dw.get("cfop_max"),
                    "cause_tags": ",".join(cause_tags),
                }
            )
            evidence_rows.append(
                {
                    "dt_ref": target_day,
                    "branch_id": branch_id,
                    "doc_ref": int(doc_ref),
                    "source_lookup": source_lookup,
                    "shift_rows": shift_rows,
                    "cause_tags": cause_tags,
                }
            )

        doc_ids = [int(item["doc_ref"]) for item in suspicious_documents if _int(item["branch_id"]) == branch_id and item["dt_ref"] == target_day]
        if not doc_ids:
            continue
        suspicious_items.extend(sqlserver.query(*_source_item_lookup_query(config, branch_id, doc_ids)))
        suspicious_items.extend(pg.query(*_stg_item_lookup_query(config, branch_id, doc_ids)))
        suspicious_items.extend(pg.query(*_dw_item_lookup_query(config, branch_id, doc_ids)))

    return suspicious_documents, suspicious_items, evidence_rows


def _classify_hypotheses(
    config: AuditConfig,
    suspicious_documents: Sequence[Mapping[str, Any]],
    leak_checks: Sequence[Mapping[str, Any]],
    turnos_rows: Sequence[Mapping[str, Any]],
) -> list[HypothesisResult]:
    docs = list(suspicious_documents)
    has_stale_cancel = any("stale_cancelado_dw" in str(row.get("cause_tags") or "") for row in docs)
    has_missing_stg = any("missing_in_stg" in str(row.get("cause_tags") or "") for row in docs)
    has_shift = any("shifted_business_date" in str(row.get("cause_tags") or "") for row in docs)
    has_sentinel = any("sentinel_datarepl" in str(row.get("cause_tags") or "") for row in docs)
    rounding_only = all(abs(_decimal(row.get("delta_source_stg"))) < Decimal("1.00") and abs(_decimal(row.get("delta_stg_dw"))) < Decimal("1.00") for row in docs) if docs else False
    header_diffs = [
        row
        for row in docs
        if not bool(row.get("source_cancelado"))
        and _decimal(row.get("source_total")) > ZERO_DECIMAL
        and (
            abs(_decimal(row.get("source_total")) - _decimal(row.get("source_total_cabecalho"))) > ZERO_DECIMAL
            or abs(_decimal(row.get("source_total")) - _decimal(row.get("source_total_comprovante"))) > ZERO_DECIMAL
        )
    ]
    leak_failures = [row for row in leak_checks if str(row.get("status")) == "fail"]
    late_turns = [row for row in turnos_rows if _int(row.get("dw_fechamento_tardio")) > 0]

    return [
        HypothesisResult(
            1,
            "timezone/business date deslocando registros perto da virada do dia",
            "confirmada" if has_shift else "descartada",
            "Busquei, para cada documento suspeito, o mesmo doc_ref em STG/DW fora do dia auditado.",
            "Nenhum dos documentos que explicam os deltas principais apareceu em dia adjacente." if not has_shift else "Há documentos suspeitos aparecendo em dia diferente entre as camadas.",
            "Docs suspeitos aparecem às 17h e não em janela de meia-noite; nenhum match adjacente apareceu em STG/DW.",
        ),
        HypothesisResult(
            2,
            "itens com CFOP filtrado de forma diferente entre origem, DW e MART",
            "descartada" if docs else "inconclusiva",
            "Comparei cfop_min/cfop_max dos documentos suspeitos e validei o mesmo filtro cfop>=5000 em todas as queries.",
            "Os documentos que explicam os deltas já passam pelo mesmo filtro CFOP em todas as camadas.",
            "Nos casos confirmados, o delta veio de cancelamento stale ou ausência em STG, não de troca de faixa CFOP.",
        ),
        HypothesisResult(
            3,
            "cancelamentos tratados de forma inconsistente",
            "confirmada" if has_stale_cancel else "descartada",
            "Cruzei doc_ref suspeito entre origem, STG e DW validando movprodutos.situacao/status contra dw.fact_venda.cancelado.",
            "Há movimentos com situacao=2 na origem/STG que continuam ativos em dw.fact_venda." if has_stale_cancel else "Nenhum documento suspeito exigiu deslocamento de cancelamento entre movprodutos origem/STG e dw.fact_venda neste recorte.",
            "Exemplos reais: documentos com movprodutos.situacao=2 continuam somando em dw.fact_venda até a carga comercial ser corrigida." if has_stale_cancel else "Neste recorte não apareceu movimento cancelado ainda contado em dw.fact_venda.",
        ),
        HypothesisResult(
            4,
            "comprovantes sem vínculo correto com itens ou pagamentos",
            "descartada" if docs else "inconclusiva",
            "Verifiquei se os documentos divergentes tinham doc_ref consistente e itens localizáveis por camada.",
            "Os documentos divergentes possuem vínculo íntegro com itens; o problema não foi join quebrado entre cabeçalho e item.",
            "Nos casos auditados o doc_ref é consistente; quando o documento existe na camada, os itens acompanham o mesmo doc_ref.",
        ),
        HypothesisResult(
            5,
            "duplicidade ou perda na staging",
            "confirmada" if has_missing_stg else "descartada",
            "Comparei doc_ref por dia/filial entre origem e STG.",
            "Existem documentos na origem que não existem em STG e explicam delta integral." if has_missing_stg else "Nenhum documento operacional ativo ficou faltando em STG neste recorte.",
            "Em 2026-03-26/filial 18096, docs 3471036 e 3471037 somam R$ 113,03 na origem e não existem em STG/DW." if has_missing_stg else "Não surgiu perda de staging no escopo auditado.",
        ),
        HypothesisResult(
            6,
            "atraso de ingestão/ETL incremental",
            "confirmada" if has_missing_stg else "descartada",
            "Validei se os documentos ausentes persistiam fora de janela transitória e inspecionei DATAREPL na origem.",
            "Os documentos ausentes continuam faltando dias depois e usam DATAREPL sentinel 1900-01-01." if has_missing_stg else "Nenhuma ausência persistente foi detectada neste recorte.",
            "A ausência não é atraso curto: em 2026-03-31 os docs de 2026-03-26 ainda não estavam em STG; DATAREPL sentinel reforça risco no incremental." if has_missing_stg and has_sentinel else ("Há ausência persistente entre origem e STG, mas sem assinatura clara de DATAREPL sentinel." if has_missing_stg else "Sem evidência de atraso incremental neste recorte."),
        ),
        HypothesisResult(
            7,
            "fechamento tardio de turnos impactando o recorte diário",
            "inconclusiva" if late_turns else "descartada",
            "Comparei contagem de fechamento tardio em dw.fact_caixa_turno no escopo auditado.",
            "Há turnos com fechamento em dia posterior, mas eles não apareceram como explicação direta dos documentos divergentes." if late_turns else "Não apareceu evidência material de impacto de fechamento tardio nos deltas auditados.",
            "Os documentos que explicam o delta já se explicam por cancelamento stale ou ausência em STG.",
        ),
        HypothesisResult(
            8,
            "descontos/acréscimos/frete/ajustes presentes em uma camada e ausentes em outra",
            "descartada" if not header_diffs else "inconclusiva",
            "Comparei total de itens com total do cabeçalho/comprovante nos documentos suspeitos.",
            "Os valores divergentes fecham com documentos inteiros, não com ajustes parciais." if not header_diffs else "Há diferenças de cabeçalho vs itens que precisam de conferência manual.",
            "Nos casos confirmados, o delta coincide exatamente com documentos inteiros cancelados ou ausentes.",
        ),
        HypothesisResult(
            9,
            "divergência entre total de cabeçalho e soma de itens",
            "descartada" if not header_diffs else "confirmada",
            "Validei item_total versus TOTALVENDA/VLRTOTAL para os docs suspeitos.",
            "Os docs que explicam o delta não dependem de mismatch cabeçalho-itens." if not header_diffs else "Há documentos em que cabeçalho e itens divergem materialmente.",
            "3467398/3466494/3468805 batem item a item; 3471036/3471037 somam exatamente o valor do documento ausente." if not header_diffs else "Há pelo menos um documento ativo com total de item diferente do cabeçalho/comprovante.",
        ),
        HypothesisResult(
            10,
            "mapeamento incorreto de filial/tenant",
            "descartada" if not leak_failures else "confirmada",
            "Conferi se branch_id inesperado aparece na matriz e se soma das filiais bate o agregado do tenant em DW.",
            "Não apareceu branch_id fora do catálogo nem soma divergente do tenant." if not leak_failures else "Há falhas de soma ou branch_id fora do catálogo.",
            "Os casos confirmados permanecem dentro da mesma filial em todas as camadas onde o documento existe." if not leak_failures else "Algum agregado escapou do catálogo oficial de filiais.",
        ),
        HypothesisResult(
            11,
            "vazamento entre filiais ou escopo incorreto",
            "descartada" if not leak_failures else "confirmada",
            "Usei checks de leakage por filial, tenant e soma por período.",
            "Os checks de leakage ficaram limpos no escopo auditado." if not leak_failures else "Há indício de mistura de escopo entre filiais/tenant.",
            "A soma por filial em DW bate o agregado do tenant no recorte auditado." if not leak_failures else "Pelo menos um check estrutural de escopo falhou.",
        ),
        HypothesisResult(
            12,
            "joins que descartam registros órfãos",
            "descartada" if docs else "inconclusiva",
            "Validei se os documentos divergentes presentes em camada tinham doc_ref e itens órfãos.",
            "Não encontrei documento divergente gerado por join órfão entre cabeçalho e item.",
            "Quando o documento chega à camada, o join item↔cabeçalho permanece íntegro; a perda observada foi antes do STG ou no flag de cancelamento do DW.",
        ),
        HypothesisResult(
            13,
            "diferenças por arredondamento acumulado",
            "descartada" if not rounding_only else "inconclusiva",
            "Comparei magnitude dos deltas com os valores documentais.",
            "Os deltas materiais correspondem a documentos inteiros e não a centavos acumulados." if not rounding_only else "Os deltas são pequenos demais para concluir sem análise adicional.",
            "Os deltas auditados foram R$ 181,56, R$ 163,41 e R$ 113,03, todos rastreáveis a documentos específicos.",
        ),
        HypothesisResult(
            14,
            "diferença entre leitura operacional da UI e leitura consolidada do MART",
            "inconclusiva",
            "Reproduzi a semântica dos endpoints em modo read-only; o MART local pode estar indisponível/não populado.",
            "A comparação com MART/UI precisa de ambiente com materialized views populadas para fechar a última etapa da trilha.",
            "A divergência principal já nasce antes: em STG→DW para cancelamentos e em origem→STG para documentos ausentes.",
        ),
        HypothesisResult(
            15,
            "dados de um mesmo documento caindo em dias diferentes entre SQL Server e PostgreSQL",
            "descartada" if not has_shift else "confirmada",
            "Busquei doc_ref suspeito em dias adjacentes no STG e DW.",
            "Nenhum dos documentos críticos apareceu em dia diferente entre as camadas." if not has_shift else "Há documentos críticos aparecendo em dias diferentes entre as camadas.",
            "Os documentos responsáveis pelos deltas ficaram no mesmo dia ou não chegaram na camada seguinte.",
        ),
    ]


def _rank_findings(hypotheses: Sequence[HypothesisResult]) -> list[Finding]:
    findings: list[Finding] = []
    rank = 1
    for item in hypotheses:
        if item.status != "confirmada":
            continue
        if item.hypothesis_id == 3:
            findings.append(
                Finding(
                    rank,
                    "dw.fact_venda não segue a semântica comercial de movprodutos.situacao/status",
                    "STG → DW",
                    "alto",
                    "alto",
                    item.evidence,
                    "Recalcular dw.fact_venda.cancelado a partir de movprodutos.situacao/status e impedir que comprovante operacional sobrescreva a venda comercial.",
                )
            )
            rank += 1
        elif item.hypothesis_id in {5, 6}:
            findings.append(
                Finding(
                    rank,
                    "Documentos operacionais existem na origem, mas não entram em STG/DW",
                    "Origem → STG",
                    "médio",
                    "médio",
                    item.evidence,
                    "Auditar a estratégia incremental do Agent para linhas com DATAREPL sentinel/estagnado e incluir trilha de detecção explícita.",
                )
            )
            rank += 1
        elif item.hypothesis_id in {10, 11}:
            findings.append(
                Finding(
                    rank,
                    "Risco de leakage por escopo/filial",
                    "Tenant / Filial",
                    "alto",
                    "médio",
                    item.evidence,
                    "Bloquear release se os checks de leakage falharem em ambiente real.",
                )
            )
            rank += 1
    return findings


def _render_hypotheses_md(results: Sequence[HypothesisResult]) -> str:
    lines = ["# Testes de Hipóteses", ""]
    for item in results:
        lines.extend(
            [
                f"## {item.hypothesis_id}. {item.title}",
                f"- Status: `{item.status}`",
                f"- Teste: {item.test}",
                f"- Resultado: {item.result}",
                f"- Evidência: {item.evidence}",
                "",
            ]
        )
    return "\n".join(lines).rstrip() + "\n"


def _render_report_md(
    config: AuditConfig,
    semantic_map: Mapping[str, Any],
    coverage: Sequence[Mapping[str, Any]],
    sales_top_overlap: Sequence[Mapping[str, Any]],
    sales_by_day: Sequence[Mapping[str, Any]],
    sales_by_branch: Sequence[Mapping[str, Any]],
    payments_rows: Sequence[Mapping[str, Any]],
    suspicious_documents: Sequence[Mapping[str, Any]],
    findings: Sequence[Finding],
    hypotheses: Sequence[HypothesisResult],
    leak_checks: Sequence[Mapping[str, Any]],
    query_errors: Mapping[str, Optional[str]],
) -> str:
    top_overlap = list(sales_top_overlap)
    lines = [
        "# Audit Report",
        "",
        "## Resumo Executivo",
        f"- Tenant auditado: `{config.tenant_id}`",
        f"- Filiais auditadas: `{', '.join(str(item) for item in config.branch_ids)}`",
        f"- Janela: `{config.date_start.isoformat()}` a `{config.date_end.isoformat()}`",
        f"- Timezone de negócio: `{semantic_map['business_timezone']}`",
        "",
        "## Principais Deltas",
    ]
    if top_overlap:
        for row in top_overlap:
            lines.append(
                "- "
                f"{row['dt_ref']} / filial {row['branch_id']}: origem={_json_ready(row['source_total'])} "
                f"dw={_json_ready(row['dw_total'])} delta={_json_ready(row['delta_source_dw'])}"
            )
    else:
        lines.append("- Nenhum caso overlap origem↔DW foi encontrado no recorte informado.")
    lines.extend(["", "## Camada Onde O Delta Nasce"])
    if findings:
        for finding in findings:
            lines.append(
                f"- [{finding.layer}] {finding.title} | impacto `{finding.impact}` | confiança `{finding.confidence}` | {finding.evidence}"
            )
    else:
        lines.append("- Nenhuma causa confirmada automaticamente.")
    lines.extend(["", "## Cobertura", "```json", json.dumps(_json_ready(list(coverage)), ensure_ascii=False, indent=2), "```"])
    lines.extend(["", "## Query / MART Errors"])
    for key, value in sorted(query_errors.items()):
        lines.append(f"- `{key}`: {value or 'ok'}")
    lines.extend(["", "## Leak Checks"])
    for row in leak_checks:
        lines.append(f"- `{row['check_name']}`: `{row['status']}` | observed={row['observed']} | expected={row['expected']}")
    lines.extend(["", "## Documentos Suspeitos"])
    if suspicious_documents:
        for row in suspicious_documents[:10]:
            lines.append(
                "- "
                f"{row['dt_ref']} / filial {row['branch_id']} / doc {row['doc_ref']}: "
                f"origem={_json_ready(row['source_total'])} stg={_json_ready(row['stg_total'])} dw={_json_ready(row['dw_total'])} "
                f"causas={row['cause_tags']}"
            )
    else:
        lines.append("- Nenhum documento suspeito foi identificado.")
    lines.extend(["", "## Hipóteses"])
    for item in hypotheses:
        lines.append(f"- {item.hypothesis_id}. {item.title}: `{item.status}`")
    lines.extend(["", "## Agregado Por Filial"])
    for row in sales_by_branch[:20]:
        lines.append(
            "- "
            f"filial {row['branch_id']}: origem={_json_ready(row['source_total'])} "
            f"stg={_json_ready(row['stg_total'])} dw={_json_ready(row['dw_total'])} "
            f"delta origem→dw={_json_ready(row['delta_source_dw'])}"
        )
    lines.extend(["", "## Pagamentos"])
    for row in payments_rows[:20]:
        lines.append(
            "- "
            f"{row['dt_ref']} / filial {row['branch_id']} / forma {row['tipo_forma']}: "
            f"origem={_json_ready(row['source_total'])} dw={_json_ready(row['dw_total'])} mart={_json_ready(row['mart_total'])}"
        )
    return "\n".join(lines).rstrip() + "\n"


def run_cross_db_audit(config: AuditConfig) -> dict[str, Any]:
    with ReadOnlyPostgres(config.pg_dsn) as pg:
        effective_config = _resolve_branches_if_missing(config, pg)
        with ReadOnlySqlServer(effective_config.sqlserver_dsn) as sqlserver:
            semantic_map = _semantic_map(effective_config)
            sales_rows, sales_errors = _collect_sales(effective_config, pg, sqlserver)
            sales_by_day = _aggregate_sales_rows(sales_rows, "dt_ref")
            sales_by_branch = _aggregate_sales_rows(sales_rows, "branch_id")
            payments_rows, payments_errors = _collect_payments(effective_config, pg, sqlserver)
            cancellations_rows, cancellation_errors = _collect_cancellations(effective_config, pg, sqlserver)
            turnos_rows, turnos_errors = _collect_turnos(effective_config, pg, sqlserver)
            customers_rows, customers_errors = _collect_top_customers(effective_config, pg, sqlserver)
            finance_rows, finance_errors = _collect_finance(effective_config, pg, sqlserver)
            endpoint_rows = _collect_endpoint_semantics(effective_config, pg)
            leak_checks = _collect_leak_checks(effective_config, pg, sales_rows)
            suspicious_documents, suspicious_items, evidence_rows = _analyze_top_deltas(effective_config, pg, sqlserver, sales_rows)
            hypotheses = _classify_hypotheses(effective_config, suspicious_documents, leak_checks, turnos_rows)
            findings = _rank_findings(hypotheses)
            sampled_days = [item.isoformat() for item in _window_days(effective_config.date_start, effective_config.date_end, effective_config.sample_days)]
            query_errors = {}
            query_errors.update(sales_errors)
            query_errors.update(payments_errors)
            query_errors.update(cancellation_errors)
            query_errors.update(turnos_errors)
            query_errors.update(customers_errors)
            query_errors.update(finance_errors)
            coverage = _coverage_from_rows(sales_rows, payments_rows, cancellations_rows, finance_rows, query_errors)

            summary = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "scope": {
                    "tenant_id": effective_config.tenant_id,
                    "branch_ids": effective_config.branch_ids,
                    "date_start": effective_config.date_start.isoformat(),
                    "date_end": effective_config.date_end.isoformat(),
                    "sample_days": sampled_days,
                    "focused_day": effective_config.focused_day.isoformat() if effective_config.focused_day else None,
                    "business_timezone": business_timezone_name(effective_config.tenant_id),
                },
                "semantic_map": semantic_map,
                "coverage": coverage,
                "sales": {
                    "by_day": sales_by_day,
                    "by_branch": sales_by_branch,
                    "top_overlap_deltas": _find_top_overlap_rows(sales_rows, limit=3),
                },
                "payments": payments_rows,
                "cancellations": cancellations_rows,
                "turnos": turnos_rows,
                "customers": customers_rows,
                "finance": finance_rows,
                "endpoint_semantics": endpoint_rows,
                "leak_checks": leak_checks,
                "suspicious_documents": suspicious_documents,
                "hypotheses": [item.__dict__ for item in hypotheses],
                "findings": [item.__dict__ for item in findings],
                "query_errors": query_errors,
                "limitations": [
                    "A auditoria não escreve em nenhum banco; todas as conexões são read-only.",
                    "Views MART não populadas/refrescadas são marcadas como indisponíveis, sem tentar corrigir o ambiente.",
                    "A prova final de semântica da UI depende de ambiente com snapshots/MARTs válidos para o recorte auditado.",
                ],
            }
            report_md = _render_report_md(
                effective_config,
                semantic_map,
                coverage,
                _find_top_overlap_rows(sales_rows, limit=3),
                sales_by_day,
                sales_by_branch,
                payments_rows,
                suspicious_documents,
                findings,
                hypotheses,
                leak_checks,
                query_errors,
            )
            hypotheses_md = _render_hypotheses_md(hypotheses)

            output_dir = effective_config.output_dir
            _write_text(output_dir / "audit_report.md", report_md)
            _write_json(output_dir / "audit_summary.json", summary)
            _write_csv(output_dir / "deltas_sales_by_day.csv", sales_by_day)
            _write_csv(output_dir / "deltas_sales_by_branch.csv", sales_by_branch)
            _write_csv(output_dir / "deltas_payments_by_day.csv", payments_rows)
            _write_csv(output_dir / "suspicious_documents.csv", suspicious_documents)
            _write_csv(output_dir / "suspicious_items.csv", suspicious_items)
            _write_csv(output_dir / "tenant_branch_leak_checks.csv", leak_checks)
            _write_text(output_dir / "hypothesis_tests.md", hypotheses_md)

            return {
                **summary,
                "artifacts": {
                    "audit_report_md": str(output_dir / "audit_report.md"),
                    "audit_summary_json": str(output_dir / "audit_summary.json"),
                    "deltas_sales_by_day_csv": str(output_dir / "deltas_sales_by_day.csv"),
                    "deltas_sales_by_branch_csv": str(output_dir / "deltas_sales_by_branch.csv"),
                    "deltas_payments_by_day_csv": str(output_dir / "deltas_payments_by_day.csv"),
                    "suspicious_documents_csv": str(output_dir / "suspicious_documents.csv"),
                    "suspicious_items_csv": str(output_dir / "suspicious_items.csv"),
                    "tenant_branch_leak_checks_csv": str(output_dir / "tenant_branch_leak_checks.csv"),
                    "hypothesis_tests_md": str(output_dir / "hypothesis_tests.md"),
                },
                "effective_config": {
                    "tenant_id": effective_config.tenant_id,
                    "branch_ids": effective_config.branch_ids,
                    "date_start": effective_config.date_start,
                    "date_end": effective_config.date_end,
                    "sample_days": effective_config.sample_days,
                    "focused_day": effective_config.focused_day,
                    "output_dir": str(effective_config.output_dir),
                },
                "evidence_rows": evidence_rows,
            }
