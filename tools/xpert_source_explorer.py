#!/usr/bin/env python3
"""TorqMind Xpert Source Explorer — read-only SQL Server discovery tool.

Connects to a client's operational SQL Server to map tables, columns,
indexes, keys, procedures, views, triggers, business objects, and more.

ALL operations are strictly read-only.  No INSERT/UPDATE/DELETE/DROP/ALTER
is ever executed.

Usage examples::

    python tools/xpert_source_explorer.py test-connection --env config/source-explorer.env
    python tools/xpert_source_explorer.py discover-schema --env config/source-explorer.env --out logs/source_explorer/schema
    python tools/xpert_source_explorer.py full-discovery --env config/source-explorer.env --out logs/source_explorer/full
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import logging
import os
import re
import sys
import textwrap
import traceback
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("xpert")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

VERSION = "0.4.0"

BUSINESS_KEYWORDS: List[str] = [
    "comprovante", "item", "produto", "venda", "cancel", "nfe", "nfce",
    "inutil", "caixa", "turno", "usuario", "funcionario", "cliente",
    "entidade", "contas", "pagar", "receber", "baixa", "parcela",
    "financeiro", "plano", "centro", "banco", "cartao", "tef", "estoque",
    "compra", "fornecedor", "custo", "margem",
]

DOMAIN_PATTERNS: Dict[str, List[str]] = {
    "vendas": ["comprovante", "venda", "cupom", "pedido"],
    "itens_venda": ["itenscomprovante", "itensvenda", "itemvenda", "itenscupom"],
    "pagamentos": ["forma", "pgto", "pagamento", "formas_pgto"],
    "produtos": ["produto", "mercadoria", "artigo"],
    "grupos_produto": ["grupo", "subgrupo", "secao", "departamento", "familia"],
    "clientes": ["cliente", "entidade", "pessoa"],
    "fornecedores": ["fornecedor", "entidade"],
    "funcionarios": ["funcionario", "operador", "frentista", "vendedor", "colaborador"],
    "usuarios": ["usuario", "user", "login"],
    "turnos": ["turno", "abertura", "fechamento"],
    "caixas": ["caixa", "pdv", "ecf", "localvenda"],
    "nfe_nfce": ["nfe", "nfce", "notafiscal", "nota_fiscal"],
    "inutilizacoes": ["inutil", "inutiliz"],
    "contas_pagar": ["contaspagar", "contas_pagar", "cp_"],
    "contas_receber": ["contasreceber", "contas_receber", "cr_"],
    "baixas": ["baixa", "liquidacao", "quitacao"],
    "parcelas": ["parcela", "duplicata", "titulo"],
    "bancos": ["banco", "contabancaria", "conta_bancaria"],
    "cartoes_tef": ["cartao", "tef", "adquirente", "bandeira"],
    "compras": ["compra", "pedidocompra", "entradanf", "notaentrada"],
    "estoque": ["estoque", "inventario", "saldo"],
    "mov_estoque": ["movproduto", "movimentacao", "movestoque"],
    "financeiro": ["financeiro", "lancamento", "movimento_financeiro"],
    "plano_contas": ["planocontas", "plano_contas", "planodecontas"],
    "centro_custo": ["centrocusto", "centro_custo"],
    "metas": ["meta", "objetivo", "target"],
    "auditoria_logs": ["auditoria", "log", "historico", "registro"],
}

FINANCE_KEYWORDS: List[str] = [
    "conta", "contas", "pagar", "receber", "parcela", "baixa",
    "vencimento", "pagamento", "fornecedor", "cliente", "entidade",
    "banco", "caixa", "movimento", "documento", "duplicata", "boleto",
    "cheque", "cartao", "tef", "juros", "multa", "desconto", "centro",
    "custo", "plano", "historico", "natureza",
]

CUSTOMER_KEYWORDS: List[str] = [
    "cliente", "entidade", "pessoa", "cpf", "cnpj", "telefone", "email",
    "endereco", "placa", "limite", "credito", "convenio",
]

NFE_KEYWORDS: List[str] = [
    "nfe", "nfce", "nota", "lote", "evento", "inutiliz", "cancel",
    "substituic", "protocolo", "chave", "numero", "serie", "status",
]

PII_PATTERNS = {
    "cpf": re.compile(r"\d{3}[\.\-]?\d{3}[\.\-]?\d{3}[\.\-]?\d{2}"),
    "cnpj": re.compile(r"\d{2}[\.\-]?\d{3}[\.\-]?\d{3}[/]?\d{4}[\-]?\d{2}"),
    "email": re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+"),
    "phone": re.compile(r"\(?\d{2}\)?[\s\-]?\d{4,5}[\-]?\d{4}"),
    "chave_nfe": re.compile(r"\d{44}"),
}

# Columns whose values should always be masked in samples
PII_COLUMN_HINTS = {
    "cpf", "cnpj", "rg", "email", "telefone", "fone", "celular",
    "endereco", "cep", "chaveacesso", "chave_acesso", "senha", "password",
    "numero_cartao", "cartao",
}

# ---------------------------------------------------------------------------
# SQL safety — NEVER allow writes
# ---------------------------------------------------------------------------

_FORBIDDEN_TOKENS = re.compile(
    r"\b("
    r"INSERT|UPDATE|DELETE|MERGE|DROP|ALTER|TRUNCATE|CREATE|GRANT|REVOKE|DENY"
    r"|EXEC\b|EXECUTE\b|xp_cmdshell|sp_configure|OPENROWSET|OPENDATASOURCE"
    r"|BULK\s+INSERT|INTO\s+#|INTO\s+\[|INTO\s+dbo"
    r")\b",
    re.IGNORECASE,
)

_ALLOWED_LEAD = re.compile(
    r"^\s*(SELECT|WITH)\b",
    re.IGNORECASE,
)


def validate_readonly_sql(sql: str) -> Tuple[bool, str]:
    """Return (ok, reason).  ok=True only for pure SELECT / WITH..SELECT."""
    stripped = sql.strip().rstrip(";").strip()
    if not stripped:
        return False, "Empty SQL"

    # Must start with SELECT or WITH
    if not _ALLOWED_LEAD.match(stripped):
        return False, f"SQL must start with SELECT or WITH, got: {stripped[:40]!r}"

    # Scan for forbidden tokens
    m = _FORBIDDEN_TOKENS.search(stripped)
    if m:
        return False, f"Forbidden token detected: {m.group()!r}"

    # Reject multiple statements (semicolons followed by non-whitespace)
    parts = [p.strip() for p in stripped.split(";") if p.strip()]
    if len(parts) > 1:
        return False, "Multiple statements detected; only single SELECT allowed"

    return True, "OK"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class Config:
    """Load SQL Server connection settings from env file or environment."""

    __slots__ = (
        "driver", "host", "port", "database", "user", "password",
        "encrypt", "trust_cert", "timeout",
        "stg_pg_host", "stg_pg_port", "stg_pg_database",
        "stg_pg_user", "stg_pg_password",
    )

    def __init__(self, env_path: Optional[str] = None):
        if env_path:
            self._load_env_file(env_path)

        self.driver = os.getenv("SQLSERVER_DRIVER", "ODBC Driver 17 for SQL Server")
        self.host = os.getenv("SQLSERVER_HOST", "")
        self.port = int(os.getenv("SQLSERVER_PORT", "1433"))
        self.database = os.getenv("SQLSERVER_DATABASE", "")
        self.user = os.getenv("SQLSERVER_USER", "")
        self.password = os.getenv("SQLSERVER_PASSWORD", "")
        self.encrypt = os.getenv("SQLSERVER_ENCRYPT", "no")
        self.trust_cert = os.getenv("SQLSERVER_TRUST_CERT", "yes")
        self.timeout = int(os.getenv("SQLSERVER_TIMEOUT_SECONDS", "20"))

        # Optional PostgreSQL STG for compare-stg
        self.stg_pg_host = os.getenv("STG_PG_HOST", os.getenv("POSTGRES_HOST", ""))
        self.stg_pg_port = int(os.getenv("STG_PG_PORT", os.getenv("POSTGRES_PORT", "5432")))
        self.stg_pg_database = os.getenv("STG_PG_DATABASE", os.getenv("POSTGRES_DB", "torqmind"))
        self.stg_pg_user = os.getenv("STG_PG_USER", os.getenv("POSTGRES_USER", ""))
        self.stg_pg_password = os.getenv("STG_PG_PASSWORD", os.getenv("POSTGRES_PASSWORD", ""))

    @staticmethod
    def _load_env_file(path: str) -> None:
        p = Path(path)
        if not p.is_file():
            log.warning("Env file not found: %s", path)
            return
        for line in p.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            val = val.strip().strip('"').strip("'")
            os.environ.setdefault(key, val)

    def safe_summary(self) -> Dict[str, Any]:
        """Return connection info WITHOUT password."""
        return {
            "driver": self.driver,
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "user": self.user,
            "encrypt": self.encrypt,
            "trust_cert": self.trust_cert,
            "timeout": self.timeout,
        }


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

_CONN_BACKEND: Optional[str] = None  # "pyodbc" | "pymssql"


def _try_pyodbc(cfg: Config):
    """Attempt connection via pyodbc."""
    import pyodbc  # noqa: F811

    host_part = cfg.host
    if not host_part.lower().startswith("tcp:"):
        host_part = f"tcp:{host_part}"

    conn_str = (
        f"DRIVER={{{cfg.driver}}};"
        f"SERVER={host_part},{cfg.port};"
        f"DATABASE={cfg.database};"
        f"UID={cfg.user};"
        f"PWD={cfg.password};"
        f"Encrypt={cfg.encrypt};"
        f"TrustServerCertificate={cfg.trust_cert};"
        f"Connection Timeout={cfg.timeout};"
    )
    return pyodbc.connect(conn_str, timeout=cfg.timeout)


def _try_pymssql(cfg: Config):
    """Attempt connection via pymssql."""
    import pymssql  # noqa: F811

    host = cfg.host.replace("tcp:", "")
    return pymssql.connect(
        server=host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
        login_timeout=cfg.timeout,
        as_dict=True,
    )


def get_connection(cfg: Config):
    """Return a DB-API2 connection.  Tries pyodbc first, then pymssql."""
    global _CONN_BACKEND
    errors: List[str] = []

    # Try pyodbc
    try:
        conn = _try_pyodbc(cfg)
        _CONN_BACKEND = "pyodbc"
        return conn
    except ImportError:
        errors.append("pyodbc not installed (pip install pyodbc)")
    except Exception as exc:
        errors.append(f"pyodbc error: {exc}")

    # Try pymssql
    try:
        conn = _try_pymssql(cfg)
        _CONN_BACKEND = "pymssql"
        return conn
    except ImportError:
        errors.append("pymssql not installed (pip install pymssql)")
    except Exception as exc:
        errors.append(f"pymssql error: {exc}")

    log.error("Cannot connect to SQL Server.  Tried:")
    for e in errors:
        log.error("  - %s", e)
    log.error("Recommended: pip install pyodbc  (requires ODBC Driver 17/18)")
    log.error("Alternative: pip install pymssql")
    sys.exit(1)


def execute_query(conn, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Execute a read-only query and return list of dicts."""
    ok, reason = validate_readonly_sql(sql)
    if not ok:
        raise ValueError(f"SQL rejected (read-only enforcement): {reason}")

    cursor = conn.cursor()
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)

    if cursor.description is None:
        return []

    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()

    result: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            result.append(row)
        else:
            result.append(dict(zip(columns, row)))
    return result


def get_pg_connection(cfg: Config):
    """Return a psycopg2 connection to PostgreSQL STG."""
    try:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(
            host=cfg.stg_pg_host,
            port=cfg.stg_pg_port,
            dbname=cfg.stg_pg_database,
            user=cfg.stg_pg_user,
            password=cfg.stg_pg_password,
        )
        conn.autocommit = True
        return conn
    except ImportError:
        try:
            import psycopg
            conn = psycopg.connect(
                host=cfg.stg_pg_host,
                port=cfg.stg_pg_port,
                dbname=cfg.stg_pg_database,
                user=cfg.stg_pg_user,
                password=cfg.stg_pg_password,
                autocommit=True,
            )
            return conn
        except ImportError:
            log.error("Neither psycopg2 nor psycopg installed.  Cannot connect to STG.")
            sys.exit(1)


def pg_query(conn, sql: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
    """Execute a PostgreSQL query and return list of dicts."""
    cursor = conn.cursor()
    if hasattr(cursor, "execute"):
        cursor.execute(sql, params)
    if cursor.description is None:
        return []
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows]


# ---------------------------------------------------------------------------
# Export helpers
# ---------------------------------------------------------------------------

def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _json_serial(obj: Any) -> Any:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, bytes):
        return obj.hex()
    if isinstance(obj, set):
        return sorted(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def write_json(data: Any, path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, indent=2, default=_json_serial, ensure_ascii=False))
    log.info("Wrote %s", p)


def write_csv(rows: List[Dict[str, Any]], path: str | Path, fieldnames: Optional[List[str]] = None) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        p.write_text("")
        return
    if fieldnames is None:
        fieldnames = list(rows[0].keys())
    with open(p, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            clean = {}
            for k, v in row.items():
                if isinstance(v, (datetime, date)):
                    clean[k] = v.isoformat()
                elif isinstance(v, Decimal):
                    clean[k] = float(v)
                elif isinstance(v, bytes):
                    clean[k] = v.hex()
                else:
                    clean[k] = v
            writer.writerow(clean)
    log.info("Wrote %s (%d rows)", p, len(rows))


def write_md(text: str, path: str | Path) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(text, encoding="utf-8")
    log.info("Wrote %s", p)


def mask_pii(value: Any, col_name: Optional[str] = None) -> Any:
    """Mask PII in a value.  Returns masked string or original."""
    if value is None:
        return None
    s = str(value)
    if not s or len(s) < 4:
        return s

    # Column name hint
    if col_name and col_name.lower() in PII_COLUMN_HINTS:
        if len(s) > 6:
            return s[:3] + "*" * (len(s) - 6) + s[-3:]
        return "***"

    # Pattern-based
    if PII_PATTERNS["chave_nfe"].fullmatch(s):
        return s[:10] + "..." + s[-6:]
    if PII_PATTERNS["cpf"].fullmatch(s.replace(".", "").replace("-", "")):
        clean = re.sub(r"[^\d]", "", s)
        return clean[:3] + ".***.***-" + clean[-2:]
    if PII_PATTERNS["cnpj"].fullmatch(s.replace(".", "").replace("-", "").replace("/", "")):
        clean = re.sub(r"[^\d]", "", s)
        return clean[:2] + ".***.***/****-" + clean[-2:]
    if PII_PATTERNS["email"].fullmatch(s):
        local, _, domain = s.partition("@")
        return local[0] + "***@" + domain[0] + "***" + domain[domain.rfind("."):]

    return s


def mask_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Mask PII in a full row dict."""
    return {k: mask_pii(v, k) for k, v in row.items()}


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: test-connection
# ═══════════════════════════════════════════════════════════════════════════

def cmd_test_connection(cfg: Config, args: argparse.Namespace) -> None:
    log.info("Testing connection to %s/%s ...", cfg.host, cfg.database)
    conn = get_connection(cfg)
    rows = execute_query(conn, "SELECT @@SERVERNAME AS server_name, @@VERSION AS version_info, DB_NAME() AS db_name, SUSER_NAME() AS login_name, GETDATE() AS server_time")
    info = rows[0] if rows else {}
    conn.close()

    log.info("=== Connection OK ===")
    log.info("  Server:   %s", info.get("server_name"))
    log.info("  Database: %s", info.get("db_name"))
    log.info("  Login:    %s", info.get("login_name"))
    log.info("  Time:     %s", info.get("server_time"))
    log.info("  Backend:  %s", _CONN_BACKEND)
    ver = str(info.get("version_info", ""))
    log.info("  Version:  %s", ver[:120])

    # Check permissions
    perms = execute_query(conn if not getattr(conn, "closed", True) else get_connection(cfg),
        "SELECT HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'SELECT') AS can_select, "
        "HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'VIEW DEFINITION') AS can_view_def"
    )
    if perms:
        log.info("  SELECT:        %s", "YES" if perms[0].get("can_select") else "NO")
        log.info("  VIEW DEF:      %s", "YES" if perms[0].get("can_view_def") else "NO")


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: discover-schema
# ═══════════════════════════════════════════════════════════════════════════

_SQL_TABLES = """\
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    t.object_id,
    t.create_date,
    t.modify_date,
    ISNULL(SUM(p.rows), 0) AS row_count,
    CAST(ISNULL(SUM(a.total_pages), 0) * 8.0 / 1024 AS DECIMAL(18,2)) AS reserved_mb,
    CAST(ISNULL(SUM(CASE WHEN a.type = 1 THEN a.used_pages ELSE 0 END), 0) * 8.0 / 1024 AS DECIMAL(18,2)) AS data_mb,
    CAST(ISNULL(SUM(CASE WHEN a.type <> 1 THEN a.used_pages ELSE 0 END), 0) * 8.0 / 1024 AS DECIMAL(18,2)) AS index_mb
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
LEFT JOIN sys.allocation_units a ON p.partition_id = a.container_id
GROUP BY s.name, t.name, t.object_id, t.create_date, t.modify_date
ORDER BY row_count DESC
"""

_SQL_COLUMNS = """\
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    c.name AS column_name,
    tp.name AS data_type,
    c.max_length,
    c.precision,
    c.scale,
    c.is_nullable,
    c.is_identity,
    dc.definition AS default_definition,
    cc.definition AS computed_definition,
    c.collation_name AS collation,
    c.column_id AS ordinal_position
FROM sys.columns c
JOIN sys.tables t ON c.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.types tp ON c.user_type_id = tp.user_type_id
LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
ORDER BY s.name, t.name, c.column_id
"""

_SQL_INDEXES = """\
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    i.name AS index_name,
    i.type_desc AS index_type,
    i.is_unique,
    i.is_primary_key,
    c.name AS column_name,
    ic.key_ordinal,
    ic.is_included_column
FROM sys.indexes i
JOIN sys.tables t ON i.object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE i.name IS NOT NULL
ORDER BY s.name, t.name, i.name, ic.key_ordinal
"""

_SQL_KEYS = """\
SELECT
    s.name AS schema_name,
    t.name AS table_name,
    kc.name AS constraint_name,
    kc.type_desc AS constraint_type,
    col.name AS column_name,
    fks.name AS fk_ref_schema,
    fkt.name AS fk_ref_table,
    fkc_ref.name AS fk_ref_column
FROM sys.key_constraints kc
JOIN sys.tables t ON kc.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.index_columns ic ON kc.unique_index_id = ic.index_id AND kc.parent_object_id = ic.object_id
JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
LEFT JOIN sys.foreign_keys fk ON 1=0
LEFT JOIN sys.tables fkt ON 1=0
LEFT JOIN sys.schemas fks ON 1=0
LEFT JOIN sys.columns fkc_ref ON 1=0
ORDER BY s.name, t.name, kc.name

UNION ALL

SELECT
    s.name AS schema_name,
    t.name AS table_name,
    fk.name AS constraint_name,
    'FOREIGN_KEY' AS constraint_type,
    pc.name AS column_name,
    rs.name AS fk_ref_schema,
    rt.name AS fk_ref_table,
    rc.name AS fk_ref_column
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.tables t ON fk.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
ORDER BY s.name, t.name, fk.name
"""

_SQL_DEPENDENCIES = """\
SELECT
    s.name AS schema_name,
    o.name AS object_name,
    o.type_desc AS object_type,
    ds.name AS ref_schema,
    d.referenced_entity_name AS ref_object,
    d.referenced_minor_name AS ref_column
FROM sys.sql_expression_dependencies d
JOIN sys.objects o ON d.referencing_id = o.object_id
JOIN sys.schemas s ON o.schema_id = s.schema_id
LEFT JOIN sys.schemas ds ON d.referenced_schema_name = ds.name
WHERE o.type IN ('V', 'P', 'FN', 'IF', 'TF', 'TR')
ORDER BY s.name, o.name
"""


def cmd_discover_schema(cfg: Config, args: argparse.Namespace) -> None:
    out = ensure_dir(args.out)
    conn = get_connection(cfg)

    log.info("Discovering tables...")
    tables = execute_query(conn, _SQL_TABLES)
    write_csv(tables, out / "schema_tables.csv")
    write_json(tables, out / "schema_tables.json")

    log.info("Discovering columns...")
    columns = execute_query(conn, _SQL_COLUMNS)
    write_csv(columns, out / "schema_columns.csv")

    log.info("Discovering indexes...")
    raw_idx = execute_query(conn, _SQL_INDEXES)
    # Aggregate index columns into single rows
    idx_map: Dict[str, Dict] = {}
    for r in raw_idx:
        key = f"{r['schema_name']}.{r['table_name']}.{r['index_name']}"
        if key not in idx_map:
            idx_map[key] = {
                "schema_name": r["schema_name"],
                "table_name": r["table_name"],
                "index_name": r["index_name"],
                "index_type": r["index_type"],
                "is_unique": r["is_unique"],
                "is_primary_key": r["is_primary_key"],
                "key_columns": [],
                "included_columns": [],
            }
        if r["is_included_column"]:
            idx_map[key]["included_columns"].append(r["column_name"])
        else:
            idx_map[key]["key_columns"].append(r["column_name"])

    indexes = []
    for v in idx_map.values():
        v["key_columns"] = ", ".join(v["key_columns"])
        v["included_columns"] = ", ".join(v["included_columns"])
        indexes.append(v)
    write_csv(indexes, out / "schema_indexes.csv")

    log.info("Discovering keys and constraints...")
    try:
        keys = execute_query(conn, _SQL_KEYS)
    except Exception:
        # Fallback: simpler query without UNION
        keys = _discover_keys_simple(conn)
    write_csv(keys, out / "schema_keys.csv")

    log.info("Discovering dependencies...")
    try:
        deps = execute_query(conn, _SQL_DEPENDENCIES)
    except Exception:
        deps = []
    write_csv(deps, out / "schema_dependencies.csv")

    # Summary
    md = _build_schema_summary_md(tables, columns, indexes, keys, deps, cfg)
    write_md(md, out / "schema_summary.md")

    conn.close()
    log.info("Schema discovery complete → %s", out)


def _discover_keys_simple(conn) -> List[Dict]:
    """Fallback key discovery without UNION."""
    pks = execute_query(conn, """\
SELECT s.name AS schema_name, t.name AS table_name,
       kc.name AS constraint_name, kc.type_desc AS constraint_type,
       col.name AS column_name,
       NULL AS fk_ref_schema, NULL AS fk_ref_table, NULL AS fk_ref_column
FROM sys.key_constraints kc
JOIN sys.tables t ON kc.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.index_columns ic ON kc.unique_index_id = ic.index_id AND kc.parent_object_id = ic.object_id
JOIN sys.columns col ON ic.object_id = col.object_id AND ic.column_id = col.column_id
ORDER BY s.name, t.name, kc.name""")

    fks = execute_query(conn, """\
SELECT s.name AS schema_name, t.name AS table_name,
       fk.name AS constraint_name, 'FOREIGN_KEY' AS constraint_type,
       pc.name AS column_name,
       rs.name AS fk_ref_schema, rt.name AS fk_ref_table, rc.name AS fk_ref_column
FROM sys.foreign_keys fk
JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
JOIN sys.tables t ON fk.parent_object_id = t.object_id
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.columns pc ON fkc.parent_object_id = pc.object_id AND fkc.parent_column_id = pc.column_id
JOIN sys.tables rt ON fk.referenced_object_id = rt.object_id
JOIN sys.schemas rs ON rt.schema_id = rs.schema_id
JOIN sys.columns rc ON fkc.referenced_object_id = rc.object_id AND fkc.referenced_column_id = rc.column_id
ORDER BY s.name, t.name, fk.name""")

    return pks + fks


def _build_schema_summary_md(tables, columns, indexes, keys, deps, cfg) -> str:
    lines = [
        f"# Schema Summary — {cfg.database}",
        "",
        f"- **Tables**: {len(tables)}",
        f"- **Columns**: {len(columns)}",
        f"- **Indexes**: {len(indexes)}",
        f"- **Keys/Constraints**: {len(keys)}",
        f"- **Dependencies**: {len(deps)}",
        "",
        "## Top 30 tables by row count",
        "",
        "| Schema | Table | Rows | Data MB | Index MB |",
        "|--------|-------|------|---------|----------|",
    ]
    for t in tables[:30]:
        lines.append(
            f"| {t['schema_name']} | {t['table_name']} | "
            f"{t.get('row_count', 0):,} | {t.get('data_mb', 0)} | {t.get('index_mb', 0)} |"
        )
    lines.append("")
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: dump-definitions
# ═══════════════════════════════════════════════════════════════════════════

_SQL_ALL_DEFINITIONS = """\
SELECT
    s.name AS schema_name,
    o.name AS object_name,
    o.type AS object_type_code,
    o.type_desc AS object_type,
    o.create_date,
    o.modify_date,
    m.definition
FROM sys.sql_modules m
JOIN sys.objects o ON m.object_id = o.object_id
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.type IN ('P', 'FN', 'IF', 'TF', 'V', 'TR')
ORDER BY o.type, s.name, o.name
"""

_TYPE_DIRS = {
    "P": "procedures",
    "FN": "functions",
    "IF": "functions",
    "TF": "functions",
    "V": "views",
    "TR": "triggers",
}


def cmd_dump_definitions(cfg: Config, args: argparse.Namespace) -> None:
    out = ensure_dir(args.out)
    conn = get_connection(cfg)

    log.info("Dumping programmability definitions...")
    rows = execute_query(conn, _SQL_ALL_DEFINITIONS)
    conn.close()

    index_rows: List[Dict] = []

    for r in rows:
        type_code = (r.get("object_type_code") or "").strip()
        subdir = _TYPE_DIRS.get(type_code, "other")
        d = ensure_dir(out / subdir)
        fname = f"{r['schema_name']}.{r['object_name']}.sql"
        definition = r.get("definition") or "-- definition not available"
        (d / fname).write_text(definition, encoding="utf-8")

        # Detect referenced tables and keywords
        def_lower = definition.lower() if definition else ""
        refs = set()
        for kw in BUSINESS_KEYWORDS:
            if kw in def_lower:
                refs.add(kw)

        index_rows.append({
            "object_type": r.get("object_type", ""),
            "schema": r["schema_name"],
            "name": r["object_name"],
            "create_date": r.get("create_date"),
            "modify_date": r.get("modify_date"),
            "referenced_keywords": ", ".join(sorted(refs)),
            "file": f"{subdir}/{fname}",
        })

    write_csv(index_rows, out / "definitions_index.csv")
    log.info("Dumped %d definitions → %s", len(rows), out)


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: profile-all-tables
# ═══════════════════════════════════════════════════════════════════════════

def cmd_profile_all_tables(cfg: Config, args: argparse.Namespace) -> None:
    out = ensure_dir(args.out)
    samples_dir = ensure_dir(out / "samples")
    sample_rows = getattr(args, "sample_rows", 50) or 50
    top_values = getattr(args, "top_values", 20) or 20
    conn = get_connection(cfg)

    tables = execute_query(conn, _SQL_TABLES)
    columns = execute_query(conn, _SQL_COLUMNS)

    # Group columns by table
    col_map: Dict[str, List[Dict]] = defaultdict(list)
    for c in columns:
        key = f"{c['schema_name']}.{c['table_name']}"
        col_map[key].append(c)

    profiles: List[Dict] = []

    for idx, tbl in enumerate(tables):
        tkey = f"{tbl['schema_name']}.{tbl['table_name']}"
        row_count = tbl.get("row_count", 0) or 0
        log.info("[%d/%d] Profiling %s (%s rows)...", idx + 1, len(tables), tkey, f"{row_count:,}")

        profile: Dict[str, Any] = {
            "schema": tbl["schema_name"],
            "table": tbl["table_name"],
            "row_count": row_count,
            "data_mb": tbl.get("data_mb"),
            "columns": [],
        }

        tcols = col_map.get(tkey, [])
        date_cols = [c for c in tcols if c["data_type"] in ("date", "datetime", "datetime2", "smalldatetime")]
        num_cols = [c for c in tcols if c["data_type"] in ("int", "bigint", "smallint", "tinyint", "decimal", "numeric", "float", "real", "money", "smallmoney")]
        str_cols = [c for c in tcols if c["data_type"] in ("varchar", "nvarchar", "char", "nchar") and (c.get("max_length") or 0) <= 200]

        # Date ranges
        for dc in date_cols:
            try:
                dr = execute_query(conn,
                    f"SELECT MIN([{dc['column_name']}]) AS min_val, MAX([{dc['column_name']}]) AS max_val, "
                    f"SUM(CASE WHEN [{dc['column_name']}] IS NULL THEN 1 ELSE 0 END) AS null_count "
                    f"FROM [{tbl['schema_name']}].[{tbl['table_name']}]")
                if dr:
                    profile["columns"].append({
                        "column": dc["column_name"],
                        "type": dc["data_type"],
                        "min": dr[0].get("min_val"),
                        "max": dr[0].get("max_val"),
                        "null_count": dr[0].get("null_count"),
                        "null_pct": round(100 * (dr[0].get("null_count") or 0) / max(row_count, 1), 1),
                    })
            except Exception:
                pass

        # Numeric min/max
        for nc in num_cols[:20]:
            try:
                nr = execute_query(conn,
                    f"SELECT MIN([{nc['column_name']}]) AS min_val, MAX([{nc['column_name']}]) AS max_val, "
                    f"SUM(CASE WHEN [{nc['column_name']}] IS NULL THEN 1 ELSE 0 END) AS null_count "
                    f"FROM [{tbl['schema_name']}].[{tbl['table_name']}]")
                if nr:
                    profile["columns"].append({
                        "column": nc["column_name"],
                        "type": nc["data_type"],
                        "min": nr[0].get("min_val"),
                        "max": nr[0].get("max_val"),
                        "null_count": nr[0].get("null_count"),
                        "null_pct": round(100 * (nr[0].get("null_count") or 0) / max(row_count, 1), 1),
                    })
            except Exception:
                pass

        # Top values for small string columns
        for sc in str_cols[:10]:
            try:
                tv = execute_query(conn,
                    f"SELECT TOP {top_values} [{sc['column_name']}] AS val, COUNT(*) AS cnt "
                    f"FROM [{tbl['schema_name']}].[{tbl['table_name']}] "
                    f"WHERE [{sc['column_name']}] IS NOT NULL "
                    f"GROUP BY [{sc['column_name']}] ORDER BY cnt DESC")
                if tv:
                    profile["columns"].append({
                        "column": sc["column_name"],
                        "type": sc["data_type"],
                        "top_values": [{"val": mask_pii(r["val"], sc["column_name"]), "cnt": r["cnt"]} for r in tv],
                    })
            except Exception:
                pass

        # Sample rows
        try:
            sample = execute_query(conn,
                f"SELECT TOP {sample_rows} * FROM [{tbl['schema_name']}].[{tbl['table_name']}]")
            masked = [mask_row(r) for r in sample]
            if masked:
                write_csv(masked, samples_dir / f"{tkey}.csv")
        except Exception:
            pass

        # Classify columns
        profile["probable_empresa_cols"] = [c["column_name"] for c in tcols
            if any(h in c["column_name"].lower() for h in ("empresa", "id_empresa", "filial", "id_filial", "loja"))]
        profile["probable_date_cols"] = [c["column_name"] for c in date_cols]
        profile["probable_value_cols"] = [c["column_name"] for c in num_cols
            if any(h in c["column_name"].lower() for h in ("valor", "total", "preco", "custo", "amount", "vlr"))]
        profile["probable_status_cols"] = [c["column_name"] for c in tcols
            if any(h in c["column_name"].lower() for h in ("status", "situacao", "cancelado", "ativo", "tipo"))]
        profile["probable_user_cols"] = [c["column_name"] for c in tcols
            if any(h in c["column_name"].lower() for h in ("usuario", "operador", "vendedor", "frentista", "funcionario", "id_usuario"))]

        profiles.append(profile)

    conn.close()

    write_json(profiles, out / "table_profiles.json")

    # Flat CSV summary
    flat = []
    for p in profiles:
        flat.append({
            "schema": p["schema"],
            "table": p["table"],
            "row_count": p["row_count"],
            "data_mb": p.get("data_mb"),
            "date_cols": ", ".join(p.get("probable_date_cols", [])),
            "value_cols": ", ".join(p.get("probable_value_cols", [])),
            "status_cols": ", ".join(p.get("probable_status_cols", [])),
            "user_cols": ", ".join(p.get("probable_user_cols", [])),
            "empresa_cols": ", ".join(p.get("probable_empresa_cols", [])),
        })
    write_csv(flat, out / "table_profiles.csv")

    # Summary MD
    md_lines = [
        f"# Table Profiles — {cfg.database}",
        "",
        f"Profiled **{len(profiles)}** tables.",
        "",
        "## Top 20 by row count",
        "",
        "| Table | Rows | Date cols | Value cols | Status cols |",
        "|-------|------|-----------|------------|-------------|",
    ]
    for p in sorted(profiles, key=lambda x: x["row_count"], reverse=True)[:20]:
        md_lines.append(
            f"| {p['schema']}.{p['table']} | {p['row_count']:,} | "
            f"{', '.join(p.get('probable_date_cols', [])[:3])} | "
            f"{', '.join(p.get('probable_value_cols', [])[:3])} | "
            f"{', '.join(p.get('probable_status_cols', [])[:3])} |"
        )
    md_lines.append("")
    write_md("\n".join(md_lines), out / "table_profile_summary.md")
    log.info("Profile complete → %s", out)


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: find-business-objects
# ═══════════════════════════════════════════════════════════════════════════

def _classify_table(table_name: str, col_names: List[str]) -> List[Tuple[str, str, float]]:
    """Return list of (domain, reason, confidence) for a table."""
    matches: List[Tuple[str, str, float]] = []
    tname = table_name.lower()
    cnames_lower = {c.lower() for c in col_names}

    for domain, patterns in DOMAIN_PATTERNS.items():
        score = 0.0
        reasons: List[str] = []
        for pat in patterns:
            if pat in tname:
                score += 0.6
                reasons.append(f"table name contains '{pat}'")
        # Column-based hints
        if domain == "vendas" and cnames_lower & {"id_comprovante", "valor", "total", "data"}:
            score += 0.3
            reasons.append("has sales-like columns")
        if domain == "nfe_nfce" and cnames_lower & {"chaveacesso", "nronf", "status", "serie"}:
            score += 0.3
            reasons.append("has NFe columns")
        if domain == "contas_pagar" and cnames_lower & {"vencimento", "valor", "id_fornecedor"}:
            score += 0.3
            reasons.append("has AP columns")
        if domain == "contas_receber" and cnames_lower & {"vencimento", "valor", "id_cliente"}:
            score += 0.3
            reasons.append("has AR columns")
        if domain == "clientes" and cnames_lower & {"cpf", "cnpj", "nome", "fantasia"}:
            score += 0.3
            reasons.append("has customer columns")
        if domain == "produtos" and cnames_lower & {"descricao", "barras", "ean", "preco"}:
            score += 0.3
            reasons.append("has product columns")
        if domain == "funcionarios" and cnames_lower & {"nome", "matricula", "cargo", "admissao"}:
            score += 0.2
            reasons.append("has employee columns")
        if domain == "turnos" and cnames_lower & {"abertura", "fechamento", "id_caixa", "id_operador"}:
            score += 0.3
            reasons.append("has shift columns")
        if domain == "estoque" and cnames_lower & {"quantidade", "saldo", "id_produto"}:
            score += 0.3
            reasons.append("has inventory columns")

        if score > 0:
            conf = "alta" if score >= 0.8 else "media" if score >= 0.5 else "baixa"
            matches.append((domain, "; ".join(reasons), score))

    return sorted(matches, key=lambda x: -x[2])


def cmd_find_business_objects(cfg: Config, args: argparse.Namespace) -> None:
    out = ensure_dir(args.out)
    conn = get_connection(cfg)

    tables = execute_query(conn, _SQL_TABLES)
    columns = execute_query(conn, _SQL_COLUMNS)
    conn.close()

    col_map: Dict[str, List[str]] = defaultdict(list)
    for c in columns:
        key = f"{c['schema_name']}.{c['table_name']}"
        col_map[key].append(c["column_name"])

    bmap: Dict[str, List[Dict]] = defaultdict(list)
    all_entries: List[Dict] = []

    for tbl in tables:
        tkey = f"{tbl['schema_name']}.{tbl['table_name']}"
        tcols = col_map.get(tkey, [])
        matches = _classify_table(tbl["table_name"], tcols)

        for domain, reason, score in matches:
            conf = "alta" if score >= 0.8 else "media" if score >= 0.5 else "baixa"
            entry = {
                "domain": domain,
                "schema": tbl["schema_name"],
                "table": tbl["table_name"],
                "row_count": tbl.get("row_count", 0),
                "confidence": conf,
                "score": round(score, 2),
                "reason": reason,
                "key_columns": ", ".join(tcols[:10]),
            }
            bmap[domain].append(entry)
            all_entries.append(entry)

    write_json(dict(bmap), out / "business_map.json")

    # MD report
    md = ["# Business Object Map", ""]
    for domain in sorted(bmap.keys()):
        md.append(f"## {domain}")
        md.append("")
        md.append("| Table | Rows | Confidence | Reason |")
        md.append("|-------|------|------------|--------|")
        for e in sorted(bmap[domain], key=lambda x: -x["score"]):
            md.append(f"| {e['schema']}.{e['table']} | {e['row_count']:,} | {e['confidence']} | {e['reason']} |")
        md.append("")

    write_md("\n".join(md), out / "business_map.md")

    # Recommendations
    rec = ["# Recommended Tables for TorqMind", ""]
    high = [e for e in all_entries if e["confidence"] == "alta"]
    med = [e for e in all_entries if e["confidence"] == "media"]
    rec.append(f"## Alta confiança ({len(high)} tabelas)")
    rec.append("")
    for e in sorted(high, key=lambda x: x["domain"]):
        rec.append(f"- **{e['domain']}**: `{e['schema']}.{e['table']}` ({e['row_count']:,} rows) — {e['reason']}")
    rec.append("")
    rec.append(f"## Média confiança ({len(med)} tabelas)")
    rec.append("")
    for e in sorted(med, key=lambda x: x["domain"]):
        rec.append(f"- **{e['domain']}**: `{e['schema']}.{e['table']}` ({e['row_count']:,} rows) — {e['reason']}")
    rec.append("")

    write_md("\n".join(rec), out / "recommended_tables_for_torqmind.md")
    log.info("Business map complete → %s", out)


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: audit-sales-day
# ═══════════════════════════════════════════════════════════════════════════

def _find_table(conn, candidates: List[str]) -> Optional[str]:
    """Find first existing table from candidate names (case-insensitive)."""
    for name in candidates:
        rows = execute_query(conn,
            f"SELECT TOP 1 s.name AS sn, t.name AS tn "
            f"FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE LOWER(t.name) = LOWER('{name}')")
        if rows:
            return f"[{rows[0]['sn']}].[{rows[0]['tn']}]"
    return None


def cmd_audit_sales_day(cfg: Config, args: argparse.Namespace) -> None:
    out = ensure_dir(args.out)
    conn = get_connection(cfg)
    id_filial = args.id_filial
    dt = args.date

    # Resolve table names
    t_comp = _find_table(conn, ["COMPROVANTES", "comprovantes", "Comprovantes"])
    t_itens = _find_table(conn, ["ITENSCOMPROVANTES", "itenscomprovantes", "ItensComprovantes"])
    t_pgto = _find_table(conn, ["FORMAS_PGTO_COMPROVANTES", "formas_pgto_comprovantes", "FormasPgtoComprovantes"])
    t_nfe = _find_table(conn, ["NFE", "nfe", "Nfe"])

    if not t_comp:
        log.error("Table COMPROVANTES not found!")
        conn.close()
        return

    log.info("Tables: comp=%s, itens=%s, pgto=%s, nfe=%s", t_comp, t_itens, t_pgto, t_nfe)

    # Check if REFERENCIA column exists in COMPROVANTES
    comp_cols = execute_query(conn,
        f"SELECT c.name FROM sys.columns c "
        f"JOIN sys.tables t ON c.object_id = t.object_id "
        f"WHERE LOWER(t.name) = 'comprovantes'")
    comp_col_names = {r["name"].upper() for r in comp_cols}
    has_referencia = "REFERENCIA" in comp_col_names

    # Build the ledger query
    # Data vem de COMPROVANTES.DATA (canônica)
    dt_start = dt
    dt_end_obj = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)
    dt_end = dt_end_obj.strftime("%Y-%m-%d")

    ref_col = "c.REFERENCIA" if has_referencia else "NULL"
    localvenda_col = "c.ID_LOCALVENDA" if "ID_LOCALVENDA" in comp_col_names else "NULL"
    cancelado_col = "c.CANCELADO" if "CANCELADO" in comp_col_names else "0"

    # Subqueries for itens aggregation
    itens_join = ""
    itens_cols = "0 AS valor_itens_bruto, 0 AS valor_itens_cfop_venda, 0 AS qtd_itens"
    if t_itens:
        itens_join = f"""
LEFT JOIN (
    SELECT ID_FILIAL, ID_DB, ID_COMPROVANTE,
           SUM(ISNULL(TOTAL, 0)) AS valor_itens_bruto,
           SUM(CASE WHEN ISNULL(CFOP, 0) >= 5000 AND ISNULL(CFOP, 0) < 6000 THEN ISNULL(TOTAL, 0) ELSE ISNULL(TOTAL, 0) END) AS valor_itens_cfop_venda,
           COUNT(*) AS qtd_itens
    FROM {t_itens}
    WHERE ID_FILIAL = '{id_filial}'
    GROUP BY ID_FILIAL, ID_DB, ID_COMPROVANTE
) i ON c.ID_FILIAL = i.ID_FILIAL AND c.ID_DB = i.ID_DB AND c.ID_COMPROVANTE = i.ID_COMPROVANTE"""
        itens_cols = "ISNULL(i.valor_itens_bruto, 0) AS valor_itens_bruto, ISNULL(i.valor_itens_cfop_venda, 0) AS valor_itens_cfop_venda, ISNULL(i.qtd_itens, 0) AS qtd_itens"

    # Subquery for pagamentos aggregation
    pgto_join = ""
    pgto_cols = "0 AS valor_pagamentos, 0 AS qtd_pagamentos"
    if t_pgto:
        # Check if payment table uses REFERENCIA or ID_COMPROVANTE
        pgto_test_cols = execute_query(conn,
            f"SELECT c.name FROM sys.columns c "
            f"JOIN sys.tables t ON c.object_id = t.object_id "
            f"WHERE LOWER(t.name) = 'formas_pgto_comprovantes'")
        pgto_col_names = {r["name"].upper() for r in pgto_test_cols}

        if "ID_REFERENCIA" in pgto_col_names and has_referencia:
            pgto_join_cond = "c.REFERENCIA = p.ID_REFERENCIA"
        elif "ID_COMPROVANTE" in pgto_col_names:
            pgto_join_cond = "c.ID_FILIAL = p.ID_FILIAL AND c.ID_DB = p.ID_DB AND c.ID_COMPROVANTE = p.ID_COMPROVANTE"
        else:
            pgto_join_cond = "1=0"

        pgto_join = f"""
LEFT JOIN (
    SELECT {'ID_REFERENCIA' if 'ID_REFERENCIA' in pgto_col_names else 'ID_FILIAL, ID_DB, ID_COMPROVANTE'},
           SUM(ISNULL(VALOR, 0)) AS valor_pagamentos,
           COUNT(*) AS qtd_pagamentos
    FROM {t_pgto}
    WHERE ID_FILIAL = '{id_filial}'
    GROUP BY {'ID_REFERENCIA' if 'ID_REFERENCIA' in pgto_col_names else 'ID_FILIAL, ID_DB, ID_COMPROVANTE'}
) p ON {pgto_join_cond}"""
        pgto_cols = "ISNULL(p.valor_pagamentos, 0) AS valor_pagamentos, ISNULL(p.qtd_pagamentos, 0) AS qtd_pagamentos"

    # NFE subquery
    nfe_join = ""
    nfe_cols = "NULL AS nfe_status, NULL AS nronf, NULL AS chaveacesso"
    if t_nfe:
        nfe_test_cols = execute_query(conn,
            f"SELECT c.name FROM sys.columns c "
            f"JOIN sys.tables t ON c.object_id = t.object_id "
            f"WHERE LOWER(t.name) = 'nfe'")
        nfe_col_names = {r["name"].upper() for r in nfe_test_cols}
        nfe_status_col = "n.STATUS" if "STATUS" in nfe_col_names else "NULL"
        nfe_nronf_col = "n.NRONF" if "NRONF" in nfe_col_names else "NULL"
        nfe_chave_col = "n.CHAVEACESSO" if "CHAVEACESSO" in nfe_col_names else "NULL"

        nfe_join = f"""
LEFT JOIN {t_nfe} n ON c.ID_FILIAL = n.ID_FILIAL AND c.ID_DB = n.ID_DB AND c.ID_COMPROVANTE = n.ID_COMPROVANTE"""
        nfe_cols = f"{nfe_status_col} AS nfe_status, {nfe_nronf_col} AS nronf, {nfe_chave_col} AS chaveacesso"

    sql = f"""\
SELECT
    c.ID_FILIAL AS id_filial,
    c.ID_DB AS id_db,
    c.ID_COMPROVANTE AS id_comprovante,
    c.DATA AS data,
    c.SITUACAO AS situacao,
    {cancelado_col} AS cancelado,
    {ref_col} AS referencia,
    c.ID_TURNO AS id_turno,
    c.ID_USUARIO AS id_usuario,
    {localvenda_col} AS id_localvenda,
    ISNULL(c.TOTAL, 0) AS valor_header,
    {itens_cols},
    {pgto_cols},
    {nfe_cols},
    CASE
        WHEN c.SITUACAO = 3 THEN 0
        WHEN {cancelado_col} = 1 THEN 0
        ELSE 1
    END AS commercial_eligible,
    CASE
        WHEN c.SITUACAO = 3 THEN 'situacao=3'
        WHEN {cancelado_col} = 1 THEN 'cancelado=1'
        ELSE ''
    END AS motivo_exclusao
FROM {t_comp} c
{itens_join}
{pgto_join}
{nfe_join}
WHERE c.ID_FILIAL = '{id_filial}'
  AND c.DATA >= '{dt_start}'
  AND c.DATA < '{dt_end}'
ORDER BY c.DATA, c.ID_COMPROVANTE
"""

    log.info("Querying sales ledger for filial %s, date %s ...", id_filial, dt)
    ledger = execute_query(conn, sql)

    # Mask chaveacesso in ledger
    for row in ledger:
        if row.get("chaveacesso"):
            row["chaveacesso"] = mask_pii(row["chaveacesso"], "chaveacesso")

    write_csv(ledger, out / "sales_day_ledger.csv")
    write_json(ledger, out / "sales_day_ledger.json")

    # Summary
    total = len(ledger)
    eligible = sum(1 for r in ledger if r.get("commercial_eligible"))
    cancelled = sum(1 for r in ledger if r.get("cancelado") == 1)
    sit3 = sum(1 for r in ledger if r.get("situacao") == 3)
    nfe5 = sum(1 for r in ledger if r.get("nfe_status") == 5)
    fat_itens = sum(float(r.get("valor_itens_cfop_venda", 0) or 0) for r in ledger if r.get("commercial_eligible"))
    total_header = sum(float(r.get("valor_header", 0) or 0) for r in ledger if r.get("commercial_eligible"))
    total_pgto = sum(float(r.get("valor_pagamentos", 0) or 0) for r in ledger if r.get("commercial_eligible"))

    summary = {
        "id_filial": id_filial,
        "date": dt,
        "total_comprovantes": total,
        "total_elegiveis": eligible,
        "total_cancelados": cancelled,
        "total_situacao_3": sit3,
        "total_nfe_status_5": nfe5,
        "faturamento_itens_cfop_venda": round(fat_itens, 2),
        "total_header": round(total_header, 2),
        "total_pagamentos": round(total_pgto, 2),
        "divergencia_header_vs_itens": round(total_header - fat_itens, 2),
        "divergencia_itens_vs_pagamentos": round(fat_itens - total_pgto, 2),
    }
    write_json(summary, out / "sales_day_summary.json")

    md = [
        f"# Sales Day Audit — Filial {id_filial} — {dt}",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
    ]
    for k, v in summary.items():
        md.append(f"| {k} | {v} |")
    md.append("")
    write_md("\n".join(md), out / "sales_day_report.md")

    conn.close()
    log.info("Sales audit complete → %s", out)


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: compare-stg-sales-day
# ═══════════════════════════════════════════════════════════════════════════

def cmd_compare_stg_sales_day(cfg: Config, args: argparse.Namespace) -> None:
    out = ensure_dir(args.out)
    id_filial = args.id_filial
    dt = args.date
    dt_end_obj = datetime.strptime(dt, "%Y-%m-%d") + timedelta(days=1)
    dt_end = dt_end_obj.strftime("%Y-%m-%d")

    # 1. Get source data from SQL Server
    log.info("Fetching source data from SQL Server...")
    conn_src = get_connection(cfg)

    t_comp = _find_table(conn_src, ["COMPROVANTES", "comprovantes"])
    if not t_comp:
        log.error("COMPROVANTES not found in source")
        return

    source_rows = execute_query(conn_src, f"""\
SELECT ID_FILIAL, ID_DB, ID_COMPROVANTE, DATA, SITUACAO,
       ISNULL(CANCELADO, 0) AS CANCELADO, ISNULL(TOTAL, 0) AS TOTAL
FROM {t_comp}
WHERE ID_FILIAL = '{id_filial}' AND DATA >= '{dt}' AND DATA < '{dt_end}'
""")
    conn_src.close()

    source_keys = {}
    for r in source_rows:
        key = (str(r.get("ID_FILIAL")), str(r.get("ID_DB")), str(r.get("ID_COMPROVANTE")))
        source_keys[key] = r

    # 2. Get STG data from PostgreSQL
    log.info("Fetching STG data from PostgreSQL...")
    if not cfg.stg_pg_host:
        log.error("STG PostgreSQL not configured (STG_PG_HOST / POSTGRES_HOST)")
        return

    pg_conn = get_pg_connection(cfg)
    stg_rows = pg_query(pg_conn, """\
SELECT id_filial::text, id_db::text, id_comprovante::text,
       (dt_evento AT TIME ZONE 'America/Sao_Paulo')::timestamp AS data,
       situacao_shadow AS situacao,
       CASE WHEN cancelado_shadow THEN 1 ELSE 0 END AS cancelado,
       COALESCE(valor_total_shadow, 0) AS total
FROM stg.comprovantes
WHERE id_filial = %s
  AND (dt_evento AT TIME ZONE 'America/Sao_Paulo')::date >= %s::date
  AND (dt_evento AT TIME ZONE 'America/Sao_Paulo')::date < %s::date
""", (str(id_filial), dt, dt_end))
    pg_conn.close()

    stg_keys = {}
    for r in stg_rows:
        key = (str(r.get("id_filial")), str(r.get("id_db")), str(r.get("id_comprovante")))
        stg_keys[key] = r

    # 3. Compare
    source_only = []
    stg_only = []
    value_mismatch = []
    status_mismatch = []

    for key, src in source_keys.items():
        if key not in stg_keys:
            source_only.append({"id_filial": key[0], "id_db": key[1], "id_comprovante": key[2],
                                "source_total": src.get("TOTAL"), "source_situacao": src.get("SITUACAO")})
        else:
            stg = stg_keys[key]
            src_total = float(src.get("TOTAL") or 0)
            stg_total = float(stg.get("total") or 0)
            if abs(src_total - stg_total) > 0.01:
                value_mismatch.append({
                    "id_filial": key[0], "id_db": key[1], "id_comprovante": key[2],
                    "source_total": src_total, "stg_total": stg_total,
                    "diff": round(src_total - stg_total, 2),
                })
            src_sit = src.get("SITUACAO")
            stg_sit = stg.get("situacao")
            src_canc = src.get("CANCELADO")
            stg_canc = stg.get("cancelado")
            if str(src_sit) != str(stg_sit) or str(src_canc) != str(stg_canc):
                status_mismatch.append({
                    "id_filial": key[0], "id_db": key[1], "id_comprovante": key[2],
                    "source_situacao": src_sit, "stg_situacao": stg_sit,
                    "source_cancelado": src_canc, "stg_cancelado": stg_canc,
                })

    for key in stg_keys:
        if key not in source_keys:
            stg_r = stg_keys[key]
            stg_only.append({"id_filial": key[0], "id_db": key[1], "id_comprovante": key[2],
                             "stg_total": stg_r.get("total"), "stg_situacao": stg_r.get("situacao")})

    write_csv(source_only, out / "source_only.csv")
    write_csv(stg_only, out / "stg_only.csv")
    write_csv(value_mismatch, out / "value_mismatch.csv")
    write_csv(status_mismatch, out / "status_mismatch.csv")

    summary = {
        "id_filial": id_filial,
        "date": dt,
        "source_count": len(source_keys),
        "stg_count": len(stg_keys),
        "source_only": len(source_only),
        "stg_only": len(stg_only),
        "value_mismatch": len(value_mismatch),
        "status_mismatch": len(status_mismatch),
    }
    write_json(summary, out / "compare_summary.json")

    md = [
        f"# Compare Source vs STG — Filial {id_filial} — {dt}",
        "",
        f"| Metric | Count |",
        f"|--------|-------|",
        f"| Source records | {len(source_keys)} |",
        f"| STG records | {len(stg_keys)} |",
        f"| Source only (missing in STG) | {len(source_only)} |",
        f"| STG only (missing in source) | {len(stg_only)} |",
        f"| Value mismatch | {len(value_mismatch)} |",
        f"| Status mismatch | {len(status_mismatch)} |",
        "",
        "## Diagnosis",
        "",
        f"- **source_only** ({len(source_only)}): Agent/API/STG falhou ou filtro do Agent está errado.",
        f"- **stg_only** ({len(stg_only)}): janela/data/fonte divergente ou dado antigo na STG.",
        f"- **value_mismatch** ({len(value_mismatch)}): valor diverge entre fonte e STG.",
        f"- **status_mismatch** ({len(status_mismatch)}): situacao/cancelado diverge.",
        "",
    ]
    write_md("\n".join(md), out / "compare_report.md")
    log.info("Compare complete → %s", out)


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: finance-discovery
# ═══════════════════════════════════════════════════════════════════════════

def cmd_finance_discovery(cfg: Config, args: argparse.Namespace) -> None:
    out = ensure_dir(args.out)
    conn = get_connection(cfg)

    tables = execute_query(conn, _SQL_TABLES)
    columns = execute_query(conn, _SQL_COLUMNS)
    conn.close()

    col_map: Dict[str, List[Dict]] = defaultdict(list)
    for c in columns:
        key = f"{c['schema_name']}.{c['table_name']}"
        col_map[key].append(c)

    candidates: List[Dict] = []
    for tbl in tables:
        tkey = f"{tbl['schema_name']}.{tbl['table_name']}"
        tname = tbl["table_name"].lower()
        tcols = col_map.get(tkey, [])
        tcol_names = [c["column_name"].lower() for c in tcols]

        score = 0
        reasons: List[str] = []
        for kw in FINANCE_KEYWORDS:
            if kw in tname:
                score += 2
                reasons.append(f"table name has '{kw}'")
            col_hits = [cn for cn in tcol_names if kw in cn]
            if col_hits:
                score += len(col_hits) * 0.5
                reasons.append(f"columns with '{kw}': {', '.join(col_hits[:3])}")

        if score > 0:
            candidates.append({
                "schema": tbl["schema_name"],
                "table": tbl["table_name"],
                "row_count": tbl.get("row_count", 0),
                "score": round(score, 1),
                "reasons": "; ".join(reasons),
                "columns": ", ".join(tcol_names[:15]),
            })

    candidates.sort(key=lambda x: -x["score"])
    write_csv(candidates, out / "finance_tables_candidates.csv")

    # Relationships MD
    md = [
        "# Finance Discovery Report",
        "",
        f"Found **{len(candidates)}** finance-related tables.",
        "",
        "## Candidate tables (sorted by relevance)",
        "",
        "| Table | Rows | Score | Key reasons |",
        "|-------|------|-------|-------------|",
    ]
    for c in candidates[:30]:
        md.append(f"| {c['schema']}.{c['table']} | {c['row_count']:,} | {c['score']} | {c['reasons'][:80]} |")
    md.append("")

    # Analysis
    cp = [c for c in candidates if any(k in c["table"].lower() for k in ("contaspagar", "contas_pagar", "cp_"))]
    cr = [c for c in candidates if any(k in c["table"].lower() for k in ("contasreceber", "contas_receber", "cr_"))]

    md.append("## Contas a Pagar (AP)")
    md.append("")
    if cp:
        for c in cp:
            md.append(f"- `{c['schema']}.{c['table']}` ({c['row_count']:,} rows) — {c['reasons'][:100]}")
    else:
        md.append("- Não encontrado com nome explícito. Verificar manualmente tabelas com 'fornecedor' + 'vencimento'.")
    md.append("")

    md.append("## Contas a Receber (AR)")
    md.append("")
    if cr:
        for c in cr:
            md.append(f"- `{c['schema']}.{c['table']}` ({c['row_count']:,} rows) — {c['reasons'][:100]}")
    else:
        md.append("- Não encontrado com nome explícito. Verificar manualmente tabelas com 'cliente' + 'vencimento'.")
    md.append("")

    md.append("## Next steps for ETL")
    md.append("")
    md.append("- Mapear campos: vencimento, valor_original, valor_pago, data_baixa, status")
    md.append("- Identificar como ligar entidade/fornecedor/cliente via FK")
    md.append("- Identificar como ligar filial (id_filial / id_empresa)")
    md.append("- Calcular fluxo de caixa previsto: vencidos + a vencer por faixa de data")
    md.append("")

    write_md("\n".join(md), out / "finance_relationships.md")

    # Field dictionary
    fd = ["# Finance Field Dictionary", ""]
    for c in candidates[:15]:
        fd.append(f"## {c['schema']}.{c['table']}")
        fd.append(f"- Rows: {c['row_count']:,}")
        fd.append(f"- Columns: {c['columns']}")
        fd.append("")

    write_md("\n".join(fd), out / "finance_field_dictionary.md")
    write_md("# Finance ETL Plan\n\nGenerated by Xpert Source Explorer.\n\nSee finance_relationships.md for details.\n", out / "finance_next_etl_plan.md")
    log.info("Finance discovery complete → %s", out)


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: customers-discovery
# ═══════════════════════════════════════════════════════════════════════════

def cmd_customers_discovery(cfg: Config, args: argparse.Namespace) -> None:
    out = ensure_dir(args.out)
    conn = get_connection(cfg)

    tables = execute_query(conn, _SQL_TABLES)
    columns = execute_query(conn, _SQL_COLUMNS)
    conn.close()

    col_map: Dict[str, List[Dict]] = defaultdict(list)
    for c in columns:
        key = f"{c['schema_name']}.{c['table_name']}"
        col_map[key].append(c)

    candidates: List[Dict] = []
    for tbl in tables:
        tkey = f"{tbl['schema_name']}.{tbl['table_name']}"
        tname = tbl["table_name"].lower()
        tcols = col_map.get(tkey, [])
        tcol_names = [c["column_name"].lower() for c in tcols]

        score = 0
        reasons: List[str] = []
        for kw in CUSTOMER_KEYWORDS:
            if kw in tname:
                score += 2
                reasons.append(f"table name has '{kw}'")
            col_hits = [cn for cn in tcol_names if kw in cn]
            if col_hits:
                score += len(col_hits) * 0.5
                reasons.append(f"columns: {', '.join(col_hits[:3])}")

        if score > 0:
            candidates.append({
                "schema": tbl["schema_name"],
                "table": tbl["table_name"],
                "row_count": tbl.get("row_count", 0),
                "score": round(score, 1),
                "reasons": "; ".join(reasons),
                "columns": ", ".join(tcol_names[:15]),
            })

    candidates.sort(key=lambda x: -x["score"])
    write_csv(candidates, out / "customers_tables_candidates.csv")

    md = ["# Customers Discovery Report", ""]
    md.append(f"Found **{len(candidates)}** customer-related tables.")
    md.append("")
    md.append("| Table | Rows | Score | Reasons |")
    md.append("|-------|------|-------|---------|")
    for c in candidates[:20]:
        md.append(f"| {c['schema']}.{c['table']} | {c['row_count']:,} | {c['score']} | {c['reasons'][:80]} |")
    md.append("")
    write_md("\n".join(md), out / "customers_field_dictionary.md")

    enrich = ["# Customer Enrichment Plan", "", "## PII columns to mask in exports", ""]
    enrich.append("- CPF/CNPJ: mask center digits")
    enrich.append("- Email: mask local part")
    enrich.append("- Telefone: mask middle digits")
    enrich.append("- Endereço: mask street number")
    enrich.append("")
    enrich.append("## Enrichment opportunities")
    enrich.append("- Link customers to comprovantes via ID_CLIENTE or ENTIDADE")
    enrich.append("- Calculate recência, frequência, valor (RFV)")
    enrich.append("- Detect churn: last purchase > 60 days")
    enrich.append("- Detect convênio/limite usage")
    enrich.append("")
    write_md("\n".join(enrich), out / "customers_enrichment_plan.md")
    log.info("Customers discovery complete → %s", out)


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: nfe-discovery
# ═══════════════════════════════════════════════════════════════════════════

def cmd_nfe_discovery(cfg: Config, args: argparse.Namespace) -> None:
    out = ensure_dir(args.out)
    conn = get_connection(cfg)

    tables = execute_query(conn, _SQL_TABLES)
    columns = execute_query(conn, _SQL_COLUMNS)

    col_map: Dict[str, List[Dict]] = defaultdict(list)
    for c in columns:
        key = f"{c['schema_name']}.{c['table_name']}"
        col_map[key].append(c)

    # Find NFE-related tables
    nfe_tables: List[Dict] = []
    for tbl in tables:
        tname = tbl["table_name"].lower()
        if any(kw in tname for kw in NFE_KEYWORDS):
            nfe_tables.append(tbl)

    # Status summary for main NFE table
    t_nfe = _find_table(conn, ["NFE", "nfe"])
    status_summary: List[Dict] = []
    if t_nfe:
        try:
            status_summary = execute_query(conn, f"""\
SELECT STATUS, COUNT(*) AS cnt,
       MIN(DATA) AS min_data, MAX(DATA) AS max_data
FROM {t_nfe}
GROUP BY STATUS
ORDER BY STATUS""")
        except Exception:
            pass

    conn.close()

    # Schema report
    md = [
        "# NFE Discovery Report",
        "",
        f"Found **{len(nfe_tables)}** NFE-related tables.",
        "",
        "## NFE tables",
        "",
        "| Table | Rows |",
        "|-------|------|",
    ]
    for t in nfe_tables:
        md.append(f"| {t['schema_name']}.{t['table_name']} | {t.get('row_count', 0):,} |")
    md.append("")

    if status_summary:
        md.append("## Status distribution (main NFE table)")
        md.append("")
        md.append("| Status | Count | Min DATA | Max DATA | Interpretation |")
        md.append("|--------|-------|----------|----------|----------------|")
        interpretations = {
            "3": "Autorizado",
            "4": "Cancelado real",
            "5": "Inutilizado (NÃO é venda, NÃO é fraude, NÃO é cancelamento)",
        }
        for s in status_summary:
            st = str(s.get("STATUS", ""))
            interp = interpretations.get(st, "Verificar")
            md.append(f"| {st} | {s.get('cnt', 0):,} | {s.get('min_data', '')} | {s.get('max_data', '')} | {interp} |")
        md.append("")

    md.append("## Canonical rules")
    md.append("")
    md.append("- STATUS=3: autorizado")
    md.append("- STATUS=4: cancelado real")
    md.append("- STATUS=5: **inutilização fiscal** — NÃO é venda, NÃO é fraude, NÃO é cancelamento")
    md.append("- **DATA** é a data relevante para filtros")
    md.append("- **DATAREPL** é informativo, **NUNCA usar como filtro/watermark**")
    md.append("- CHAVEACESSO: chave de acesso (44 dígitos)")
    md.append("- NRONF: número da nota")
    md.append("")

    write_md("\n".join(md), out / "nfe_schema_report.md")
    write_csv(status_summary, out / "nfe_status_summary.csv")

    # Relationships
    rel = ["# NFE Relationships", ""]
    for t in nfe_tables:
        tkey = f"{t['schema_name']}.{t['table_name']}"
        tcols = col_map.get(tkey, [])
        rel.append(f"## {tkey}")
        rel.append(f"- Columns: {', '.join(c['column_name'] for c in tcols[:20])}")
        rel.append("")
    write_md("\n".join(rel), out / "nfe_relationships.md")
    log.info("NFE discovery complete → %s", out)


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: query
# ═══════════════════════════════════════════════════════════════════════════

def cmd_query(cfg: Config, args: argparse.Namespace) -> None:
    out = ensure_dir(args.out)
    sql_file = Path(args.sql_file)

    if not sql_file.is_file():
        log.error("SQL file not found: %s", sql_file)
        sys.exit(1)

    sql = sql_file.read_text(encoding="utf-8").strip()
    ok, reason = validate_readonly_sql(sql)
    if not ok:
        log.error("SQL rejected: %s", reason)
        sys.exit(1)

    # Log the SQL (without passwords)
    write_md(f"```sql\n{sql}\n```\n", out / "executed_query.md")

    conn = get_connection(cfg)
    log.info("Executing query from %s ...", sql_file)
    rows = execute_query(conn, sql)
    conn.close()

    write_csv(rows, out / "query_result.csv")
    write_json(rows, out / "query_result.json")
    log.info("Query returned %d rows → %s", len(rows), out)


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: full-discovery
# ═══════════════════════════════════════════════════════════════════════════

def cmd_full_discovery(cfg: Config, args: argparse.Namespace) -> None:
    base_out = ensure_dir(args.out)

    steps = [
        ("test-connection", cmd_test_connection),
        ("discover-schema", cmd_discover_schema),
        ("dump-definitions", cmd_dump_definitions),
        ("profile-all-tables", cmd_profile_all_tables),
        ("find-business-objects", cmd_find_business_objects),
        ("finance-discovery", cmd_finance_discovery),
        ("customers-discovery", cmd_customers_discovery),
        ("nfe-discovery", cmd_nfe_discovery),
    ]

    results: Dict[str, str] = {}

    for name, fn in steps:
        log.info("=" * 60)
        log.info("Running: %s", name)
        log.info("=" * 60)

        sub_args = argparse.Namespace()
        sub_args.out = str(base_out / name.replace("-", "_"))
        sub_args.env = getattr(args, "env", None)
        sub_args.sample_rows = 50
        sub_args.top_values = 20

        try:
            fn(cfg, sub_args)
            results[name] = "OK"
        except Exception as exc:
            log.error("FAILED: %s — %s", name, exc)
            traceback.print_exc()
            results[name] = f"FAIL: {exc}"

    # Master report
    md = [
        "# XPERT SOURCE DISCOVERY REPORT",
        "",
        f"Database: **{cfg.database}** @ {cfg.host}",
        f"User: {cfg.user}",
        f"Backend: {_CONN_BACKEND}",
        "",
        "## Step results",
        "",
        "| Step | Status |",
        "|------|--------|",
    ]
    for name, status in results.items():
        icon = "✅" if status == "OK" else "❌"
        md.append(f"| {name} | {icon} {status} |")
    md.append("")

    # Load sub-reports for summary
    schema_path = base_out / "discover_schema" / "schema_tables.json"
    if schema_path.is_file():
        schema_data = json.loads(schema_path.read_text())
        md.append(f"## Schema overview")
        md.append(f"- Tables: {len(schema_data)}")
        md.append(f"- Largest: {schema_data[0]['table_name'] if schema_data else 'N/A'} ({schema_data[0].get('row_count', 0):,} rows)" if schema_data else "")
        md.append("")

    md.append("## Next steps")
    md.append("")
    md.append("1. Review business_map.md for table classification")
    md.append("2. Run audit-sales-day for a specific date")
    md.append("3. Run compare-stg-sales-day to validate Agent/STG sync")
    md.append("4. Review finance and customer tables for new ETL pipelines")
    md.append("")

    write_md("\n".join(md), base_out / "XPERT_SOURCE_DISCOVERY_REPORT.md")
    log.info("Full discovery complete → %s", base_out)


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: test-stg-connection
# ═══════════════════════════════════════════════════════════════════════════

def cmd_test_stg_connection(cfg: Config, args: argparse.Namespace) -> None:
    """Test PostgreSQL STG connection."""
    log.info("Testing STG PostgreSQL connection to %s/%s ...", cfg.stg_pg_host, cfg.stg_pg_database)
    if not cfg.stg_pg_host:
        log.error("STG_PG_HOST not configured. Set STG_PG_HOST or POSTGRES_HOST in env.")
        sys.exit(1)
    conn = get_pg_connection(cfg)
    info = pg_query(conn, "SELECT current_database() AS db, current_user AS usr, now() AS server_time")
    if info:
        log.info("=== STG Connection OK ===")
        log.info("  Database: %s", info[0].get("db"))
        log.info("  User:     %s", info[0].get("usr"))
        log.info("  Time:     %s", info[0].get("server_time"))

    # Count key tables
    for tbl in ["stg.comprovantes", "stg.nfe", "stg.itenscomprovantes", "stg.formas_pgto_comprovantes"]:
        try:
            cnt = pg_query(conn, f"SELECT COUNT(*) AS cnt FROM {tbl}")
            log.info("  %s: %s rows", tbl, f"{cnt[0]['cnt']:,}" if cnt else "N/A")
        except Exception as exc:
            log.warning("  %s: %s", tbl, exc)
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# Subcommand: compare-stg-comprovantes-range
# ═══════════════════════════════════════════════════════════════════════════

def classify_comprovante(situacao, cancelado, nfe_status) -> str:
    """Classify a comprovante for commercial analysis."""
    sit = int(situacao or 0)
    canc = int(cancelado or 0)
    nfe_st = int(nfe_status) if nfe_status is not None else None

    if sit == 3:
        return "situacao_3_ignorada"
    if canc == 1 and nfe_st == 5:
        return "nfe_inutilizada"
    if canc == 1 and nfe_st == 4:
        return "cancelamento_real"
    if canc == 1:
        return "cancelado_sem_nfe_cancelamento"
    return "comercial"


def is_commercial(situacao, cancelado) -> bool:
    sit = int(situacao or 0)
    canc = int(cancelado or 0)
    return sit != 3 and canc == 0


def _get_sp_today():
    """Get today's date in America/Sao_Paulo timezone."""
    try:
        import zoneinfo
        tz = zoneinfo.ZoneInfo("America/Sao_Paulo")
        return datetime.now(tz).date()
    except (ImportError, KeyError):
        # Python < 3.9 or tzdata not available
        return date.today()


# ═══════════════════════════════════════════════════════════════════════════
# Shared comparison helpers
# ═══════════════════════════════════════════════════════════════════════════


def _compare_docs(source_docs: List[Dict], stg_docs: List[Dict]) -> Dict[str, List[Dict]]:
    """Compare by key (id_filial, id_db, id_comprovante). Returns categorized dicts."""
    src_map = {(d["id_filial"], d["id_db"], d["id_comprovante"]): d for d in source_docs}
    stg_map = {(d["id_filial"], d["id_db"], d["id_comprovante"]): d for d in stg_docs}

    source_only = []
    stg_only = []
    total_mismatch = []
    status_mismatch = []
    nfe_mismatch = []
    nfe_missing_in_stg = []
    nfe_missing_in_source = []
    classification_mismatch = []

    all_keys = set(src_map.keys()) | set(stg_map.keys())

    for key in all_keys:
        src = src_map.get(key)
        stg = stg_map.get(key)

        if src and not stg:
            source_only.append(src)
            continue
        if stg and not src:
            stg_only.append(stg)
            continue

        # Both exist
        if abs(src["total_header"] - stg["total_header"]) > 0.01:
            total_mismatch.append({
                **src,
                "stg_total_header": stg["total_header"],
                "diff": round(src["total_header"] - stg["total_header"], 2),
            })

        src_sit = int(src.get("situacao") or 0)
        stg_sit = int(stg.get("situacao") or 0)
        src_canc = int(src.get("cancelado") or 0)
        stg_canc = int(stg.get("cancelado") or 0)
        if src_sit != stg_sit or src_canc != stg_canc:
            status_mismatch.append({
                **src,
                "stg_situacao": stg.get("situacao"),
                "stg_cancelado": stg.get("cancelado"),
            })

        src_nfe = src.get("nfe_status")
        stg_nfe = stg.get("nfe_status")
        if src_nfe is not None and stg_nfe is not None:
            if int(src_nfe) != int(stg_nfe):
                nfe_mismatch.append({
                    **src,
                    "stg_nfe_status": stg_nfe,
                })
        elif src_nfe is not None and stg_nfe is None:
            nfe_missing_in_stg.append(src)
        elif src_nfe is None and stg_nfe is not None:
            nfe_missing_in_source.append({**stg, "note": "nfe_exists_only_in_stg"})

        if src["classification"] != stg["classification"]:
            classification_mismatch.append({
                **src,
                "stg_classification": stg["classification"],
            })

    return {
        "source_only": source_only,
        "stg_only": stg_only,
        "total_mismatch": total_mismatch,
        "status_mismatch": status_mismatch,
        "nfe_mismatch": nfe_mismatch,
        "nfe_missing_in_stg": nfe_missing_in_stg,
        "nfe_missing_in_source": nfe_missing_in_source,
        "classification_mismatch": classification_mismatch,
    }


def _compute_day_summaries(
    source_docs: List[Dict],
    stg_docs: List[Dict],
    source_only: List[Dict],
    stg_only: List[Dict],
    total_mismatch: List[Dict],
    status_mismatch: List[Dict],
    nfe_mismatch: List[Dict],
) -> List[Dict]:
    """Compute day-by-day summary stats."""
    all_days = sorted(set(
        [d["data_dia"] for d in source_docs] +
        [d["data_dia"] for d in stg_docs]
    ))

    day_summaries = []
    for dia in all_days:
        src_day = [d for d in source_docs if d["data_dia"] == dia]
        stg_day = [d for d in stg_docs if d["data_dia"] == dia]

        src_comercial = [d for d in src_day if d["commercial_eligible"] == 1]
        stg_comercial = [d for d in stg_day if d["commercial_eligible"] == 1]
        src_sit3 = [d for d in src_day if d["classification"] == "situacao_3_ignorada"]
        stg_sit3 = [d for d in stg_day if d["classification"] == "situacao_3_ignorada"]
        src_canc = [d for d in src_day if d["cancelado"] == 1]
        stg_canc = [d for d in stg_day if d["cancelado"] == 1]
        src_inut = [d for d in src_day if d["classification"] == "nfe_inutilizada"]
        stg_inut = [d for d in stg_day if d["classification"] == "nfe_inutilizada"]

        day_source_only = [d for d in source_only if d["data_dia"] == dia]
        day_stg_only = [d for d in stg_only if d["data_dia"] == dia]
        day_total_mismatch = [d for d in total_mismatch if d["data_dia"] == dia]
        day_status_mismatch = [d for d in status_mismatch if d["data_dia"] == dia]
        day_nfe_mismatch = [d for d in nfe_mismatch if d["data_dia"] == dia]

        count_delta = len(src_day) - len(stg_day)
        src_total_all = sum(d["total_header"] for d in src_day)
        stg_total_all = sum(d["total_header"] for d in stg_day)
        src_total_comercial = sum(d["total_header"] for d in src_comercial)
        stg_total_comercial = sum(d["total_header"] for d in stg_comercial)

        day_pass = (
            count_delta == 0
            and abs(src_total_all - stg_total_all) < 0.01
            and len(day_source_only) == 0
            and len(day_stg_only) == 0
            and len(day_total_mismatch) == 0
            and len(day_status_mismatch) == 0
            and len(day_nfe_mismatch) == 0
        )

        day_summaries.append({
            "data_dia": dia,
            "source_count_all": len(src_day),
            "stg_count_all": len(stg_day),
            "count_delta": count_delta,
            "source_total_all": round(src_total_all, 2),
            "stg_total_all": round(stg_total_all, 2),
            "total_all_delta": round(src_total_all - stg_total_all, 2),
            "source_count_comercial": len(src_comercial),
            "stg_count_comercial": len(stg_comercial),
            "count_comercial_delta": len(src_comercial) - len(stg_comercial),
            "source_total_comercial": round(src_total_comercial, 2),
            "stg_total_comercial": round(stg_total_comercial, 2),
            "total_comercial_delta": round(src_total_comercial - stg_total_comercial, 2),
            "source_count_situacao_3": len(src_sit3),
            "stg_count_situacao_3": len(stg_sit3),
            "source_total_situacao_3": round(sum(d["total_header"] for d in src_sit3), 2),
            "stg_total_situacao_3": round(sum(d["total_header"] for d in stg_sit3), 2),
            "source_count_cancelado": len(src_canc),
            "stg_count_cancelado": len(stg_canc),
            "source_total_cancelado": round(sum(d["total_header"] for d in src_canc), 2),
            "stg_total_cancelado": round(sum(d["total_header"] for d in stg_canc), 2),
            "source_count_nfe_inutilizada": len(src_inut),
            "stg_count_nfe_inutilizada": len(stg_inut),
            "source_total_nfe_inutilizada": round(sum(d["total_header"] for d in src_inut), 2),
            "stg_total_nfe_inutilizada": round(sum(d["total_header"] for d in stg_inut), 2),
            "source_only_count": len(day_source_only),
            "stg_only_count": len(day_stg_only),
            "total_mismatch_count": len(day_total_mismatch),
            "status_mismatch_count": len(day_status_mismatch),
            "nfe_mismatch_count": len(day_nfe_mismatch),
            "day_pass": day_pass,
        })

    return day_summaries


def _compute_delta_explanations(
    day_summaries: List[Dict],
    source_only: List[Dict],
    stg_only: List[Dict],
    total_mismatch: List[Dict],
    status_mismatch: List[Dict],
    nfe_missing_in_stg: List[Dict],
    classification_mismatch: List[Dict],
) -> List[Dict]:
    """Compute delta explanation by day."""
    delta_explanations = []
    for ds in day_summaries:
        dia = ds["data_dia"]
        delta_total_comercial = ds["total_comercial_delta"]
        if abs(delta_total_comercial) < 0.01:
            continue

        day_source_only_docs = [d for d in source_only if d["data_dia"] == dia]
        day_stg_only_docs = [d for d in stg_only if d["data_dia"] == dia]
        day_total_mismatch_docs = [d for d in total_mismatch if d["data_dia"] == dia]
        day_status_mismatch_docs = [d for d in status_mismatch if d["data_dia"] == dia]

        source_only_comercial_total = sum(
            d["total_header"] for d in day_source_only_docs if d["commercial_eligible"] == 1
        )
        stg_only_comercial_total = sum(
            d["total_header"] for d in day_stg_only_docs if d["commercial_eligible"] == 1
        )
        total_mismatch_diff = sum(d.get("diff", 0) for d in day_total_mismatch_docs)

        # Status mismatch effect
        status_mismatch_effect = 0.0
        for d in day_status_mismatch_docs:
            src_eligible = d["commercial_eligible"]
            stg_eligible_val = is_commercial(d.get("stg_situacao"), d.get("stg_cancelado"))
            if src_eligible == 1 and not stg_eligible_val:
                status_mismatch_effect += d["total_header"]
            elif src_eligible == 0 and stg_eligible_val:
                status_mismatch_effect -= d["total_header"]

        # NFE inutilizada effect
        nfe_inutilizada_effect = sum(
            d["total_header"] for d in nfe_missing_in_stg
            if d["data_dia"] == dia and d.get("nfe_status") is not None and int(d["nfe_status"]) == 5
        )

        # Situacao 3 effect
        situacao_3_effect = 0.0
        day_class_mismatch = [d for d in classification_mismatch if d["data_dia"] == dia]
        for d in day_class_mismatch:
            if d["classification"] == "situacao_3_ignorada" and d.get("stg_classification") == "comercial":
                situacao_3_effect -= d["total_header"]
            elif d["classification"] == "comercial" and d.get("stg_classification") == "situacao_3_ignorada":
                situacao_3_effect += d["total_header"]

        delta_explained = (
            source_only_comercial_total
            - stg_only_comercial_total
            + total_mismatch_diff
            + status_mismatch_effect
            + nfe_inutilizada_effect
            + situacao_3_effect
        )
        unexplained = delta_total_comercial - delta_explained

        delta_explanations.append({
            "data_dia": dia,
            "delta_total_comercial": round(delta_total_comercial, 2),
            "source_only_comercial_total": round(source_only_comercial_total, 2),
            "stg_only_comercial_total": round(stg_only_comercial_total, 2),
            "total_mismatch_diff": round(total_mismatch_diff, 2),
            "status_mismatch_effect": round(status_mismatch_effect, 2),
            "nfe_inutilizada_effect": round(nfe_inutilizada_effect, 2),
            "situacao_3_effect": round(situacao_3_effect, 2),
            "delta_explained_amount": round(delta_explained, 2),
            "unexplained_delta": round(unexplained, 2),
        })

    return delta_explanations


def cmd_compare_stg_comprovantes_range(cfg: Config, args: argparse.Namespace) -> None:
    """Compare source SQL Server vs STG PostgreSQL comprovantes for a date range.

    With --stg-only, skips SQL Server and produces a STG consistency audit.
    """
    out_dir = ensure_dir(args.out)
    id_filial = args.id_filial
    stg_only_mode = getattr(args, "stg_only", False)

    # Date range defaults
    today_sp = _get_sp_today()
    if args.date_to:
        date_to = datetime.strptime(args.date_to, "%Y-%m-%d").date()
    else:
        date_to = today_sp - timedelta(days=1)
    if args.date_from:
        date_from = datetime.strptime(args.date_from, "%Y-%m-%d").date()
    else:
        date_from = date_to - timedelta(days=14)

    date_to_exclusive = date_to + timedelta(days=1)

    log.info("Compare range: filial=%s, from=%s, to=%s (exclusive=%s)%s",
             id_filial, date_from, date_to, date_to_exclusive,
             " [STG-ONLY MODE]" if stg_only_mode else "")

    source_docs: List[Dict] = []
    t_comp_name = "N/A (stg-only)"
    t_nfe_name = "N/A (stg-only)"

    if not stg_only_mode:
        # ─── A) Source SQL Server ─────────────────────────────────────────────
        log.info("Connecting to SQL Server source...")
        conn_src = get_connection(cfg)

        t_comp = _find_table(conn_src, ["COMPROVANTES", "comprovantes", "Comprovantes"])
        t_nfe = _find_table(conn_src, ["NFE", "nfe", "Nfe"])
        if not t_comp:
            log.error("COMPROVANTES table not found in source!")
            conn_src.close()
            sys.exit(1)
        t_comp_name = t_comp
        t_nfe_name = t_nfe or "not found"

        # Detect optional columns
        comp_cols_q = execute_query(conn_src,
            "SELECT c.name FROM sys.columns c "
            "JOIN sys.tables t ON c.object_id = t.object_id "
            "WHERE LOWER(t.name) = 'comprovantes'")
        comp_col_names = {r["name"].upper() for r in comp_cols_q}
        has_cancelado = "CANCELADO" in comp_col_names
        has_referencia = "REFERENCIA" in comp_col_names
        has_id_turno = "ID_TURNO" in comp_col_names
        has_id_usuario = "ID_USUARIO" in comp_col_names

        cancelado_expr = "ISNULL(c.CANCELADO, 0)" if has_cancelado else "0"
        extra_cols = ""
        if has_referencia:
            extra_cols += ",\n    c.REFERENCIA AS referencia"
        if has_id_turno:
            extra_cols += ",\n    c.ID_TURNO AS id_turno"
        if has_id_usuario:
            extra_cols += ",\n    c.ID_USUARIO AS id_usuario"

        nfe_cte_src = ""
        nfe_join_src = ""
        nfe_cols_src = ",\n    NULL AS nfe_status,\n    NULL AS nfe_nronf,\n    NULL AS nfe_chaveacesso,\n    NULL AS nfe_data"
        if t_nfe:
            nfe_cte_src = f"""\
WITH nfe_ranked AS (
  SELECT n.*,
         ROW_NUMBER() OVER (
           PARTITION BY n.ID_FILIAL, n.ID_DB, n.ID_COMPROVANTE
           ORDER BY
             CASE WHEN n.STATUS = 5 THEN 1 WHEN n.STATUS = 4 THEN 2 WHEN n.STATUS = 3 THEN 3 ELSE 9 END,
             n.DATA DESC,
             n.ID_NFE DESC
         ) AS rn
  FROM {t_nfe} n
  WHERE n.ID_FILIAL = '{id_filial}'
)
"""
            nfe_join_src = (
                "\nLEFT JOIN nfe_ranked n ON c.ID_FILIAL = n.ID_FILIAL "
                "AND c.ID_DB = n.ID_DB AND c.ID_COMPROVANTE = n.ID_COMPROVANTE AND n.rn = 1"
            )
            nfe_cols_src = (
                ",\n    n.STATUS AS nfe_status,"
                "\n    n.NRONF AS nfe_nronf,"
                "\n    n.CHAVEACESSO AS nfe_chaveacesso,"
                "\n    n.DATA AS nfe_data"
            )

        sql_source = f"""\
{nfe_cte_src}SELECT
    c.ID_FILIAL AS id_filial,
    c.ID_DB AS id_db,
    c.ID_COMPROVANTE AS id_comprovante,
    c.DATA AS data,
    CAST(c.DATA AS date) AS data_dia,
    c.SITUACAO AS situacao,
    {cancelado_expr} AS cancelado,
    ISNULL(c.TOTAL, 0) AS total_header{extra_cols}{nfe_cols_src}
FROM {t_comp} c{nfe_join_src}
WHERE c.ID_FILIAL = '{id_filial}'
  AND c.DATA >= '{date_from}'
  AND c.DATA < '{date_to_exclusive}'
ORDER BY c.DATA, c.ID_COMPROVANTE
"""
        log.info("Querying source SQL Server...")
        source_rows_raw = execute_query(conn_src, sql_source)
        conn_src.close()
        log.info("Source returned %d rows.", len(source_rows_raw))

        # Process source rows
        for r in source_rows_raw:
            doc = {
                "id_filial": str(r.get("id_filial", "")),
                "id_db": str(r.get("id_db", "")),
                "id_comprovante": str(r.get("id_comprovante", "")),
                "data": str(r.get("data", "")),
                "data_dia": str(r.get("data_dia", "")),
                "situacao": r.get("situacao"),
                "cancelado": int(r.get("cancelado") or 0),
                "total_header": float(r.get("total_header") or 0),
                "nfe_status": r.get("nfe_status"),
                "nfe_nronf": r.get("nfe_nronf"),
                "nfe_chaveacesso": mask_pii(r.get("nfe_chaveacesso"), "chaveacesso") if r.get("nfe_chaveacesso") else None,
                "nfe_data": str(r.get("nfe_data", "")) if r.get("nfe_data") else None,
            }
            if has_referencia:
                doc["referencia"] = r.get("referencia")
            if has_id_turno:
                doc["id_turno"] = r.get("id_turno")
            if has_id_usuario:
                doc["id_usuario"] = r.get("id_usuario")
            doc["classification"] = classify_comprovante(doc["situacao"], doc["cancelado"], doc["nfe_status"])
            doc["commercial_eligible"] = 1 if is_commercial(doc["situacao"], doc["cancelado"]) else 0
            source_docs.append(doc)

    # ─── B) STG PostgreSQL ────────────────────────────────────────────────
    log.info("Connecting to STG PostgreSQL...")
    if not cfg.stg_pg_host:
        log.error("STG_PG_HOST not configured. Set STG_PG_HOST or POSTGRES_HOST in env.")
        sys.exit(1)

    pg_conn = get_pg_connection(cfg)

    # Try with NFE join first
    # STG uses shadow columns: dt_evento (UTC), situacao_shadow, cancelado_shadow (bool),
    # valor_total_shadow, referencia_shadow, id_turno_shadow, id_usuario_shadow
    # NFE: status_shadow, numero_nfe_shadow, chave_nfe_shadow, data_emissao_shadow
    sql_stg_with_nfe = """\
SELECT
    c.id_filial::text AS id_filial,
    c.id_db::text AS id_db,
    c.id_comprovante::text AS id_comprovante,
    (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::timestamp AS data,
    (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date AS data_dia,
    c.situacao_shadow AS situacao,
    CASE WHEN c.cancelado_shadow THEN 1 ELSE 0 END AS cancelado,
    COALESCE(c.valor_total_shadow, 0) AS total_header,
    c.referencia_shadow AS referencia,
    c.id_turno_shadow AS id_turno,
    c.id_usuario_shadow AS id_usuario,
    n.status_shadow AS nfe_status,
    n.numero_nfe_shadow AS nfe_nronf,
    n.chave_nfe_shadow AS nfe_chaveacesso,
    (n.data_emissao_shadow AT TIME ZONE 'America/Sao_Paulo')::timestamp AS nfe_data
FROM stg.comprovantes c
LEFT JOIN stg.nfe n ON c.id_filial = n.id_filial AND c.id_db = n.id_db AND c.id_comprovante = n.id_comprovante
WHERE c.id_filial = %s
  AND (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date >= %s::date
  AND (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date < %s::date
ORDER BY c.dt_evento, c.id_comprovante
"""

    sql_stg_without_nfe = """\
SELECT
    c.id_filial::text AS id_filial,
    c.id_db::text AS id_db,
    c.id_comprovante::text AS id_comprovante,
    (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::timestamp AS data,
    (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date AS data_dia,
    c.situacao_shadow AS situacao,
    CASE WHEN c.cancelado_shadow THEN 1 ELSE 0 END AS cancelado,
    COALESCE(c.valor_total_shadow, 0) AS total_header,
    c.referencia_shadow AS referencia,
    c.id_turno_shadow AS id_turno,
    c.id_usuario_shadow AS id_usuario,
    NULL::integer AS nfe_status,
    NULL::text AS nfe_nronf,
    NULL::text AS nfe_chaveacesso,
    NULL::timestamp AS nfe_data
FROM stg.comprovantes c
WHERE c.id_filial = %s
  AND (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date >= %s::date
  AND (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date < %s::date
ORDER BY c.dt_evento, c.id_comprovante
"""

    stg_rows_raw = []
    try:
        stg_rows_raw = pg_query(pg_conn, sql_stg_with_nfe,
                                (str(id_filial), str(date_from), str(date_to_exclusive)))
    except Exception as exc:
        log.warning("STG query with NFE join failed (%s), retrying without NFE...", exc)
        try:
            pg_conn.close()
            pg_conn = get_pg_connection(cfg)
            stg_rows_raw = pg_query(pg_conn, sql_stg_without_nfe,
                                    (str(id_filial), str(date_from), str(date_to_exclusive)))
        except Exception as exc2:
            log.error("STG query without NFE also failed: %s", exc2)
            pg_conn.close()
            sys.exit(1)
    pg_conn.close()
    log.info("STG returned %d rows.", len(stg_rows_raw))

    # Process STG rows
    stg_docs = []
    for r in stg_rows_raw:
        doc = {
            "id_filial": str(r.get("id_filial", "")),
            "id_db": str(r.get("id_db", "")),
            "id_comprovante": str(r.get("id_comprovante", "")),
            "data": str(r.get("data", "")),
            "data_dia": str(r.get("data_dia", "")),
            "situacao": r.get("situacao"),
            "cancelado": int(r.get("cancelado") or 0),
            "total_header": float(r.get("total_header") or 0),
            "referencia": r.get("referencia"),
            "id_turno": r.get("id_turno"),
            "id_usuario": r.get("id_usuario"),
            "nfe_status": r.get("nfe_status"),
            "nfe_nronf": r.get("nfe_nronf"),
            "nfe_chaveacesso": mask_pii(r.get("nfe_chaveacesso"), "chaveacesso") if r.get("nfe_chaveacesso") else None,
            "nfe_data": str(r.get("nfe_data", "")) if r.get("nfe_data") else None,
        }
        doc["classification"] = classify_comprovante(doc["situacao"], doc["cancelado"], doc["nfe_status"])
        doc["commercial_eligible"] = 1 if is_commercial(doc["situacao"], doc["cancelado"]) else 0
        stg_docs.append(doc)

    # ─── C) Compare by key ────────────────────────────────────────────────
    cmp = _compare_docs(source_docs, stg_docs)
    source_only = cmp["source_only"]
    stg_only = cmp["stg_only"]
    total_mismatch = cmp["total_mismatch"]
    status_mismatch = cmp["status_mismatch"]
    nfe_mismatch = cmp["nfe_mismatch"]
    nfe_missing_in_stg = cmp["nfe_missing_in_stg"]
    nfe_missing_in_source = cmp["nfe_missing_in_source"]
    classification_mismatch = cmp["classification_mismatch"]

    # ─── D) Totalizadores por dia ─────────────────────────────────────────
    day_summaries = _compute_day_summaries(
        source_docs, stg_docs, source_only, stg_only,
        total_mismatch, status_mismatch, nfe_mismatch,
    )

    # ─── E) Delta explanation by day ──────────────────────────────────────
    delta_explanations = _compute_delta_explanations(
        day_summaries, source_only, stg_only, total_mismatch,
        status_mismatch, nfe_missing_in_stg, classification_mismatch,
    )

    # ─── F) Write outputs ─────────────────────────────────────────────────
    # Ledgers
    write_csv(source_docs, out_dir / "source_ledger.csv")
    write_csv(stg_docs, out_dir / "stg_ledger.csv")

    # Divergences
    write_csv(source_only, out_dir / "source_only.csv")
    write_csv(stg_only, out_dir / "stg_only.csv")
    write_csv(total_mismatch, out_dir / "total_mismatch.csv")
    write_csv(status_mismatch, out_dir / "status_mismatch.csv")
    write_csv(nfe_mismatch, out_dir / "nfe_mismatch.csv")
    write_csv(nfe_missing_in_stg, out_dir / "nfe_missing_in_stg.csv")
    write_csv(nfe_missing_in_source, out_dir / "nfe_missing_in_source.csv")
    write_csv(classification_mismatch, out_dir / "classification_mismatch.csv")

    # Day summaries
    write_csv(day_summaries, out_dir / "compare_summary_by_day.csv")
    write_json(day_summaries, out_dir / "compare_summary_by_day.json")

    # Delta explanation
    write_csv(delta_explanations, out_dir / "delta_explanation_by_day.csv")
    write_json(delta_explanations, out_dir / "delta_explanation_by_day.json")

    # Top delta documents (source_only + total_mismatch sorted by absolute value)
    top_delta_docs = []
    for d in source_only:
        if d["commercial_eligible"] == 1:
            top_delta_docs.append({**d, "delta_type": "source_only", "delta_value": d["total_header"]})
    for d in stg_only:
        if d["commercial_eligible"] == 1:
            top_delta_docs.append({**d, "delta_type": "stg_only", "delta_value": -d["total_header"]})
    for d in total_mismatch:
        top_delta_docs.append({**d, "delta_type": "total_mismatch", "delta_value": d.get("diff", 0)})
    top_delta_docs.sort(key=lambda x: abs(x.get("delta_value", 0)), reverse=True)
    write_csv(top_delta_docs[:100], out_dir / "top_delta_documents.csv")

    # NFE status=5 docs
    nfe_status5_src = [d for d in source_docs if d.get("nfe_status") is not None and int(d["nfe_status"]) == 5]
    nfe_status5_stg = [d for d in stg_docs if d.get("nfe_status") is not None and int(d["nfe_status"]) == 5]
    write_csv(nfe_status5_src, out_dir / "nfe_status5_source.csv")
    write_csv(nfe_status5_stg, out_dir / "nfe_status5_stg.csv")

    # Situacao 3 docs
    sit3_src = [d for d in source_docs if d["classification"] == "situacao_3_ignorada"]
    sit3_stg = [d for d in stg_docs if d["classification"] == "situacao_3_ignorada"]
    write_csv(sit3_src, out_dir / "situacao3_source.csv")
    write_csv(sit3_stg, out_dir / "situacao3_stg.csv")

    # Cancelados docs
    canc_src = [d for d in source_docs if d["cancelado"] == 1]
    canc_stg = [d for d in stg_docs if d["cancelado"] == 1]
    write_csv(canc_src, out_dir / "cancelados_source.csv")
    write_csv(canc_stg, out_dir / "cancelados_stg.csv")

    # Summary JSON
    all_days = [ds["data_dia"] for ds in day_summaries]
    overall_pass = all(ds["day_pass"] for ds in day_summaries) if day_summaries else False
    summary = {
        "id_filial": id_filial,
        "date_from": str(date_from),
        "date_to": str(date_to),
        "days_audited": len(all_days),
        "overall_pass": overall_pass,
        "source_count_total": len(source_docs),
        "stg_count_total": len(stg_docs),
        "source_only_count": len(source_only),
        "stg_only_count": len(stg_only),
        "total_mismatch_count": len(total_mismatch),
        "status_mismatch_count": len(status_mismatch),
        "nfe_mismatch_count": len(nfe_mismatch),
        "nfe_missing_in_stg_count": len(nfe_missing_in_stg),
        "nfe_missing_in_source_count": len(nfe_missing_in_source),
        "classification_mismatch_count": len(classification_mismatch),
        "source_total_comercial": round(sum(d["total_header"] for d in source_docs if d["commercial_eligible"] == 1), 2),
        "stg_total_comercial": round(sum(d["total_header"] for d in stg_docs if d["commercial_eligible"] == 1), 2),
        "sql_server_table": t_comp_name,
        "stg_tables": "stg.comprovantes LEFT JOIN stg.nfe",
    }
    summary["total_comercial_delta"] = round(summary["source_total_comercial"] - summary["stg_total_comercial"], 2)
    write_json(summary, out_dir / "compare_summary.json")

    # ─── Compare report MD ────────────────────────────────────────────────
    days_pass = sum(1 for ds in day_summaries if ds["day_pass"])
    days_fail = len(day_summaries) - days_pass

    md_lines = [
        f"# Compare Source vs STG — Comprovantes Range",
        "",
        f"## 1. Período auditado",
        f"- De: **{date_from}**",
        f"- Até: **{date_to}** (inclusive)",
        f"- Dias: **{len(all_days)}**",
        "",
        f"## 2. Filial",
        f"- ID: **{id_filial}**",
        "",
        f"## 3. Conexões usadas",
        f"- SQL Server: `{cfg.host}/{cfg.database}` (user: {cfg.user})",
        f"- PostgreSQL STG: `{cfg.stg_pg_host}/{cfg.stg_pg_database}` (user: {cfg.stg_pg_user})",
        "",
        f"## 4. Tabela SQL Server",
        f"- Comprovantes: `{t_comp_name}`",
        f"- NFE: `{t_nfe_name}`",
        "",
        f"## 5. Tabelas STG",
        f"- `stg.comprovantes`",
        f"- `stg.nfe` (LEFT JOIN)",
        "",
        f"## 6. Resumo geral",
        "",
        f"| Metric | Source | STG | Delta |",
        f"|--------|--------|-----|-------|",
        f"| Total docs | {len(source_docs):,} | {len(stg_docs):,} | {len(source_docs) - len(stg_docs):+,} |",
        f"| Comerciais | {summary['source_total_comercial']:,.2f} | {summary['stg_total_comercial']:,.2f} | {summary['total_comercial_delta']:+,.2f} |",
        f"| Source only | {len(source_only)} | — | — |",
        f"| STG only | — | {len(stg_only)} | — |",
        f"| Total mismatch | {len(total_mismatch)} | — | — |",
        f"| Status mismatch | {len(status_mismatch)} | — | — |",
        f"| NFE mismatch | {len(nfe_mismatch)} | — | — |",
        f"| NFE missing STG | {len(nfe_missing_in_stg)} | — | — |",
        f"| NFE missing source | — | {len(nfe_missing_in_source)} | — |",
        f"| Classification mismatch | {len(classification_mismatch)} | — | — |",
        "",
        f"## 7. Resumo por dia",
        "",
        "| Dia | Src | STG | Delta | Comercial Src | Comercial STG | Delta Com. | PASS |",
        "|-----|-----|-----|-------|---------------|---------------|------------|------|",
    ]
    for ds in day_summaries:
        icon = "✅" if ds["day_pass"] else "❌"
        md_lines.append(
            f"| {ds['data_dia']} | {ds['source_count_all']} | {ds['stg_count_all']} | "
            f"{ds['count_delta']:+} | {ds['source_total_comercial']:,.2f} | "
            f"{ds['stg_total_comercial']:,.2f} | {ds['total_comercial_delta']:+,.2f} | {icon} |"
        )
    md_lines.append("")

    md_lines.append(f"## 8. Dias PASS/FAIL")
    md_lines.append(f"- PASS: **{days_pass}**")
    md_lines.append(f"- FAIL: **{days_fail}**")
    md_lines.append(f"- Overall: **{'PASS' if overall_pass else 'FAIL'}**")
    md_lines.append("")

    md_lines.append(f"## 9. Delta comercial por dia")
    md_lines.append("")
    if delta_explanations:
        md_lines.append("| Dia | Delta | Src Only | STG Only | Mismatch | Status | NFE Inut | Sit3 | Explained | Unexplained |")
        md_lines.append("|-----|-------|----------|----------|----------|--------|----------|------|-----------|-------------|")
        for de in delta_explanations:
            md_lines.append(
                f"| {de['data_dia']} | {de['delta_total_comercial']:+,.2f} | "
                f"{de['source_only_comercial_total']:,.2f} | {de['stg_only_comercial_total']:,.2f} | "
                f"{de['total_mismatch_diff']:,.2f} | {de['status_mismatch_effect']:,.2f} | "
                f"{de['nfe_inutilizada_effect']:,.2f} | {de['situacao_3_effect']:,.2f} | "
                f"{de['delta_explained_amount']:,.2f} | {de['unexplained_delta']:,.2f} |"
            )
    else:
        md_lines.append("Nenhum dia com delta comercial significativo.")
    md_lines.append("")

    md_lines.append("## 10. Explicação do delta")
    md_lines.append("")
    md_lines.append("- **source_only_comercial_total**: Faturamento de docs comerciais presentes apenas na fonte")
    md_lines.append("- **stg_only_comercial_total**: Faturamento de docs comerciais presentes apenas na STG")
    md_lines.append("- **total_mismatch_diff**: Soma das diferenças de valor para docs com total divergente")
    md_lines.append("- **status_mismatch_effect**: Efeito de docs com status comercial divergente")
    md_lines.append("- **nfe_inutilizada_effect**: Docs com NFE inutilizada na fonte, sem NFE na STG")
    md_lines.append("- **situacao_3_effect**: Docs com classificação situacao_3 divergente")
    md_lines.append("")

    md_lines.append("## 11. Top documentos responsáveis")
    md_lines.append("")
    if top_delta_docs:
        md_lines.append("| Filial | DB | Comprovante | Dia | Tipo | Valor Delta |")
        md_lines.append("|--------|----|-------------|-----|------|-------------|")
        for d in top_delta_docs[:20]:
            md_lines.append(
                f"| {d['id_filial']} | {d['id_db']} | {d['id_comprovante']} | "
                f"{d.get('data_dia', '')} | {d['delta_type']} | {d.get('delta_value', 0):+,.2f} |"
            )
    else:
        md_lines.append("Nenhum documento com delta significativo.")
    md_lines.append("")

    md_lines.append("## 12. NFE inutilizadas (status=5)")
    md_lines.append(f"- Fonte: {len(nfe_status5_src)} docs")
    md_lines.append(f"- STG: {len(nfe_status5_stg)} docs")
    md_lines.append("")

    md_lines.append("## 13. Situação=3")
    md_lines.append(f"- Fonte: {len(sit3_src)} docs")
    md_lines.append(f"- STG: {len(sit3_stg)} docs")
    md_lines.append("")

    md_lines.append("## 14. Cancelados")
    md_lines.append(f"- Fonte: {len(canc_src)} docs")
    md_lines.append(f"- STG: {len(canc_stg)} docs")
    md_lines.append("")

    # Diagnosis
    md_lines.append("## 15. Diagnóstico provável por camada")
    md_lines.append("")
    if len(source_only) > 0:
        md_lines.append("- **source_only > 0**: Fonte SQL Server possui documentos ausentes na STG. "
                        "Investigar Agent, filtro de coleta, watermark, spool, API ingest ou rejeição.")
    if len(stg_only) > 0:
        md_lines.append("- **stg_only > 0**: STG possui documentos não encontrados na fonte. "
                        "Investigar banco/host, data, timezone, registros antigos ou ambiente errado.")
    if len(status_mismatch) > 0:
        md_lines.append("- **status_mismatch > 0**: Status SITUACAO/CANCELADO diverge. "
                        "Investigar atualização incremental, CDC, upsert ou schema mapping.")
    if len(nfe_missing_in_stg) > 0:
        md_lines.append("- **nfe_missing_in_stg > 0**: NFE existe na fonte e não na STG. "
                        "Investigar dataset NFE do Agent/API ingest.")
    if (len(source_only) == 0 and len(stg_only) == 0 and len(total_mismatch) == 0
            and len(status_mismatch) == 0 and summary["total_comercial_delta"] != 0):
        md_lines.append("- **raw matches but commercial doesn't**: Dados brutos chegaram, "
                        "mas classificação comercial/status diverge.")
    if overall_pass:
        md_lines.append("- **Tudo confere**: Problema não está entre fonte e STG. "
                        "Próxima camada: ClickHouse slim/mart/API/Web.")
    md_lines.append("")

    md_lines.append("## 16. Próxima ação recomendada")
    md_lines.append("")
    if overall_pass:
        md_lines.append("Fonte e STG estão alinhadas. Investigar camadas seguintes: "
                        "CDC Consumer → ClickHouse raw → current → slim → mart_rt → API → Web.")
    elif len(source_only) > 0:
        md_lines.append("Prioridade: investigar documentos presentes na fonte mas ausentes na STG. "
                        "Verificar Agent watermark, API ingest logs, rejeições de schema.")
    elif len(stg_only) > 0:
        md_lines.append("Prioridade: investigar documentos na STG sem correspondência na fonte. "
                        "Verificar se o banco/filial/host está correto.")
    else:
        md_lines.append("Investigar mismatches de valor/status. Verificar CDC/upsert logic.")
    md_lines.append("")

    write_md("\n".join(md_lines), out_dir / "compare_report.md")
    log.info("Compare complete → %s (overall: %s)", out_dir, "PASS" if overall_pass else "FAIL")


# ═══════════════════════════════════════════════════════════════════════════
# export-source-comprovantes-range
# ═══════════════════════════════════════════════════════════════════════════


def cmd_export_source_comprovantes_range(cfg: Config, args: argparse.Namespace) -> None:
    """Export comprovantes from SQL Server source to local files."""
    out_dir = ensure_dir(args.out)
    id_filial = args.id_filial
    date_from = datetime.strptime(args.date_from, "%Y-%m-%d").date()
    date_to = datetime.strptime(args.date_to, "%Y-%m-%d").date()
    date_to_exclusive = date_to + timedelta(days=1)

    log.info("Export source: filial=%s, from=%s, to=%s (exclusive=%s)",
             id_filial, date_from, date_to, date_to_exclusive)

    # Connect to SQL Server
    conn = get_connection(cfg)

    t_comp = _find_table(conn, ["COMPROVANTES", "comprovantes", "Comprovantes"])
    t_nfe = _find_table(conn, ["NFE", "nfe", "Nfe"])
    if not t_comp:
        log.error("COMPROVANTES table not found in source!")
        conn.close()
        sys.exit(1)

    # Detect optional columns
    comp_cols_q = execute_query(conn,
        "SELECT c.name FROM sys.columns c "
        "JOIN sys.tables t ON c.object_id = t.object_id "
        "WHERE LOWER(t.name) = 'comprovantes'")
    comp_col_names = {r["name"].upper() for r in comp_cols_q}
    has_cancelado = "CANCELADO" in comp_col_names
    has_referencia = "REFERENCIA" in comp_col_names
    has_id_turno = "ID_TURNO" in comp_col_names
    has_id_usuario = "ID_USUARIO" in comp_col_names
    has_id_localvenda = "ID_LOCALVENDA" in comp_col_names

    warnings_list: List[str] = []

    # Detect critical columns
    has_data = "DATA" in comp_col_names
    if not has_data:
        log.error("COMPROVANTES.DATA column not found! Cannot proceed.")
        conn.close()
        sys.exit(1)

    has_situacao = "SITUACAO" in comp_col_names
    if not has_situacao:
        warnings_list.append("SITUACAO column not found; defaulting to 0")

    # Total column detection
    total_col = None
    for candidate in ["TOTAL", "VLRTOTAL", "VALORTOTAL", "TOTALCOMPROVANTE"]:
        if candidate in comp_col_names:
            total_col = candidate
            break
    if not total_col:
        total_col = "TOTAL"  # Try anyway, may fail
        warnings_list.append("No known total column found; trying TOTAL")

    cancelado_expr = "ISNULL(c.CANCELADO, 0)" if has_cancelado else "0"
    referencia_expr = "c.REFERENCIA" if has_referencia else "NULL"
    id_turno_expr = "c.ID_TURNO" if has_id_turno else "NULL"
    id_usuario_expr = "c.ID_USUARIO" if has_id_usuario else "NULL"
    id_localvenda_expr = "c.ID_LOCALVENDA" if has_id_localvenda else "NULL"
    situacao_expr = "c.SITUACAO AS situacao" if has_situacao else "0 AS situacao"

    nfe_cte = ""
    nfe_join = ""
    nfe_cols = ",\n    NULL AS nfe_status,\n    NULL AS nfe_nronf,\n    NULL AS nfe_chaveacesso,\n    NULL AS nfe_data"
    nfe_join_strategy = "no_nfe"
    if t_nfe:
        nfe_cte = f"""\
WITH nfe_ranked AS (
  SELECT n.*,
         ROW_NUMBER() OVER (
           PARTITION BY n.ID_FILIAL, n.ID_DB, n.ID_COMPROVANTE
           ORDER BY
             CASE WHEN n.STATUS = 5 THEN 1 WHEN n.STATUS = 4 THEN 2 WHEN n.STATUS = 3 THEN 3 ELSE 9 END,
             n.DATA DESC,
             n.ID_NFE DESC
         ) AS rn
  FROM {t_nfe} n
  WHERE n.ID_FILIAL = '{id_filial}'
)
"""
        nfe_join = (
            "\nLEFT JOIN nfe_ranked n ON c.ID_FILIAL = n.ID_FILIAL "
            "AND c.ID_DB = n.ID_DB AND c.ID_COMPROVANTE = n.ID_COMPROVANTE AND n.rn = 1"
        )
        nfe_cols = (
            ",\n    n.STATUS AS nfe_status,"
            "\n    n.NRONF AS nfe_nronf,"
            "\n    n.CHAVEACESSO AS nfe_chaveacesso,"
            "\n    n.DATA AS nfe_data"
        )
        nfe_join_strategy = "nfe_ranked_rn1"

    sql_source = f"""\
{nfe_cte}SELECT
    c.ID_FILIAL AS id_filial,
    c.ID_DB AS id_db,
    c.ID_COMPROVANTE AS id_comprovante,
    c.DATA AS data,
    CAST(c.DATA AS date) AS data_dia,
    {situacao_expr},
    {cancelado_expr} AS cancelado,
    {referencia_expr} AS referencia,
    {id_turno_expr} AS id_turno,
    {id_usuario_expr} AS id_usuario,
    {id_localvenda_expr} AS id_localvenda,
    ISNULL(c.{total_col}, 0) AS total_header{nfe_cols}
FROM {t_comp} c{nfe_join}
WHERE c.ID_FILIAL = '{id_filial}'
  AND c.DATA >= '{date_from}'
  AND c.DATA < '{date_to_exclusive}'
ORDER BY c.DATA, c.ID_COMPROVANTE
"""
    log.info("Querying source SQL Server...")
    source_rows_raw = execute_query(conn, sql_source)
    conn.close()
    log.info("Source returned %d rows.", len(source_rows_raw))

    if not t_nfe:
        warnings_list.append("NFE table not found; nfe_* columns are NULL")
    if not has_cancelado:
        warnings_list.append("CANCELADO column not found; defaulting to 0")

    # Detect duplicate keys
    key_counts: Dict[Tuple, int] = defaultdict(int)
    for r in source_rows_raw:
        k = (str(r.get("id_filial", "")), str(r.get("id_db", "")), str(r.get("id_comprovante", "")))
        key_counts[k] += 1

    duplicates = {k: v for k, v in key_counts.items() if v > 1}
    if duplicates:
        dup_rows = [{"id_filial": k[0], "id_db": k[1], "id_comprovante": k[2], "count": v} for k, v in duplicates.items()]
        write_csv(dup_rows, out_dir / "source_duplicate_keys.csv")
        warnings_list.append(f"Found {len(duplicates)} duplicate keys ({sum(v for v in duplicates.values())} total rows)")
        log.warning("Duplicate keys detected: %d", len(duplicates))

    # Process rows
    source_docs: List[Dict] = []
    for r in source_rows_raw:
        chave_raw = r.get("nfe_chaveacesso") or r.get("nfe_chaveacesso", None)
        chave_masked = mask_pii(chave_raw, "chaveacesso") if chave_raw else None

        doc = {
            "id_filial": str(r.get("id_filial", "")),
            "id_db": str(r.get("id_db", "")),
            "id_comprovante": str(r.get("id_comprovante", "")),
            "data": str(r.get("data", "")),
            "data_dia": str(r.get("data_dia", "")),
            "situacao": r.get("situacao"),
            "cancelado": int(r.get("cancelado") or 0),
            "referencia": r.get("referencia"),
            "id_turno": r.get("id_turno"),
            "id_usuario": r.get("id_usuario"),
            "id_localvenda": r.get("id_localvenda"),
            "total_header": float(r.get("total_header") or 0),
            "nfe_status": r.get("nfe_status"),
            "nfe_nronf": r.get("nfe_nronf"),
            "nfe_chaveacesso_masked": chave_masked,
            "nfe_data": str(r.get("nfe_data", "")) if r.get("nfe_data") else None,
        }
        doc["classification"] = classify_comprovante(doc["situacao"], doc["cancelado"], doc["nfe_status"])
        doc["commercial_eligible"] = 1 if is_commercial(doc["situacao"], doc["cancelado"]) else 0
        source_docs.append(doc)

    # Write source_ledger.csv
    ledger_fieldnames = [
        "id_filial", "id_db", "id_comprovante", "data", "data_dia", "situacao",
        "cancelado", "referencia", "id_turno", "id_usuario", "id_localvenda",
        "total_header", "nfe_status", "nfe_nronf", "nfe_chaveacesso_masked",
        "nfe_data", "classification", "commercial_eligible",
    ]
    write_csv(source_docs, out_dir / "source_ledger.csv", fieldnames=ledger_fieldnames)

    # Write source_ledger.jsonl
    jsonl_path = out_dir / "source_ledger.jsonl"
    with open(jsonl_path, "w", encoding="utf-8") as f:
        for doc in source_docs:
            f.write(json.dumps(doc, default=_json_serial, ensure_ascii=False) + "\n")
    log.info("Wrote %s (%d lines)", jsonl_path, len(source_docs))

    # Compute SHA256 of source_ledger.csv
    csv_bytes = (out_dir / "source_ledger.csv").read_bytes()
    sha256_hex = hashlib.sha256(csv_bytes).hexdigest()

    # source_summary_by_day
    all_days = sorted(set(d["data_dia"] for d in source_docs))
    day_stats = []
    for dia in all_days:
        day_docs = [d for d in source_docs if d["data_dia"] == dia]
        comercial = [d for d in day_docs if d["commercial_eligible"] == 1]
        sit3 = [d for d in day_docs if d["classification"] == "situacao_3_ignorada"]
        canc = [d for d in day_docs if d["cancelado"] == 1]
        inut = [d for d in day_docs if d["classification"] == "nfe_inutilizada"]
        nfe5 = [d for d in day_docs if d.get("nfe_status") is not None and int(d["nfe_status"]) == 5]
        day_stats.append({
            "data_dia": dia,
            "count_all": len(day_docs),
            "total_all": round(sum(d["total_header"] for d in day_docs), 2),
            "count_comercial": len(comercial),
            "total_comercial": round(sum(d["total_header"] for d in comercial), 2),
            "count_situacao_3": len(sit3),
            "total_situacao_3": round(sum(d["total_header"] for d in sit3), 2),
            "count_cancelado": len(canc),
            "total_cancelado": round(sum(d["total_header"] for d in canc), 2),
            "count_nfe_inutilizada": len(inut),
            "total_nfe_inutilizada": round(sum(d["total_header"] for d in inut), 2),
            "count_nfe_status5": len(nfe5),
            "total_nfe_status5": round(sum(d["total_header"] for d in nfe5), 2),
        })
    write_csv(day_stats, out_dir / "source_summary_by_day.csv")
    write_json(day_stats, out_dir / "source_summary_by_day.json")

    # source_manifest.json
    manifest = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "host": cfg.host,
        "database": cfg.database,
        "backend": _CONN_BACKEND,
        "id_filial": id_filial,
        "date_from": str(date_from),
        "date_to": str(date_to),
        "row_count": len(source_docs),
        "unique_key_count": len(set((d["id_filial"], d["id_db"], d["id_comprovante"]) for d in source_docs)),
        "duplicate_key_count": len(duplicates) if duplicates else 0,
        "source_duplicate_keys_file": "source_duplicate_keys.csv" if duplicates else None,
        "sqlserver_table_comprovantes": t_comp,
        "sqlserver_table_nfe": t_nfe or "not found",
        "nfe_join_strategy": nfe_join_strategy,
        "total_all": round(sum(d["total_header"] for d in source_docs), 2),
        "total_comercial": round(sum(d["total_header"] for d in source_docs if d["commercial_eligible"] == 1), 2),
        "sha256": sha256_hex,
        "query_version": "2.0",
        "timezone": "America/Sao_Paulo",
        "warnings": warnings_list,
    }
    write_json(manifest, out_dir / "source_manifest.json")

    # source_export_report.md
    total_raw = len(source_docs)
    total_comercial = sum(1 for d in source_docs if d["commercial_eligible"] == 1)
    total_sit3 = sum(1 for d in source_docs if d["classification"] == "situacao_3_ignorada")
    total_canc = sum(1 for d in source_docs if d["cancelado"] == 1)
    total_nfe5 = sum(1 for d in source_docs if d.get("nfe_status") is not None and int(d["nfe_status"]) == 5)

    md_lines = [
        "# Source Export Report",
        "",
        "## Período",
        f"- De: **{date_from}**",
        f"- Até: **{date_to}** (inclusive)",
        f"- Dias: **{len(all_days)}**",
        "",
        "## Filial",
        f"- ID: **{id_filial}**",
        "",
        "## Conexão",
        f"- Host: `{cfg.host}`",
        f"- Database: `{cfg.database}`",
        f"- User: `{cfg.user}`",
        f"- Backend: `{_CONN_BACKEND}`",
        "",
        "## Tabelas detectadas",
        f"- COMPROVANTES: `{t_comp}`",
        f"- NFE: `{t_nfe or 'não encontrada'}`",
        "",
        "## Totais por dia",
        "",
        "| Dia | Total | Comercial | Sit3 | Cancelados | NFE Status=5 |",
        "|-----|-------|-----------|------|------------|--------------|",
    ]
    for ds in day_stats:
        md_lines.append(
            f"| {ds['data_dia']} | {ds['count_all']} ({ds['total_all']:,.2f}) | "
            f"{ds['count_comercial']} ({ds['total_comercial']:,.2f}) | "
            f"{ds['count_situacao_3']} ({ds['total_situacao_3']:,.2f}) | "
            f"{ds['count_cancelado']} ({ds['total_cancelado']:,.2f}) | "
            f"{ds['count_nfe_status5']} ({ds['total_nfe_status5']:,.2f}) |"
        )
    md_lines.append("")
    md_lines.append("## Totais gerais")
    md_lines.append(f"- Total raw: **{total_raw}**")
    md_lines.append(f"- Total comercial: **{total_comercial}**")
    md_lines.append(f"- Situação=3: **{total_sit3}**")
    md_lines.append(f"- Cancelados: **{total_canc}**")
    md_lines.append(f"- NFE status=5: **{total_nfe5}**")
    md_lines.append(f"- SHA256: `{sha256_hex}`")
    md_lines.append("")
    write_md("\n".join(md_lines), out_dir / "source_export_report.md")

    # Filtered CSVs
    nfe5_docs = [d for d in source_docs if d.get("nfe_status") is not None and int(d["nfe_status"]) == 5]
    sit3_docs = [d for d in source_docs if d["classification"] == "situacao_3_ignorada"]
    canc_docs = [d for d in source_docs if d["cancelado"] == 1]
    write_csv(nfe5_docs, out_dir / "nfe_status5_source.csv", fieldnames=ledger_fieldnames)
    write_csv(sit3_docs, out_dir / "situacao3_source.csv", fieldnames=ledger_fieldnames)
    write_csv(canc_docs, out_dir / "cancelados_source.csv", fieldnames=ledger_fieldnames)

    log.info("Export complete → %s (%d docs, sha256=%s)", out_dir, len(source_docs), sha256_hex[:16])


# ═══════════════════════════════════════════════════════════════════════════
# compare-source-ledger-to-stg
# ═══════════════════════════════════════════════════════════════════════════


def _validate_manifest_match(manifest: dict, id_filial: str, date_from: str, date_to: str) -> Tuple[bool, List[str]]:
    """Validate manifest against compare args. Returns (ok, warnings).

    ok=False means a hard mismatch (filial), warnings collects date mismatches.
    """
    warnings: List[str] = []
    manifest_filial = str(manifest.get("id_filial", ""))
    if manifest_filial and manifest_filial != str(id_filial):
        return False, [f"Manifest id_filial={manifest_filial} does not match id_filial={id_filial}"]

    manifest_from = manifest.get("date_from", "")
    manifest_to = manifest.get("date_to", "")
    if manifest_from and manifest_from != str(date_from):
        warnings.append(f"Manifest date_from mismatch: {manifest_from} vs {date_from}")
    if manifest_to and manifest_to != str(date_to):
        warnings.append(f"Manifest date_to mismatch: {manifest_to} vs {date_to}")

    return True, warnings


def cmd_compare_source_ledger_to_stg(cfg: Config, args: argparse.Namespace) -> None:
    """Compare exported source CSV against STG PostgreSQL."""
    out_dir = ensure_dir(args.out)
    id_filial = args.id_filial
    date_from = datetime.strptime(args.date_from, "%Y-%m-%d").date()
    date_to = datetime.strptime(args.date_to, "%Y-%m-%d").date()
    date_to_exclusive = date_to + timedelta(days=1)

    # ─── 1) Read manifest and validate SHA256 ─────────────────────────────
    manifest_path = Path(args.source_manifest)
    ledger_path = Path(args.source_ledger)

    if not manifest_path.is_file():
        log.error("Manifest file not found: %s", manifest_path)
        sys.exit(1)
    if not ledger_path.is_file():
        log.error("Source ledger CSV not found: %s", ledger_path)
        sys.exit(1)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    csv_bytes = ledger_path.read_bytes()
    actual_sha256 = hashlib.sha256(csv_bytes).hexdigest()
    expected_sha256 = manifest.get("sha256", "")

    warnings_list: List[str] = []
    if actual_sha256 != expected_sha256:
        log.error("SHA256 MISMATCH! Source ledger may have been modified after export.")
        log.error("Expected: %s", expected_sha256)
        log.error("Actual:   %s", actual_sha256)
        sys.exit(1)

    # Validate manifest against args
    manifest_filial = str(manifest.get("id_filial", ""))
    if manifest_filial and manifest_filial != str(id_filial):
        log.error("Manifest id_filial=%s does not match --id-filial=%s", manifest_filial, id_filial)
        sys.exit(1)

    manifest_from = manifest.get("date_from", "")
    manifest_to = manifest.get("date_to", "")
    if manifest_from and manifest_from != str(date_from):
        log.warning("Manifest date_from=%s differs from --date-from=%s", manifest_from, date_from)
        warnings_list.append(f"Manifest date_from mismatch: {manifest_from} vs {date_from}")
    if manifest_to and manifest_to != str(date_to):
        log.warning("Manifest date_to=%s differs from --date-to=%s", manifest_to, date_to)
        warnings_list.append(f"Manifest date_to mismatch: {manifest_to} vs {date_to}")

    # Snapshot time warning
    today_sp = _get_sp_today()
    if date_to >= today_sp:
        warnings_list.append(f"Period includes today or future ({date_to}); STG may have changed since export.")
        log.warning("Period includes open day %s; delta may be ongoing ingestion.", date_to)

    # ─── 2) Read source CSV ───────────────────────────────────────────────
    source_docs_raw: List[Dict] = []
    with open(ledger_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source_docs_raw.append(row)

    # Parse types and filter
    source_docs: List[Dict] = []
    for row in source_docs_raw:
        doc = {
            "id_filial": str(row.get("id_filial", "")),
            "id_db": str(row.get("id_db", "")),
            "id_comprovante": str(row.get("id_comprovante", "")),
            "data": row.get("data", ""),
            "data_dia": str(row.get("data_dia", "")),
            "situacao": int(row["situacao"]) if row.get("situacao") not in (None, "", "None") else 0,
            "cancelado": int(row["cancelado"]) if row.get("cancelado") not in (None, "", "None") else 0,
            "referencia": row.get("referencia") if row.get("referencia") not in ("None", "") else None,
            "id_turno": row.get("id_turno") if row.get("id_turno") not in ("None", "") else None,
            "id_usuario": row.get("id_usuario") if row.get("id_usuario") not in ("None", "") else None,
            "id_localvenda": row.get("id_localvenda") if row.get("id_localvenda") not in ("None", "") else None,
            "total_header": float(row["total_header"]) if row.get("total_header") not in (None, "", "None") else 0.0,
            "nfe_status": int(row["nfe_status"]) if row.get("nfe_status") not in (None, "", "None") else None,
            "nfe_nronf": row.get("nfe_nronf") if row.get("nfe_nronf") not in ("None", "") else None,
            "nfe_chaveacesso_masked": row.get("nfe_chaveacesso_masked") if row.get("nfe_chaveacesso_masked") not in ("None", "") else None,
            "nfe_data": row.get("nfe_data") if row.get("nfe_data") not in ("None", "") else None,
            "commercial_eligible": int(row["commercial_eligible"]) if row.get("commercial_eligible") not in (None, "", "None") else 0,
            "classification": row.get("classification", ""),
        }
        # Filter by id_filial and date range
        if doc["id_filial"] != str(id_filial):
            continue
        if doc["data_dia"] < str(date_from) or doc["data_dia"] >= str(date_to_exclusive):
            continue
        source_docs.append(doc)

    log.info("Loaded %d source docs from CSV (filtered from %d raw).", len(source_docs), len(source_docs_raw))

    # ─── 3) Query STG ─────────────────────────────────────────────────────
    if not cfg.stg_pg_host:
        log.error("STG_PG_HOST not configured.")
        sys.exit(1)

    pg_conn = get_pg_connection(cfg)

    sql_stg_with_nfe = """\
SELECT
    c.id_filial::text AS id_filial,
    c.id_db::text AS id_db,
    c.id_comprovante::text AS id_comprovante,
    (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::timestamp AS data,
    (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date AS data_dia,
    c.situacao_shadow AS situacao,
    CASE WHEN c.cancelado_shadow THEN 1 ELSE 0 END AS cancelado,
    COALESCE(c.valor_total_shadow, 0) AS total_header,
    c.referencia_shadow AS referencia,
    c.id_turno_shadow AS id_turno,
    c.id_usuario_shadow AS id_usuario,
    n.status_shadow AS nfe_status,
    n.numero_nfe_shadow AS nfe_nronf,
    n.chave_nfe_shadow AS nfe_chaveacesso,
    (n.data_emissao_shadow AT TIME ZONE 'America/Sao_Paulo')::timestamp AS nfe_data
FROM stg.comprovantes c
LEFT JOIN stg.nfe n ON c.id_filial = n.id_filial AND c.id_db = n.id_db AND c.id_comprovante = n.id_comprovante
WHERE c.id_filial = %s
  AND (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date >= %s::date
  AND (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date < %s::date
ORDER BY c.dt_evento, c.id_comprovante
"""

    sql_stg_without_nfe = """\
SELECT
    c.id_filial::text AS id_filial,
    c.id_db::text AS id_db,
    c.id_comprovante::text AS id_comprovante,
    (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::timestamp AS data,
    (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date AS data_dia,
    c.situacao_shadow AS situacao,
    CASE WHEN c.cancelado_shadow THEN 1 ELSE 0 END AS cancelado,
    COALESCE(c.valor_total_shadow, 0) AS total_header,
    c.referencia_shadow AS referencia,
    c.id_turno_shadow AS id_turno,
    c.id_usuario_shadow AS id_usuario,
    NULL::integer AS nfe_status,
    NULL::text AS nfe_nronf,
    NULL::text AS nfe_chaveacesso,
    NULL::timestamp AS nfe_data
FROM stg.comprovantes c
WHERE c.id_filial = %s
  AND (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date >= %s::date
  AND (c.dt_evento AT TIME ZONE 'America/Sao_Paulo')::date < %s::date
ORDER BY c.dt_evento, c.id_comprovante
"""

    stg_rows_raw = []
    try:
        stg_rows_raw = pg_query(pg_conn, sql_stg_with_nfe,
                                (str(id_filial), str(date_from), str(date_to_exclusive)))
    except Exception as exc:
        log.warning("STG query with NFE join failed (%s), retrying without NFE...", exc)
        try:
            pg_conn.close()
            pg_conn = get_pg_connection(cfg)
            stg_rows_raw = pg_query(pg_conn, sql_stg_without_nfe,
                                    (str(id_filial), str(date_from), str(date_to_exclusive)))
        except Exception as exc2:
            log.error("STG query without NFE also failed: %s", exc2)
            pg_conn.close()
            sys.exit(1)
    pg_conn.close()
    stg_query_time = datetime.utcnow().isoformat() + "Z"
    log.info("STG returned %d rows.", len(stg_rows_raw))

    # Process STG rows
    stg_docs: List[Dict] = []
    for r in stg_rows_raw:
        doc = {
            "id_filial": str(r.get("id_filial", "")),
            "id_db": str(r.get("id_db", "")),
            "id_comprovante": str(r.get("id_comprovante", "")),
            "data": str(r.get("data", "")),
            "data_dia": str(r.get("data_dia", "")),
            "situacao": r.get("situacao"),
            "cancelado": int(r.get("cancelado") or 0),
            "total_header": float(r.get("total_header") or 0),
            "referencia": r.get("referencia"),
            "id_turno": r.get("id_turno"),
            "id_usuario": r.get("id_usuario"),
            "nfe_status": r.get("nfe_status"),
            "nfe_nronf": r.get("nfe_nronf"),
            "nfe_chaveacesso": mask_pii(r.get("nfe_chaveacesso"), "chaveacesso") if r.get("nfe_chaveacesso") else None,
            "nfe_data": str(r.get("nfe_data", "")) if r.get("nfe_data") else None,
        }
        doc["classification"] = classify_comprovante(doc["situacao"], doc["cancelado"], doc["nfe_status"])
        doc["commercial_eligible"] = 1 if is_commercial(doc["situacao"], doc["cancelado"]) else 0
        stg_docs.append(doc)

    # ─── 4) Compare ───────────────────────────────────────────────────────
    cmp = _compare_docs(source_docs, stg_docs)
    source_only = cmp["source_only"]
    stg_only = cmp["stg_only"]
    total_mismatch = cmp["total_mismatch"]
    status_mismatch = cmp["status_mismatch"]
    nfe_mismatch = cmp["nfe_mismatch"]
    nfe_missing_in_stg = cmp["nfe_missing_in_stg"]
    nfe_missing_in_source = cmp["nfe_missing_in_source"]
    classification_mismatch = cmp["classification_mismatch"]

    day_summaries = _compute_day_summaries(
        source_docs, stg_docs, source_only, stg_only,
        total_mismatch, status_mismatch, nfe_mismatch,
    )
    delta_explanations = _compute_delta_explanations(
        day_summaries, source_only, stg_only, total_mismatch,
        status_mismatch, nfe_missing_in_stg, classification_mismatch,
    )

    # ─── 5) Write outputs ─────────────────────────────────────────────────
    # Ledgers
    write_csv(source_docs, out_dir / "source_ledger.csv")
    write_csv(stg_docs, out_dir / "stg_ledger.csv")

    # Freeze STG snapshot if requested
    if getattr(args, "freeze_stg_snapshot_out", False):
        stg_csv_bytes = (out_dir / "stg_ledger.csv").read_bytes()
        stg_sha256 = hashlib.sha256(stg_csv_bytes).hexdigest()
        stg_manifest = {
            "generated_at": stg_query_time,
            "stg_host": cfg.stg_pg_host,
            "stg_database": cfg.stg_pg_database,
            "row_count": len(stg_docs),
            "sha256": stg_sha256,
        }
        write_json(stg_manifest, out_dir / "stg_manifest.json")

    # Divergences
    write_csv(source_only, out_dir / "source_only.csv")
    write_csv(stg_only, out_dir / "stg_only.csv")
    write_csv(total_mismatch, out_dir / "total_mismatch.csv")
    write_csv(status_mismatch, out_dir / "status_mismatch.csv")
    write_csv(nfe_mismatch, out_dir / "nfe_mismatch.csv")
    write_csv(nfe_missing_in_stg, out_dir / "nfe_missing_in_stg.csv")
    write_csv(nfe_missing_in_source, out_dir / "nfe_missing_in_source.csv")
    write_csv(classification_mismatch, out_dir / "classification_mismatch.csv")

    # Day summaries
    write_csv(day_summaries, out_dir / "compare_summary_by_day.csv")
    write_json(day_summaries, out_dir / "compare_summary_by_day.json")

    # Delta explanation
    write_csv(delta_explanations, out_dir / "delta_explanation_by_day.csv")
    write_json(delta_explanations, out_dir / "delta_explanation_by_day.json")

    # Top delta documents
    top_delta_docs = []
    for d in source_only:
        if d["commercial_eligible"] == 1:
            top_delta_docs.append({**d, "delta_type": "source_only", "delta_value": d["total_header"]})
    for d in stg_only:
        if d["commercial_eligible"] == 1:
            top_delta_docs.append({**d, "delta_type": "stg_only", "delta_value": -d["total_header"]})
    for d in total_mismatch:
        top_delta_docs.append({**d, "delta_type": "total_mismatch", "delta_value": d.get("diff", 0)})
    top_delta_docs.sort(key=lambda x: abs(x.get("delta_value", 0)), reverse=True)
    write_csv(top_delta_docs[:100], out_dir / "top_delta_documents.csv")

    # NFE status=5
    nfe_status5_src = [d for d in source_docs if d.get("nfe_status") is not None and int(d["nfe_status"]) == 5]
    nfe_status5_stg = [d for d in stg_docs if d.get("nfe_status") is not None and int(d["nfe_status"]) == 5]
    write_csv(nfe_status5_src, out_dir / "nfe_status5_source.csv")
    write_csv(nfe_status5_stg, out_dir / "nfe_status5_stg.csv")

    # Situacao 3
    sit3_src = [d for d in source_docs if d["classification"] == "situacao_3_ignorada"]
    sit3_stg = [d for d in stg_docs if d["classification"] == "situacao_3_ignorada"]
    write_csv(sit3_src, out_dir / "situacao3_source.csv")
    write_csv(sit3_stg, out_dir / "situacao3_stg.csv")

    # Cancelados
    canc_src = [d for d in source_docs if d["cancelado"] == 1]
    canc_stg = [d for d in stg_docs if d["cancelado"] == 1]
    write_csv(canc_src, out_dir / "cancelados_source.csv")
    write_csv(canc_stg, out_dir / "cancelados_stg.csv")

    # Summary JSON
    all_days = sorted(set(
        [d["data_dia"] for d in source_docs] +
        [d["data_dia"] for d in stg_docs]
    ))
    overall_pass = all(ds["day_pass"] for ds in day_summaries) if day_summaries else False
    summary = {
        "id_filial": id_filial,
        "date_from": str(date_from),
        "date_to": str(date_to),
        "days_audited": len(all_days),
        "overall_pass": overall_pass,
        "source_count_total": len(source_docs),
        "stg_count_total": len(stg_docs),
        "source_only_count": len(source_only),
        "stg_only_count": len(stg_only),
        "total_mismatch_count": len(total_mismatch),
        "status_mismatch_count": len(status_mismatch),
        "nfe_mismatch_count": len(nfe_mismatch),
        "nfe_missing_in_stg_count": len(nfe_missing_in_stg),
        "nfe_missing_in_source_count": len(nfe_missing_in_source),
        "classification_mismatch_count": len(classification_mismatch),
        "source_total_comercial": round(sum(d["total_header"] for d in source_docs if d["commercial_eligible"] == 1), 2),
        "stg_total_comercial": round(sum(d["total_header"] for d in stg_docs if d["commercial_eligible"] == 1), 2),
        "manifest_file": str(manifest_path),
        "sha256_valid": actual_sha256 == expected_sha256,
        "source_generated_at": manifest.get("generated_at"),
        "stg_queried_at": stg_query_time,
        "warnings": warnings_list,
    }
    summary["total_comercial_delta"] = round(summary["source_total_comercial"] - summary["stg_total_comercial"], 2)
    write_json(summary, out_dir / "compare_summary.json")

    # ─── Compare report MD ────────────────────────────────────────────────
    days_pass = sum(1 for ds in day_summaries if ds["day_pass"])
    days_fail = len(day_summaries) - days_pass

    md_lines = [
        "# Compare Source Ledger vs STG",
        "",
        "## 1. Período auditado",
        f"- De: **{date_from}**",
        f"- Até: **{date_to}** (inclusive)",
        f"- Dias: **{len(all_days)}**",
        "",
        "## 2. Filial",
        f"- ID: **{id_filial}**",
        "",
        "## 3. Manifest da fonte",
        f"- Host: `{manifest.get('host', 'N/A')}`",
        f"- Database: `{manifest.get('database', 'N/A')}`",
        f"- Backend: `{manifest.get('backend', 'N/A')}`",
        f"- Row count: {manifest.get('row_count', 'N/A')}",
        f"- SHA256 valid: **{'YES' if actual_sha256 == expected_sha256 else 'NO'}**",
        f"- Fonte exportada em: {manifest.get('generated_at', 'N/A')}",
        f"- STG consultada em: {stg_query_time}",
        "",
        "## 4. Conexão STG",
        f"- PostgreSQL: `{cfg.stg_pg_host}/{cfg.stg_pg_database}` (user: {cfg.stg_pg_user})",
        "",
        "## 5. Resumo geral",
        "",
        "| Metric | Source | STG | Delta |",
        "|--------|--------|-----|-------|",
        f"| Total docs | {len(source_docs):,} | {len(stg_docs):,} | {len(source_docs) - len(stg_docs):+,} |",
        f"| Comerciais | {summary['source_total_comercial']:,.2f} | {summary['stg_total_comercial']:,.2f} | {summary['total_comercial_delta']:+,.2f} |",
        f"| Source only | {len(source_only)} | — | — |",
        f"| STG only | — | {len(stg_only)} | — |",
        f"| Total mismatch | {len(total_mismatch)} | — | — |",
        f"| Status mismatch | {len(status_mismatch)} | — | — |",
        f"| NFE mismatch | {len(nfe_mismatch)} | — | — |",
        f"| NFE missing STG | {len(nfe_missing_in_stg)} | — | — |",
        f"| NFE missing source | — | {len(nfe_missing_in_source)} | — |",
        f"| Classification mismatch | {len(classification_mismatch)} | — | — |",
        "",
        "## 6. Resumo por dia",
        "",
        "| Dia | Src | STG | Delta | Comercial Src | Comercial STG | Delta Com. | PASS |",
        "|-----|-----|-----|-------|---------------|---------------|------------|------|",
    ]
    for ds in day_summaries:
        icon = "✅" if ds["day_pass"] else "❌"
        md_lines.append(
            f"| {ds['data_dia']} | {ds['source_count_all']} | {ds['stg_count_all']} | "
            f"{ds['count_delta']:+} | {ds['source_total_comercial']:,.2f} | "
            f"{ds['stg_total_comercial']:,.2f} | {ds['total_comercial_delta']:+,.2f} | {icon} |"
        )
    md_lines.append("")

    md_lines.append("## 7. Dias PASS/FAIL")
    md_lines.append(f"- PASS: **{days_pass}**")
    md_lines.append(f"- FAIL: **{days_fail}**")
    md_lines.append(f"- Overall: **{'PASS' if overall_pass else 'FAIL'}**")
    md_lines.append("")

    md_lines.append("## 8. Delta comercial por dia")
    md_lines.append("")
    if delta_explanations:
        md_lines.append("| Dia | Delta | Src Only | STG Only | Mismatch | Status | NFE Inut | Sit3 | Explained | Unexplained |")
        md_lines.append("|-----|-------|----------|----------|----------|--------|----------|------|-----------|-------------|")
        for de in delta_explanations:
            md_lines.append(
                f"| {de['data_dia']} | {de['delta_total_comercial']:+,.2f} | "
                f"{de['source_only_comercial_total']:,.2f} | {de['stg_only_comercial_total']:,.2f} | "
                f"{de['total_mismatch_diff']:,.2f} | {de['status_mismatch_effect']:,.2f} | "
                f"{de['nfe_inutilizada_effect']:,.2f} | {de['situacao_3_effect']:,.2f} | "
                f"{de['delta_explained_amount']:,.2f} | {de['unexplained_delta']:,.2f} |"
            )
    else:
        md_lines.append("Nenhum dia com delta comercial significativo.")
    md_lines.append("")

    md_lines.append("## 9. Delta raw/all por dia")
    md_lines.append("")
    md_lines.append("| Dia | Src All | STG All | Delta All |")
    md_lines.append("|-----|---------|---------|-----------|")
    for ds in day_summaries:
        md_lines.append(
            f"| {ds['data_dia']} | {ds['source_total_all']:,.2f} | "
            f"{ds['stg_total_all']:,.2f} | {ds['total_all_delta']:+,.2f} |"
        )
    md_lines.append("")

    md_lines.append("## 10. Explicação do delta")
    md_lines.append("")
    md_lines.append("- **source_only_comercial_total**: Faturamento de docs comerciais presentes apenas na fonte")
    md_lines.append("- **stg_only_comercial_total**: Faturamento de docs comerciais presentes apenas na STG")
    md_lines.append("- **total_mismatch_diff**: Soma das diferenças de valor para docs com total divergente")
    md_lines.append("- **status_mismatch_effect**: Efeito de docs com status comercial divergente")
    md_lines.append("- **nfe_inutilizada_effect**: Docs com NFE inutilizada na fonte, sem NFE na STG")
    md_lines.append("- **situacao_3_effect**: Docs com classificação situacao_3 divergente")
    md_lines.append("")

    md_lines.append("## 11. Top documentos responsáveis")
    md_lines.append("")
    if top_delta_docs:
        md_lines.append("| Filial | DB | Comprovante | Dia | Tipo | Valor Delta |")
        md_lines.append("|--------|----|-------------|-----|------|-------------|")
        for d in top_delta_docs[:20]:
            md_lines.append(
                f"| {d['id_filial']} | {d['id_db']} | {d['id_comprovante']} | "
                f"{d.get('data_dia', '')} | {d['delta_type']} | {d.get('delta_value', 0):+,.2f} |"
            )
    else:
        md_lines.append("Nenhum documento com delta significativo.")
    md_lines.append("")

    md_lines.append("## 12. NFE inutilizadas (status=5)")
    md_lines.append(f"- Fonte: {len(nfe_status5_src)} docs")
    md_lines.append(f"- STG: {len(nfe_status5_stg)} docs")
    md_lines.append("")

    md_lines.append("## 13. Situação=3")
    md_lines.append(f"- Fonte: {len(sit3_src)} docs")
    md_lines.append(f"- STG: {len(sit3_stg)} docs")
    md_lines.append("")

    md_lines.append("## 14. Cancelados")
    md_lines.append(f"- Fonte: {len(canc_src)} docs")
    md_lines.append(f"- STG: {len(canc_stg)} docs")
    md_lines.append("")

    md_lines.append("## 15. Diagnóstico provável por camada")
    md_lines.append("")
    if not summary["sha256_valid"]:
        md_lines.append("- **SHA256 INVÁLIDO**: O CSV fonte foi alterado após export. Dados não confiáveis.")
    if len(source_only) > 0:
        md_lines.append("- **source_only > 0**: Fonte possui documentos ausentes na STG. "
                        "Investigar Agent, watermark, API ingest ou rejeição.")
    if len(stg_only) > 0:
        md_lines.append("- **stg_only > 0**: STG possui documentos não encontrados na fonte. "
                        "Investigar banco/host, data, timezone, registros antigos ou ambiente errado.")
    if len(status_mismatch) > 0:
        md_lines.append("- **status_mismatch > 0**: Status SITUACAO/CANCELADO diverge. "
                        "Investigar atualização incremental, CDC, upsert ou schema mapping.")
    if len(nfe_missing_in_stg) > 0:
        md_lines.append("- **nfe_missing_in_stg > 0**: NFE existe na fonte e não na STG. "
                        "Investigar dataset NFE do Agent/API ingest.")
    if (len(source_only) == 0 and len(stg_only) == 0 and len(total_mismatch) == 0
            and len(status_mismatch) == 0 and summary["total_comercial_delta"] != 0):
        md_lines.append("- **raw matches but commercial doesn't**: Dados brutos chegaram, "
                        "mas classificação comercial/status diverge.")
    if overall_pass:
        md_lines.append("- **Fonte exportada e STG alinham para o período.** "
                        "Verificar se export é da fonte canônica real (CENTRALVR/ATXDADOS).")
    md_lines.append("")

    md_lines.append("## 16. Próxima ação recomendada")
    md_lines.append("")
    if overall_pass:
        md_lines.append("Fonte e STG estão alinhadas. Investigar camadas seguintes: "
                        "CDC Consumer → ClickHouse raw → current → slim → mart_rt → API → Web.")
    elif len(source_only) > 0:
        md_lines.append("Prioridade: investigar documentos presentes na fonte mas ausentes na STG. "
                        "Verificar Agent watermark, API ingest logs, rejeições de schema.")
    elif len(stg_only) > 0:
        md_lines.append("Prioridade: investigar documentos na STG sem correspondência na fonte. "
                        "Verificar se o banco/filial/host está correto.")
    else:
        md_lines.append("Investigar mismatches de valor/status. Verificar CDC/upsert logic.")
    md_lines.append("")

    write_md("\n".join(md_lines), out_dir / "compare_report.md")
    log.info("Compare complete → %s (overall: %s)", out_dir, "PASS" if overall_pass else "FAIL")


# ═══════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="xpert_source_explorer",
        description="TorqMind Xpert Source Explorer — read-only SQL Server discovery",
    )
    p.add_argument("--version", action="version", version=f"%(prog)s {VERSION}")
    sub = p.add_subparsers(dest="command", required=True)

    # test-connection
    tc = sub.add_parser("test-connection", help="Test SQL Server connection")
    tc.add_argument("--env", default=None, help="Path to env file")
    tc.set_defaults(func=cmd_test_connection)

    # discover-schema
    ds = sub.add_parser("discover-schema", help="Discover full schema")
    ds.add_argument("--env", default=None)
    ds.add_argument("--out", required=True, help="Output directory")
    ds.set_defaults(func=cmd_discover_schema)

    # dump-definitions
    dd = sub.add_parser("dump-definitions", help="Dump procedures/views/functions/triggers")
    dd.add_argument("--env", default=None)
    dd.add_argument("--out", required=True)
    dd.set_defaults(func=cmd_dump_definitions)

    # profile-all-tables
    pa = sub.add_parser("profile-all-tables", help="Profile all tables")
    pa.add_argument("--env", default=None)
    pa.add_argument("--out", required=True)
    pa.add_argument("--sample-rows", type=int, default=50)
    pa.add_argument("--top-values", type=int, default=20)
    pa.set_defaults(func=cmd_profile_all_tables)

    # find-business-objects
    fb = sub.add_parser("find-business-objects", help="Classify tables by business domain")
    fb.add_argument("--env", default=None)
    fb.add_argument("--out", required=True)
    fb.set_defaults(func=cmd_find_business_objects)

    # audit-sales-day
    asd = sub.add_parser("audit-sales-day", help="Audit sales for a specific day")
    asd.add_argument("--env", default=None)
    asd.add_argument("--id-filial", required=True)
    asd.add_argument("--date", required=True, help="YYYY-MM-DD")
    asd.add_argument("--out", required=True)
    asd.set_defaults(func=cmd_audit_sales_day)

    # compare-stg-sales-day
    csd = sub.add_parser("compare-stg-sales-day", help="Compare source SQL Server vs PostgreSQL STG")
    csd.add_argument("--env", default=None)
    csd.add_argument("--id-filial", required=True)
    csd.add_argument("--date", required=True, help="YYYY-MM-DD")
    csd.add_argument("--out", required=True)
    csd.set_defaults(func=cmd_compare_stg_sales_day)

    # finance-discovery
    fd = sub.add_parser("finance-discovery", help="Discover finance-related tables")
    fd.add_argument("--env", default=None)
    fd.add_argument("--out", required=True)
    fd.set_defaults(func=cmd_finance_discovery)

    # customers-discovery
    cd = sub.add_parser("customers-discovery", help="Discover customer-related tables")
    cd.add_argument("--env", default=None)
    cd.add_argument("--out", required=True)
    cd.set_defaults(func=cmd_customers_discovery)

    # nfe-discovery
    nd = sub.add_parser("nfe-discovery", help="Discover NFE/NFC-e tables and status")
    nd.add_argument("--env", default=None)
    nd.add_argument("--out", required=True)
    nd.set_defaults(func=cmd_nfe_discovery)

    # query
    q = sub.add_parser("query", help="Execute a raw SELECT query from file")
    q.add_argument("--env", default=None)
    q.add_argument("--sql-file", required=True, help="Path to .sql file")
    q.add_argument("--out", required=True)
    q.set_defaults(func=cmd_query)

    # full-discovery
    full = sub.add_parser("full-discovery", help="Run all discovery steps")
    full.add_argument("--env", default=None)
    full.add_argument("--out", required=True)
    full.set_defaults(func=cmd_full_discovery)

    # test-stg-connection
    tsc = sub.add_parser("test-stg-connection", help="Test PostgreSQL STG connection")
    tsc.add_argument("--env", default=None)
    tsc.set_defaults(func=cmd_test_stg_connection)

    # compare-stg-comprovantes-range
    csr = sub.add_parser("compare-stg-comprovantes-range", help="Compare source vs STG comprovantes for date range")
    csr.add_argument("--env", default=None)
    csr.add_argument("--id-filial", required=True)
    csr.add_argument("--date-from", default=None, help="Start date YYYY-MM-DD (default: 15 days ago)")
    csr.add_argument("--date-to", default=None, help="End date YYYY-MM-DD inclusive (default: yesterday)")
    csr.add_argument("--stg-only", action="store_true", default=False, help="Skip SQL Server, audit STG data only")
    csr.add_argument("--out", required=True)
    csr.set_defaults(func=cmd_compare_stg_comprovantes_range)

    # export-source-comprovantes-range
    esc = sub.add_parser("export-source-comprovantes-range", help="Export source SQL Server comprovantes to files")
    esc.add_argument("--env", default=None)
    esc.add_argument("--id-filial", required=True)
    esc.add_argument("--date-from", required=True, help="Start date YYYY-MM-DD (inclusive)")
    esc.add_argument("--date-to", required=True, help="End date YYYY-MM-DD (inclusive)")
    esc.add_argument("--out", required=True)
    esc.set_defaults(func=cmd_export_source_comprovantes_range)

    # compare-source-ledger-to-stg
    csl = sub.add_parser("compare-source-ledger-to-stg", help="Compare exported source CSV against STG PostgreSQL")
    csl.add_argument("--env", default=None)
    csl.add_argument("--source-ledger", required=True, help="Path to source_ledger.csv from export")
    csl.add_argument("--source-manifest", required=True, help="Path to source_manifest.json from export")
    csl.add_argument("--id-filial", required=True)
    csl.add_argument("--date-from", required=True, help="Start date YYYY-MM-DD (inclusive)")
    csl.add_argument("--date-to", required=True, help="End date YYYY-MM-DD (inclusive)")
    csl.add_argument("--freeze-stg-snapshot-out", action="store_true", default=False,
                     help="Save STG snapshot manifest alongside outputs")
    csl.add_argument("--out", required=True)
    csl.set_defaults(func=cmd_compare_source_ledger_to_stg)

    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    env_path = getattr(args, "env", None)
    cfg = Config(env_path)
    args.func(cfg, args)


if __name__ == "__main__":
    main()
