from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import psycopg

from app.cash_operational_truth import missing_runtime_relation_columns
from app.db import _conn_str

EXPECTED_RUNTIME_COLUMNS: tuple[tuple[str, str, str], ...] = (
    ("auth", "users", "nome"),
    ("auth", "users", "role"),
    ("auth", "user_tenants", "channel_id"),
    ("auth", "user_tenants", "valid_from"),
    ("auth", "filiais", "valid_from"),
    ("app", "tenants", "channel_id"),
    ("app", "tenants", "sales_history_days"),
    ("app", "tenants", "default_product_scope_days"),
    ("billing", "contracts", "tenant_id"),
)

TRACKING_SCHEMA = "app"
TRACKING_TABLE = "schema_migrations"
TRACKING_FQN = f"{TRACKING_SCHEMA}.{TRACKING_TABLE}"
TORQMIND_SCHEMAS = ("auth", "app", "stg", "dw", "mart", "etl", "billing")
BOOTSTRAP_ONLY_SCHEMAS = frozenset({"auth", "app"})
NONTRANSACTIONAL_MARKER = "-- @nontransactional"
ACCEPTED_CHECKSUM_ALIASES: dict[str, frozenset[str]] = {
    # 036 shipped first as a transactional CREATE INDEX migration. Accept that checksum so
    # already-tracked environments can move forward after the deploy-safe rewrite.
    "036_operational_publication_overlay_indexes.sql": frozenset(
        {"d7fa2ec538e1632e3d88fd5b8ec0a09d1d0f83177960f5f2af5facbc83485674"}
    ),
    # 071 was applied in one environment before a no-op CTE cleanup landed in git.
    # Keep the tracked checksum accepted so deployed databases do not block future migrations.
    "071_payment_notification_hash_schema_compat.sql": frozenset(
        {"40bc100c429efcd86c67f112a5493f91ac4a28c3ce0293347189ab60f8c51196"}
    ),
}

# v1 = sha256(raw bytes) used by some historical baselines.
# v2 = sha256(decoded SQL encoded UTF-8) used by managed apply / new ledger rows.
CHECKSUM_ALGO_VERSION = 2
PROTECTED_ENVIRONMENTS = frozenset(
    {"prod", "production", "homolog", "homologacao", "homologation"}
)
PREFIX_RE = re.compile(r"^(\d{3})_")
NEW_FILE_UTF8_REQUIRED_AFTER_PREFIX = 102  # files after this prefix must be UTF-8 on disk


@dataclass
class MigrationRunResult:
    mode: str
    applied: list[Path]
    skipped: list[Path]
    baselined: list[Path]


@dataclass(frozen=True)
class MigrationSpec:
    path: Path
    checksum: str
    transactional: bool


def _candidate_dirs() -> Iterable[Path]:
    here = Path(__file__).resolve()
    yield Path("/app/sql/migrations")
    yield here.parents[4] / "sql" / "migrations"
    yield here.parents[3] / "sql" / "migrations"


def resolve_migrations_dir(explicit: str | None = None) -> Path:
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.is_dir():
            raise FileNotFoundError(f"Migrations directory not found: {path}")
        return path

    for candidate in _candidate_dirs():
        if candidate.is_dir():
            return candidate

    raise FileNotFoundError("Unable to locate sql/migrations directory")


def list_migration_files(migrations_dir: Path) -> list[Path]:
    files = sorted(path for path in migrations_dir.glob("*.sql") if path.is_file())
    if not files:
        raise FileNotFoundError(f"No SQL migrations found in {migrations_dir}")
    return files


def manifest_path(migrations_dir: Path) -> Path:
    return migrations_dir / "MANIFEST.json"


def load_migration_manifest(migrations_dir: Path) -> dict[str, Any]:
    path = manifest_path(migrations_dir)
    if not path.is_file():
        raise FileNotFoundError(f"Migration manifest not found: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or "files" not in payload:
        raise RuntimeError(f"Invalid migration manifest: {path}")
    return payload


def current_app_env() -> str:
    return (
        os.environ.get("APP_ENV")
        or os.environ.get("TORQMIND_ENV")
        or os.environ.get("RESET_ENV")
        or ""
    ).strip().lower()


def is_protected_environment(env: str | None = None) -> bool:
    value = (env if env is not None else current_app_env()).strip().lower()
    return value in PROTECTED_ENVIRONMENTS


def _file_prefix(name: str) -> str | None:
    match = PREFIX_RE.match(name)
    return match.group(1) if match else None


def _is_utf8_bytes(raw: bytes) -> bool:
    try:
        raw.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return True


def checksum_v1_raw(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def checksum_v2_normalized(path: Path) -> str:
    return hashlib.sha256(_read_sql_file(path).encode("utf-8")).hexdigest()


def migration_checksum(path: Path) -> str:
    """Canonical checksum written for NEW ledger rows (algorithm v2)."""
    return checksum_v2_normalized(path)


def accepted_checksums(path: Path, expected: str | None = None) -> frozenset[str]:
    values = {
        checksum_v1_raw(path),
        checksum_v2_normalized(path),
    }
    if expected:
        values.add(expected)
    values.update(ACCEPTED_CHECKSUM_ALIASES.get(path.name, frozenset()))
    return frozenset(values)


def validate_migration_chain(migrations_dir: Path, files: list[Path] | None = None) -> None:
    """Static safety gate. Never executes SQL."""
    files = files if files is not None else list_migration_files(migrations_dir)
    manifest = load_migration_manifest(migrations_dir)
    disk_names = [path.name for path in files]
    manifest_names = [str(item["filename"]) for item in manifest["files"]]
    if disk_names != manifest_names:
        missing = sorted(set(manifest_names) - set(disk_names))
        extra = sorted(set(disk_names) - set(manifest_names))
        raise RuntimeError(
            "Migration directory does not match sql/migrations/MANIFEST.json. "
            f"missing={missing or '-'} extra={extra or '-'}"
        )

    historical = {
        str(prefix): [str(name) for name in names]
        for prefix, names in dict(manifest.get("historical_duplicate_prefixes") or {}).items()
    }
    by_prefix: dict[str, list[str]] = {}
    for path in files:
        prefix = _file_prefix(path.name)
        if prefix is None:
            raise RuntimeError(f"Migration filename must start with NNN_: {path.name}")
        by_prefix.setdefault(prefix, []).append(path.name)

    for prefix, names in by_prefix.items():
        if len(names) == 1:
            continue
        allowed = historical.get(prefix)
        if allowed is None:
            raise RuntimeError(
                f"New duplicate migration prefix {prefix} is forbidden: {names}. "
                "Do not renumber applied files; choose the next free prefix."
            )
        if sorted(names) != sorted(allowed):
            raise RuntimeError(
                f"Historical duplicate prefix {prefix} changed: expected {allowed}, found {names}"
            )

    destructive = {
        str(item["filename"])
        for item in manifest["files"]
        if item.get("kind") == "bootstrap_destructive"
    }
    for path in files:
        prefix = _file_prefix(path.name) or "000"
        raw = path.read_bytes()
        utf8 = _is_utf8_bytes(raw)
        if not utf8 and int(prefix) > NEW_FILE_UTF8_REQUIRED_AFTER_PREFIX:
            raise RuntimeError(
                f"New migration {path.name} must be UTF-8. Legacy encodings are frozen, not extended."
            )
        if path.name in destructive and path.name != "003_mart_demo.sql":
            raise RuntimeError(f"Unexpected bootstrap-destructive migration: {path.name}")


def assert_not_protected_bootstrap(env: str | None = None) -> None:
    if is_protected_environment(env):
        raise RuntimeError(
            "Refusing to replay the full historical migration chain on a protected "
            f"environment ({current_app_env() or env}). Homolog and production are not "
            "disposable. Use managed apply of additive files only after explicit authorization."
        )


def migration_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _is_nontransactional_migration(sql_text: str) -> bool:
    for raw_line in sql_text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.lower() == NONTRANSACTIONAL_MARKER:
            return True
        if stripped.startswith("--"):
            continue
        return False
    return False


def _read_sql_file(path: Path) -> str:
    """Read migration SQL with UTF-8 first; fallback for legacy Windows-1252 files."""
    raw = path.read_bytes()
    for encoding in ("utf-8", "cp1252", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _load_migration_spec(path: Path) -> MigrationSpec:
    sql_text = _read_sql_file(path)
    return MigrationSpec(
        path=path,
        checksum=hashlib.sha256(sql_text.encode("utf-8")).hexdigest(),
        transactional=not _is_nontransactional_migration(sql_text),
    )


def _split_sql_statements(sql_text: str) -> list[str]:
    statements: list[str] = []
    buffer: list[str] = []
    idx = 0
    length = len(sql_text)
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False
    dollar_tag: str | None = None

    while idx < length:
        char = sql_text[idx]
        next_char = sql_text[idx + 1] if idx + 1 < length else ""

        if in_line_comment:
            buffer.append(char)
            if char == "\n":
                in_line_comment = False
            idx += 1
            continue

        if in_block_comment:
            buffer.append(char)
            if char == "*" and next_char == "/":
                buffer.append(next_char)
                idx += 2
                in_block_comment = False
            else:
                idx += 1
            continue

        if dollar_tag is not None:
            if sql_text.startswith(dollar_tag, idx):
                buffer.append(dollar_tag)
                idx += len(dollar_tag)
                dollar_tag = None
            else:
                buffer.append(char)
                idx += 1
            continue

        if in_single_quote:
            buffer.append(char)
            if char == "'" and next_char == "'":
                buffer.append(next_char)
                idx += 2
                continue
            if char == "'":
                in_single_quote = False
            idx += 1
            continue

        if in_double_quote:
            buffer.append(char)
            if char == '"':
                in_double_quote = False
            idx += 1
            continue

        if char == "-" and next_char == "-":
            buffer.append(char)
            buffer.append(next_char)
            idx += 2
            in_line_comment = True
            continue

        if char == "/" and next_char == "*":
            buffer.append(char)
            buffer.append(next_char)
            idx += 2
            in_block_comment = True
            continue

        if char == "'":
            buffer.append(char)
            idx += 1
            in_single_quote = True
            continue

        if char == '"':
            buffer.append(char)
            idx += 1
            in_double_quote = True
            continue

        if char == "$":
            closing = sql_text.find("$", idx + 1)
            if closing != -1:
                candidate = sql_text[idx : closing + 1]
                if candidate == "$$" or candidate.replace("_", "").isalnum():
                    buffer.append(candidate)
                    idx = closing + 1
                    dollar_tag = candidate
                    continue

        if char == ";":
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            idx += 1
            continue

        buffer.append(char)
        idx += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


def _checksum_matches(path: Path, recorded_checksum: str | None, expected_checksum: str) -> bool:
    if recorded_checksum is None:
        return False
    return recorded_checksum in accepted_checksums(path, expected_checksum)


def _tracking_table_exists(conn: psycopg.Connection) -> bool:
    row = conn.execute("SELECT to_regclass(%s)", (TRACKING_FQN,)).fetchone()
    return bool(row and row[0])


def _ensure_tracking_table(conn: psycopg.Connection) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {TRACKING_SCHEMA}")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TRACKING_FQN} (
          filename text PRIMARY KEY,
          checksum text NOT NULL,
          applied_at timestamptz NOT NULL DEFAULT now(),
          execution_kind text NOT NULL DEFAULT 'applied'
            CHECK (execution_kind IN ('applied', 'baseline'))
        )
        """
    )
    conn.commit()


def _load_applied_migrations(conn: psycopg.Connection) -> dict[str, str]:
    rows = conn.execute(
        f"""
        SELECT filename, checksum
        FROM {TRACKING_FQN}
        ORDER BY filename
        """
    ).fetchall()
    return {str(row[0]): str(row[1]) for row in rows}


def _existing_torqmind_schemas(conn: psycopg.Connection) -> set[str]:
    rows = conn.execute(
        """
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name = ANY(%s)
        """,
        (list(TORQMIND_SCHEMAS),),
    ).fetchall()
    return {str(row[0]) for row in rows}


def _is_blank_or_bootstrap_only(conn: psycopg.Connection) -> bool:
    schemas = _existing_torqmind_schemas(conn)
    if not schemas:
        return True
    return schemas.issubset(BOOTSTRAP_ONLY_SCHEMAS)


def _missing_runtime_columns(conn: psycopg.Connection) -> list[str]:
    missing: list[str] = []
    with conn.cursor() as cur:
        for schema_name, table_name, column_name in EXPECTED_RUNTIME_COLUMNS:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                  AND column_name = %s
                """,
                (schema_name, table_name, column_name),
            )
            if cur.fetchone() is None:
                missing.append(f"{schema_name}.{table_name}.{column_name}")
    return missing


def _record_migration(conn: psycopg.Connection, path: Path, checksum: str, execution_kind: str) -> None:
    conn.execute(
        f"""
        INSERT INTO {TRACKING_FQN} (filename, checksum, execution_kind)
        VALUES (%s, %s, %s)
        """,
        (path.name, checksum, execution_kind),
    )


def _apply_transactional_sql_file(conn: psycopg.Connection, path: Path) -> None:
    sql = _read_sql_file(path)
    try:
        conn.execute(sql)
        conn.commit()
    except Exception as exc:  # noqa: BLE001
        conn.rollback()
        raise RuntimeError(f"Migration failed at {path.name}: {exc}") from exc


def _apply_nontransactional_sql_file(path: Path) -> None:
    statements = _split_sql_statements(_read_sql_file(path))
    if not statements:
        return

    try:
        with psycopg.connect(_conn_str(), autocommit=True) as conn:
            for statement in statements:
                conn.execute(statement)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Non-transactional migration failed at {path.name}: {exc}") from exc


def _apply_sql_file(conn: psycopg.Connection, spec: MigrationSpec) -> None:
    if spec.transactional:
        _apply_transactional_sql_file(conn, spec.path)
        return
    _apply_nontransactional_sql_file(spec.path)


def _apply_all_from_scratch(conn: psycopg.Connection, files: list[Path]) -> MigrationRunResult:
    assert_not_protected_bootstrap()
    specs = [_load_migration_spec(path) for path in files]
    applied: list[Path] = []
    for spec in specs:
        if spec.path.name == "003_mart_demo.sql":
            # Bootstrap-only destructive reset of schemas. Allowed solely on a blank local DB.
            pass
        _apply_sql_file(conn, spec)
        applied.append(spec.path)

    _ensure_tracking_table(conn)
    for spec in specs:
        _record_migration(conn, spec.path, spec.checksum, "applied")
    conn.commit()

    return MigrationRunResult(mode="bootstrap", applied=applied, skipped=[], baselined=[])


def _baseline_current_database(conn: psycopg.Connection, files: list[Path]) -> MigrationRunResult:
    if _is_blank_or_bootstrap_only(conn):
        raise RuntimeError(
            "--baseline-current exige um banco TorqMind já existente. "
            "Para banco novo ou bootstrap inicial, rode o migrate sem esse flag."
        )

    missing = _missing_runtime_columns(conn) + missing_runtime_relation_columns(conn)
    if missing:
        raise RuntimeError(
            "Baseline recusado porque o runtime atual não passou na verificação de schema. "
            "Colunas ausentes: " + ", ".join(sorted(missing))
        )

    _ensure_tracking_table(conn)
    baselined: list[Path] = []
    for path in files:
        _record_migration(conn, path, migration_checksum(path), "baseline")
        baselined.append(path)
    conn.commit()
    return MigrationRunResult(mode="baseline", applied=[], skipped=[], baselined=baselined)


def _apply_managed_migrations(conn: psycopg.Connection, files: list[Path]) -> MigrationRunResult:
    _ensure_tracking_table(conn)
    applied_checksums = _load_applied_migrations(conn)

    applied: list[Path] = []
    skipped: list[Path] = []
    for path in files:
        spec = _load_migration_spec(path)
        checksum = spec.checksum
        recorded = applied_checksums.get(path.name)
        if recorded is not None:
            if not _checksum_matches(path, recorded, checksum):
                raise RuntimeError(
                    f"Checksum mismatch for already applied migration {path.name}. "
                    "Edite migrations existentes apenas com um plano explícito de recuperação. "
                    "Checksums v1 (raw) e v2 (utf-8 normalizado) são aceitos sem reescrever o ledger."
                )
            skipped.append(path)
            continue

        if path.name == "003_mart_demo.sql":
            raise RuntimeError(
                "Refusing to execute bootstrap-destructive migration 003_mart_demo.sql "
                "on a managed database. This file is retired from the incremental track."
            )

        _apply_sql_file(conn, spec)
        _record_migration(conn, path, checksum, "applied")
        conn.commit()
        applied.append(path)

    return MigrationRunResult(mode="managed", applied=applied, skipped=skipped, baselined=[])


def apply_migrations(migrations_dir: Path, baseline_current: bool = False) -> MigrationRunResult:
    files = list_migration_files(migrations_dir)
    validate_migration_chain(migrations_dir, files)
    with psycopg.connect(_conn_str()) as conn:
        if _tracking_table_exists(conn):
            return _apply_managed_migrations(conn, files)

        if baseline_current:
            return _baseline_current_database(conn, files)

        if _is_blank_or_bootstrap_only(conn):
            return _apply_all_from_scratch(conn, files)

        raise RuntimeError(
            "Existing TorqMind schemas detected without app.schema_migrations. "
            "Refusing to replay sql/migrations because legacy files include destructive resets such as "
            "003_mart_demo.sql. If this database is already healthy, rerun with --baseline-current to "
            "register the current chain without executing SQL. Otherwise, restore from backup or rebuild "
            "a clean local ephemeral database before migrating."
        )


def verify_runtime_schema() -> None:
    with psycopg.connect(_conn_str()) as conn:
        missing = _missing_runtime_columns(conn) + missing_runtime_relation_columns(conn)

    if missing:
        raise RuntimeError(
            "Runtime schema verification failed. Missing required columns: "
            + ", ".join(sorted(missing))
        )


def _print_summary(result: MigrationRunResult) -> None:
    if result.mode == "baseline":
        print(f"Baselined {len(result.baselined)} migration file(s) without executing SQL.")
        for path in result.baselined:
            print(f" - {path.name} [baseline]")
        return

    if result.mode == "bootstrap":
        print("Detected a new/bootstrap TorqMind database. Applied the full migration chain once.")

    print(f"Applied {len(result.applied)} new migration file(s).")
    for path in result.applied:
        print(f" - {path.name}")

    print(f"Skipped {len(result.skipped)} already applied migration file(s).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply TorqMind SQL migrations in order")
    parser.add_argument(
        "--migrations-dir",
        default=None,
        help="Override the directory that contains the ordered SQL migrations.",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Validate critical runtime columns without applying migrations.",
    )
    parser.add_argument(
        "--baseline-current",
        action="store_true",
        help="Register the current migration chain without executing SQL. Use only on a healthy existing DB.",
    )
    args = parser.parse_args()

    if args.verify_only and args.baseline_current:
        parser.error("--verify-only and --baseline-current cannot be used together.")

    migrations_dir = resolve_migrations_dir(args.migrations_dir)
    print(f"Using migrations from: {migrations_dir}")
    validate_migration_chain(migrations_dir)

    if not args.verify_only:
        result = apply_migrations(migrations_dir, baseline_current=args.baseline_current)
        _print_summary(result)

    verify_runtime_schema()
    print("Runtime schema verification passed.")


if __name__ == "__main__":
    main()
