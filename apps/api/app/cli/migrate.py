from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable

import psycopg

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


def apply_migrations(migrations_dir: Path) -> list[Path]:
    applied: list[Path] = []
    conn = psycopg.connect(_conn_str())
    try:
        for path in list_migration_files(migrations_dir):
            sql = path.read_text(encoding="utf-8")
            try:
                conn.execute(sql)
                conn.commit()
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                raise RuntimeError(f"Migration failed at {path.name}: {exc}") from exc
            applied.append(path)
    finally:
        conn.close()
    return applied


def verify_runtime_schema() -> None:
    missing: list[str] = []
    with psycopg.connect(_conn_str()) as conn:
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

    if missing:
        raise RuntimeError(
            "Runtime schema verification failed. Missing required columns: "
            + ", ".join(sorted(missing))
        )


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
    args = parser.parse_args()

    migrations_dir = resolve_migrations_dir(args.migrations_dir)
    print(f"Using migrations from: {migrations_dir}")

    if not args.verify_only:
        applied = apply_migrations(migrations_dir)
        print(f"Applied {len(applied)} migration file(s).")
        for path in applied:
            print(f" - {path.name}")

    verify_runtime_schema()
    print("Runtime schema verification passed.")


if __name__ == "__main__":
    main()
