from __future__ import annotations

import os
import subprocess
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from fastapi.testclient import TestClient

from app.cli.migrate import resolve_migrations_dir
from app.config import settings
from app.main import app


SAFE_INTERNAL_MESSAGE = "Falha interna do servidor. Tente novamente em instantes."
REAL_MASTER_EMAIL = "teko94@gmail.com"
REAL_MASTER_PASSWORD = "@Crmjr105"
CHANNEL_BOOTSTRAP_EMAIL = "master@torqmind.com"
CHANNEL_BOOTSTRAP_PASSWORD = "TorqMind@123"


def _admin_dsn(dbname: str) -> str:
    return (
        f"host={settings.pg_host} port={settings.pg_port} dbname={dbname} "
        f"user={settings.pg_user} password={settings.pg_password}"
    )


@contextmanager
def temporary_database():
    db_name = f"tm_release_{uuid4().hex[:12]}"
    with psycopg.connect(_admin_dsn("postgres"), autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    try:
        yield db_name
    finally:
        with psycopg.connect(_admin_dsn("postgres"), autocommit=True) as conn:
            conn.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s
                  AND pid <> pg_backend_pid()
                """,
                (db_name,),
            )
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))


def _run_sql_file(db_name: str, path: Path) -> None:
    sql_text = path.read_text(encoding="utf-8")
    with psycopg.connect(_admin_dsn(db_name)) as conn:
        conn.execute(sql_text)
        conn.commit()


def _column_exists(db_name: str, schema_name: str, table_name: str, column_name: str) -> bool:
    with psycopg.connect(_admin_dsn(db_name)) as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            """,
            (schema_name, table_name, column_name),
        ).fetchone()
    return row is not None


def _fetchone(db_name: str, query: str, params: tuple[object, ...] = ()) -> dict[str, object] | None:
    with psycopg.connect(_admin_dsn(db_name), row_factory=dict_row) as conn:
        row = conn.execute(query, params).fetchone()
    return row if row else None


def _subprocess_env(db_name: str) -> dict[str, str]:
    env = os.environ.copy()
    env.update(
        {
            "PG_HOST": str(settings.pg_host),
            "PG_PORT": str(settings.pg_port),
            "PG_DATABASE": db_name,
            "PG_USER": str(settings.pg_user),
            "PG_PASSWORD": str(settings.pg_password),
            "DATABASE_URL": (
                f"postgresql+asyncpg://{settings.pg_user}:{settings.pg_password}"
                f"@{settings.pg_host}:{settings.pg_port}/{db_name}"
            ),
            "SEED_PASSWORD": "TorqMind@123",
            "APP_ENV": "test",
        }
    )
    return env


def _run_python(args: list[str], env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, *args],
        env=env,
        text=True,
        capture_output=True,
        timeout=240,
        check=False,
    )


class ReleaseHardeningTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)
        cls.migrations_dir = resolve_migrations_dir()
        cls.auth_v1_path = cls.migrations_dir / "001_auth.sql"

    def test_migrate_repairs_existing_database_before_seed_and_login(self) -> None:
        with temporary_database() as db_name:
            _run_sql_file(db_name, self.auth_v1_path)
            self.assertFalse(_column_exists(db_name, "auth", "users", "nome"))

            env = _subprocess_env(db_name)
            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)
            self.assertTrue(_column_exists(db_name, "auth", "users", "nome"))

            seed = _run_python(["-m", "app.cli.seed"], {**env, "SEED_MODE": "master-only"})
            self.assertEqual(seed.returncode, 0, seed.stderr or seed.stdout)

            login = _run_python(
                [
                    "-c",
                    (
                        "from app import repos_auth; "
                        f"session = repos_auth.verify_login('{REAL_MASTER_EMAIL}', '{REAL_MASTER_PASSWORD}'); "
                        "print(session['email'])"
                    ),
                ],
                env,
            )
            self.assertEqual(login.returncode, 0, login.stderr or login.stdout)
            self.assertIn(REAL_MASTER_EMAIL, login.stdout)

    def test_migrate_and_seed_work_on_clean_database(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)
            self.assertTrue(_column_exists(db_name, "auth", "users", "nome"))

            seed = _run_python(["-m", "app.cli.seed"], {**env, "SEED_MODE": "master-only"})
            self.assertEqual(seed.returncode, 0, seed.stderr or seed.stdout)

            login = _run_python(
                [
                    "-c",
                    (
                        "from app import repos_auth; "
                        f"session = repos_auth.verify_login('{REAL_MASTER_EMAIL}', '{REAL_MASTER_PASSWORD}'); "
                        "print(session['home_path'])"
                    ),
                ],
                env,
            )
            self.assertEqual(login.returncode, 0, login.stderr or login.stdout)
            self.assertIn("/platform", login.stdout)

    def test_master_only_seed_bootstraps_real_master_and_channel_admin(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            seed = _run_python(["-m", "app.cli.seed"], {**env, "SEED_MODE": "master-only"})
            self.assertEqual(seed.returncode, 0, seed.stderr or seed.stdout)

            master_user = _fetchone(
                db_name,
                """
                SELECT email, role, must_change_password, is_active
                FROM auth.users
                WHERE lower(email) = lower(%s)
                """,
                (REAL_MASTER_EMAIL,),
            )
            self.assertIsNotNone(master_user)
            self.assertEqual(master_user["role"], "platform_master")
            self.assertFalse(bool(master_user["must_change_password"]))
            self.assertTrue(bool(master_user["is_active"]))

            channel_user = _fetchone(
                db_name,
                """
                SELECT email, role, must_change_password, is_active
                FROM auth.users
                WHERE lower(email) = lower(%s)
                """,
                (CHANNEL_BOOTSTRAP_EMAIL,),
            )
            self.assertIsNotNone(channel_user)
            self.assertEqual(channel_user["role"], "channel_admin")
            self.assertFalse(bool(channel_user["must_change_password"]))
            self.assertTrue(bool(channel_user["is_active"]))

            master_scope = _fetchone(
                db_name,
                """
                SELECT ut.role, ut.channel_id, ut.id_empresa, ut.id_filial
                FROM auth.user_tenants ut
                JOIN auth.users u ON u.id = ut.user_id
                WHERE lower(u.email) = lower(%s)
                """,
                (REAL_MASTER_EMAIL,),
            )
            self.assertEqual(master_scope, {"role": "platform_master", "channel_id": None, "id_empresa": None, "id_filial": None})

            channel_scope = _fetchone(
                db_name,
                """
                SELECT ut.role, ut.channel_id, ut.id_empresa, ut.id_filial, c.name AS channel_name
                FROM auth.user_tenants ut
                JOIN auth.users u ON u.id = ut.user_id
                LEFT JOIN app.channels c ON c.id = ut.channel_id
                WHERE lower(u.email) = lower(%s)
                """,
                (CHANNEL_BOOTSTRAP_EMAIL,),
            )
            self.assertIsNotNone(channel_scope)
            self.assertEqual(channel_scope["role"], "channel_admin")
            self.assertIsNotNone(channel_scope["channel_id"])
            self.assertIsNone(channel_scope["id_empresa"])
            self.assertIsNone(channel_scope["id_filial"])
            self.assertEqual(channel_scope["channel_name"], "Canal TorqMind")

            master_login = _run_python(
                [
                    "-c",
                    (
                        "import json; "
                        "from app import repos_auth; "
                        f"session = repos_auth.verify_login('{REAL_MASTER_EMAIL}', '{REAL_MASTER_PASSWORD}'); "
                        "print(json.dumps({'role': session['user_role'], 'platform_finance': session['access']['platform_finance'], 'home_path': session['home_path']}))"
                    ),
                ],
                env,
            )
            self.assertEqual(master_login.returncode, 0, master_login.stderr or master_login.stdout)
            self.assertIn('"role": "platform_master"', master_login.stdout)
            self.assertIn('"platform_finance": true', master_login.stdout)
            self.assertIn('"home_path": "/platform"', master_login.stdout)

            channel_login = _run_python(
                [
                    "-c",
                    (
                        "import json; "
                        "from app import repos_auth; "
                        f"session = repos_auth.verify_login('{CHANNEL_BOOTSTRAP_EMAIL}', '{CHANNEL_BOOTSTRAP_PASSWORD}'); "
                        "print(json.dumps({'role': session['user_role'], 'platform': session['access']['platform'], 'platform_finance': session['access']['platform_finance'], 'channel_ids': session['channel_ids']}))"
                    ),
                ],
                env,
            )
            self.assertEqual(channel_login.returncode, 0, channel_login.stderr or channel_login.stdout)
            self.assertIn('"role": "channel_admin"', channel_login.stdout)
            self.assertIn('"platform": true', channel_login.stdout)
            self.assertIn('"platform_finance": false', channel_login.stdout)

    def test_master_only_seed_reconciles_legacy_master_to_channel_scope(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            legacy_seed = _run_python(
                [
                    "-c",
                    (
                        "from app.cli.seed import _upsert_user, _replace_scopes; "
                        f"user_id = _upsert_user('{CHANNEL_BOOTSTRAP_EMAIL}', '{CHANNEL_BOOTSTRAP_PASSWORD}', 'Legacy Master', 'platform_master'); "
                        "_replace_scopes(user_id, [{'role': 'platform_master', 'channel_id': None, 'id_empresa': None, 'id_filial': None, 'is_enabled': True, 'valid_from': None, 'valid_until': None}]); "
                        "print(user_id)"
                    ),
                ],
                env,
            )
            self.assertEqual(legacy_seed.returncode, 0, legacy_seed.stderr or legacy_seed.stdout)

            seed = _run_python(["-m", "app.cli.seed"], {**env, "SEED_MODE": "master-only"})
            self.assertEqual(seed.returncode, 0, seed.stderr or seed.stdout)

            channel_user = _fetchone(
                db_name,
                """
                SELECT email, role
                FROM auth.users
                WHERE lower(email) = lower(%s)
                """,
                (CHANNEL_BOOTSTRAP_EMAIL,),
            )
            self.assertIsNotNone(channel_user)
            self.assertEqual(channel_user["role"], "channel_admin")

            channel_scope = _fetchone(
                db_name,
                """
                SELECT ut.role, ut.channel_id, ut.id_empresa, ut.id_filial
                FROM auth.user_tenants ut
                JOIN auth.users u ON u.id = ut.user_id
                WHERE lower(u.email) = lower(%s)
                """,
                (CHANNEL_BOOTSTRAP_EMAIL,),
            )
            self.assertIsNotNone(channel_scope)
            self.assertEqual(channel_scope["role"], "channel_admin")
            self.assertIsNotNone(channel_scope["channel_id"])
            self.assertIsNone(channel_scope["id_empresa"])
            self.assertIsNone(channel_scope["id_filial"])

    def test_internal_sql_errors_do_not_leak_in_login_response(self) -> None:
        client = TestClient(app, raise_server_exceptions=False)
        with patch("app.repos_auth.verify_login", side_effect=RuntimeError('column "nome" does not exist')):
            response = client.post(
                "/auth/login",
                json={"email": "master@torqmind.com", "password": "x"},
            )

        self.assertEqual(response.status_code, 500, response.text)
        body = response.json()
        self.assertEqual(body["error"], "internal_error")
        self.assertEqual(body["detail"]["message"], SAFE_INTERNAL_MESSAGE)
        self.assertNotIn('column "nome" does not exist', response.text)


if __name__ == "__main__":
    unittest.main()
