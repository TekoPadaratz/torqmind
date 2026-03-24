from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from unittest.mock import patch
from urllib.parse import urlparse, unquote
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from fastapi.testclient import TestClient

from app.cli.migrate import list_migration_files, resolve_migrations_dir
from app.config import settings
from app.deps import get_current_claims
from app.main import app


SAFE_INTERNAL_MESSAGE = "Falha interna do servidor. Tente novamente em instantes."
SAFE_ETL_MESSAGE = "Falha ao atualizar dados. Tente novamente em instantes."
REAL_MASTER_EMAIL = "teko94@gmail.com"
REAL_MASTER_PASSWORD = "@Crmjr105"
CHANNEL_BOOTSTRAP_EMAIL = "master@torqmind.com"
CHANNEL_BOOTSTRAP_PASSWORD = "TorqMind@123"


def _admin_dsn(dbname: str) -> str:
    if settings.database_url:
        parsed = urlparse(settings.database_url)
        if parsed.scheme.startswith("postgresql"):
            user = unquote(parsed.username or settings.pg_user)
            password = unquote(parsed.password or settings.pg_password)
            host = parsed.hostname or settings.pg_host
            port = parsed.port or settings.pg_port
            return f"host={host} port={port} dbname={dbname} user={user} password={password}"

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


def _fetchscalar(db_name: str, query: str, params: tuple[object, ...] = ()) -> object | None:
    with psycopg.connect(_admin_dsn(db_name)) as conn:
        row = conn.execute(query, params).fetchone()
    return row[0] if row else None


def _execute_sql(db_name: str, query: str, params: tuple[object, ...] = ()) -> None:
    with psycopg.connect(_admin_dsn(db_name)) as conn:
        conn.execute(query, params)
        conn.commit()


def _fetch_function_definition(db_name: str, signature: str) -> str:
    with psycopg.connect(_admin_dsn(db_name)) as conn:
        row = conn.execute(
            "SELECT pg_get_functiondef(%s::regprocedure) AS definition",
            (signature,),
        ).fetchone()
    return str(row[0]) if row and row[0] is not None else ""


def _index_definition(db_name: str, schema_name: str, index_name: str) -> str | None:
    row = _fetchone(
        db_name,
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = %s
          AND indexname = %s
        """,
        (schema_name, index_name),
    )
    return str(row["indexdef"]) if row else None


def _relation_column_type(db_name: str, schema_name: str, relation_name: str, column_name: str) -> str | None:
    with psycopg.connect(_admin_dsn(db_name)) as conn:
        row = conn.execute(
            """
            SELECT format_type(a.atttypid, a.atttypmod) AS data_type
            FROM pg_attribute a
            JOIN pg_class c
              ON c.oid = a.attrelid
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            """,
            (schema_name, relation_name, column_name),
        ).fetchone()
    return str(row[0]) if row else None


def _subprocess_env(db_name: str) -> dict[str, str]:
    env = os.environ.copy()
    admin_dsn = _admin_dsn(db_name)
    conn_kwargs = dict(
        item.split("=", 1)
        for item in admin_dsn.split()
        if "=" in item
    )
    env.update(
        {
            "PG_HOST": str(conn_kwargs.get("host", settings.pg_host)),
            "PG_PORT": str(conn_kwargs.get("port", settings.pg_port)),
            "PG_DATABASE": db_name,
            "PG_USER": str(conn_kwargs.get("user", settings.pg_user)),
            "PG_PASSWORD": str(conn_kwargs.get("password", settings.pg_password)),
            "DATABASE_URL": (
                f"postgresql+asyncpg://{conn_kwargs.get('user', settings.pg_user)}"
                f":{conn_kwargs.get('password', settings.pg_password)}"
                f"@{conn_kwargs.get('host', settings.pg_host)}"
                f":{conn_kwargs.get('port', settings.pg_port)}/{db_name}"
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
        cls.migration_files = list_migration_files(cls.migrations_dir)
        cls.auth_v1_path = cls.migrations_dir / "001_auth.sql"

    def test_migrate_repairs_existing_database_before_seed_and_login(self) -> None:
        with temporary_database() as db_name:
            _run_sql_file(db_name, self.auth_v1_path)
            self.assertFalse(_column_exists(db_name, "auth", "users", "nome"))

            env = _subprocess_env(db_name)
            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)
            self.assertTrue(_column_exists(db_name, "auth", "users", "nome"))
            self.assertTrue(_column_exists(db_name, "app", "tenants", "sales_history_days"))
            self.assertTrue(_column_exists(db_name, "app", "tenants", "default_product_scope_days"))
            self.assertTrue(_column_exists(db_name, "stg", "comprovantes", "referencia_shadow"))
            self.assertTrue(_column_exists(db_name, "dw", "fact_venda_item", "discount_source"))

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
            self.assertTrue(_column_exists(db_name, "app", "tenants", "sales_history_days"))
            self.assertTrue(_column_exists(db_name, "app", "tenants", "default_product_scope_days"))
            self.assertTrue(_column_exists(db_name, "stg", "comprovantes", "referencia_shadow"))
            self.assertTrue(_column_exists(db_name, "dw", "fact_venda_item", "discount_source"))

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
            self.assertIn("/dashboard?", login.stdout)

    def test_migrate_is_idempotent_and_skips_already_applied_files(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            first = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(first.returncode, 0, first.stderr or first.stdout)
            self.assertEqual(
                _fetchscalar(db_name, "SELECT COUNT(*) FROM app.schema_migrations"),
                len(self.migration_files),
            )

            second = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(second.returncode, 0, second.stderr or second.stdout)
            self.assertIn("Applied 0 new migration file(s).", second.stdout)
            self.assertIn(
                f"Skipped {len(self.migration_files)} already applied migration file(s).",
                second.stdout,
            )
            self.assertEqual(
                _fetchscalar(db_name, "SELECT COUNT(*) FROM app.schema_migrations"),
                len(self.migration_files),
            )

    def test_migrate_refuses_to_replay_untracked_existing_runtime_database(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            _execute_sql(
                db_name,
                """
                INSERT INTO app.tenants (id_empresa, nome)
                VALUES (999, 'Sentinel Tenant')
                """,
            )
            _execute_sql(db_name, "DROP TABLE app.schema_migrations")

            rerun = _run_python(["-m", "app.cli.migrate"], env)
            self.assertNotEqual(rerun.returncode, 0)
            self.assertIn("Refusing to replay sql/migrations", rerun.stderr or rerun.stdout)
            self.assertEqual(
                _fetchscalar(db_name, "SELECT COUNT(*) FROM app.tenants WHERE id_empresa = 999"),
                1,
            )
            self.assertEqual(
                _fetchscalar(
                    db_name,
                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'app' AND table_name = 'schema_migrations'",
                ),
                0,
            )

    def test_migrate_can_baseline_existing_runtime_database_without_replaying_sql(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            _execute_sql(
                db_name,
                """
                INSERT INTO app.tenants (id_empresa, nome)
                VALUES (999, 'Sentinel Tenant')
                """,
            )
            _execute_sql(db_name, "DROP TABLE app.schema_migrations")

            baseline = _run_python(["-m", "app.cli.migrate", "--baseline-current"], env)
            self.assertEqual(baseline.returncode, 0, baseline.stderr or baseline.stdout)
            self.assertIn("Baselined", baseline.stdout)
            self.assertEqual(
                _fetchscalar(db_name, "SELECT COUNT(*) FROM app.tenants WHERE id_empresa = 999"),
                1,
            )
            self.assertEqual(
                _fetchscalar(db_name, "SELECT COUNT(*) FROM app.schema_migrations"),
                len(self.migration_files),
            )
            self.assertEqual(
                _fetchscalar(
                    db_name,
                    "SELECT COUNT(*) FROM app.schema_migrations WHERE execution_kind = 'baseline'",
                ),
                len(self.migration_files),
            )

    def test_migrate_installs_risk_event_hotpath_indexes(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            cancel_window_idx = _index_definition(db_name, "dw", "ix_fact_comprovante_risk_cancel_window")
            venda_window_idx = _index_definition(db_name, "dw", "ix_fact_venda_risk_user_window")
            venda_item_cover_idx = _index_definition(db_name, "dw", "ix_fact_venda_item_risk_cover")

            self.assertIsNotNone(cancel_window_idx)
            self.assertIsNotNone(venda_window_idx)
            self.assertIsNotNone(venda_item_cover_idx)
            self.assertIn("cancelado = true", str(cancel_window_idx).lower())
            self.assertIn("id_usuario", str(venda_window_idx))
            self.assertIn("id_movprodutos", str(venda_item_cover_idx))

    def test_compute_risk_events_is_idempotent_and_cleans_stale_rows_in_recomputed_window(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)
            tenant_id = 4001

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            with psycopg.connect(_admin_dsn(db_name), row_factory=dict_row) as conn:
                conn.execute(
                    """
                    INSERT INTO app.tenants (id_empresa, nome, is_active, status, billing_status, valid_from)
                    VALUES (%s, 'Tenant Risk', true, 'active', 'current', CURRENT_DATE)
                    """
                    ,
                    (tenant_id,),
                )
                conn.execute(
                    """
                    INSERT INTO auth.filiais (id_empresa, id_filial, nome, is_active, valid_from)
                    VALUES (%s, 1, 'Filial 1', true, CURRENT_DATE)
                    """,
                    (tenant_id,),
                )
                conn.execute(
                    """
                    INSERT INTO dw.fact_comprovante (
                      id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                      id_usuario, id_turno, valor_total, cancelado, situacao, payload
                    )
                    VALUES
                      (%s, 1, 1, 91001, TIMESTAMPTZ '2026-03-10 10:00:00+00', 20260310, 88, 51, 500, true, 1, '{"CFOP":"5102"}'::jsonb)
                    """,
                    (tenant_id,),
                )
                conn.execute(
                    """
                    INSERT INTO dw.fact_venda (
                      id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                      id_usuario, id_cliente, id_comprovante, id_turno, saidas_entradas,
                      total_venda, cancelado, payload
                    )
                    VALUES
                      (%s, 1, 1, 91001, TIMESTAMPTZ '2026-03-10 10:01:00+00', 20260310, 88, NULL, 91001, 51, 1, 500, false, '{}'::jsonb)
                    """,
                    (tenant_id,),
                )
                conn.commit()

                first_run = conn.execute(
                    "SELECT etl.compute_risk_events(%s, %s, %s, %s) AS rows",
                    (tenant_id, False, 14, "2026-03-10 23:59:59+00"),
                ).fetchone()
                first_total = conn.execute(
                    "SELECT COUNT(*) AS total FROM dw.fact_risco_evento WHERE id_empresa = %s",
                    (tenant_id,),
                ).fetchone()["total"]

                second_run = conn.execute(
                    "SELECT etl.compute_risk_events(%s, %s, %s, %s) AS rows",
                    (tenant_id, False, 14, "2026-03-10 23:59:59+00"),
                ).fetchone()
                second_total = conn.execute(
                    "SELECT COUNT(*) AS total FROM dw.fact_risco_evento WHERE id_empresa = %s",
                    (tenant_id,),
                ).fetchone()["total"]

                conn.execute(
                    """
                    UPDATE dw.fact_comprovante
                    SET cancelado = false
                    WHERE id_empresa = %s
                      AND id_filial = 1
                      AND id_db = 1
                      AND id_comprovante = 91001
                    """,
                    (tenant_id,),
                )
                conn.commit()

                third_run = conn.execute(
                    "SELECT etl.compute_risk_events(%s, %s, %s, %s) AS rows",
                    (tenant_id, False, 14, "2026-03-10 23:59:59+00"),
                ).fetchone()
                third_total = conn.execute(
                    "SELECT COUNT(*) AS total FROM dw.fact_risco_evento WHERE id_empresa = %s",
                    (tenant_id,),
                ).fetchone()["total"]
                conn.commit()

            self.assertGreater(int(first_run["rows"] or 0), 0)
            self.assertEqual(int(first_total), int(second_total))
            self.assertGreaterEqual(int(second_run["rows"] or 0), 0)
            self.assertEqual(int(third_run["rows"] or 0), 0)
            self.assertEqual(int(third_total), 0)

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
            self.assertIn('"home_path": "/dashboard?', master_login.stdout)

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

    def test_payment_anomaly_relation_stays_type_compatible_with_notifications(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            self.assertEqual(
                _relation_column_type(db_name, "mart", "pagamentos_anomalias_diaria", "insight_id"),
                "text",
            )
            self.assertEqual(
                _relation_column_type(db_name, "mart", "pagamentos_anomalias_diaria", "insight_id_hash"),
                "bigint",
            )

    def test_incremental_etl_release_uses_three_phase_backbone_without_global_snapshot_backfill(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            refresh_def = _fetch_function_definition(db_name, "etl.refresh_marts(jsonb, date)").lower()
            clock_meta_def = _fetch_function_definition(db_name, "etl.collect_tenant_clock_meta(integer, date)").lower()
            post_refresh_def = _fetch_function_definition(db_name, "etl.run_tenant_post_refresh(integer, jsonb, date)").lower()
            run_all_def = _fetch_function_definition(db_name, "etl.run_all(integer, boolean, boolean, date)").lower()
            purge_def = _fetch_function_definition(db_name, "etl.purge_sales_history(integer, date)").lower()
            churn_mv = _fetchone(
                db_name,
                """
                SELECT definition
                FROM pg_matviews
                WHERE schemaname = 'mart'
                  AND matviewname = 'clientes_churn_risco'
                """,
            )
            cash_mv = _fetchone(
                db_name,
                """
                SELECT definition
                FROM pg_matviews
                WHERE schemaname = 'mart'
                  AND matviewname = 'agg_caixa_turno_aberto'
                """,
            )

            self.assertIn("etl.run_tenant_phase", run_all_def)
            self.assertIn("etl.collect_tenant_clock_meta", run_all_def)
            self.assertIn("etl.refresh_marts", run_all_def)
            self.assertIn("etl.run_tenant_post_refresh", run_all_def)
            self.assertNotIn("refresh materialized view", run_all_def)

            self.assertIn("clock_churn_mart_refresh", refresh_def)
            self.assertIn("clock_cash_open_refresh", refresh_def)
            self.assertIn("daily_rollover_window", clock_meta_def)
            self.assertNotIn("run_operational_snapshot_backfill", refresh_def)
            self.assertNotIn("run_operational_snapshot_backfill", post_refresh_def)
            self.assertNotIn("backfill_customer_sales_daily_range(null", refresh_def)
            self.assertNotIn("backfill_customer_sales_daily_range(null", post_refresh_def)
            self.assertNotIn("backfill_customer_rfm_range(null", refresh_def)
            self.assertNotIn("backfill_customer_rfm_range(null", post_refresh_def)
            self.assertNotIn("backfill_customer_churn_risk_range(null", refresh_def)
            self.assertNotIn("backfill_customer_churn_risk_range(null", post_refresh_def)
            self.assertNotIn("backfill_finance_aging_range(null", refresh_def)
            self.assertNotIn("backfill_finance_aging_range(null", post_refresh_def)
            self.assertNotIn("backfill_health_score_range(null", refresh_def)
            self.assertNotIn("backfill_health_score_range(null", post_refresh_def)
            self.assertIn("etl.change_domains", purge_def)
            self.assertNotIn("refresh materialized view mart.financeiro_vencimentos_diaria", purge_def)
            self.assertNotIn("refresh materialized view mart.agg_risco_diaria", purge_def)
            self.assertNotIn("refresh materialized view mart.agg_caixa_forma_pagamento", purge_def)
            self.assertNotIn("refresh materialized view mart.alerta_caixa_aberto", purge_def)

            self.assertIsNotNone(churn_mv)
            self.assertIn("etl.runtime_ref_date()", str(churn_mv["definition"]).lower())
            self.assertIsNotNone(cash_mv)
            self.assertIn("etl.runtime_now()", str(cash_mv["definition"]).lower())

    def test_incremental_etl_daily_rollover_updates_time_driven_artifacts_without_full_sales_refresh(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            _execute_sql(
                db_name,
                """
                INSERT INTO app.tenants (id_empresa, nome, ingest_key)
                VALUES (1, 'Tenant 1', gen_random_uuid())
                ON CONFLICT (id_empresa)
                DO UPDATE SET
                  nome = EXCLUDED.nome,
                  ingest_key = COALESCE(app.tenants.ingest_key, EXCLUDED.ingest_key);

                INSERT INTO auth.filiais (id_empresa, id_filial, nome, cnpj, is_active, valid_from)
                VALUES (1, 1, 'Filial 1', '12345678000199', true, DATE '2026-03-01')
                ON CONFLICT (id_empresa, id_filial)
                DO UPDATE SET
                  nome = EXCLUDED.nome,
                  cnpj = EXCLUDED.cnpj,
                  is_active = EXCLUDED.is_active,
                  valid_from = EXCLUDED.valid_from;

                INSERT INTO dw.dim_cliente (id_empresa, id_filial, id_cliente, nome)
                VALUES (1, 1, 100, 'Cliente 100');

                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  id_usuario, id_cliente, id_comprovante, id_turno, saidas_entradas,
                  total_venda, cancelado, payload
                )
                VALUES (
                  1, 1, 1, 10, TIMESTAMP '2026-03-13 10:00:00', 20260313,
                  1, 100, 500, 7, 1, 120.00, false, '{}'::jsonb
                );

                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, id_grupo_produto, id_local_venda, id_funcionario, cfop,
                  qtd, valor_unitario, total, desconto, custo_total, margem, payload
                )
                VALUES (
                  1, 1, 1, 10, 1, 20260313,
                  1, NULL, NULL, NULL, 5102,
                  1, 120.00, 120.00, 0, 90.00, 30.00, '{}'::jsonb
                );

                INSERT INTO dw.fact_financeiro (
                  id_empresa, id_filial, id_db, tipo_titulo, id_titulo, id_entidade,
                  data_emissao, data_key_emissao, vencimento, data_key_venc,
                  data_pagamento, data_key_pgto, valor, valor_pago, payload
                )
                VALUES (
                  1, 1, 1, 1, 900, 300,
                  DATE '2026-03-10', 20260310, DATE '2026-03-17', 20260317,
                  NULL, NULL, 100.00, 0, '{}'::jsonb
                );
                """,
            )

            with psycopg.connect(_admin_dsn(db_name)) as conn:
                conn.execute("REFRESH MATERIALIZED VIEW mart.agg_vendas_diaria")
                conn.execute("SELECT set_config('etl.ref_date', %s, true)", ("2026-03-18",))
                conn.execute("REFRESH MATERIALIZED VIEW mart.clientes_churn_risco")
                conn.commit()
            _fetchscalar(
                db_name,
                "SELECT etl.backfill_customer_sales_daily_range(%s, %s, %s)",
                (1, date(2026, 3, 13), date(2026, 3, 18)),
            )
            _fetchscalar(
                db_name,
                "SELECT etl.backfill_customer_rfm_range(%s, %s, %s)",
                (1, date(2026, 3, 18), date(2026, 3, 18)),
            )
            _fetchscalar(
                db_name,
                "SELECT etl.backfill_customer_churn_risk_range(%s, %s, %s)",
                (1, date(2026, 3, 18), date(2026, 3, 18)),
            )
            _fetchscalar(
                db_name,
                "SELECT etl.backfill_finance_aging_range(%s, %s, %s)",
                (1, date(2026, 3, 18), date(2026, 3, 18)),
            )
            _fetchscalar(
                db_name,
                "SELECT etl.backfill_health_score_range(%s, %s, %s)",
                (1, date(2026, 3, 18), date(2026, 3, 18)),
            )

            churn_before = _fetchone(
                db_name,
                """
                SELECT (reasons->>'ref_date')::date AS ref_date
                FROM mart.clientes_churn_risco
                WHERE id_empresa = 1
                  AND id_filial = 1
                  AND id_cliente = 100
                """,
            )
            self.assertIsNotNone(churn_before)
            self.assertEqual(churn_before["ref_date"], date(2026, 3, 18))

            clock_meta = _fetchscalar(
                db_name,
                "SELECT etl.collect_tenant_clock_meta(%s, %s::date)",
                (1, date(2026, 3, 19)),
            )
            if isinstance(clock_meta, str):
                clock_meta = json.loads(clock_meta)
            self.assertIsInstance(clock_meta, dict)
            self.assertEqual(clock_meta["clock_customer_rfm_start_dt_ref"], "2026-03-19")
            self.assertEqual(clock_meta["clock_finance_aging_start_dt_ref"], "2026-03-19")
            self.assertEqual(clock_meta["clock_health_score_start_dt_ref"], "2026-03-19")

            refresh_meta = _fetchscalar(
                db_name,
                "SELECT etl.refresh_marts(%s::jsonb, %s::date)",
                (json.dumps(clock_meta), date(2026, 3, 19)),
            )
            if isinstance(refresh_meta, str):
                refresh_meta = json.loads(refresh_meta)
            post_meta = _fetchscalar(
                db_name,
                "SELECT etl.run_tenant_post_refresh(%s, %s::jsonb, %s::date)",
                (1, json.dumps(clock_meta), date(2026, 3, 19)),
            )
            if isinstance(post_meta, str):
                post_meta = json.loads(post_meta)

            self.assertFalse(refresh_meta["sales_marts_refreshed"])
            self.assertFalse(refresh_meta["finance_mart_refreshed"])
            self.assertTrue(refresh_meta["churn_clock_mart_refreshed"])
            self.assertFalse(post_meta["customer_sales_daily_refreshed"])
            self.assertTrue(post_meta["customer_rfm_refreshed"])
            self.assertTrue(post_meta["customer_churn_risk_refreshed"])
            self.assertTrue(post_meta["finance_aging_refreshed"])
            self.assertTrue(post_meta["health_score_refreshed"])
            self.assertTrue(post_meta["customer_rfm_clock_driven"])
            self.assertTrue(post_meta["finance_aging_clock_driven"])
            self.assertTrue(post_meta["health_score_clock_driven"])

            churn_after = _fetchone(
                db_name,
                """
                SELECT (reasons->>'ref_date')::date AS ref_date
                FROM mart.clientes_churn_risco
                WHERE id_empresa = 1
                  AND id_filial = 1
                  AND id_cliente = 100
                """,
            )
            self.assertIsNotNone(churn_after)
            self.assertEqual(churn_after["ref_date"], date(2026, 3, 19))
            self.assertEqual(
                _fetchscalar(
                    db_name,
                    "SELECT MAX(dt_ref) FROM mart.customer_churn_risk_daily WHERE id_empresa = %s",
                    (1,),
                ),
                date(2026, 3, 19),
            )
            self.assertEqual(
                _fetchscalar(
                    db_name,
                    "SELECT MAX(dt_ref) FROM mart.finance_aging_daily WHERE id_empresa = %s",
                    (1,),
                ),
                date(2026, 3, 19),
            )
            self.assertEqual(
                _fetchscalar(
                    db_name,
                    "SELECT MAX(dt_ref) FROM mart.health_score_daily WHERE id_empresa = %s",
                    (1,),
                ),
                date(2026, 3, 19),
            )

    def test_incremental_etl_open_cash_turn_evolves_with_clock_without_new_ingest(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            _execute_sql(
                db_name,
                """
                INSERT INTO app.tenants (id_empresa, nome, ingest_key)
                VALUES (1, 'Tenant 1', gen_random_uuid())
                ON CONFLICT (id_empresa)
                DO UPDATE SET
                  nome = EXCLUDED.nome,
                  ingest_key = COALESCE(app.tenants.ingest_key, EXCLUDED.ingest_key);

                INSERT INTO auth.filiais (id_empresa, id_filial, nome, cnpj, is_active, valid_from)
                VALUES (1, 1, 'Filial Caixa', '12345678000199', true, DATE '2026-03-01')
                ON CONFLICT (id_empresa, id_filial)
                DO UPDATE SET
                  nome = EXCLUDED.nome,
                  cnpj = EXCLUDED.cnpj,
                  is_active = EXCLUDED.is_active,
                  valid_from = EXCLUDED.valid_from;

                INSERT INTO dw.dim_usuario_caixa (id_empresa, id_filial, id_usuario, nome, payload)
                VALUES (1, 1, 900, 'Operador Caixa', '{}'::jsonb);

                INSERT INTO dw.fact_caixa_turno (
                  id_empresa, id_filial, id_turno, id_db, id_usuario, abertura_ts,
                  fechamento_ts, data_key_abertura, data_key_fechamento,
                  encerrante_fechamento, is_aberto, status_raw, payload
                )
                VALUES (
                  1, 1, 77, 1, 900, TIMESTAMPTZ '2026-03-19 03:00:00+00',
                  NULL, 20260319, NULL, NULL, true, 'OPEN', '{}'::jsonb
                );
                """,
            )

            with psycopg.connect(_admin_dsn(db_name), row_factory=dict_row) as conn:
                clock_meta = conn.execute(
                    "SELECT etl.collect_tenant_clock_meta(%s, %s::date) AS meta",
                    (1, date(2026, 3, 19)),
                ).fetchone()["meta"]
                if isinstance(clock_meta, str):
                    clock_meta = json.loads(clock_meta)

                conn.execute("SELECT set_config('etl.now', %s, true)", ("2026-03-19 10:00:00+00",))
                refresh_one = conn.execute(
                    "SELECT etl.refresh_marts(%s::jsonb, %s::date) AS meta",
                    (json.dumps(clock_meta), date(2026, 3, 19)),
                ).fetchone()["meta"]
                if isinstance(refresh_one, str):
                    refresh_one = json.loads(refresh_one)
                post_one = conn.execute(
                    "SELECT etl.run_tenant_post_refresh(%s, %s::jsonb, %s::date) AS meta",
                    (1, json.dumps(clock_meta), date(2026, 3, 19)),
                ).fetchone()["meta"]
                if isinstance(post_one, str):
                    post_one = json.loads(post_one)
                agg_one = conn.execute(
                    """
                    SELECT horas_aberto, severity
                    FROM mart.agg_caixa_turno_aberto
                    WHERE id_empresa = 1
                      AND id_turno = 77
                    """
                ).fetchone()
                alerts_one = conn.execute(
                    "SELECT COUNT(*) AS total FROM mart.alerta_caixa_aberto WHERE id_empresa = %s",
                    (1,),
                ).fetchone()["total"]
                notifications_one = conn.execute(
                    "SELECT COUNT(*) AS total FROM app.notifications WHERE id_empresa = %s",
                    (1,),
                ).fetchone()["total"]
                conn.commit()

            with psycopg.connect(_admin_dsn(db_name), row_factory=dict_row) as conn:
                clock_meta = conn.execute(
                    "SELECT etl.collect_tenant_clock_meta(%s, %s::date) AS meta",
                    (1, date(2026, 3, 20)),
                ).fetchone()["meta"]
                if isinstance(clock_meta, str):
                    clock_meta = json.loads(clock_meta)

                conn.execute("SELECT set_config('etl.now', %s, true)", ("2026-03-20 12:30:00+00",))
                refresh_two = conn.execute(
                    "SELECT etl.refresh_marts(%s::jsonb, %s::date) AS meta",
                    (json.dumps(clock_meta), date(2026, 3, 20)),
                ).fetchone()["meta"]
                if isinstance(refresh_two, str):
                    refresh_two = json.loads(refresh_two)
                post_two = conn.execute(
                    "SELECT etl.run_tenant_post_refresh(%s, %s::jsonb, %s::date) AS meta",
                    (1, json.dumps(clock_meta), date(2026, 3, 20)),
                ).fetchone()["meta"]
                if isinstance(post_two, str):
                    post_two = json.loads(post_two)
                agg_two = conn.execute(
                    """
                    SELECT horas_aberto, severity
                    FROM mart.agg_caixa_turno_aberto
                    WHERE id_empresa = 1
                      AND id_turno = 77
                    """
                ).fetchone()
                alert_two = conn.execute(
                    """
                    SELECT severity, title
                    FROM mart.alerta_caixa_aberto
                    WHERE id_empresa = 1
                      AND id_turno = 77
                    """
                ).fetchone()
                notification_two = conn.execute(
                    """
                    SELECT severity, title
                    FROM app.notifications
                    WHERE id_empresa = 1
                      AND id_filial = 1
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ).fetchone()
                conn.commit()

            self.assertTrue(refresh_one["cash_open_alert_marts_refreshed"])
            self.assertFalse(refresh_one["cash_marts_refreshed"])
            self.assertEqual(float(agg_one["horas_aberto"]), 7.0)
            self.assertEqual(agg_one["severity"], "WARN")
            self.assertEqual(alerts_one, 0)
            self.assertEqual(post_one["cash_notifications"], 0)
            self.assertEqual(notifications_one, 0)

            self.assertTrue(refresh_two["cash_open_alert_marts_refreshed"])
            self.assertFalse(refresh_two["cash_marts_refreshed"])
            self.assertGreater(float(agg_two["horas_aberto"]), float(agg_one["horas_aberto"]))
            self.assertEqual(agg_two["severity"], "CRITICAL")
            self.assertEqual(alert_two["severity"], "CRITICAL")
            self.assertIn("Caixa 77 aberto", alert_two["title"])
            self.assertEqual(post_two["cash_notifications"], 1)
            self.assertTrue(post_two["cash_notifications_clock_driven"])
            self.assertEqual(notification_two["severity"], "CRITICAL")
            self.assertIn("Caixa 77 aberto", notification_two["title"])

    def test_incremental_refresh_matviews_have_unique_indexes_for_concurrent_path(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            for index_name in (
                "ux_mart_agg_produtos_diaria",
                "ux_mart_agg_grupos_diaria",
                "ux_mart_agg_funcionarios_diaria",
                "ux_mart_fraude_cancelamentos_eventos",
                "ux_mart_financeiro_vencimentos_diaria",
            ):
                indexdef = _index_definition(db_name, "mart", index_name)
                self.assertIsNotNone(indexdef, index_name)
                self.assertIn("CREATE UNIQUE INDEX", str(indexdef).upper(), index_name)

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

    def test_internal_sql_errors_do_not_leak_in_etl_response(self) -> None:
        client = TestClient(app, raise_server_exceptions=False)
        claims = {
            "sub": "00000000-0000-0000-0000-000000000001",
            "role": "OWNER",
            "user_role": "tenant_admin",
            "tenant_ids": [1],
            "branch_ids": [],
            "channel_ids": [],
            "access": {
                "product": True,
                "product_readonly": False,
                "platform": False,
                "platform_finance": False,
                "platform_operations": False,
            },
        }
        app.dependency_overrides[get_current_claims] = lambda: claims
        try:
            with patch("app.routes_etl.resolve_scope", return_value=(1, None)), patch(
                "app.routes_etl.run_incremental_cycle",
                side_effect=RuntimeError(
                    'column "insight_id" is of type bigint but expression is of type text'
                ),
            ):
                response = client.post("/etl/run?refresh_mart=true")
        finally:
            app.dependency_overrides.pop(get_current_claims, None)

        self.assertEqual(response.status_code, 500, response.text)
        body = response.json()
        self.assertEqual(body["error"], "etl_failed")
        self.assertEqual(body["detail"]["message"], SAFE_ETL_MESSAGE)
        self.assertNotIn('column "insight_id" is of type bigint but expression is of type text', response.text)


if __name__ == "__main__":
    unittest.main()
