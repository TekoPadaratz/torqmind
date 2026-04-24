from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from urllib.parse import urlparse, unquote
from uuid import uuid4

import app.main as main_module
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from fastapi.testclient import TestClient

from app.cli.migrate import (
    ACCEPTED_CHECKSUM_ALIASES,
    apply_migrations,
    list_migration_files,
    migration_checksum,
    resolve_migrations_dir,
)
from app.config import settings
from app.deps import get_current_claims
from app.main import app
from app.services.etl_orchestrator import TENANT_TRACK_LOCK_NAMESPACE


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


def _extract_sql_block(path: Path, start_marker: str, end_marker: str) -> str:
    sql_text = path.read_text(encoding="utf-8")
    start_idx = sql_text.index(start_marker)
    end_idx = sql_text.index(end_marker, start_idx)
    return sql_text[start_idx:end_idx]


def _run_sql_block(db_name: str, sql_text: str) -> None:
    with psycopg.connect(_admin_dsn(db_name)) as conn:
        conn.execute(sql_text)
        conn.commit()


def _seed_cash_open_fixture(db_name: str, fixture_day: date) -> None:
    data_key = int(fixture_day.strftime("%Y%m%d"))
    valid_from = fixture_day.isoformat()
    open_ts = f"{fixture_day.isoformat()} 08:00:00+00"
    sale_ts = f"{fixture_day.isoformat()} 09:30:00+00"
    payment_ts = f"{fixture_day.isoformat()} 09:35:00+00"

    _execute_sql(
        db_name,
        """
        INSERT INTO app.tenants (id_empresa, nome, ingest_key)
        VALUES (1, 'Tenant Cash Fixture', gen_random_uuid())
        ON CONFLICT (id_empresa)
        DO UPDATE SET
          nome = EXCLUDED.nome,
          ingest_key = COALESCE(app.tenants.ingest_key, EXCLUDED.ingest_key);
        """,
    )
    _execute_sql(
        db_name,
        """
        INSERT INTO auth.filiais (id_empresa, id_filial, nome, is_active, valid_from)
        VALUES (1, 1, 'Filial Fixture', true, %s::date)
        ON CONFLICT (id_empresa, id_filial)
        DO UPDATE SET
          nome = EXCLUDED.nome,
          is_active = EXCLUDED.is_active,
          valid_from = EXCLUDED.valid_from;
        """,
        (valid_from,),
    )
    _execute_sql(
        db_name,
        """
        INSERT INTO dw.dim_usuario_caixa (id_empresa, id_filial, id_usuario, nome, payload)
        VALUES (1, 1, 910, 'Operadora Fixture', '{}'::jsonb)
        ON CONFLICT (id_empresa, id_filial, id_usuario)
        DO UPDATE SET nome = EXCLUDED.nome, payload = EXCLUDED.payload;
        """,
    )
    _execute_sql(
        db_name,
        """
        INSERT INTO dw.fact_caixa_turno (
          id_empresa, id_filial, id_turno, id_db, id_usuario, abertura_ts,
          fechamento_ts, data_key_abertura, data_key_fechamento,
          encerrante_fechamento, is_aberto, status_raw, payload
        )
        VALUES (1, 1, 77, 1, 910, %s::timestamptz, NULL, %s::int, NULL, NULL, true, 'OPEN', '{}'::jsonb)
        ON CONFLICT (id_empresa, id_filial, id_turno)
        DO UPDATE SET
          id_usuario = EXCLUDED.id_usuario,
          abertura_ts = EXCLUDED.abertura_ts,
          fechamento_ts = EXCLUDED.fechamento_ts,
          data_key_abertura = EXCLUDED.data_key_abertura,
          data_key_fechamento = EXCLUDED.data_key_fechamento,
          encerrante_fechamento = EXCLUDED.encerrante_fechamento,
          is_aberto = EXCLUDED.is_aberto,
          status_raw = EXCLUDED.status_raw,
          payload = EXCLUDED.payload;
        """,
        (open_ts, data_key),
    )
    _execute_sql(
        db_name,
        """
        INSERT INTO dw.fact_comprovante (
          id_empresa, id_filial, id_db, id_comprovante, data, data_key,
          id_usuario, id_turno, valor_total, cancelado, situacao, payload
        )
        VALUES (1, 1, 1, 77001, %s::timestamptz, %s::int, 910, 77, 180, false, 1, '{"CFOP":"5102"}'::jsonb)
        ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante)
        DO UPDATE SET
          data = EXCLUDED.data,
          data_key = EXCLUDED.data_key,
          id_usuario = EXCLUDED.id_usuario,
          id_turno = EXCLUDED.id_turno,
          valor_total = EXCLUDED.valor_total,
          cancelado = EXCLUDED.cancelado,
          situacao = EXCLUDED.situacao,
          payload = EXCLUDED.payload;
        """,
        (sale_ts, data_key),
    )
    _execute_sql(
        db_name,
        """
        INSERT INTO dw.fact_pagamento_comprovante (
          id_empresa, id_filial, referencia, id_db, id_comprovante, id_turno, id_usuario,
          tipo_forma, valor, dt_evento, data_key, payload
        )
        VALUES (1, 1, 77001, 1, 77001, 77, 910, 1, 180, %s::timestamptz, %s::int, '{}'::jsonb)
        ON CONFLICT (id_empresa, id_filial, referencia, tipo_forma)
        DO UPDATE SET
          id_db = EXCLUDED.id_db,
          id_comprovante = EXCLUDED.id_comprovante,
          id_turno = EXCLUDED.id_turno,
          id_usuario = EXCLUDED.id_usuario,
          valor = EXCLUDED.valor,
          dt_evento = EXCLUDED.dt_evento,
          data_key = EXCLUDED.data_key,
          payload = EXCLUDED.payload;
        """,
        (payment_ts, data_key),
    )


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

    def test_health_returns_degraded_when_startup_soft_fails(self) -> None:
        previous_status = dict(main_module._startup_status)
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"db": "torqmind_test", "now": "2026-03-26T12:00:00+00:00"}
        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn

        try:
            with patch.object(main_module, "_ensure_dev_seed", side_effect=RuntimeError("seed mismatch")):
                main_module.startup_event()

            with patch.object(main_module, "get_conn", return_value=mock_conn):
                response = main_module.health()

            self.assertEqual(response.status_code, 503)
            body = json.loads(response.body)
            self.assertFalse(body["ok"])
            self.assertEqual(body["status"], "degraded")
            self.assertEqual(body["startup"]["message"], "seed mismatch")
        finally:
            main_module._startup_status = previous_status

    def test_hard_reset_path_requires_explicit_guard_variables(self) -> None:
        reset_sql = Path(__file__).resolve().parents[1] / "sql" / "torqmind_reset_db_v2.sql"
        makefile_path = Path(__file__).resolve().parents[1] / "Makefile"
        if not reset_sql.exists() or not makefile_path.exists():
            self.skipTest("Reset artifacts are validated from the repository root, outside the packaged API image.")
        reset_sql_text = reset_sql.read_text(encoding="utf-8")
        makefile_text = makefile_path.read_text(encoding="utf-8")

        self.assertIn("TM_ALLOW_RESET=1 is required", reset_sql_text)
        self.assertIn("TM_RESET_ENV must be dev or homolog", reset_sql_text)
        self.assertIn('RESET_CONFIRM=1', makefile_text)
        self.assertIn('RESET_ENV=dev or RESET_ENV=homolog', makefile_text)
        self.assertIn('TM_ALLOW_RESET=1', makefile_text)

    def test_hard_reset_script_replays_latest_business_date_fix_migrations(self) -> None:
        reset_sql = Path(__file__).resolve().parents[1] / "sql" / "torqmind_reset_db_v2.sql"
        if not reset_sql.exists():
            self.skipTest("Reset script is validated from the repository root, outside the packaged API image.")

        reset_sql_text = reset_sql.read_text(encoding="utf-8")
        self.assertIn(r"\ir migrations/039_fact_venda_cancel_sync.sql", reset_sql_text)
        self.assertIn(r"\ir migrations/040_business_date_semantics_fix.sql", reset_sql_text)
        self.assertIn(r"\ir migrations/041_financial_semantics_operational_dashboards.sql", reset_sql_text)
        self.assertIn(r"\ir migrations/042_operational_incremental_fastpath.sql", reset_sql_text)
        self.assertIn(r"\ir migrations/043_operational_incremental_semantics_fix.sql", reset_sql_text)
        self.assertIn(r"\ir migrations/044_payment_comprovante_hotpath_refactor.sql", reset_sql_text)
        self.assertIn(r"\ir migrations/045_default_scope_today.sql", reset_sql_text)
        self.assertIn(r"\ir migrations/046_risk_events_delta_fine.sql", reset_sql_text)
        self.assertIn(r"\ir migrations/047_sales_status_semantics_fix.sql", reset_sql_text)

    def test_risk_delta_migration_uses_changed_day_expansion_not_full_window_rebuild(self) -> None:
        migration_sql = Path(__file__).resolve().parents[1] / "sql" / "migrations" / "046_risk_events_delta_fine.sql"
        if not migration_sql.exists():
            self.skipTest("Risk delta migration is validated from the repository root, outside the packaged API image.")

        migration_text = migration_sql.read_text(encoding="utf-8")
        self.assertIn("updated_at > v_wm", migration_text)
        self.assertIn("tmp_risk_affected_cancel_days", migration_text)
        self.assertIn("tmp_risk_affected_discount_days", migration_text)
        self.assertNotIn("tmp_risk_sales_window", migration_text)
        self.assertNotIn("tmp_risk_sale_items_window", migration_text)
        self.assertNotIn("WHERE r.id_empresa = p_id_empresa", migration_text)

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
            self.assertTrue(_column_exists(db_name, "dw", "fact_comprovante", "data_conta"))
            self.assertTrue(_column_exists(db_name, "dw", "fact_comprovante", "cash_eligible"))
            self.assertTrue(_column_exists(db_name, "dw", "fact_pagamento_comprovante", "data_conta"))
            self.assertTrue(_column_exists(db_name, "dw", "fact_pagamento_comprovante", "cash_eligible"))

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

    def test_migrate_fixes_operational_business_date_semantics_independent_of_session_timezone(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)
            tenant_id = 4021

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            with psycopg.connect(_admin_dsn(db_name), row_factory=dict_row) as conn:
                conn.execute(
                    """
                    INSERT INTO app.tenants (
                      id_empresa, nome, is_active, status, billing_status,
                      valid_from, sales_history_days, default_product_scope_days, ingest_key
                    )
                    VALUES (%s, 'Tenant TZ Semantics', true, 'active', 'current', CURRENT_DATE, 30, 30, gen_random_uuid())
                    """,
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
                    INSERT INTO dw.dim_produto (id_empresa, id_filial, id_produto, nome, custo_medio)
                    VALUES (%s, 1, 700, 'Produto Teste', 10)
                    ON CONFLICT (id_empresa, id_filial, id_produto)
                    DO UPDATE SET nome = EXCLUDED.nome, custo_medio = EXCLUDED.custo_medio
                    """,
                    (tenant_id,),
                )

                sales_cases = [
                    {
                        "label": "old_23_59",
                        "id_comprovante": 9100,
                        "id_movprodutos": 9200,
                        "referencia": 9800,
                        "event_ts": "2026-03-30 23:59:00-03",
                        "payload_ts": "2026-03-30T23:59:00-03:00",
                        "expected_local": "2026-03-30T23:59:00",
                        "expected_key": 20260330,
                    },
                    {
                        "label": "21_30",
                        "id_comprovante": 9101,
                        "id_movprodutos": 9201,
                        "referencia": 9801,
                        "event_ts": "2026-03-31 21:30:00-03",
                        "payload_ts": "2026-03-31T21:30:00-03:00",
                        "expected_local": "2026-03-31T21:30:00",
                        "expected_key": 20260331,
                    },
                    {
                        "label": "22_33",
                        "id_comprovante": 9102,
                        "id_movprodutos": 9202,
                        "referencia": 9802,
                        "event_ts": None,
                        "payload_ts": "2026-03-31T22:33:00-03:00",
                        "expected_local": "2026-03-31T22:33:00",
                        "expected_key": 20260331,
                    },
                    {
                        "label": "23_59",
                        "id_comprovante": 9103,
                        "id_movprodutos": 9203,
                        "referencia": 9803,
                        "event_ts": "2026-03-31 23:59:00-03",
                        "payload_ts": "2026-03-31T23:59:00-03:00",
                        "expected_local": "2026-03-31T23:59:00",
                        "expected_key": 20260331,
                    },
                    {
                        "label": "00_10",
                        "id_comprovante": 9104,
                        "id_movprodutos": 9204,
                        "referencia": 9804,
                        "event_ts": "2026-04-01 00:10:00-03",
                        "payload_ts": "2026-04-01T00:10:00-03:00",
                        "expected_local": "2026-04-01T00:10:00",
                        "expected_key": 20260401,
                    },
                ]

                for idx, item in enumerate(sales_cases, start=1):
                    conn.execute(
                        """
                        INSERT INTO stg.comprovantes (
                          id_empresa, id_filial, id_db, id_comprovante,
                          dt_evento, payload, referencia_shadow,
                          id_usuario_shadow, id_turno_shadow, id_cliente_shadow,
                          valor_total_shadow, cancelado_shadow, situacao_shadow,
                          received_at
                        )
                        VALUES (
                          %s, 1, 1, %s,
                          %s::timestamptz, %s::jsonb, %s,
                          111, 71, 501,
                          %s, false, 1,
                          COALESCE(%s::timestamptz, now())
                        )
                        """,
                        (
                            tenant_id,
                            item["id_comprovante"],
                            item["event_ts"],
                            json.dumps(
                                {
                                    "ID_FILIAL": 1,
                                    "ID_DB": 1,
                                    "ID_COMPROVANTE": item["id_comprovante"],
                                    "ID_USUARIOS": 111,
                                    "ID_TURNOS": 71,
                                    "ID_ENTIDADE": 501,
                                    "VLRTOTAL": 100 + idx,
                                    "REFERENCIA": item["referencia"],
                                    "DATA": item["payload_ts"],
                                }
                            ),
                            item["referencia"],
                            100 + idx,
                            item["event_ts"],
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO stg.movprodutos (
                          id_empresa, id_filial, id_db, id_movprodutos,
                          dt_evento, payload, id_comprovante_shadow,
                          id_usuario_shadow, id_turno_shadow, id_cliente_shadow,
                          saidas_entradas_shadow, total_venda_shadow, received_at
                        )
                        VALUES (
                          %s, 1, 1, %s,
                          %s::timestamptz, %s::jsonb, %s,
                          111, 71, 501,
                          1, %s, COALESCE(%s::timestamptz, now())
                        )
                        """,
                        (
                            tenant_id,
                            item["id_movprodutos"],
                            item["event_ts"],
                            json.dumps(
                                {
                                    "ID_FILIAL": 1,
                                    "ID_DB": 1,
                                    "ID_MOVPRODUTOS": item["id_movprodutos"],
                                    "ID_COMPROVANTE": item["id_comprovante"],
                                    "ID_USUARIOS": 111,
                                    "ID_TURNOS": 71,
                                    "ID_ENTIDADE": 501,
                                    "SAIDAS_ENTRADAS": 1,
                                    "TOTALVENDA": 100 + idx,
                                    "DATA": item["payload_ts"],
                                }
                            ),
                            item["id_comprovante"],
                            100 + idx,
                            item["event_ts"],
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO stg.itensmovprodutos (
                          id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos,
                          dt_evento, payload,
                          id_produto_shadow, id_grupo_produto_shadow, id_local_venda_shadow,
                          id_funcionario_shadow, cfop_shadow, qtd_shadow,
                          valor_unitario_shadow, total_shadow, desconto_shadow,
                          custo_unitario_shadow, received_at
                        )
                        VALUES (
                          %s, 1, 1, %s, 1,
                          %s::timestamptz, %s::jsonb,
                          700, 10, 20,
                          30, 5102, 1,
                          %s, %s, 0,
                          10, COALESCE(%s::timestamptz, now())
                        )
                        """,
                        (
                            tenant_id,
                            item["id_movprodutos"],
                            None if item["label"] == "22_33" else item["event_ts"],
                            json.dumps(
                                {
                                    "ID_PRODUTOS": 700,
                                    "ID_GRUPOPRODUTOS": 10,
                                    "ID_LOCALVENDAS": 20,
                                    "ID_FUNCIONARIOS": 30,
                                    "CFOP": 5102,
                                    "QTDE": 1,
                                    "VLRUNITARIO": 100 + idx,
                                    "TOTAL": 100 + idx,
                                    "VLRCUSTO": 10,
                                }
                            ),
                            100 + idx,
                            100 + idx,
                            item["event_ts"],
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO stg.formas_pgto_comprovantes (
                          id_empresa, id_filial, id_referencia, tipo_forma,
                          dt_evento, payload, id_db_shadow, valor_shadow, received_at
                        )
                        VALUES (
                          %s, 1, %s, 28,
                          %s::timestamptz, %s::jsonb, 1, %s, COALESCE(%s::timestamptz, now())
                        )
                        """,
                        (
                            tenant_id,
                            item["referencia"],
                            None if item["label"] == "22_33" else item["event_ts"],
                            json.dumps(
                                {
                                    "ID_FILIAL": 1,
                                    "ID_DB": 1,
                                    "TIPO_FORMA": 28,
                                    "VALOR": 100 + idx,
                                    "DATAHORA": item["payload_ts"],
                                }
                            ),
                            100 + idx,
                            item["event_ts"],
                        ),
                    )

                conn.execute(
                    """
                    INSERT INTO stg.turnos (id_empresa, id_filial, id_turno, payload, received_at)
                    VALUES (
                      %s, 1, 771,
                      %s::jsonb,
                      '2026-04-01 00:10:00-03'::timestamptz
                    )
                    """,
                    (
                        tenant_id,
                        json.dumps(
                            {
                                "ID_DB": 1,
                                "ID_USUARIOS": 111,
                                "DTABERTURA": "2026-03-31T21:30:00-03:00",
                                "DTFECHAMENTO": "2026-04-01T00:10:00-03:00",
                                "ENCERRANTEFECHAMENTO": 1,
                                "STATUS": "FECHADO",
                            }
                        ),
                    ),
                )
                conn.commit()

                func_sql = """
                    WITH src(label, ts) AS (
                      VALUES
                        ('21:30', '2026-03-31 21:30:00-03'::timestamptz),
                        ('22:33', '2026-03-31 22:33:00-03'::timestamptz),
                        ('23:59', '2026-03-31 23:59:00-03'::timestamptz),
                        ('00:10', '2026-04-01 00:10:00-03'::timestamptz)
                    )
                    SELECT
                      label,
                      etl.business_date(ts) AS business_date,
                      etl.business_date_key(ts) AS data_key,
                      ts::date AS session_date
                    FROM src
                    ORDER BY label
                """

                conn.execute("SET TIME ZONE 'UTC'")
                utc_rows = conn.execute(func_sql).fetchall()
                conn.execute("SET TIME ZONE 'America/Sao_Paulo'")
                sp_rows = conn.execute(func_sql).fetchall()

                utc_map = {str(row["label"]): row for row in utc_rows}
                sp_map = {str(row["label"]): row for row in sp_rows}
                expected_keys = {"21:30": 20260331, "22:33": 20260331, "23:59": 20260331, "00:10": 20260401}
                expected_dates = {"21:30": "2026-03-31", "22:33": "2026-03-31", "23:59": "2026-03-31", "00:10": "2026-04-01"}

                for label, expected_key in expected_keys.items():
                    self.assertEqual(int(utc_map[label]["data_key"]), expected_key)
                    self.assertEqual(int(sp_map[label]["data_key"]), expected_key)
                    self.assertEqual(utc_map[label]["business_date"].isoformat(), expected_dates[label])
                    self.assertEqual(sp_map[label]["business_date"].isoformat(), expected_dates[label])

                self.assertEqual(utc_map["23:59"]["session_date"].isoformat(), "2026-04-01")
                self.assertEqual(sp_map["23:59"]["session_date"].isoformat(), "2026-03-31")

                helper_def = _fetch_function_definition(db_name, "etl.date_key(timestamptz)").lower()
                venda_def = _fetch_function_definition(db_name, "etl.load_fact_venda(integer)").lower()
                pagamento_def = _fetch_function_definition(db_name, "etl.load_fact_pagamento_comprovante(integer)").lower()
                purge_def = _fetch_function_definition(db_name, "etl.purge_sales_history(integer, date)").lower()
                caixa_def = _fetch_function_definition(db_name, "etl.load_fact_caixa_turno(integer)").lower()

                self.assertIn("etl.business_date_key", helper_def)
                self.assertNotIn("p_ts::timestamp", helper_def)
                self.assertNotIn("dt_evento::date", venda_def)
                self.assertNotIn("::timestamp", pagamento_def)
                self.assertIn("etl.business_date(", purge_def)
                self.assertIn("etl.business_date_key", caixa_def)

                conn.execute("SET TIME ZONE 'UTC'")
                self.assertEqual(
                    int(conn.execute("SELECT etl.load_fact_comprovante(%s) AS total", (tenant_id,)).fetchone()["total"]),
                    5,
                )
                self.assertEqual(
                    int(conn.execute("SELECT etl.load_fact_venda(%s) AS total", (tenant_id,)).fetchone()["total"]),
                    5,
                )
                self.assertEqual(
                    int(conn.execute("SELECT etl.load_fact_venda_item(%s) AS total", (tenant_id,)).fetchone()["total"]),
                    5,
                )
                self.assertEqual(
                    int(conn.execute("SELECT etl.load_fact_pagamento_comprovante(%s) AS total", (tenant_id,)).fetchone()["total"]),
                    5,
                )
                self.assertEqual(
                    int(conn.execute("SELECT etl.load_fact_caixa_turno(%s) AS total", (tenant_id,)).fetchone()["total"]),
                    1,
                )
                conn.commit()

                fact_comprovante = conn.execute(
                    """
                    SELECT id_comprovante, data, data_key
                    FROM dw.fact_comprovante
                    WHERE id_empresa = %s
                    ORDER BY id_comprovante
                    """,
                    (tenant_id,),
                ).fetchall()
                fact_venda = conn.execute(
                    """
                    SELECT id_movprodutos, data, data_key
                    FROM dw.fact_venda
                    WHERE id_empresa = %s
                    ORDER BY id_movprodutos
                    """,
                    (tenant_id,),
                ).fetchall()
                fact_venda_item = conn.execute(
                    """
                    SELECT id_movprodutos, data_key
                    FROM dw.fact_venda_item
                    WHERE id_empresa = %s
                    ORDER BY id_movprodutos
                    """,
                    (tenant_id,),
                ).fetchall()
                fact_pagamento = conn.execute(
                    """
                    SELECT referencia, dt_evento AT TIME ZONE 'America/Sao_Paulo' AS dt_evento_local, data_key
                    FROM dw.fact_pagamento_comprovante
                    WHERE id_empresa = %s
                    ORDER BY referencia
                    """,
                    (tenant_id,),
                ).fetchall()
                caixa_row = conn.execute(
                    """
                    SELECT
                      abertura_ts AT TIME ZONE 'America/Sao_Paulo' AS abertura_local,
                      fechamento_ts AT TIME ZONE 'America/Sao_Paulo' AS fechamento_local,
                      data_key_abertura,
                      data_key_fechamento
                    FROM dw.fact_caixa_turno
                    WHERE id_empresa = %s
                      AND id_turno = 771
                    """,
                    (tenant_id,),
                ).fetchone()

                expected_by_comprovante = {
                    int(item["id_comprovante"]): (str(item["expected_local"]), int(item["expected_key"]))
                    for item in sales_cases
                }
                expected_by_movimento = {
                    int(item["id_movprodutos"]): (str(item["expected_local"]), int(item["expected_key"]))
                    for item in sales_cases
                }
                expected_by_referencia = {
                    int(item["referencia"]): (str(item["expected_local"]), int(item["expected_key"]))
                    for item in sales_cases
                }

                for row in fact_comprovante:
                    expected_local, expected_key = expected_by_comprovante[int(row["id_comprovante"])]
                    self.assertEqual(row["data"].isoformat(), expected_local)
                    self.assertEqual(int(row["data_key"]), expected_key)

                for row in fact_venda:
                    expected_local, expected_key = expected_by_movimento[int(row["id_movprodutos"])]
                    self.assertEqual(row["data"].isoformat(), expected_local)
                    self.assertEqual(int(row["data_key"]), expected_key)

                for row in fact_venda_item:
                    _, expected_key = expected_by_movimento[int(row["id_movprodutos"])]
                    self.assertEqual(int(row["data_key"]), expected_key)

                for row in fact_pagamento:
                    expected_local, expected_key = expected_by_referencia[int(row["referencia"])]
                    self.assertEqual(row["dt_evento_local"].isoformat(), expected_local)
                    self.assertEqual(int(row["data_key"]), expected_key)

                self.assertIsNotNone(caixa_row)
                self.assertEqual(caixa_row["abertura_local"].isoformat(), "2026-03-31T21:30:00")
                self.assertEqual(caixa_row["fechamento_local"].isoformat(), "2026-04-01T00:10:00")
                self.assertEqual(int(caixa_row["data_key_abertura"]), 20260331)
                self.assertEqual(int(caixa_row["data_key_fechamento"]), 20260401)

                conn.execute(
                    "UPDATE app.tenants SET sales_history_days = 2 WHERE id_empresa = %s",
                    (tenant_id,),
                )
                conn.execute("SET TIME ZONE 'UTC'")
                purge_result = conn.execute(
                    "SELECT etl.purge_sales_history(%s, %s::date) AS result",
                    (tenant_id, "2026-04-01"),
                ).fetchone()["result"]
                conn.commit()

                self.assertTrue(bool(purge_result["ok"]))
                self.assertGreaterEqual(int(purge_result["dw_fact_comprovante_deleted"]), 1)
                self.assertGreaterEqual(int(purge_result["dw_fact_venda_deleted"]), 1)
                self.assertGreaterEqual(int(purge_result["dw_fact_pagamento_comprovante_deleted"]), 1)

                remaining_keys = conn.execute(
                    """
                    SELECT
                      (SELECT array_agg(data_key ORDER BY data_key) FROM dw.fact_comprovante WHERE id_empresa = %s) AS comp_keys,
                      (SELECT array_agg(data_key ORDER BY data_key) FROM dw.fact_venda WHERE id_empresa = %s) AS venda_keys,
                      (SELECT array_agg(data_key ORDER BY data_key) FROM dw.fact_pagamento_comprovante WHERE id_empresa = %s) AS pagamento_keys
                    """,
                    (tenant_id, tenant_id, tenant_id),
                ).fetchone()

                self.assertEqual(list(remaining_keys["comp_keys"]), [20260331, 20260331, 20260331, 20260401])
                self.assertEqual(list(remaining_keys["venda_keys"]), [20260331, 20260331, 20260331, 20260401])
                self.assertEqual(list(remaining_keys["pagamento_keys"]), [20260331, 20260331, 20260331, 20260401])

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

    def test_migrate_installs_operational_overlay_indexes_and_tracks_current_036_checksum(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            overlay_path = self.migrations_dir / "036_operational_publication_overlay_indexes.sql"
            venda_overlay_idx = _index_definition(db_name, "dw", "ix_fact_venda_live_overlay")
            venda_item_overlay_idx = _index_definition(db_name, "dw", "ix_fact_venda_item_live_overlay")

            self.assertIsNotNone(venda_overlay_idx)
            self.assertIsNotNone(venda_item_overlay_idx)
            self.assertIn("id_movprodutos", str(venda_overlay_idx))
            self.assertIn("id_produto", str(venda_item_overlay_idx))
            self.assertEqual(
                _fetchscalar(
                    db_name,
                    "SELECT checksum FROM app.schema_migrations WHERE filename = %s",
                    (overlay_path.name,),
                ),
                migration_checksum(overlay_path),
            )

    def test_migrate_supports_nontransactional_migrations_and_tracks_them_normally(self) -> None:
        with temporary_database() as db_name, tempfile.TemporaryDirectory() as tmp_dir:
            migrations_dir = Path(tmp_dir)
            (migrations_dir / "001_app_schema.sql").write_text(
                """
                CREATE SCHEMA IF NOT EXISTS app;
                CREATE TABLE IF NOT EXISTS app.sample (
                  id bigserial PRIMARY KEY,
                  tenant_id integer NOT NULL,
                  created_at timestamptz NOT NULL DEFAULT now()
                );
                """.strip()
                + "\n",
                encoding="utf-8",
            )
            nontransactional_path = migrations_dir / "002_sample_index.sql"
            nontransactional_path.write_text(
                """
                -- @nontransactional
                CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_sample_tenant_created
                  ON app.sample (tenant_id, created_at DESC);
                """.strip()
                + "\n",
                encoding="utf-8",
            )

            with patch("app.cli.migrate._conn_str", return_value=_admin_dsn(db_name)):
                first = apply_migrations(migrations_dir)
                second = apply_migrations(migrations_dir)

            self.assertEqual(first.mode, "bootstrap")
            self.assertEqual([path.name for path in first.applied], ["001_app_schema.sql", "002_sample_index.sql"])
            self.assertEqual(_fetchscalar(db_name, "SELECT COUNT(*) FROM app.schema_migrations"), 2)
            self.assertEqual(
                _fetchscalar(
                    db_name,
                    "SELECT checksum FROM app.schema_migrations WHERE filename = '002_sample_index.sql'",
                ),
                migration_checksum(nontransactional_path),
            )
            self.assertIsNotNone(_index_definition(db_name, "app", "ix_sample_tenant_created"))
            self.assertEqual(second.mode, "managed")
            self.assertEqual([path.name for path in second.skipped], ["001_app_schema.sql", "002_sample_index.sql"])

    def test_migrate_accepts_legacy_checksum_for_safe_rewritten_036(self) -> None:
        with temporary_database() as db_name, tempfile.TemporaryDirectory() as tmp_dir:
            migrations_dir = Path(tmp_dir)
            source_path = self.migrations_dir / "036_operational_publication_overlay_indexes.sql"
            target_path = migrations_dir / source_path.name
            target_path.write_text(source_path.read_text(encoding="utf-8"), encoding="utf-8")

            legacy_checksum = next(iter(ACCEPTED_CHECKSUM_ALIASES[source_path.name]))
            _execute_sql(
                db_name,
                """
                CREATE SCHEMA IF NOT EXISTS app;
                CREATE TABLE IF NOT EXISTS app.schema_migrations (
                  filename text PRIMARY KEY,
                  checksum text NOT NULL,
                  applied_at timestamptz NOT NULL DEFAULT now(),
                  execution_kind text NOT NULL DEFAULT 'applied'
                    CHECK (execution_kind IN ('applied', 'baseline'))
                );
                """,
            )
            _execute_sql(
                db_name,
                """
                INSERT INTO app.schema_migrations (filename, checksum, execution_kind)
                VALUES (%s, %s, 'applied')
                """,
                (source_path.name, legacy_checksum),
            )

            with patch("app.cli.migrate._conn_str", return_value=_admin_dsn(db_name)):
                result = apply_migrations(migrations_dir)

            self.assertEqual(result.mode, "managed")
            self.assertEqual([path.name for path in result.applied], [])
            self.assertEqual([path.name for path in result.skipped], [source_path.name])
            self.assertEqual(
                _fetchscalar(
                    db_name,
                    "SELECT checksum FROM app.schema_migrations WHERE filename = %s",
                    (source_path.name,),
                ),
                legacy_checksum,
            )

    def test_compute_risk_events_v2_is_idempotent_and_cleans_stale_rows_in_recomputed_window(self) -> None:
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
                    "SELECT etl.compute_risk_events_v2(%s, %s, %s, %s) AS rows",
                    (tenant_id, False, 14, "2026-03-10 23:59:59+00"),
                ).fetchone()
                first_total = conn.execute(
                    "SELECT COUNT(*) AS total FROM dw.fact_risco_evento WHERE id_empresa = %s",
                    (tenant_id,),
                ).fetchone()["total"]

                second_run = conn.execute(
                    "SELECT etl.compute_risk_events_v2(%s, %s, %s, %s) AS rows",
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
                    "SELECT etl.compute_risk_events_v2(%s, %s, %s, %s) AS rows",
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

    def test_load_fact_venda_follows_movprodutos_situacao_not_comprovante_cancelado(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)
            tenant_id = 4011
            fixture_day = date.today() - timedelta(days=10)
            fixture_ts = f"{fixture_day.isoformat()} 10:00:00+00"

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            _execute_sql(
                db_name,
                """
                INSERT INTO app.tenants (id_empresa, nome, is_active, status, billing_status, valid_from)
                VALUES (%s, 'Tenant Cancel Sync', true, 'active', 'current', CURRENT_DATE)
                """,
                (tenant_id,),
            )
            _execute_sql(
                db_name,
                """
                INSERT INTO auth.filiais (id_empresa, id_filial, nome, is_active, valid_from)
                VALUES (%s, 1, 'Filial 1', true, CURRENT_DATE)
                """,
                (tenant_id,),
            )

            comprovante_open = json.dumps(
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 91001,
                    "ID_USUARIOS": 111,
                    "ID_TURNOS": 71,
                    "VLRTOTAL": 180,
                    "CANCELADO": 0,
                    "SITUACAO": 1,
                    "CFOP": "5102",
                    "DATA": f"{fixture_day.isoformat()} 10:00:00",
                }
            )
            comprovante_cancelled = json.dumps(
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 91001,
                    "ID_USUARIOS": 111,
                    "ID_TURNOS": 71,
                    "VLRTOTAL": 180,
                    "CANCELADO": 1,
                    "SITUACAO": 2,
                    "CFOP": "5102",
                    "DATA": f"{fixture_day.isoformat()} 10:00:00",
                }
            )
            mov_payload = json.dumps(
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 91001,
                    "ID_COMPROVANTE": 91001,
                    "ID_USUARIOS": 111,
                    "ID_TURNOS": 71,
                    "TOTALVENDA": 180,
                    "SITUACAO": 1,
                    "DATA": f"{fixture_day.isoformat()} 10:00:00",
                }
            )

            _execute_sql(
                db_name,
                """
                INSERT INTO stg.comprovantes (
                  id_empresa, id_filial, id_db, id_comprovante, dt_evento, payload, received_at, cancelado_shadow
                )
                VALUES (%s, 1, 1, 91001, %s::timestamptz, %s::jsonb, %s::timestamptz, false)
                """,
                (tenant_id, fixture_ts, comprovante_open, fixture_ts),
            )
            _execute_sql(
                db_name,
                """
                INSERT INTO stg.movprodutos (
                  id_empresa, id_filial, id_db, id_movprodutos, dt_evento, payload, received_at, id_comprovante_shadow, total_venda_shadow, situacao_shadow
                )
                VALUES (%s, 1, 1, 91001, %s::timestamptz, %s::jsonb, %s::timestamptz, 91001, 180, 1)
                """,
                (tenant_id, fixture_ts, mov_payload, fixture_ts),
            )

            venda_def = _fetch_function_definition(db_name, "etl.load_fact_venda(integer)").lower()
            comprovante_def = _fetch_function_definition(db_name, "etl.load_fact_comprovante(integer)").lower()
            self.assertIn("movimento_venda_is_cancelled", venda_def)
            self.assertIn("movimento_venda_situacao", venda_def)
            self.assertNotIn("dw.fact_comprovante", venda_def)
            self.assertNotIn("synced_venda_cancel", comprovante_def)

            self.assertEqual(_fetchscalar(db_name, "SELECT etl.load_fact_comprovante(%s)", (tenant_id,)), 1)
            self.assertEqual(_fetchscalar(db_name, "SELECT etl.load_fact_venda(%s)", (tenant_id,)), 1)
            first_sale = _fetchone(
                db_name,
                """
                SELECT cancelado, situacao
                FROM dw.fact_venda
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_movprodutos = 91001
                """,
                (tenant_id,),
            )
            self.assertIsNotNone(first_sale)
            self.assertFalse(bool(first_sale["cancelado"]))
            self.assertEqual(int(first_sale["situacao"]), 1)

            _execute_sql(
                db_name,
                """
                UPDATE stg.comprovantes
                SET payload = %s::jsonb,
                    cancelado_shadow = true,
                    situacao_shadow = 2,
                    received_at = now()
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_comprovante = 91001
                """,
                (comprovante_cancelled, tenant_id),
            )

            self.assertEqual(_fetchscalar(db_name, "SELECT etl.load_fact_comprovante(%s)", (tenant_id,)), 1)
            self.assertEqual(_fetchscalar(db_name, "SELECT etl.load_fact_venda(%s)", (tenant_id,)), 0)
            second_sale = _fetchone(
                db_name,
                """
                SELECT cancelado, situacao
                FROM dw.fact_venda
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_movprodutos = 91001
                """,
                (tenant_id,),
            )
            self.assertIsNotNone(second_sale)
            self.assertFalse(bool(second_sale["cancelado"]))
            self.assertEqual(int(second_sale["situacao"]), 1)

            mov_payload_cancelled = json.dumps(
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 91001,
                    "ID_COMPROVANTE": 91001,
                    "ID_USUARIOS": 111,
                    "ID_TURNOS": 71,
                    "TOTALVENDA": 180,
                    "SITUACAO": 2,
                    "DATA": f"{fixture_day.isoformat()} 10:00:00",
                }
            )
            _execute_sql(
                db_name,
                """
                UPDATE stg.movprodutos
                SET payload = %s::jsonb,
                    situacao_shadow = 2,
                    received_at = now()
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_movprodutos = 91001
                """,
                (mov_payload_cancelled, tenant_id),
            )

            self.assertEqual(_fetchscalar(db_name, "SELECT etl.load_fact_venda(%s)", (tenant_id,)), 1)
            final_sale = _fetchone(
                db_name,
                """
                SELECT cancelado, situacao
                FROM dw.fact_venda
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_movprodutos = 91001
                """,
                (tenant_id,),
            )
            self.assertIsNotNone(final_sale)
            self.assertTrue(bool(final_sale["cancelado"]))
            self.assertEqual(int(final_sale["situacao"]), 2)

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

    def test_master_only_seed_does_not_echo_bootstrap_passwords(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            seed = _run_python(["-m", "app.cli.seed"], {**env, "SEED_MODE": "master-only"})
            self.assertEqual(seed.returncode, 0, seed.stderr or seed.stdout)
            self.assertIn("senha bootstrap mascarada", seed.stdout)
            self.assertNotIn(REAL_MASTER_PASSWORD, seed.stdout)
            self.assertNotIn(CHANNEL_BOOTSTRAP_PASSWORD, seed.stdout)
            self.assertNotIn("TorqMind@123", seed.stdout)

    def test_production_seed_rejects_insecure_bootstrap_defaults(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            seed = _run_python(["-m", "app.cli.seed"], {**env, "APP_ENV": "prod", "SEED_MODE": "master-only"})
            self.assertNotEqual(seed.returncode, 0)
            self.assertIn("Unsafe production seed bootstrap", seed.stderr or seed.stdout)

    def test_production_seed_accepts_explicit_safe_bootstrap(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            safe_env = {
                **env,
                "APP_ENV": "prod",
                "SEED_MODE": "master-only",
                "SEED_PASSWORD": "Seed#Safe123",
                "PLATFORM_MASTER_EMAIL": "ops-master@example.com",
                "PLATFORM_MASTER_PASSWORD": "Master#Safe123",
                "CHANNEL_BOOTSTRAP_PASSWORD": "Channel#Safe123",
            }
            seed = _run_python(["-m", "app.cli.seed"], safe_env)
            self.assertEqual(seed.returncode, 0, seed.stderr or seed.stdout)
            self.assertNotIn("Master#Safe123", seed.stdout)
            self.assertNotIn("Channel#Safe123", seed.stdout)

            login = _run_python(
                [
                    "-c",
                    (
                        "from app import repos_auth; "
                        "session = repos_auth.verify_login('ops-master@example.com', 'Master#Safe123'); "
                        "print(session['email'])"
                    ),
                ],
                safe_env,
            )
            self.assertEqual(login.returncode, 0, login.stderr or login.stdout)
            self.assertIn("ops-master@example.com", login.stdout)

    def test_master_only_seed_channel_bootstrap_gets_product_access_for_its_channel_portfolio(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            seed = _run_python(["-m", "app.cli.seed"], {**env, "SEED_MODE": "master-only"})
            self.assertEqual(seed.returncode, 0, seed.stderr or seed.stdout)

            channel_id = _fetchscalar(
                db_name,
                "SELECT id FROM app.channels WHERE lower(name) = lower('Canal TorqMind') LIMIT 1",
            )
            self.assertIsNotNone(channel_id)

            _execute_sql(
                db_name,
                """
                INSERT INTO app.tenants (
                  nome,
                  channel_id,
                  status,
                  billing_status,
                  valid_from,
                  is_active
                )
                VALUES ('Empresa Canal Bootstrap', %s, 'active', 'current', CURRENT_DATE, true)
                """,
                (channel_id,),
            )

            channel_login = _run_python(
                [
                    "-c",
                    (
                        "import json; "
                        "from app import repos_auth; "
                        f"session = repos_auth.verify_login('{CHANNEL_BOOTSTRAP_EMAIL}', '{CHANNEL_BOOTSTRAP_PASSWORD}'); "
                        "print(json.dumps({'role': session['user_role'], 'platform': session['access']['platform'], 'product': session['access']['product'], 'platform_finance': session['access']['platform_finance'], 'tenant_ids': session['tenant_ids'], 'home_path': session['home_path']}))"
                    ),
                ],
                env,
            )
            self.assertEqual(channel_login.returncode, 0, channel_login.stderr or channel_login.stdout)
            self.assertIn('"role": "channel_admin"', channel_login.stdout)
            self.assertIn('"platform": true', channel_login.stdout)
            self.assertIn('"product": true', channel_login.stdout)
            self.assertIn('"platform_finance": false', channel_login.stdout)
            self.assertIn('"home_path": "/dashboard?', channel_login.stdout)

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
            self.assertEqual(
                int(_fetchscalar(db_name, "SELECT etl.sync_payment_anomaly_notifications(%s, CURRENT_DATE)", (1,)) or 0),
                0,
            )

    def test_payment_loader_detail_uses_bridge_delta_and_keeps_tenant_isolation(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            _execute_sql(
                db_name,
                """
                INSERT INTO app.tenants (id_empresa, nome, ingest_key)
                VALUES
                  (1, 'Tenant Payment A', gen_random_uuid()),
                  (2, 'Tenant Payment B', gen_random_uuid())
                ON CONFLICT (id_empresa)
                DO UPDATE SET nome = EXCLUDED.nome;
                """,
            )

            _execute_sql(
                db_name,
                """
                INSERT INTO stg.comprovantes (
                  id_empresa, id_filial, id_db, id_comprovante, dt_evento, received_at,
                  referencia_shadow, id_usuario_shadow, id_turno_shadow, payload
                )
                VALUES
                  (%s, 1, 1, 5000, %s::timestamptz, %s::timestamptz, 70001, 300, 10, %s::jsonb),
                  (%s, 1, 1, 5001, %s::timestamptz, %s::timestamptz, 70001, 301, 11, %s::jsonb),
                  (%s, 1, 1, 6001, %s::timestamptz, %s::timestamptz, 70001, 901, 91, %s::jsonb)
                """,
                (
                    1,
                    "2026-04-07 09:58:00+00",
                    "2026-04-07 09:59:00+00",
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_COMPROVANTE": 5000,
                            "REFERENCIA": 70001,
                            "ID_USUARIOS": 300,
                            "ID_TURNOS": 10,
                            "VLRTOTAL": 150.00,
                            "SITUACAO": 3,
                            "DATA": "2026-04-07 06:58:00-03",
                            "DATA_CONTA": "2026-04-07 06:58:00-03",
                        }
                    ),
                    1,
                    "2026-04-07 10:00:00+00",
                    "2026-04-07 10:01:00+00",
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_COMPROVANTE": 5001,
                            "REFERENCIA": 70001,
                            "ID_USUARIOS": 301,
                            "ID_TURNOS": 11,
                            "VLRTOTAL": 157.35,
                            "SITUACAO": 3,
                            "DATA": "2026-04-07 07:00:00-03",
                            "DATA_CONTA": "2026-04-07 07:00:00-03",
                        }
                    ),
                    2,
                    "2026-04-07 11:00:00+00",
                    "2026-04-07 11:01:00+00",
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_COMPROVANTE": 6001,
                            "REFERENCIA": 70001,
                            "ID_USUARIOS": 901,
                            "ID_TURNOS": 91,
                            "VLRTOTAL": 999.99,
                            "SITUACAO": 3,
                            "DATA": "2026-04-07 08:00:00-03",
                            "DATA_CONTA": "2026-04-07 08:00:00-03",
                        }
                    ),
                ),
            )

            _execute_sql(
                db_name,
                """
                INSERT INTO stg.formas_pgto_comprovantes (
                  id_empresa, id_filial, id_referencia, tipo_forma, dt_evento, received_at,
                  valor_shadow, payload
                )
                VALUES
                  (%s, 1, 70001, 28, %s::timestamptz, %s::timestamptz, 157.35, %s::jsonb),
                  (%s, 1, 70001, 28, %s::timestamptz, %s::timestamptz, 999.99, %s::jsonb)
                """,
                (
                    1,
                    "2026-04-07 10:05:00+00",
                    "2026-04-07 10:06:00+00",
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_REFERENCIA": 70001,
                            "TIPO_FORMA": 28,
                            "VALOR_PAGO": 157.35,
                            "DATA": "2026-04-07 07:05:00-03",
                            "NSU": "NSU-70001",
                        }
                    ),
                    2,
                    "2026-04-07 11:05:00+00",
                    "2026-04-07 11:06:00+00",
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_REFERENCIA": 70001,
                            "TIPO_FORMA": 28,
                            "VALOR_PAGO": 999.99,
                            "DATA": "2026-04-07 08:05:00-03",
                            "NSU": "NSU-70001-T2",
                        }
                    ),
                ),
            )

            self.assertEqual(int(_fetchscalar(db_name, "SELECT etl.load_fact_comprovante(%s)", (1,)) or 0), 2)

            first_detail = _fetchone(
                db_name,
                "SELECT etl.load_fact_pagamento_comprovante_detail(%s) AS result",
                (1,),
            )["result"]

            self.assertEqual(int(first_detail["rows"]), 1)
            self.assertEqual(int(first_detail["candidate_count"]), 1)
            self.assertEqual(int(first_detail["upsert_inserts"]), 1)
            self.assertEqual(int(first_detail["upsert_updates"]), 0)
            self.assertEqual(int(first_detail["conflict_count"]), 0)
            self.assertEqual(int(first_detail["bridge_miss_count"]), 0)
            self.assertGreaterEqual(int(first_detail["bridge_resolve_ms"]), 0)
            self.assertGreaterEqual(int(first_detail["total_ms"]), 0)

            payment_row = _fetchone(
                db_name,
                """
                SELECT
                  id_empresa,
                  id_comprovante,
                  id_turno,
                  id_usuario,
                  valor,
                  data_conta,
                  cash_eligible,
                  row_hash
                FROM dw.fact_pagamento_comprovante
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND referencia = 70001
                  AND tipo_forma = 28
                """,
                (1,),
            )
            self.assertIsNotNone(payment_row)
            self.assertEqual(int(payment_row["id_empresa"]), 1)
            self.assertEqual(int(payment_row["id_comprovante"]), 5001)
            self.assertEqual(int(payment_row["id_turno"]), 11)
            self.assertEqual(int(payment_row["id_usuario"]), 301)
            self.assertAlmostEqual(float(payment_row["valor"]), 157.35, places=2)
            self.assertEqual(payment_row["data_conta"].isoformat(), "2026-04-07")
            self.assertTrue(bool(payment_row["cash_eligible"]))
            self.assertTrue(bool(payment_row["row_hash"]))
            self.assertEqual(
                int(_fetchscalar(db_name, "SELECT COUNT(*) FROM dw.fact_pagamento_comprovante WHERE id_empresa = %s", (2,)) or 0),
                0,
            )

            _execute_sql(
                db_name,
                """
                UPDATE stg.comprovantes
                SET
                  received_at = %s::timestamptz,
                  id_turno_shadow = 12,
                  payload = %s::jsonb
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_comprovante = 5001
                """,
                (
                    "2026-04-07 10:20:00+00",
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_COMPROVANTE": 5001,
                            "REFERENCIA": 70001,
                            "ID_USUARIOS": 301,
                            "ID_TURNOS": 12,
                            "VLRTOTAL": 157.35,
                            "SITUACAO": 3,
                            "DATA": "2026-04-07 07:00:00-03",
                            "DATA_CONTA": "2026-04-08 07:00:00-03",
                        }
                    ),
                    1,
                ),
            )

            self.assertEqual(int(_fetchscalar(db_name, "SELECT etl.load_fact_comprovante(%s)", (1,)) or 0), 1)

            second_detail = _fetchone(
                db_name,
                "SELECT etl.load_fact_pagamento_comprovante_detail(%s) AS result",
                (1,),
            )["result"]

            self.assertEqual(int(second_detail["rows"]), 1)
            self.assertEqual(int(second_detail["candidate_count"]), 1)
            self.assertEqual(int(second_detail["upsert_inserts"]), 0)
            self.assertEqual(int(second_detail["upsert_updates"]), 1)
            self.assertEqual(int(second_detail["conflict_count"]), 1)
            self.assertGreaterEqual(int(second_detail["bridge_resolve_ms"]), 0)
            self.assertGreaterEqual(int(second_detail["total_ms"]), 0)

            updated_row = _fetchone(
                db_name,
                """
                SELECT id_turno, data_conta, cash_eligible
                FROM dw.fact_pagamento_comprovante
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND referencia = 70001
                  AND tipo_forma = 28
                """,
                (1,),
            )
            self.assertIsNotNone(updated_row)
            self.assertEqual(int(updated_row["id_turno"]), 12)
            self.assertEqual(updated_row["data_conta"].isoformat(), "2026-04-08")
            self.assertFalse(bool(updated_row["cash_eligible"]))

            third_detail = _fetchone(
                db_name,
                "SELECT etl.load_fact_pagamento_comprovante_detail(%s) AS result",
                (1,),
            )["result"]
            self.assertEqual(int(third_detail["rows"]), 0)
            self.assertEqual(int(third_detail["candidate_count"]), 1)
            self.assertEqual(int(third_detail["upsert_inserts"]), 0)
            self.assertEqual(int(third_detail["upsert_updates"]), 0)

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

    def test_migrate_verify_only_fails_fast_when_cash_operational_matview_regresses_to_legacy_shape(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            legacy_cash_sql = _extract_sql_block(
                self.migrations_dir / "024_etl_clock_driven_rollover.sql",
                "DROP MATERIALIZED VIEW IF EXISTS mart.alerta_caixa_aberto CASCADE;",
                "CREATE OR REPLACE FUNCTION etl.daily_rollover_window(",
            )
            _run_sql_block(db_name, legacy_cash_sql)

            verify = _run_python(["-m", "app.cli.migrate", "--verify-only"], env)
            self.assertNotEqual(verify.returncode, 0, verify.stdout)
            self.assertIn("mart.agg_caixa_turno_aberto.is_operational_live", verify.stderr or verify.stdout)
            self.assertIn("mart.agg_caixa_turno_aberto.last_activity_ts", verify.stderr or verify.stdout)
            self.assertIn("mart.agg_caixa_forma_pagamento.forma_category", verify.stderr or verify.stdout)

    def test_cash_overview_repo_supports_legacy_cash_operational_truth_shape_without_500(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)
            fixture_day = date.today() - timedelta(days=1)
            dt_ini = fixture_day - timedelta(days=1)
            dt_fim = fixture_day + timedelta(days=1)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            _seed_cash_open_fixture(db_name, fixture_day)
            legacy_cash_sql = _extract_sql_block(
                self.migrations_dir / "024_etl_clock_driven_rollover.sql",
                "DROP MATERIALIZED VIEW IF EXISTS mart.alerta_caixa_aberto CASCADE;",
                "CREATE OR REPLACE FUNCTION etl.daily_rollover_window(",
            )
            _run_sql_block(db_name, legacy_cash_sql)

            run_repo = _run_python(
                [
                    "-c",
                    (
                        "import json; "
                        "from datetime import date; "
                        "from app import repos_mart; "
                        f"body = repos_mart.cash_overview('MASTER', 1, 1, date({dt_ini.year}, {dt_ini.month}, {dt_ini.day}), date({dt_fim.year}, {dt_fim.month}, {dt_fim.day})); "
                        "print(json.dumps({'source_status': body['source_status'], 'open': body['live_now']['kpis']['caixas_abertos'], "
                        "'schema_mode': body['live_now']['kpis'].get('schema_mode')}, ensure_ascii=False, default=str))"
                    ),
                ],
                env,
            )
            self.assertEqual(run_repo.returncode, 0, run_repo.stderr or run_repo.stdout)
            repo_body = json.loads(run_repo.stdout)
            self.assertIn(repo_body["source_status"], {"ok", "partial"})
            self.assertEqual(repo_body["open"], 1)
            self.assertEqual(repo_body["schema_mode"], "legacy")

    def test_cash_overview_repo_uses_rich_cash_operational_truth_shape_after_alignment(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)
            fixture_day = date.today() - timedelta(days=1)
            dt_ini = fixture_day - timedelta(days=1)
            dt_fim = fixture_day + timedelta(days=1)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            _seed_cash_open_fixture(db_name, fixture_day)
            _execute_sql(
                db_name,
                """
                REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto;
                REFRESH MATERIALIZED VIEW mart.alerta_caixa_aberto;
                REFRESH MATERIALIZED VIEW mart.agg_caixa_forma_pagamento;
                """,
            )

            run_repo = _run_python(
                [
                    "-c",
                    (
                        "import json; "
                        "from datetime import date; "
                        "from app import repos_mart; "
                        f"body = repos_mart.cash_overview('MASTER', 1, 1, date({dt_ini.year}, {dt_ini.month}, {dt_ini.day}), date({dt_fim.year}, {dt_fim.month}, {dt_fim.day})); "
                        "print(json.dumps({'source_status': body['source_status'], 'open': body['live_now']['kpis']['caixas_abertos'], "
                        "'schema_mode': body['live_now']['kpis'].get('schema_mode')}, ensure_ascii=False, default=str))"
                    ),
                ],
                env,
            )
            self.assertEqual(run_repo.returncode, 0, run_repo.stderr or run_repo.stdout)
            repo_body = json.loads(run_repo.stdout)
            self.assertIn(repo_body["source_status"], {"ok", "partial"})
            self.assertEqual(repo_body["open"], 1)
            self.assertEqual(repo_body["schema_mode"], "rich")

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

    def test_operational_truth_cli_can_purge_rebuild_and_validate_cash_fraud_domain(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            _execute_sql(
                db_name,
                """
                INSERT INTO app.tenants (id_empresa, nome, ingest_key)
                VALUES (1, 'Tenant Truth', gen_random_uuid())
                ON CONFLICT (id_empresa)
                DO UPDATE SET
                  nome = EXCLUDED.nome,
                  ingest_key = COALESCE(app.tenants.ingest_key, EXCLUDED.ingest_key);

                INSERT INTO auth.filiais (id_empresa, id_filial, nome, cnpj, is_active, valid_from)
                VALUES (1, 1, 'Filial Truth', '12345678000199', true, DATE '2026-03-01')
                ON CONFLICT (id_empresa, id_filial)
                DO UPDATE SET
                  nome = EXCLUDED.nome,
                  cnpj = EXCLUDED.cnpj,
                  is_active = EXCLUDED.is_active,
                  valid_from = EXCLUDED.valid_from;

                INSERT INTO stg.filiais (id_empresa, id_filial, payload)
                VALUES (1, 1, '{"ID_FILIAL":1,"NOMEFILIAL":"Filial Truth","CNPJ":"12345678000199"}'::jsonb)
                ON CONFLICT (id_empresa, id_filial)
                DO UPDATE SET payload = EXCLUDED.payload, received_at = now();

                INSERT INTO stg.usuarios (id_empresa, id_filial, id_usuario, dt_evento, payload)
                VALUES
                  (1, 1, 910, TIMESTAMPTZ '2026-03-25 07:50:00+00', '{"ID_USUARIO":910,"NOMEUSUARIOS":"Operadora do Caixa"}'::jsonb),
                  (1, 1, 911, TIMESTAMPTZ '2026-03-25 09:50:00+00', '{"ID_USUARIO":911,"NOMEUSUARIOS":"Operador Encerrado"}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_usuario)
                DO UPDATE SET payload = EXCLUDED.payload, dt_evento = EXCLUDED.dt_evento, received_at = now();

                INSERT INTO stg.turnos (id_empresa, id_filial, id_turno, dt_evento, payload)
                VALUES
                  (
                    1,
                    1,
                    71,
                    TIMESTAMPTZ '2026-03-25 08:00:00+00',
                    '{
                      "ID_DB":1,
                      "ID_USUARIOS":910,
                      "DATA":"2026-03-25 08:00:00",
                      "ENCERRANTEFECHAMENTO":0,
                      "STATUSTURNO":"ABERTO"
                    }'::jsonb
                  ),
                  (
                    1,
                    1,
                    72,
                    TIMESTAMPTZ '2026-03-25 10:00:00+00',
                    '{
                      "ID_DB":1,
                      "ID_USUARIOS":910,
                      "DATA":"2026-03-25 10:00:00",
                      "DATAFECHAMENTO":"2026-03-25 18:00:00",
                      "ENCERRANTEFECHAMENTO":901,
                      "STATUSTURNO":"FECHADO"
                    }'::jsonb
                  )
                ON CONFLICT (id_empresa, id_filial, id_turno)
                DO UPDATE SET payload = EXCLUDED.payload, dt_evento = EXCLUDED.dt_evento, received_at = now();

                INSERT INTO stg.comprovantes (id_empresa, id_filial, id_db, id_comprovante, dt_evento, payload)
                VALUES
                  (
                    1,
                    1,
                    1,
                    71001,
                    TIMESTAMPTZ '2026-03-25 09:00:00+00',
                    '{
                      "ID_FILIAL":1,
                      "ID_DB":1,
                      "ID_COMPROVANTE":71001,
                      "ID_USUARIOS":111,
                      "ID_TURNOS":71,
                      "VLRTOTAL":180,
                      "REFERENCIA":971001,
                      "CANCELADO":0,
                      "SITUACAO":1,
                      "CFOP":"5102",
                      "DATA":"2026-03-25 09:00:00"
                    }'::jsonb
                  ),
                  (
                    1,
                    1,
                    1,
                    72001,
                    TIMESTAMPTZ '2026-03-25 17:00:00+00',
                    '{
                      "ID_FILIAL":1,
                      "ID_DB":1,
                      "ID_COMPROVANTE":72001,
                      "ID_USUARIOS":111,
                      "ID_TURNOS":72,
                      "VLRTOTAL":300,
                      "REFERENCIA":972001,
                      "CANCELADO":1,
                      "SITUACAO":1,
                      "CFOP":"5102",
                      "DATA":"2026-03-25 17:00:00"
                    }'::jsonb
                  )
                ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante)
                DO UPDATE SET payload = EXCLUDED.payload, dt_evento = EXCLUDED.dt_evento, received_at = now();

                INSERT INTO stg.movprodutos (id_empresa, id_filial, id_db, id_movprodutos, dt_evento, payload)
                VALUES
                  (
                    1,
                    1,
                    1,
                    71001,
                    TIMESTAMPTZ '2026-03-25 09:00:00+00',
                    '{
                      "ID_FILIAL":1,
                      "ID_DB":1,
                      "ID_MOVPRODUTOS":71001,
                      "ID_COMPROVANTE":71001,
                      "ID_USUARIOS":111,
                      "ID_TURNOS":71,
                      "TOTALVENDA":180,
                      "DATA":"2026-03-25 09:00:00"
                    }'::jsonb
                  ),
                  (
                    1,
                    1,
                    1,
                    72001,
                    TIMESTAMPTZ '2026-03-25 17:00:00+00',
                    '{
                      "ID_FILIAL":1,
                      "ID_DB":1,
                      "ID_MOVPRODUTOS":72001,
                      "ID_COMPROVANTE":72001,
                      "ID_USUARIOS":111,
                      "ID_TURNOS":72,
                      "TOTALVENDA":300,
                      "DATA":"2026-03-25 17:00:00"
                    }'::jsonb
                  )
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos)
                DO UPDATE SET payload = EXCLUDED.payload, dt_evento = EXCLUDED.dt_evento, received_at = now();

                INSERT INTO stg.itensmovprodutos (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, dt_evento, payload
                )
                VALUES
                  (
                    1,
                    1,
                    1,
                    71001,
                    1,
                    TIMESTAMPTZ '2026-03-25 09:00:00+00',
                    '{"ID_PRODUTOS":1,"ID_FUNCIONARIOS":777,"QTDE":36,"VLRUNITARIO":5,"TOTAL":180,"CFOP":5102}'::jsonb
                  ),
                  (
                    1,
                    1,
                    1,
                    72001,
                    1,
                    TIMESTAMPTZ '2026-03-25 17:00:00+00',
                    '{"ID_PRODUTOS":1,"ID_FUNCIONARIOS":777,"QTDE":60,"VLRUNITARIO":5,"TOTAL":300,"CFOP":5102}'::jsonb
                  )
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
                DO UPDATE SET payload = EXCLUDED.payload, dt_evento = EXCLUDED.dt_evento, received_at = now();

                INSERT INTO stg.formas_pgto_comprovantes (
                  id_empresa, id_filial, id_referencia, tipo_forma, dt_evento, payload
                )
                VALUES
                  (1, 1, 971001, 28, TIMESTAMPTZ '2026-03-25 09:01:00+00', '{"ID_FILIAL":1,"TIPO_FORMA":28,"VALOR":180}'::jsonb),
                  (1, 1, 972001, 28, TIMESTAMPTZ '2026-03-25 17:01:00+00', '{"ID_FILIAL":1,"TIPO_FORMA":28,"VALOR":300}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_referencia, tipo_forma)
                DO UPDATE SET payload = EXCLUDED.payload, dt_evento = EXCLUDED.dt_evento, received_at = now();
                """,
            )

            legacy_cash_sql = _extract_sql_block(
                self.migrations_dir / "024_etl_clock_driven_rollover.sql",
                "DROP MATERIALIZED VIEW IF EXISTS mart.alerta_caixa_aberto CASCADE;",
                "CREATE OR REPLACE FUNCTION etl.daily_rollover_window(",
            )
            _run_sql_block(db_name, legacy_cash_sql)
            self.assertIsNone(
                _relation_column_type(db_name, "mart", "agg_caixa_turno_aberto", "is_operational_live"),
            )
            self.assertIsNone(
                _relation_column_type(db_name, "mart", "agg_caixa_forma_pagamento", "forma_category"),
            )

            rebuild = _run_python(
                ["-m", "app.cli.operational_truth", "rebuild", "--tenant-id", "1", "--ref-date", "2026-03-25"],
                env,
            )
            self.assertEqual(rebuild.returncode, 0, rebuild.stderr or rebuild.stdout)
            rebuild_body = json.loads(rebuild.stdout)
            self.assertTrue(rebuild_body["schema_repaired"], rebuild.stdout)

            validate = _run_python(
                [
                    "-m",
                    "app.cli.operational_truth",
                    "validate",
                    "--tenant-id",
                    "1",
                    "--dt-ini",
                    "2026-03-25",
                    "--dt-fim",
                    "2026-03-25",
                ],
                env,
            )
            self.assertEqual(validate.returncode, 0, validate.stderr or validate.stdout)
            validate_body = json.loads(validate.stdout)
            self.assertTrue(validate_body["ok"], validate.stdout)
            self.assertEqual(validate_body["diagnostic"]["schema"]["cash_open_mode"], "rich")
            self.assertEqual(validate_body["diagnostic"]["schema"]["missing_columns"], [])

            self.assertEqual(
                _relation_column_type(db_name, "mart", "agg_caixa_turno_aberto", "is_operational_live"),
                "boolean",
            )
            self.assertEqual(
                _relation_column_type(db_name, "mart", "agg_caixa_turno_aberto", "last_activity_ts"),
                "timestamp with time zone",
            )
            self.assertEqual(
                _relation_column_type(db_name, "mart", "agg_caixa_forma_pagamento", "forma_category"),
                "text",
            )

            self.assertEqual(
                _fetchscalar(db_name, "SELECT COUNT(*) FROM dw.fact_caixa_turno WHERE id_empresa = 1 AND is_aberto = true"),
                1,
            )
            self.assertEqual(
                _fetchscalar(db_name, "SELECT COUNT(*) FROM dw.fact_caixa_turno WHERE id_empresa = 1 AND is_aberto = false"),
                1,
            )
            self.assertEqual(
                _fetchscalar(
                    db_name,
                    """
                    SELECT nome
                    FROM dw.dim_usuario_caixa
                    WHERE id_empresa = 1
                      AND id_filial = 1
                      AND id_usuario = 910
                    """,
                ),
                "Operadora do Caixa",
            )
            self.assertEqual(
                _fetchscalar(
                    db_name,
                    """
                    SELECT id_usuario
                    FROM mart.fraude_cancelamentos_eventos
                    WHERE id_empresa = 1
                      AND id_filial = 1
                      AND id_comprovante = 72001
                    """,
                ),
                910,
            )

            purge = _run_python(
                ["-m", "app.cli.operational_truth", "purge", "--tenant-id", "1", "--scope", "cash-fraud"],
                env,
            )
            self.assertEqual(purge.returncode, 0, purge.stderr or purge.stdout)
            self.assertEqual(_fetchscalar(db_name, "SELECT COUNT(*) FROM dw.fact_caixa_turno WHERE id_empresa = 1"), 0)
            self.assertEqual(_fetchscalar(db_name, "SELECT COUNT(*) FROM dw.fact_comprovante WHERE id_empresa = 1"), 0)

            rebuild_again = _run_python(
                ["-m", "app.cli.operational_truth", "rebuild", "--tenant-id", "1", "--ref-date", "2026-03-25"],
                env,
            )
            self.assertEqual(rebuild_again.returncode, 0, rebuild_again.stderr or rebuild_again.stdout)

            validate_again = _run_python(
                [
                    "-m",
                    "app.cli.operational_truth",
                    "validate",
                    "--tenant-id",
                    "1",
                    "--dt-ini",
                    "2026-03-25",
                    "--dt-fim",
                    "2026-03-25",
                ],
                env,
            )
            self.assertEqual(validate_again.returncode, 0, validate_again.stderr or validate_again.stdout)

    def test_operational_truth_preflight_and_rebuild_refuse_active_tenant_lock(self) -> None:
        with temporary_database() as db_name:
            env = _subprocess_env(db_name)

            migrate = _run_python(["-m", "app.cli.migrate"], env)
            self.assertEqual(migrate.returncode, 0, migrate.stderr or migrate.stdout)

            with psycopg.connect(_admin_dsn(db_name)) as conn:
                conn.execute("SELECT pg_advisory_lock(%s, %s)", (TENANT_TRACK_LOCK_NAMESPACE, 1))

                preflight = _run_python(
                    ["-m", "app.cli.operational_truth", "preflight", "--tenant-id", "1"],
                    env,
                )
                self.assertNotEqual(preflight.returncode, 0, preflight.stdout)
                preflight_body = json.loads(preflight.stdout)
                self.assertFalse(preflight_body["ok"])
                self.assertEqual(preflight_body["error"], "etl_busy")
                self.assertIn("tenant_busy_operational", preflight_body["blocking_reasons"])

                rebuild = _run_python(
                    ["-m", "app.cli.operational_truth", "rebuild", "--tenant-id", "1", "--ref-date", "2026-03-25"],
                    env,
                )
                self.assertNotEqual(rebuild.returncode, 0, rebuild.stdout)
                rebuild_body = json.loads(rebuild.stdout)
                self.assertFalse(rebuild_body["ok"])
                self.assertEqual(rebuild_body["error"], "etl_busy")
                self.assertIn("operational-truth-preflight", rebuild_body["message"])
                self.assertIn("operational-truth-preflight", rebuild_body["operator_hint"])

                conn.execute("SELECT pg_advisory_unlock(%s, %s)", (TENANT_TRACK_LOCK_NAMESPACE, 1))
                conn.commit()

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
