import tempfile
import unittest
from datetime import date
import os
from pathlib import Path
from unittest.mock import patch

from app.cross_db_audit import (
    _classify_hypotheses,
    _ensure_application_intent_readonly,
    _pg_default_dsn,
    _sales_doc_diff_query_source,
    _sales_source_query,
    _sales_stg_query,
    _source_doc_lookup_query,
    _window_days,
    ensure_read_only_query,
    parse_branch_ids,
    resolve_audit_config,
)


class CrossDbAuditTests(unittest.TestCase):
    def _make_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "config.local.yaml"
            cfg_path.write_text(
                """
id_empresa: 1
sqlserver:
  server: host-x
  database: db-x
  user: user-x
  password: secret-x
datasets:
  comprovantes: {table: dbo.CMP}
  movprodutos: {table: dbo.MOV}
  itensmovprodutos: {table: dbo.ITM}
  formas_pgto_comprovantes: {table: dbo.PAG}
  turnos: {table: dbo.TUR}
  entidades: {table: dbo.ENT}
  contaspagar: {table: dbo.CP}
  contasreceber: {table: dbo.CR}
                """.strip(),
                encoding="utf-8",
            )
            return resolve_audit_config(
                tenant_id=1,
                branch_ids=[14122],
                date_start=date(2026, 3, 1),
                date_end=date(2026, 3, 31),
                sample_days=3,
                output_dir=Path(tempfile.gettempdir()) / "audit-out",
                pg_dsn="postgresql://user:pass@localhost/db",
                sqlserver_dsn="DRIVER=X;SERVER=Y",
                agent_config_path=cfg_path,
            )

    def test_parse_branch_ids_keeps_order_unique_sorted(self) -> None:
        self.assertEqual(parse_branch_ids(" 18096,14122,18096,16305 "), [14122, 16305, 18096])

    def test_read_only_guard_accepts_select_and_rejects_write(self) -> None:
        self.assertTrue(ensure_read_only_query("  -- comment\nSELECT 1").startswith("SELECT"))
        with self.assertRaisesRegex(Exception, "read-only"):
            ensure_read_only_query("DELETE FROM dw.fact_venda")

    def test_application_intent_readonly_is_appended_once(self) -> None:
        self.assertIn(
            "ApplicationIntent=ReadOnly",
            _ensure_application_intent_readonly("DRIVER=X;SERVER=Y"),
        )
        self.assertEqual(
            "DRIVER=X;ApplicationIntent=ReadOnly",
            _ensure_application_intent_readonly("DRIVER=X;ApplicationIntent=ReadOnly"),
        )

    def test_window_days_is_deterministic(self) -> None:
        first = _window_days(date(2026, 3, 1), date(2026, 3, 31), 5)
        second = _window_days(date(2026, 3, 1), date(2026, 3, 31), 5)
        self.assertEqual(first, second)
        self.assertEqual(5, len(first))

    def test_pg_default_dsn_normalizes_asyncpg_url(self) -> None:
        with patch("app.cross_db_audit.settings.database_url", "postgresql+asyncpg://audit:secret@pg-host:55432/TORQMIND"):
            with patch("app.cross_db_audit.settings.pg_user", "postgres"):
                with patch("app.cross_db_audit.settings.pg_password", "postgres"):
                    with patch("app.cross_db_audit.settings.pg_host", "localhost"):
                        with patch("app.cross_db_audit.settings.pg_port", 5432):
                            with patch("app.cross_db_audit.settings.pg_database", "postgres"):
                                dsn = _pg_default_dsn()
        self.assertEqual(
            "host=pg-host port=55432 dbname=TORQMIND user=audit password=secret",
            dsn,
        )

    def test_resolve_config_uses_agent_yaml_fallbacks(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cfg_path = Path(tmp) / "config.local.yaml"
            cfg_path.write_text(
                """
id_empresa: 99
sqlserver:
  driver: FreeTDS
  server: host-x
  database: db-x
  user: user-x
  password: secret-x
  encrypt: false
  trust_server_certificate: true
datasets:
  comprovantes: {table: dbo.CMP}
  movprodutos: {table: dbo.MOV}
  itensmovprodutos: {table: dbo.ITM}
  formas_pgto_comprovantes: {table: dbo.PAG}
  turnos: {table: dbo.TUR}
  entidades: {table: dbo.ENT}
  contaspagar: {table: dbo.CP}
  contasreceber: {table: dbo.CR}
                """.strip(),
                encoding="utf-8",
            )
            config = resolve_audit_config(
                tenant_id=7,
                branch_ids=[10, 20],
                date_start=date(2026, 3, 1),
                date_end=date(2026, 3, 31),
                sample_days=4,
                output_dir=Path(tmp) / "out",
                pg_dsn="postgresql://user:pass@localhost/db",
                agent_config_path=cfg_path,
            )

        self.assertEqual(7, config.tenant_id)
        self.assertEqual([10, 20], config.branch_ids)
        self.assertEqual("dbo.MOV", config.sqlserver_tables["movprodutos"])
        self.assertIn("DRIVER={FreeTDS}", config.sqlserver_dsn)
        self.assertIn("ApplicationIntent=ReadOnly", config.sqlserver_dsn)
        self.assertEqual(Path(tmp) / "out", config.output_dir)

    def test_resolve_config_accepts_cwd_relative_agent_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            cfg_dir = tmp_path / "cfg"
            cfg_dir.mkdir()
            cfg_path = cfg_dir / "config.local.yaml"
            cfg_path.write_text(
                """
id_empresa: 1
sqlserver:
  server: host-x
  database: db-x
  user: user-x
  password: secret-x
datasets:
  comprovantes: {table: dbo.CMP}
  movprodutos: {table: dbo.MOV}
  itensmovprodutos: {table: dbo.ITM}
                """.strip(),
                encoding="utf-8",
            )
            previous_cwd = Path.cwd()
            try:
                os.chdir(tmp_path)
                config = resolve_audit_config(
                    tenant_id=1,
                    branch_ids=[14122],
                    date_start=date(2026, 3, 1),
                    date_end=date(2026, 3, 31),
                    sample_days=3,
                    output_dir=tmp_path / "out",
                    pg_dsn="postgresql://user:pass@localhost/db",
                    agent_config_path=Path("cfg/config.local.yaml"),
                )
            finally:
                os.chdir(previous_cwd)

        self.assertIn("host-x", config.sqlserver_dsn)

    def test_resolve_config_accepts_explicit_dsns_without_agent_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            config = resolve_audit_config(
                tenant_id=1,
                branch_ids=[14122],
                date_start=date(2026, 3, 1),
                date_end=date(2026, 3, 31),
                sample_days=3,
                output_dir=tmp_path / "out",
                pg_dsn="host=pg port=5432 dbname=TORQMIND user=postgres password=postgres",
                sqlserver_dsn="Driver={ODBC Driver 18 for SQL Server};Server=host-x,1433;Database=db-x;Uid=user-x;Pwd=secret-x",
                agent_config_path=tmp_path / "missing-config.local.yaml",
            )

        self.assertEqual("dbo.MOVPRODUTOS", config.sqlserver_tables["movprodutos"])
        self.assertIn("ApplicationIntent=ReadOnly", config.sqlserver_dsn)
        self.assertIsNone(config.agent_config_path)

    def test_classify_hypotheses_marks_cancel_and_missing_stg(self) -> None:
        fake_config = self._make_config()
        docs = [
            {
                "doc_ref": 3467398,
                "cause_tags": "stale_cancelado_dw",
                "delta_source_stg": 0,
                "delta_stg_dw": 181.56,
                "source_total": 169.56,
                "source_total_cabecalho": 169.56,
                "source_total_comprovante": 169.56,
            },
            {
                "doc_ref": 3471036,
                "cause_tags": "missing_in_stg,sentinel_datarepl",
                "delta_source_stg": -10.00,
                "delta_stg_dw": 0,
                "source_total": 10.00,
                "source_total_cabecalho": 10.00,
                "source_total_comprovante": 10.00,
            },
        ]
        leak_checks = [{"check_name": "tenant_sum_matches_branch_sum_dw_sales", "status": "pass"}]
        turnos_rows = [{"branch_id": 14122, "dw_fechamento_tardio": 0}]

        hypotheses = _classify_hypotheses(fake_config, docs, leak_checks, turnos_rows)
        by_id = {item.hypothesis_id: item for item in hypotheses}

        self.assertEqual("confirmada", by_id[3].status)
        self.assertEqual("confirmada", by_id[5].status)
        self.assertEqual("confirmada", by_id[6].status)
        self.assertEqual("descartada", by_id[1].status)
        self.assertEqual("descartada", by_id[11].status)

    def test_sales_queries_use_movprodutos_status_not_comprovante_cancelado(self) -> None:
        config = self._make_config()

        sales_source_sql, _ = _sales_source_query(config)
        self.assertIn("m.SITUACAO", sales_source_sql)
        self.assertNotIn("ISNULL(c.CANCELADO, 0) AS cancelado", sales_source_sql)

        sales_stg_sql, _ = _sales_stg_query(config)
        self.assertIn("etl.movimento_venda_situacao", sales_stg_sql)
        self.assertNotIn("cancelado_shadow", sales_stg_sql)

        doc_diff_sql, _ = _sales_doc_diff_query_source(config, date(2026, 3, 24), 14122)
        self.assertIn("situacao_movimento", doc_diff_sql)
        self.assertIn("CASE WHEN COALESCE", doc_diff_sql)

        doc_lookup_sql, _ = _source_doc_lookup_query(config, 14122, 3467398)
        self.assertIn("situacao_movimento", doc_lookup_sql)
        self.assertIn("comprovante_cancelado", doc_lookup_sql)


if __name__ == "__main__":
    unittest.main()
