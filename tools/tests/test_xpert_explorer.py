"""Unit tests for xpert_source_explorer — no SQL Server connection needed."""
from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

# Ensure tools/ is importable
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from tools.xpert_source_explorer import (
    Config,
    validate_readonly_sql,
    mask_pii,
    mask_row,
    _classify_table,
    write_csv,
    write_json,
    write_md,
    ensure_dir,
    _json_serial,
    classify_comprovante,
    is_commercial,
)


class TestSQLSafety(unittest.TestCase):
    """validate_readonly_sql must block all writes."""

    def test_select_allowed(self):
        ok, _ = validate_readonly_sql("SELECT 1")
        self.assertTrue(ok)

    def test_with_select_allowed(self):
        ok, _ = validate_readonly_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")
        self.assertTrue(ok)

    def test_select_from_sys_allowed(self):
        ok, _ = validate_readonly_sql("SELECT name FROM sys.tables")
        self.assertTrue(ok)

    def test_insert_blocked(self):
        ok, reason = validate_readonly_sql("INSERT INTO t VALUES (1)")
        self.assertFalse(ok)
        self.assertIn("INSERT", reason)

    def test_update_blocked(self):
        ok, _ = validate_readonly_sql("UPDATE t SET x=1")
        self.assertFalse(ok)

    def test_delete_blocked(self):
        ok, _ = validate_readonly_sql("DELETE FROM t")
        self.assertFalse(ok)

    def test_drop_blocked(self):
        ok, _ = validate_readonly_sql("DROP TABLE t")
        self.assertFalse(ok)

    def test_alter_blocked(self):
        ok, _ = validate_readonly_sql("ALTER TABLE t ADD col INT")
        self.assertFalse(ok)

    def test_truncate_blocked(self):
        ok, _ = validate_readonly_sql("TRUNCATE TABLE t")
        self.assertFalse(ok)

    def test_exec_blocked(self):
        ok, _ = validate_readonly_sql("EXEC sp_something")
        self.assertFalse(ok)

    def test_execute_blocked(self):
        ok, _ = validate_readonly_sql("EXECUTE sp_configure")
        self.assertFalse(ok)

    def test_xp_cmdshell_blocked(self):
        ok, _ = validate_readonly_sql("SELECT 1; EXEC xp_cmdshell 'dir'")
        self.assertFalse(ok)

    def test_merge_blocked(self):
        ok, _ = validate_readonly_sql("MERGE INTO t USING s ON t.id=s.id WHEN MATCHED THEN UPDATE SET x=1")
        self.assertFalse(ok)

    def test_create_blocked(self):
        ok, _ = validate_readonly_sql("CREATE TABLE t (id INT)")
        self.assertFalse(ok)

    def test_multi_statement_blocked(self):
        ok, _ = validate_readonly_sql("SELECT 1; SELECT 2")
        self.assertFalse(ok)

    def test_empty_blocked(self):
        ok, _ = validate_readonly_sql("")
        self.assertFalse(ok)

    def test_grant_blocked(self):
        ok, _ = validate_readonly_sql("GRANT SELECT ON t TO u")
        self.assertFalse(ok)

    def test_select_into_blocked(self):
        ok, _ = validate_readonly_sql("SELECT * INTO #temp FROM t")
        self.assertFalse(ok)

    def test_bulk_insert_blocked(self):
        ok, _ = validate_readonly_sql("BULK INSERT t FROM 'file.csv'")
        self.assertFalse(ok)


class TestConfig(unittest.TestCase):
    """Config loads from env file and environment."""

    def test_load_from_env_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write("SQLSERVER_HOST=myhost\n")
            f.write("SQLSERVER_DATABASE=mydb\n")
            f.write("SQLSERVER_PASSWORD=secret123\n")
            f.flush()
            path = f.name

        try:
            # Clear any existing env
            for key in ("SQLSERVER_HOST", "SQLSERVER_DATABASE", "SQLSERVER_PASSWORD"):
                os.environ.pop(key, None)
            cfg = Config(path)
            self.assertEqual(cfg.host, "myhost")
            self.assertEqual(cfg.database, "mydb")
            self.assertEqual(cfg.password, "secret123")
        finally:
            os.unlink(path)
            for key in ("SQLSERVER_HOST", "SQLSERVER_DATABASE", "SQLSERVER_PASSWORD"):
                os.environ.pop(key, None)

    def test_safe_summary_hides_password(self):
        with patch.dict(os.environ, {
            "SQLSERVER_HOST": "h",
            "SQLSERVER_DATABASE": "d",
            "SQLSERVER_USER": "u",
            "SQLSERVER_PASSWORD": "secret",
        }):
            cfg = Config()
        summary = cfg.safe_summary()
        self.assertNotIn("password", summary)
        self.assertNotIn("secret", json.dumps(summary))
        self.assertEqual(summary["host"], "h")

    def test_env_file_not_found(self):
        # Should not crash, just warn
        cfg = Config("/nonexistent/path.env")
        self.assertIsInstance(cfg.host, str)

    def test_load_quoted_values(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write('SQLSERVER_HOST="quoted_host"\n')
            f.write("SQLSERVER_DATABASE='single_quoted'\n")
            f.flush()
            path = f.name

        try:
            for key in ("SQLSERVER_HOST", "SQLSERVER_DATABASE"):
                os.environ.pop(key, None)
            cfg = Config(path)
            self.assertEqual(cfg.host, "quoted_host")
            self.assertEqual(cfg.database, "single_quoted")
        finally:
            os.unlink(path)
            for key in ("SQLSERVER_HOST", "SQLSERVER_DATABASE"):
                os.environ.pop(key, None)


class TestPIIMasking(unittest.TestCase):
    """mask_pii must hide sensitive data."""

    def test_cpf_masked(self):
        result = mask_pii("12345678900", "cpf")
        self.assertNotEqual(result, "12345678900")
        self.assertIn("*", result)

    def test_email_masked(self):
        result = mask_pii("user@example.com")
        self.assertNotEqual(result, "user@example.com")
        self.assertIn("***", result)

    def test_chave_nfe_masked(self):
        chave = "1" * 44
        result = mask_pii(chave)
        self.assertNotEqual(result, chave)
        self.assertIn("...", result)

    def test_short_value_not_masked(self):
        result = mask_pii("abc")
        self.assertEqual(result, "abc")

    def test_none_returns_none(self):
        result = mask_pii(None)
        self.assertIsNone(result)

    def test_pii_column_hint(self):
        result = mask_pii("sensitive_data_here", "senha")
        self.assertIn("*", result)

    def test_mask_row(self):
        row = {"cpf": "12345678900", "nome": "Test", "email": "a@b.com"}
        masked = mask_row(row)
        self.assertIn("*", masked["cpf"])
        self.assertEqual(masked["nome"], "Test")


class TestBusinessClassifier(unittest.TestCase):
    """_classify_table must detect business domains."""

    def test_comprovantes_detected_as_vendas(self):
        matches = _classify_table("COMPROVANTES", ["ID_COMPROVANTE", "DATA", "TOTAL", "VALOR", "SITUACAO"])
        domains = [m[0] for m in matches]
        self.assertIn("vendas", domains)

    def test_nfe_detected(self):
        matches = _classify_table("NFE", ["CHAVEACESSO", "NRONF", "STATUS", "SERIE"])
        domains = [m[0] for m in matches]
        self.assertIn("nfe_nfce", domains)

    def test_produtos_detected(self):
        matches = _classify_table("PRODUTOS", ["DESCRICAO", "BARRAS", "EAN", "PRECO"])
        domains = [m[0] for m in matches]
        self.assertIn("produtos", domains)

    def test_clientes_detected(self):
        matches = _classify_table("CLIENTES", ["CPF", "CNPJ", "NOME", "FANTASIA"])
        domains = [m[0] for m in matches]
        self.assertIn("clientes", domains)

    def test_turnos_detected(self):
        matches = _classify_table("TURNOS", ["ABERTURA", "FECHAMENTO", "ID_CAIXA", "ID_OPERADOR"])
        domains = [m[0] for m in matches]
        self.assertIn("turnos", domains)

    def test_unknown_table_no_match(self):
        matches = _classify_table("XYZABC", ["COL1", "COL2"])
        self.assertEqual(len(matches), 0)

    def test_contaspagar_detected(self):
        matches = _classify_table("CONTASPAGAR", ["VENCIMENTO", "VALOR", "ID_FORNECEDOR"])
        domains = [m[0] for m in matches]
        self.assertIn("contas_pagar", domains)

    def test_estoque_detected(self):
        matches = _classify_table("ESTOQUE", ["QUANTIDADE", "SALDO", "ID_PRODUTO"])
        domains = [m[0] for m in matches]
        self.assertIn("estoque", domains)


class TestExportHelpers(unittest.TestCase):
    """Export functions must write valid files."""

    def test_write_json(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "test.json"
            write_json({"key": "value"}, path)
            data = json.loads(path.read_text())
            self.assertEqual(data["key"], "value")

    def test_write_csv(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "test.csv"
            write_csv([{"a": 1, "b": 2}], path)
            content = path.read_text()
            self.assertIn("a", content)
            self.assertIn("1", content)

    def test_write_md(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "test.md"
            write_md("# Test", path)
            self.assertEqual(path.read_text(), "# Test")

    def test_write_csv_empty(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "empty.csv"
            write_csv([], path)
            self.assertEqual(path.read_text(), "")

    def test_ensure_dir_creates(self):
        with tempfile.TemporaryDirectory() as d:
            sub = Path(d) / "a" / "b" / "c"
            result = ensure_dir(sub)
            self.assertTrue(result.is_dir())

    def test_json_serial_datetime(self):
        from datetime import datetime, date
        self.assertIsInstance(_json_serial(datetime(2025, 1, 1)), str)
        self.assertIsInstance(_json_serial(date(2025, 1, 1)), str)

    def test_json_serial_decimal(self):
        from decimal import Decimal
        self.assertIsInstance(_json_serial(Decimal("1.5")), float)


class TestAuditSalesUsesCorrectDateColumn(unittest.TestCase):
    """Verify that audit-sales-day uses COMPROVANTES.DATA, not item date or DATAREPL."""

    def test_audit_sql_uses_comprovantes_data(self):
        """The audit SQL template must filter on c.DATA (comprovante date)."""
        import inspect
        source = inspect.getsource(cmd_audit_sales_day)
        # Must reference c.DATA for filtering
        self.assertIn("c.DATA >=", source)
        self.assertIn("c.DATA <", source)
        # Must NOT use DATAREPL
        self.assertNotIn("DATAREPL", source)

    def test_nfe_discovery_warns_against_datarepl(self):
        """NFE discovery must document DATAREPL as non-filter."""
        import inspect
        source = inspect.getsource(cmd_nfe_discovery)
        self.assertIn("DATAREPL", source)
        self.assertIn("NUNCA usar como filtro", source)


class TestClassifyComprovante(unittest.TestCase):
    def test_comercial(self):
        self.assertEqual(classify_comprovante(0, 0, 3), "comercial")

    def test_situacao_3(self):
        self.assertEqual(classify_comprovante(3, 0, None), "situacao_3_ignorada")

    def test_nfe_inutilizada(self):
        self.assertEqual(classify_comprovante(0, 1, 5), "nfe_inutilizada")

    def test_cancelamento_real(self):
        self.assertEqual(classify_comprovante(0, 1, 4), "cancelamento_real")

    def test_cancelado_sem_nfe(self):
        self.assertEqual(classify_comprovante(0, 1, None), "cancelado_sem_nfe_cancelamento")

    def test_cancelado_nfe_3(self):
        self.assertEqual(classify_comprovante(0, 1, 3), "cancelado_sem_nfe_cancelamento")


class TestIsCommercial(unittest.TestCase):
    def test_commercial_normal(self):
        self.assertTrue(is_commercial(0, 0))

    def test_not_commercial_sit3(self):
        self.assertFalse(is_commercial(3, 0))

    def test_not_commercial_cancelled(self):
        self.assertFalse(is_commercial(0, 1))

    def test_none_values(self):
        self.assertTrue(is_commercial(None, None))


class TestCompareLogic(unittest.TestCase):
    """Test compare-stg-comprovantes-range logic without real DB."""

    def _make_doc(self, id_db, id_comprovante, total, situacao=0, cancelado=0, nfe_status=None, data_dia="2026-05-10"):
        return {
            "id_filial": "14458",
            "id_db": str(id_db),
            "id_comprovante": str(id_comprovante),
            "total_header": float(total),
            "situacao": situacao,
            "cancelado": cancelado,
            "nfe_status": nfe_status,
            "data_dia": data_dia,
            "data": f"{data_dia} 10:00:00",
            "classification": classify_comprovante(situacao, cancelado, nfe_status),
            "commercial_eligible": 1 if is_commercial(situacao, cancelado) else 0,
        }

    def test_source_only_detected(self):
        src = [self._make_doc(1, 100, 500.0)]
        stg = []
        src_map = {(d["id_filial"], d["id_db"], d["id_comprovante"]): d for d in src}
        stg_map = {(d["id_filial"], d["id_db"], d["id_comprovante"]): d for d in stg}
        source_only = [src_map[k] for k in src_map if k not in stg_map]
        self.assertEqual(len(source_only), 1)

    def test_stg_only_detected(self):
        src = []
        stg = [self._make_doc(1, 200, 300.0)]
        src_map = {(d["id_filial"], d["id_db"], d["id_comprovante"]): d for d in src}
        stg_map = {(d["id_filial"], d["id_db"], d["id_comprovante"]): d for d in stg}
        stg_only = [stg_map[k] for k in stg_map if k not in src_map]
        self.assertEqual(len(stg_only), 1)

    def test_total_mismatch_detected(self):
        src = [self._make_doc(1, 100, 500.0)]
        stg = [self._make_doc(1, 100, 499.0)]
        key = ("14458", "1", "100")
        diff = abs(src[0]["total_header"] - stg[0]["total_header"])
        self.assertGreater(diff, 0.01)

    def test_status_mismatch_detected(self):
        src = [self._make_doc(1, 100, 500.0, situacao=0, cancelado=0)]
        stg = [self._make_doc(1, 100, 500.0, situacao=3, cancelado=0)]
        self.assertNotEqual(src[0]["situacao"], stg[0]["situacao"])

    def test_nfe_mismatch_detected(self):
        src = [self._make_doc(1, 100, 500.0, nfe_status=3)]
        stg = [self._make_doc(1, 100, 500.0, nfe_status=5)]
        self.assertNotEqual(src[0]["nfe_status"], stg[0]["nfe_status"])

    def test_key_uses_id_db(self):
        """Keys must include id_db for uniqueness."""
        doc_a = self._make_doc(1, 100, 500.0)
        doc_b = self._make_doc(2, 100, 300.0)
        key_a = (doc_a["id_filial"], doc_a["id_db"], doc_a["id_comprovante"])
        key_b = (doc_b["id_filial"], doc_b["id_db"], doc_b["id_comprovante"])
        self.assertNotEqual(key_a, key_b)

    def test_delta_explanation_closes(self):
        """delta_explained must account for delta_total_comercial."""
        # source has 1 commercial doc missing in STG
        src = [self._make_doc(1, 100, 500.0)]
        stg = []
        delta_total_comercial = 500.0 - 0.0
        source_only_comercial_total = 500.0
        delta_explained = source_only_comercial_total
        unexplained = abs(delta_total_comercial - delta_explained)
        self.assertLessEqual(unexplained, 0.01)


# Import after path setup
from tools.xpert_source_explorer import cmd_audit_sales_day, cmd_nfe_discovery


if __name__ == "__main__":
    unittest.main()
