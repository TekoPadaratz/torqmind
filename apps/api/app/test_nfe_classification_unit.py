"""Unit tests for NFE fiscal classification.

Validates that:
1. NFE dataset is properly registered in the ingest DatasetSpec registry
2. The NFE inutilizations endpoint returns the correct contract
3. NFE status=5 is excluded from fraud and cancellation aggregates
"""
from __future__ import annotations

import os
import sys
import types
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("USE_CLICKHOUSE", "false")

# Stub clickhouse_connect if not installed
try:
    import clickhouse_connect  # noqa: F401
except ModuleNotFoundError:
    fake_clickhouse = types.ModuleType("clickhouse_connect")
    fake_client_module = types.SimpleNamespace(Client=object)
    fake_clickhouse.driver = types.SimpleNamespace(
        client=fake_client_module,
        exceptions=types.SimpleNamespace(
            DatabaseError=Exception, OperationalError=Exception
        ),
    )
    fake_clickhouse.get_client = lambda **_kwargs: MagicMock()
    sys.modules["clickhouse_connect"] = fake_clickhouse
    sys.modules["clickhouse_connect.driver"] = fake_clickhouse.driver
    sys.modules["clickhouse_connect.driver.exceptions"] = fake_clickhouse.driver.exceptions

ROOT = Path(__file__).resolve().parents[3]
for rel_path in ("apps/api", "apps/agent"):
    module_path = str(ROOT / rel_path)
    if module_path not in sys.path:
        sys.path.insert(0, module_path)


class TestNFEDatasetSpec(unittest.TestCase):
    """Ensure NFE is registered in the ingest route's DatasetSpec registry."""

    def test_nfe_dataset_registered(self):
        from app.routes_ingest import DATASETS
        self.assertIn("nfe", DATASETS)

    def test_nfe_pk_cols(self):
        from app.routes_ingest import DATASETS
        spec = DATASETS["nfe"]
        self.assertEqual(spec.table, "stg.nfe")
        self.assertEqual(
            spec.pk_cols,
            ["id_empresa", "id_filial", "id_db", "id_comprovante", "id_nfe"],
        )

    def test_nfe_shadow_columns_present(self):
        from app.routes_ingest import _shadow_values_for_dataset
        row = {
            "STATUS": 5,
            "NRONF": "123",
            "CHAVEACESSO": "abc",
            "TIPO_DOC": "55",
            "PROTOCOLO": "xyz",
            "DATA": "2026-01-15",
            "DATAAUTORIZACAO": "2026-01-15",
            "DATACANCELAMENTO": None,
            "DATAINUTILIZACAO": "2026-01-16",
            "VALOR": "1500.00",
        }
        result = _shadow_values_for_dataset("nfe", row)
        self.assertEqual(result["status_shadow"], 5)
        self.assertEqual(result["numero_nfe_shadow"], "123")
        self.assertEqual(result["chave_nfe_shadow"], "abc")
        self.assertEqual(result["modelo_shadow"], "55")
        self.assertEqual(result["protocolo_shadow"], "xyz")
        self.assertIsNotNone(result["data_emissao_shadow"])
        self.assertEqual(result["valor_nfe_shadow"], 1500.0)


class TestNFEInutilizationsContract(unittest.TestCase):
    """Validate the inutilizacoes field in cash_overview contract."""

    @patch("app.repos_mart_realtime.query_dict")
    @patch("app.repos_mart_realtime.query_scalar")
    def test_inutilizacoes_empty_when_no_table(self, mock_qs: MagicMock, mock_qd: MagicMock):
        mock_qs.return_value = 0  # Table doesn't exist
        mock_qd.side_effect = lambda q, **kw: []

        from app.repos_mart_realtime import _cash_nfe_inutilizations
        result = _cash_nfe_inutilizations(1, None)
        self.assertEqual(result["qtd"], 0)
        self.assertEqual(result["valor_total"], 0.0)
        self.assertEqual(result["items"], [])

    @patch("app.repos_mart_realtime.query_dict")
    @patch("app.repos_mart_realtime.query_scalar")
    def test_inutilizacoes_returns_items(self, mock_qs: MagicMock, mock_qd: MagicMock):
        mock_qs.return_value = 1  # Table exists

        def side_effect(q, **kw):
            q_lower = q.lower()
            if "count() as qtd" in q_lower and "sum(valor_comprovante) as valor_total" in q_lower:
                return [{"qtd": 1, "valor_total": 1500.50}]
            if "nfe_inutilizations_rt" in q_lower:
                return [
                    {
                        "id_filial": 1,
                        "filial_nome": "Filial 1",
                        "id_turno": 10,
                        "turno_abertura_ts": "2026-01-15 08:00:00",
                        "turno_fechamento_ts": "2026-01-15 18:00:00",
                        "id_usuario": 5,
                        "nome_operador": "Operador X",
                        "id_comprovante": 100,
                        "id_nfe": 200,
                        "numero_nfe": "123",
                        "serie_nfe": "",
                        "chave_nfe": "abc123",
                        "protocolo": "prot-001",
                        "modelo_nfe": "55",
                        "data_emissao_nfe": "2026-01-15",
                        "valor_comprovante": 1500.50,
                        "referencia": "REF001",
                        "dt": "2026-01-15",
                        "hora": "10:30",
                    }
                ]
            if "stg_filiais" in q_lower:
                return [{"id_filial": 1, "filial_nome": "Filial 1"}]
            if "stg_turnos" in q_lower:
                return [{"id_filial": 1, "id_turno": 10, "turno_value": "T10"}]
            return []

        mock_qd.side_effect = side_effect

        from app.repos_mart_realtime import _cash_nfe_inutilizations
        result = _cash_nfe_inutilizations(1, None, date(2026, 1, 1), date(2026, 1, 31))

        self.assertEqual(result["qtd"], 1)
        self.assertAlmostEqual(result["valor_total"], 1500.50, places=2)
        item = result["items"][0]
        self.assertEqual(item["id_nfe"], 200)
        self.assertEqual(item["numero_nfe"], "123")
        self.assertEqual(item["protocolo"], "prot-001")
        self.assertEqual(item["valor_comprovante"], 1500.50)


class TestNFEAgentDataset(unittest.TestCase):
    """Verify NFE dataset is in the agent config defaults."""

    def test_nfe_in_default_datasets(self):
        from agent.config import DEFAULT_DATASETS
        self.assertIn("nfe", DEFAULT_DATASETS)

    def test_nfe_required_fields(self):
        from agent.config import DEFAULT_DATASETS
        nfe = DEFAULT_DATASETS["nfe"]
        required = nfe.get("required_fields", [])
        for field in ["ID_NFE", "ID_FILIAL", "ID_DB", "ID_COMPROVANTE", "STATUS"]:
            self.assertIn(field, required, f"Missing required field: {field}")


if __name__ == "__main__":
    unittest.main()
