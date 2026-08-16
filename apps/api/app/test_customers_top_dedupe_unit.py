"""Top clientes: dedupe do espelho Central (id_db matriz vs posto)."""
from __future__ import annotations

import unittest
from pathlib import Path


class TestCustomersTopCentralDedupe(unittest.TestCase):
    def test_sql_prefers_local_posto_db(self):
        src = Path(__file__).resolve().parent / "repos_mart_realtime.py"
        text = src.read_text(encoding="utf-8")
        idx = text.find("def customers_top(")
        end = text.find("\ndef customers_rfm_snapshot(", idx)
        chunk = text[idx:end]
        self.assertIn("s.id_db = s.id_filial", chunk)
        self.assertIn("s.id_cliente AS id_cliente", chunk)
        self.assertIn("espelho da Central", chunk)
        self.assertIn("s.cancelado = 0", chunk)
        self.assertIn("s.situacao != 3", chunk)
        self.assertIn("s.commercial_eligible = 1", chunk)
        # Não usar dedupe por valor (colapsa vendas reais no mesmo dia).
        self.assertNotIn("row_number() OVER (", chunk)


class TestCfopShadowPoisonGuard(unittest.TestCase):
    def test_mart_builder_prefers_payload_when_shadow_lt_100(self):
        src = (
            Path(__file__).resolve().parents[2]
            / "cdc_consumer"
            / "torqmind_cdc_consumer"
            / "mart_builder.py"
        )
        text = src.read_text(encoding="utf-8")
        self.assertIn("ifNull(i.cfop_shadow, 0) >= 100", text)
        self.assertIn("replaceAll(JSONExtractString(i.payload, 'CFOP'), '.', '')", text)


if __name__ == "__main__":
    unittest.main()
