from __future__ import annotations

import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import patch

from app import repos_mart


class _FetchOneCursor:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _FetchAllCursor:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _CashHistoricalConnStub:
    def __init__(self, *, summary_row, by_day_rows, payment_mix_rows, top_turnos_rows):
        self.summary_row = summary_row
        self.by_day_rows = by_day_rows
        self.payment_mix_rows = payment_mix_rows
        self.top_turnos_rows = top_turnos_rows
        self.calls: list[tuple[str, list | tuple]] = []

    def execute(self, sql, params):
        params_list = list(params) if isinstance(params, (list, tuple)) else [params]
        self.calls.append((sql, params_list))

        if "FROM vendas v" in sql and "CROSS JOIN pagamentos p" in sql:
            return _FetchOneCursor(self.summary_row)
        if "FULL OUTER JOIN pagamentos p" in sql:
            return _FetchAllCursor(self.by_day_rows)
        if "FROM dw.fact_pagamento_comprovante p" in sql and "COALESCE(m.label, 'NÃO IDENTIFICADO')" in sql:
            return _FetchAllCursor(self.payment_mix_rows)
        if "ORDER BY c.total_vendas DESC" in sql:
            return _FetchAllCursor(self.top_turnos_rows)
        raise AssertionError(f"Unexpected SQL in test stub: {sql[:220]}")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):  # noqa: ARG002
        return False


class CashPaymentMixUnitTest(unittest.TestCase):
    def test_cash_historical_payment_mix_uses_dw_payment_facts_for_single_branch_period(self) -> None:
        conn = _CashHistoricalConnStub(
            summary_row={
                "caixas_periodo": 2,
                "dias_com_movimento": 3,
                "total_vendas": Decimal("550.00"),
                "qtd_vendas": 4,
                "total_cancelamentos": Decimal("40.00"),
                "qtd_cancelamentos": 1,
                "caixas_com_cancelamento": 1,
                "total_devolucoes": Decimal("15.00"),
                "qtd_devolucoes": 1,
                "caixas_com_devolucao": 1,
                "min_data_key": 20260401,
                "max_data_key": 20260407,
                "total_pagamentos": Decimal("510.00"),
            },
            by_day_rows=[
                {
                    "data_key": 20260401,
                    "caixas": 1,
                    "total_vendas": Decimal("300.00"),
                    "total_cancelamentos": Decimal("0.00"),
                    "qtd_cancelamentos": 0,
                    "total_devolucoes": Decimal("15.00"),
                    "qtd_devolucoes": 1,
                    "total_pagamentos": Decimal("300.00"),
                }
            ],
            payment_mix_rows=[
                {
                    "label": "PIX",
                    "category": "PIX",
                    "total_valor": Decimal("300.00"),
                    "qtd_comprovantes": 2,
                    "qtd_turnos": 1,
                },
                {
                    "label": "Depósito Bancário",
                    "category": "DEPOSITO",
                    "total_valor": Decimal("210.00"),
                    "qtd_comprovantes": 2,
                    "qtd_turnos": 1,
                },
            ],
            top_turnos_rows=[
                {
                    "id_filial": 101,
                    "filial_nome": "Filial 101",
                    "id_turno": 41,
                    "turno_value": "1",
                    "id_usuario": 910,
                    "usuario_nome": "Operador 910",
                    "abertura_ts": None,
                    "fechamento_ts": None,
                    "is_aberto": False,
                    "first_event_at": None,
                    "last_event_at": None,
                    "total_vendas": Decimal("550.00"),
                    "qtd_vendas": 4,
                    "total_cancelamentos": Decimal("40.00"),
                    "qtd_cancelamentos": 1,
                    "total_devolucoes": Decimal("15.00"),
                    "qtd_devolucoes": 1,
                    "total_pagamentos": Decimal("510.00"),
                }
            ],
        )

        with patch("app.repos_mart.get_conn", return_value=conn):
            payload = repos_mart._cash_historical_overview(
                "OWNER",
                7,
                101,
                date(2026, 4, 1),
                date(2026, 4, 7),
            )

        self.assertEqual(payload["source_status"], "ok")
        self.assertEqual(len(payload["payment_mix"]), 2)
        self.assertEqual(payload["payment_mix"][0]["label"], "PIX")
        self.assertEqual(payload["payment_mix"][1]["label"], "Depósito Bancário")
        self.assertEqual(float(payload["kpis"]["total_devolucoes"]), 15.0)
        self.assertEqual(float(payload["kpis"]["caixa_liquido"]), 495.0)
        self.assertEqual(float(payload["by_day"][0]["caixa_liquido"]), 285.0)
        self.assertEqual(float(payload["top_turnos"][0]["total_devolucoes"]), 15.0)
        self.assertEqual(float(payload["top_turnos"][0]["caixa_liquido"]), 495.0)
        summary_call = next(
            (item for item in conn.calls if "FROM vendas v" in item[0] and "CROSS JOIN pagamentos p" in item[0]),
            None,
        )
        self.assertIsNotNone(summary_call)
        self.assertIn("dw.fact_venda v", summary_call[0])
        self.assertIn("dw.fact_venda_item i", summary_call[0])
        self.assertIn("COALESCE(v.situacao, 0) IN (1, 2, 3)", summary_call[0])
        self.assertIn("COALESCE(i.cfop, 0) > 5000", summary_call[0])
        self.assertNotIn("COALESCE(fc.cancelado, false)", summary_call[0])
        payment_mix_call = next(
            (item for item in conn.calls if "FROM dw.fact_pagamento_comprovante p" in item[0] and "COALESCE(m.label, 'NÃO IDENTIFICADO')" in item[0]),
            None,
        )
        self.assertIsNotNone(payment_mix_call)
        self.assertNotIn("mart.agg_pagamentos_turno", payment_mix_call[0])
        self.assertNotIn("is_operational_live", payment_mix_call[0])
        self.assertEqual(payment_mix_call[1], [7, 20260401, 20260407, 101])

    def test_cash_historical_payment_mix_supports_multi_branch_scope_for_all_active_branches(self) -> None:
        conn = _CashHistoricalConnStub(
            summary_row={
                "caixas_periodo": 3,
                "dias_com_movimento": 1,
                "total_vendas": Decimal("400.00"),
                "qtd_vendas": 3,
                "total_cancelamentos": Decimal("0.00"),
                "qtd_cancelamentos": 0,
                "caixas_com_cancelamento": 0,
                "total_devolucoes": Decimal("0.00"),
                "qtd_devolucoes": 0,
                "caixas_com_devolucao": 0,
                "min_data_key": 20260408,
                "max_data_key": 20260408,
                "total_pagamentos": Decimal("400.00"),
            },
            by_day_rows=[],
            payment_mix_rows=[
                {
                    "label": "Cartão de Crédito",
                    "category": "CARTAO_CREDITO",
                    "total_valor": Decimal("400.00"),
                    "qtd_comprovantes": 3,
                    "qtd_turnos": 2,
                }
            ],
            top_turnos_rows=[],
        )

        with patch("app.repos_mart.get_conn", return_value=conn):
            payload = repos_mart._cash_historical_overview(
                "OWNER",
                7,
                [101, 103],
                date(2026, 4, 8),
                date(2026, 4, 8),
            )

        self.assertEqual(payload["source_status"], "ok")
        self.assertEqual(payload["payment_mix"][0]["qtd_turnos"], 2)
        payment_mix_call = next(
            item for item in conn.calls if "FROM dw.fact_pagamento_comprovante p" in item[0] and "COALESCE(m.label, 'NÃO IDENTIFICADO')" in item[0]
        )
        self.assertIn("p.id_filial = ANY(%s)", payment_mix_call[0])
        self.assertEqual(payment_mix_call[1], [7, 20260408, 20260408, [101, 103]])

    def test_cash_historical_payment_mix_returns_empty_when_period_has_no_payments(self) -> None:
        conn = _CashHistoricalConnStub(
            summary_row={
                "caixas_periodo": 0,
                "dias_com_movimento": 0,
                "total_vendas": Decimal("0.00"),
                "qtd_vendas": 0,
                "total_cancelamentos": Decimal("0.00"),
                "qtd_cancelamentos": 0,
                "caixas_com_cancelamento": 0,
                "total_devolucoes": Decimal("0.00"),
                "qtd_devolucoes": 0,
                "caixas_com_devolucao": 0,
                "min_data_key": None,
                "max_data_key": None,
                "total_pagamentos": Decimal("0.00"),
            },
            by_day_rows=[],
            payment_mix_rows=[],
            top_turnos_rows=[],
        )

        with patch("app.repos_mart.get_conn", return_value=conn):
            payload = repos_mart._cash_historical_overview(
                "OWNER",
                7,
                101,
                date(2026, 4, 8),
                date(2026, 4, 8),
            )

        self.assertEqual(payload["source_status"], "unavailable")
        self.assertEqual(payload["payment_mix"], [])
        self.assertEqual(float(payload["kpis"]["total_pagamentos"]), 0.0)


if __name__ == "__main__":
    unittest.main()
