from __future__ import annotations

import json
import unittest
from datetime import date, timedelta

from fastapi.testclient import TestClient

from app.db import get_conn
from app.main import app


class SalesRetentionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def _current_date(self) -> date:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute("SELECT CURRENT_DATE AS today").fetchone()
        return row["today"]

    def _create_tenant(self, name: str, sales_history_days: int = 365, default_product_scope_days: int = 30) -> tuple[int, str]:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                """
                INSERT INTO app.tenants (
                  nome,
                  is_active,
                  status,
                  billing_status,
                  valid_from,
                  sales_history_days,
                  default_product_scope_days
                )
                VALUES (%s, true, 'active', 'current', CURRENT_DATE, %s, %s)
                RETURNING id_empresa, ingest_key::text
                """,
                (name, sales_history_days, default_product_scope_days),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO auth.filiais (id_empresa, id_filial, nome, is_active, valid_from)
                VALUES (%s, 1, %s, true, CURRENT_DATE)
                ON CONFLICT (id_empresa, id_filial)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (row["id_empresa"], f"Filial {name}"),
            )
            conn.commit()
        return int(row["id_empresa"]), str(row["ingest_key"])

    def _post_ndjson(self, dataset: str, ingest_key: str, rows: list[dict[str, object]]) -> dict[str, object]:
        payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"
        response = self.client.post(
            f"/ingest/{dataset}",
            data=payload.encode("utf-8"),
            headers={
                "X-Ingest-Key": ingest_key,
                "Content-Type": "application/x-ndjson",
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def _scalar(self, query: str, params: tuple[object, ...]) -> int:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(query, params).fetchone()
        return int(row[0] if isinstance(row, tuple) else next(iter(dict(row).values())))

    def test_ingest_enforces_sales_retention_only_for_short_commercial_datasets(self) -> None:
        tenant_id, ingest_key = self._create_tenant("Tenant Ingest Retention")
        ref_date = self._current_date()
        inside_date = (ref_date - timedelta(days=10)).isoformat()
        outside_date = (ref_date - timedelta(days=370)).isoformat()

        comprovantes = self._post_ndjson(
            "comprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 101,
                    "ID_USUARIOS": 1,
                    "ID_TURNOS": 1,
                    "ID_ENTIDADE": 10,
                    "VLRTOTAL": 120,
                    "REFERENCIA": 9001,
                    "DATA": inside_date,
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 102,
                    "ID_USUARIOS": 1,
                    "ID_TURNOS": 1,
                    "ID_ENTIDADE": 10,
                    "VLRTOTAL": 130,
                    "REFERENCIA": 9002,
                    "DATA": outside_date,
                },
            ],
        )
        self.assertEqual(int(comprovantes["inserted_or_updated"]), 1)
        self.assertEqual(int(comprovantes["rejected_by_retention"]), 1)
        self.assertEqual(comprovantes["retention_policy"]["name"], "sales_history_days")
        self.assertIsNotNone(comprovantes["retention_cutoff"])

        movprodutos = self._post_ndjson(
            "movprodutos",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 201,
                    "ID_COMPROVANTE": 101,
                    "ID_ENTIDADE": 10,
                    "TOTALVENDA": 120,
                    "DATA": inside_date,
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 202,
                    "ID_COMPROVANTE": 102,
                    "ID_ENTIDADE": 10,
                    "TOTALVENDA": 130,
                    "DATA": outside_date,
                },
            ],
        )
        self.assertEqual(int(movprodutos["inserted_or_updated"]), 1)
        self.assertEqual(int(movprodutos["rejected_by_retention"]), 1)

        clientes = self._post_ndjson(
            "clientes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_ENTIDADE": 10,
                    "NOME": "Cliente Histórico",
                    "DTCADASTRO": outside_date,
                }
            ],
        )
        self.assertEqual(int(clientes["inserted_or_updated"]), 1)
        self.assertEqual(int(clientes["rejected_by_retention"]), 0)

        contaspagar = self._post_ndjson(
            "contaspagar",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_CONTASPAGAR": 301,
                    "DTAVCTO": outside_date,
                }
            ],
        )
        self.assertEqual(int(contaspagar["inserted_or_updated"]), 1)
        self.assertEqual(int(contaspagar["rejected_by_retention"]), 0)

        contasreceber = self._post_ndjson(
            "contasreceber",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_CONTASRECEBER": 401,
                    "DTAVCTO": outside_date,
                }
            ],
        )
        self.assertEqual(int(contasreceber["inserted_or_updated"]), 1)
        self.assertEqual(int(contasreceber["rejected_by_retention"]), 0)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            stg_counts = conn.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM stg.comprovantes WHERE id_empresa = %s) AS comprovantes,
                  (SELECT COUNT(*) FROM stg.movprodutos WHERE id_empresa = %s) AS movprodutos,
                  (SELECT COUNT(*) FROM stg.entidades WHERE id_empresa = %s) AS clientes,
                  (SELECT COUNT(*) FROM stg.contaspagar WHERE id_empresa = %s) AS contaspagar,
                  (SELECT COUNT(*) FROM stg.contasreceber WHERE id_empresa = %s) AS contasreceber
                """,
                (tenant_id, tenant_id, tenant_id, tenant_id, tenant_id),
            ).fetchone()

        self.assertEqual(int(stg_counts["comprovantes"]), 1)
        self.assertEqual(int(stg_counts["movprodutos"]), 1)
        self.assertEqual(int(stg_counts["clientes"]), 1)
        self.assertEqual(int(stg_counts["contaspagar"]), 1)
        self.assertEqual(int(stg_counts["contasreceber"]), 1)

    def test_sales_loaders_and_purge_do_not_touch_long_finance_or_customer_dimension(self) -> None:
        tenant_id, _ = self._create_tenant("Tenant SQL Retention")
        ref_date = self._current_date()
        old_date = ref_date - timedelta(days=370)
        new_date = ref_date - timedelta(days=10)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_filial (id_empresa, id_filial, nome)
                VALUES (%s, 1, %s)
                ON CONFLICT (id_empresa, id_filial) DO NOTHING
                """,
                (tenant_id, f"Filial {tenant_id}"),
            )
            conn.execute(
                """
                INSERT INTO dw.dim_cliente (id_empresa, id_filial, id_cliente, nome)
                VALUES (%s, 1, 10, 'Cliente Mantido')
                ON CONFLICT (id_empresa, id_filial, id_cliente) DO NOTHING
                """,
                (tenant_id,),
            )
            conn.execute(
                """
                INSERT INTO dw.dim_produto (id_empresa, id_filial, id_produto, nome, custo_medio)
                VALUES (%s, 1, 700, 'Produto 700', 10)
                ON CONFLICT (id_empresa, id_filial, id_produto) DO NOTHING
                """,
                (tenant_id,),
            )
            conn.execute(
                """
                INSERT INTO stg.comprovantes (
                  id_empresa, id_filial, id_db, id_comprovante, dt_evento, payload
                )
                VALUES
                  (%s, 1, 1, 1101, %s, %s::jsonb),
                  (%s, 1, 1, 1102, %s, %s::jsonb)
                """,
                (
                    tenant_id,
                    old_date,
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_COMPROVANTE": 1101,
                            "ID_USUARIOS": 1,
                            "ID_TURNOS": 1,
                            "ID_ENTIDADE": 10,
                            "VLRTOTAL": 90,
                            "REFERENCIA": 9101,
                            "DATA": f"{old_date.isoformat()} 09:00:00",
                        }
                    ),
                    tenant_id,
                    new_date,
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_COMPROVANTE": 1102,
                            "ID_USUARIOS": 1,
                            "ID_TURNOS": 1,
                            "ID_ENTIDADE": 10,
                            "VLRTOTAL": 120,
                            "REFERENCIA": 9102,
                            "DATA": f"{new_date.isoformat()} 09:00:00",
                        }
                    ),
                ),
            )
            conn.execute(
                """
                INSERT INTO stg.movprodutos (
                  id_empresa, id_filial, id_db, id_movprodutos, dt_evento, payload
                )
                VALUES
                  (%s, 1, 1, 2101, %s, %s::jsonb),
                  (%s, 1, 1, 2102, %s, %s::jsonb)
                """,
                (
                    tenant_id,
                    old_date,
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_MOVPRODUTOS": 2101,
                            "ID_COMPROVANTE": 1101,
                            "ID_ENTIDADE": 10,
                            "TOTALVENDA": 90,
                            "DATA": f"{old_date.isoformat()} 09:00:00",
                        }
                    ),
                    tenant_id,
                    new_date,
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_MOVPRODUTOS": 2102,
                            "ID_COMPROVANTE": 1102,
                            "ID_ENTIDADE": 10,
                            "TOTALVENDA": 120,
                            "DATA": f"{new_date.isoformat()} 09:00:00",
                        }
                    ),
                ),
            )
            conn.execute(
                """
                INSERT INTO stg.itensmovprodutos (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, dt_evento, payload
                )
                VALUES
                  (%s, 1, 1, 2101, 1, %s, %s::jsonb),
                  (%s, 1, 1, 2102, 1, %s, %s::jsonb)
                """,
                (
                    tenant_id,
                    old_date,
                    json.dumps(
                        {
                            "ID_PRODUTOS": 700,
                            "QTDE": 1,
                            "VLRUNITARIO": 90,
                            "TOTAL": 90,
                            "CFOP": 5102,
                        }
                    ),
                    tenant_id,
                    new_date,
                    json.dumps(
                        {
                            "ID_PRODUTOS": 700,
                            "QTDE": 2,
                            "VLRUNITARIO": 60,
                            "TOTAL": 120,
                            "CFOP": 5102,
                        }
                    ),
                ),
            )
            conn.execute(
                """
                INSERT INTO stg.formas_pgto_comprovantes (
                  id_empresa, id_filial, id_referencia, tipo_forma, dt_evento, payload
                )
                VALUES
                  (%s, 1, 9101, 28, %s, %s::jsonb),
                  (%s, 1, 9102, 28, %s, %s::jsonb)
                """,
                (
                    tenant_id,
                    old_date,
                    json.dumps({"ID_FILIAL": 1, "TIPO_FORMA": 28, "VALOR": 90}),
                    tenant_id,
                    new_date,
                    json.dumps({"ID_FILIAL": 1, "TIPO_FORMA": 28, "VALOR": 120}),
                ),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_financeiro (
                  id_empresa, id_filial, id_db, tipo_titulo, id_titulo,
                  data_emissao, data_key_emissao, vencimento, data_key_venc,
                  valor, payload
                )
                VALUES (%s, 1, 1, 0, 5001, %s, %s, %s, %s, 999, '{}'::jsonb)
                """,
                (
                    tenant_id,
                    old_date,
                    int(old_date.strftime("%Y%m%d")),
                    old_date,
                    int(old_date.strftime("%Y%m%d")),
                ),
            )
            conn.execute(
                """
                INSERT INTO mart.customer_sales_daily (
                  dt_ref, id_empresa, id_filial, id_cliente, compras_dia, valor_dia
                )
                VALUES
                  (%s, %s, 1, 10, 1, 90),
                  (%s, %s, 1, 10, 1, 120)
                """,
                (old_date, tenant_id, new_date, tenant_id),
            )
            conn.execute(
                """
                INSERT INTO mart.customer_rfm_daily (
                  dt_ref, id_empresa, id_filial, id_cliente, cliente_nome, last_purchase,
                  recency_days, frequency_30, frequency_90, monetary_30, monetary_90,
                  ticket_30, expected_cycle_days, trend_frequency, trend_monetary
                )
                VALUES
                  (%s, %s, 1, 10, 'Cliente Mantido', %s, 30, 1, 2, 90, 180, 90, 30, 0, 0),
                  (%s, %s, 1, 10, 'Cliente Mantido', %s, 10, 2, 3, 120, 200, 60, 30, 1, 20)
                """,
                (old_date, tenant_id, old_date, new_date, tenant_id, new_date),
            )
            conn.execute(
                """
                INSERT INTO mart.customer_churn_risk_daily (
                  dt_ref, id_empresa, id_filial, id_cliente, cliente_nome, last_purchase,
                  recency_days, frequency_30, frequency_90, monetary_30, monetary_90,
                  ticket_30, expected_cycle_days, churn_score, revenue_at_risk_30d,
                  recommendation, reasons
                )
                VALUES
                  (%s, %s, 1, 10, 'Cliente Mantido', %s, 30, 1, 2, 90, 180, 90, 30, 70, 50, 'acao', '{}'::jsonb),
                  (%s, %s, 1, 10, 'Cliente Mantido', %s, 10, 2, 3, 120, 200, 60, 30, 40, 20, 'acao', '{}'::jsonb)
                """,
                (old_date, tenant_id, old_date, new_date, tenant_id, new_date),
            )
            conn.execute(
                """
                INSERT INTO mart.finance_aging_daily (
                  dt_ref, id_empresa, id_filial, receber_total_aberto, receber_total_vencido,
                  pagar_total_aberto, pagar_total_vencido, bucket_0_7, bucket_8_15,
                  bucket_16_30, bucket_31_60, bucket_60_plus, top5_concentration_pct, data_gaps
                )
                VALUES (%s, %s, 1, 10, 5, 2, 1, 1, 1, 1, 1, 1, 10, false)
                """,
                (old_date, tenant_id),
            )
            conn.execute(
                """
                INSERT INTO mart.health_score_daily (
                  dt_ref, id_empresa, id_filial, comp_margem, comp_fraude, comp_churn,
                  comp_finance, comp_operacao, comp_dados, score_total, components, reasons
                )
                VALUES (%s, %s, 1, 1, 1, 1, 1, 1, 1, 6, '{}'::jsonb, '{}'::jsonb)
                """,
                (old_date, tenant_id),
            )
            conn.commit()

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            comp_rows = conn.execute("SELECT etl.load_fact_comprovante(%s) AS total", (tenant_id,)).fetchone()
            venda_rows = conn.execute("SELECT etl.load_fact_venda(%s) AS total", (tenant_id,)).fetchone()
            item_rows = conn.execute("SELECT etl.load_fact_venda_item(%s) AS total", (tenant_id,)).fetchone()
            pgto_rows = conn.execute("SELECT etl.load_fact_pagamento_comprovante(%s) AS total", (tenant_id,)).fetchone()
            conn.commit()

        self.assertEqual(int(comp_rows["total"]), 1)
        self.assertEqual(int(venda_rows["total"]), 1)
        self.assertEqual(int(item_rows["total"]), 1)
        self.assertEqual(int(pgto_rows["total"]), 1)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.fact_comprovante (
                  id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                  id_usuario, id_turno, id_cliente, valor_total, cancelado, situacao, payload
                )
                VALUES (%s, 1, 1, 1199, %s, %s, 1, 1, 10, 90, false, 1, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante) DO NOTHING
                """,
                (tenant_id, f"{old_date.isoformat()} 09:00:00", int(old_date.strftime("%Y%m%d"))),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  id_usuario, id_cliente, id_comprovante, id_turno, saidas_entradas,
                  total_venda, cancelado, payload
                )
                VALUES (%s, 1, 1, 2199, %s, %s, 1, 10, 1199, 1, 1, 90, false, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos) DO NOTHING
                """,
                (tenant_id, f"{old_date.isoformat()} 09:00:00", int(old_date.strftime("%Y%m%d"))),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, qtd, valor_unitario, total, desconto, custo_total, margem, payload
                )
                VALUES (%s, 1, 1, 2199, 2999, %s, 700, 1, 90, 90, 0, 70, 20, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos) DO NOTHING
                """,
                (tenant_id, int(old_date.strftime("%Y%m%d"))),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_pagamento_comprovante (
                  id_empresa, id_filial, referencia, id_db, id_comprovante, id_turno, id_usuario,
                  tipo_forma, valor, dt_evento, data_key, payload
                )
                VALUES (%s, 1, 9199, 1, 1199, 1, 1, 1, 90, %s, %s, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, referencia, tipo_forma) DO NOTHING
                """,
                (tenant_id, f"{old_date.isoformat()} 09:00:00", int(old_date.strftime("%Y%m%d"))),
            )
            conn.commit()

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            pre_purge = conn.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM dw.fact_comprovante WHERE id_empresa = %s) AS fact_comprovante,
                  (SELECT COUNT(*) FROM dw.fact_venda WHERE id_empresa = %s) AS fact_venda,
                  (SELECT COUNT(*) FROM dw.fact_venda_item WHERE id_empresa = %s) AS fact_venda_item,
                  (SELECT COUNT(*) FROM dw.fact_pagamento_comprovante WHERE id_empresa = %s) AS fact_pagamento
                """,
                (tenant_id, tenant_id, tenant_id, tenant_id),
            ).fetchone()
            purge = conn.execute(
                "SELECT etl.purge_sales_history(%s, %s) AS result",
                (tenant_id, ref_date),
            ).fetchone()["result"]
            conn.commit()

        self.assertEqual(int(pre_purge["fact_comprovante"]), 2)
        self.assertEqual(int(pre_purge["fact_venda"]), 2)
        self.assertEqual(int(pre_purge["fact_venda_item"]), 2)
        self.assertEqual(int(pre_purge["fact_pagamento"]), 2)
        self.assertTrue(bool(purge["ok"]))
        self.assertGreaterEqual(int(purge["mart_customer_sales_daily_deleted"]), 1)
        self.assertGreaterEqual(int(purge["mart_customer_rfm_daily_deleted"]), 1)
        self.assertGreaterEqual(int(purge["mart_customer_churn_risk_daily_deleted"]), 1)
        self.assertTrue(bool(purge["refresh_meta"]["sales_marts_refreshed"]))
        self.assertTrue(bool(purge["refresh_meta"]["payments_marts_refreshed"]))
        self.assertTrue(bool(purge["refresh_meta"]["churn_mart_refreshed"]))
        self.assertFalse(bool(purge["refresh_meta"]["refresh_domains"]["finance"]))
        self.assertFalse(bool(purge["refresh_meta"]["refresh_domains"]["risk"]))
        self.assertFalse(bool(purge["refresh_meta"]["refresh_domains"]["cash"]))
        self.assertIn("mart.clientes_churn_risco", purge["marts_refreshed"])
        self.assertIn("mart.agg_pagamentos_diaria", purge["marts_refreshed"])
        self.assertNotIn("mart.financeiro_vencimentos_diaria", purge["marts_refreshed"])
        self.assertNotIn("mart.agg_risco_diaria", purge["marts_refreshed"])
        self.assertNotIn("mart.agg_caixa_forma_pagamento", purge["marts_refreshed"])
        self.assertNotIn("mart.alerta_caixa_aberto", purge["marts_refreshed"])

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            post_purge = conn.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM stg.comprovantes WHERE id_empresa = %s) AS stg_comprovantes,
                  (SELECT COUNT(*) FROM stg.movprodutos WHERE id_empresa = %s) AS stg_movprodutos,
                  (SELECT COUNT(*) FROM stg.itensmovprodutos WHERE id_empresa = %s) AS stg_itens,
                  (SELECT COUNT(*) FROM stg.formas_pgto_comprovantes WHERE id_empresa = %s) AS stg_formas,
                  (SELECT COUNT(*) FROM dw.fact_comprovante WHERE id_empresa = %s) AS fact_comprovante,
                  (SELECT COUNT(*) FROM dw.fact_venda WHERE id_empresa = %s) AS fact_venda,
                  (SELECT COUNT(*) FROM dw.fact_venda_item WHERE id_empresa = %s) AS fact_venda_item,
                  (SELECT COUNT(*) FROM dw.fact_pagamento_comprovante WHERE id_empresa = %s) AS fact_pagamento,
                  (SELECT COUNT(*) FROM dw.fact_financeiro WHERE id_empresa = %s) AS fact_financeiro,
                  (SELECT COUNT(*) FROM dw.dim_cliente WHERE id_empresa = %s) AS dim_cliente,
                  (SELECT COUNT(*) FROM mart.customer_sales_daily WHERE id_empresa = %s) AS customer_sales_daily,
                  (SELECT COUNT(*) FROM mart.customer_rfm_daily WHERE id_empresa = %s) AS customer_rfm_daily,
                  (SELECT COUNT(*) FROM mart.customer_churn_risk_daily WHERE id_empresa = %s) AS customer_churn_risk_daily,
                  (SELECT COUNT(*) FROM mart.finance_aging_daily WHERE id_empresa = %s) AS finance_aging_daily,
                  (SELECT COUNT(*) FROM mart.health_score_daily WHERE id_empresa = %s) AS health_score_daily
                """,
                (
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                    tenant_id,
                ),
            ).fetchone()

        self.assertEqual(int(post_purge["stg_comprovantes"]), 1)
        self.assertEqual(int(post_purge["stg_movprodutos"]), 1)
        self.assertEqual(int(post_purge["stg_itens"]), 1)
        self.assertEqual(int(post_purge["stg_formas"]), 1)
        self.assertEqual(int(post_purge["fact_comprovante"]), 1)
        self.assertEqual(int(post_purge["fact_venda"]), 1)
        self.assertEqual(int(post_purge["fact_venda_item"]), 1)
        self.assertEqual(int(post_purge["fact_pagamento"]), 1)
        self.assertEqual(int(post_purge["fact_financeiro"]), 1)
        self.assertEqual(int(post_purge["dim_cliente"]), 1)
        self.assertEqual(int(post_purge["customer_sales_daily"]), 1)
        self.assertEqual(int(post_purge["customer_rfm_daily"]), 1)
        self.assertEqual(int(post_purge["customer_churn_risk_daily"]), 1)
        self.assertEqual(int(post_purge["finance_aging_daily"]), 1)
        self.assertEqual(int(post_purge["health_score_daily"]), 1)


if __name__ == "__main__":
    unittest.main()
