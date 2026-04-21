from __future__ import annotations

import json
import unittest
from datetime import date, timedelta
from unittest.mock import patch

from fastapi.testclient import TestClient

import app.routes_ingest as routes_ingest
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

    def _create_tenant(self, name: str, sales_history_days: int = 365, default_product_scope_days: int = 1) -> tuple[int, str]:
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

    def _function_def(self, function_name: str) -> str:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                """
                SELECT pg_get_functiondef(p.oid) AS definition
                FROM pg_proc p
                JOIN pg_namespace n
                  ON n.oid = p.pronamespace
                WHERE n.nspname = 'etl'
                  AND p.proname = %s
                ORDER BY p.oid DESC
                LIMIT 1
                """,
                (function_name,),
            ).fetchone()
        return str((row or {}).get("definition") or "")

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

    def test_ingest_retention_override_accepts_historical_movprodutos_replay(self) -> None:
        tenant_id, ingest_key = self._create_tenant("Tenant Historical Replay Retention")
        historical_date = date(2025, 1, 1).isoformat()
        historical_row = {
            "ID_FILIAL": 1,
            "ID_DB": 1,
            "ID_MOVPRODUTOS": 1201,
            "ID_COMPROVANTE": 2201,
            "ID_ENTIDADE": 10,
            "TOTALVENDA": 130.75,
            "DATA": historical_date,
        }

        rejected = self._post_ndjson("movprodutos", ingest_key, [historical_row])
        self.assertEqual(int(rejected["inserted_or_updated"]), 0)
        self.assertEqual(int(rejected["rejected_by_retention"]), 1)
        self.assertEqual(int(rejected["inserted"]), 0)
        self.assertEqual(int(rejected["updated"]), 0)
        self.assertEqual(int(rejected["duplicates_in_batch"]), 0)
        self.assertEqual(rejected["retention_policy"]["cutoff_source"], "sales_history_days")
        self.assertFalse(bool(rejected["retention_policy"]["override"]["active"]))
        self.assertEqual(
            self._scalar("SELECT COUNT(*) FROM stg.movprodutos WHERE id_empresa = %s", (tenant_id,)),
            0,
        )

        with patch.object(routes_ingest.settings, "ingest_retention_override_min_date", date(2025, 1, 1)), patch.object(
            routes_ingest.settings,
            "ingest_retention_override_datasets",
            "movprodutos,comprovantes,itensmovprodutos,formas_pgto_comprovantes,turnos",
        ):
            accepted = self._post_ndjson("movprodutos", ingest_key, [historical_row])

        self.assertEqual(int(accepted["inserted_or_updated"]), 1)
        self.assertEqual(int(accepted["inserted"]), 1)
        self.assertEqual(int(accepted["updated"]), 0)
        self.assertEqual(int(accepted["rejected_by_retention"]), 0)
        self.assertEqual(accepted["retention_cutoff"], historical_date)
        self.assertEqual(accepted["retention_policy"]["default_cutoff"], rejected["retention_policy"]["cutoff"])
        self.assertEqual(accepted["retention_policy"]["cutoff_source"], "override_min_date")
        self.assertTrue(bool(accepted["retention_policy"]["override"]["configured"]))
        self.assertTrue(bool(accepted["retention_policy"]["override"]["active"]))
        self.assertEqual(accepted["retention_policy"]["override"]["min_date"], historical_date)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            inserted_row = conn.execute(
                """
                SELECT
                  id_movprodutos,
                  total_venda_shadow,
                  CAST(dt_evento AS date) AS dt_evento
                FROM stg.movprodutos
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_movprodutos = 1201
                """,
                (tenant_id,),
            ).fetchone()

        self.assertIsNotNone(inserted_row)
        self.assertEqual(int(inserted_row["id_movprodutos"]), 1201)
        self.assertAlmostEqual(float(inserted_row["total_venda_shadow"]), 130.75, places=2)
        self.assertEqual(inserted_row["dt_evento"].isoformat(), historical_date)

    def test_ingest_populates_typed_shadow_columns_and_reports_updates(self) -> None:
        tenant_id, ingest_key = self._create_tenant("Tenant Typed Shadows")
        ref_date = self._current_date().isoformat()

        first_comprovante = self._post_ndjson(
            "comprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 701,
                    "ID_USUARIOS": 77,
                    "ID_TURNOS": 12,
                    "ID_ENTIDADE": 900,
                    "VLRTOTAL": 150.5,
                    "CANCELADO": False,
                    "SITUACAO": 3,
                    "REFERENCIA": 8801,
                    "DATA": ref_date,
                }
            ],
        )
        self.assertEqual(int(first_comprovante["inserted"]), 1)
        self.assertEqual(int(first_comprovante["updated"]), 0)

        second_comprovante = self._post_ndjson(
            "comprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 701,
                    "ID_USUARIOS": 78,
                    "ID_TURNOS": 13,
                    "ID_ENTIDADE": 901,
                    "VLRTOTAL": 199.9,
                    "CANCELADO": True,
                    "SITUACAO": 5,
                    "REFERENCIA": 8801,
                    "DATA": ref_date,
                }
            ],
        )
        self.assertEqual(int(second_comprovante["inserted"]), 0)
        self.assertEqual(int(second_comprovante["updated"]), 1)

        self._post_ndjson(
            "movprodutos",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 801,
                    "ID_COMPROVANTE": 701,
                    "ID_USUARIOS": 78,
                    "ID_TURNOS": 13,
                    "ID_ENTIDADE": 901,
                    "SAIDAS_ENTRADAS": 1,
                    "TOTALVENDA": 199.9,
                    "SITUACAO": 2,
                    "DATA": ref_date,
                }
            ],
        )
        self._post_ndjson(
            "itensmovprodutos",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 801,
                    "ID_ITENSMOVPRODUTOS": 1,
                    "ID_PRODUTOS": 123,
                    "ID_GRUPOPRODUTOS": 456,
                    "ID_LOCALVENDAS": 3,
                    "ID_FUNCIONARIOS": 9,
                    "CFOP": 5102,
                    "QTDE": 10,
                    "VLRUNITARIO": 19.99,
                    "TOTAL": 199.9,
                    "VLRDESCONTO": 10,
                    "VLRCUSTOCOMICMS": 17,
                    "VLRCUSTO": 15,
                    "DATA": ref_date,
                }
            ],
        )
        self._post_ndjson(
            "formas_pgto_comprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_REFERENCIA": 8801,
                    "TIPO_FORMA": 28,
                    "VALOR": 199.9,
                    "NSU": "ABC123",
                    "AUTORIZACAO": "ZX9",
                    "BANDEIRA": "VISA",
                    "REDE": "REDE1",
                    "TEF": "TEF1",
                    "DATA": ref_date,
                }
            ],
        )

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            comprovante = conn.execute(
                """
                SELECT
                  referencia_shadow,
                  id_usuario_shadow,
                  id_turno_shadow,
                  id_cliente_shadow,
                  valor_total_shadow,
                  cancelado_shadow,
                  situacao_shadow
                FROM stg.comprovantes
                WHERE id_empresa = %s AND id_filial = 1 AND id_db = 1 AND id_comprovante = 701
                """,
                (tenant_id,),
            ).fetchone()
            movimento = conn.execute(
                """
                SELECT
                  id_comprovante_shadow,
                  id_usuario_shadow,
                  id_turno_shadow,
                  id_cliente_shadow,
                  saidas_entradas_shadow,
                  total_venda_shadow,
                  situacao_shadow
                FROM stg.movprodutos
                WHERE id_empresa = %s AND id_filial = 1 AND id_db = 1 AND id_movprodutos = 801
                """,
                (tenant_id,),
            ).fetchone()
            item = conn.execute(
                """
                SELECT
                  id_produto_shadow,
                  id_grupo_produto_shadow,
                  id_local_venda_shadow,
                  id_funcionario_shadow,
                  cfop_shadow,
                  qtd_shadow,
                  valor_unitario_shadow,
                  total_shadow,
                  desconto_shadow,
                  custo_unitario_shadow
                FROM stg.itensmovprodutos
                WHERE id_empresa = %s AND id_filial = 1 AND id_db = 1 AND id_movprodutos = 801 AND id_itensmovprodutos = 1
                """,
                (tenant_id,),
            ).fetchone()
            pagamento = conn.execute(
                """
                SELECT valor_shadow, nsu_shadow, autorizacao_shadow, bandeira_shadow, rede_shadow, tef_shadow
                FROM stg.formas_pgto_comprovantes
                WHERE id_empresa = %s AND id_filial = 1 AND id_referencia = 8801 AND tipo_forma = 28
                """,
                (tenant_id,),
            ).fetchone()

        self.assertEqual(int(comprovante["referencia_shadow"]), 8801)
        self.assertEqual(int(comprovante["id_usuario_shadow"]), 78)
        self.assertEqual(int(comprovante["id_turno_shadow"]), 13)
        self.assertEqual(int(comprovante["id_cliente_shadow"]), 901)
        self.assertAlmostEqual(float(comprovante["valor_total_shadow"]), 199.9, places=2)
        self.assertTrue(bool(comprovante["cancelado_shadow"]))
        self.assertEqual(int(comprovante["situacao_shadow"]), 5)

        self.assertEqual(int(movimento["id_comprovante_shadow"]), 701)
        self.assertEqual(int(movimento["id_usuario_shadow"]), 78)
        self.assertEqual(int(movimento["id_turno_shadow"]), 13)
        self.assertEqual(int(movimento["id_cliente_shadow"]), 901)
        self.assertEqual(int(movimento["saidas_entradas_shadow"]), 1)
        self.assertAlmostEqual(float(movimento["total_venda_shadow"]), 199.9, places=2)
        self.assertEqual(int(movimento["situacao_shadow"]), 2)

        self.assertEqual(int(item["id_produto_shadow"]), 123)
        self.assertEqual(int(item["id_grupo_produto_shadow"]), 456)
        self.assertEqual(int(item["id_local_venda_shadow"]), 3)
        self.assertEqual(int(item["id_funcionario_shadow"]), 9)
        self.assertEqual(int(item["cfop_shadow"]), 5102)
        self.assertAlmostEqual(float(item["qtd_shadow"]), 10, places=3)
        self.assertAlmostEqual(float(item["valor_unitario_shadow"]), 19.99, places=2)
        self.assertAlmostEqual(float(item["total_shadow"]), 199.9, places=2)
        self.assertAlmostEqual(float(item["desconto_shadow"]), 10, places=2)
        self.assertAlmostEqual(float(item["custo_unitario_shadow"]), 17, places=2)

        self.assertAlmostEqual(float(pagamento["valor_shadow"]), 199.9, places=2)
        self.assertEqual(pagamento["nsu_shadow"], "ABC123")
        self.assertEqual(pagamento["autorizacao_shadow"], "ZX9")
        self.assertEqual(pagamento["bandeira_shadow"], "VISA")
        self.assertEqual(pagamento["rede_shadow"], "REDE1")
        self.assertEqual(pagamento["tef_shadow"], "TEF1")

    def test_itenscomprovantes_accepts_id_itenscomprovante_alias_and_persists(self) -> None:
        tenant_id, ingest_key = self._create_tenant("Tenant Itenscomprovantes Alias Real")
        ref_date = self._current_date().isoformat()

        response = self._post_ndjson(
            "itenscomprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 9101,
                    "ID_ITENSCOMPROVANTE": 401,
                    "ID_PRODUTOS": 9901,
                    "CFOP": 5102,
                    "QTDE": 2,
                    "VLRUNITARIO": 21.5,
                    "TOTAL": 43.0,
                    "DATA": ref_date,
                }
            ],
        )

        self.assertEqual(int(response["inserted_or_updated"]), 1)
        self.assertEqual(int(response["inserted"]), 1)
        self.assertEqual(int(response["updated"]), 0)
        self.assertEqual(int(response["rejected_invalid"]), 0)
        self.assertEqual(int(response["duplicates_in_batch"]), 0)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                """
                SELECT
                  id_empresa,
                  id_filial,
                  id_db,
                  id_comprovante,
                  id_itemcomprovante,
                  id_produto_shadow,
                  cfop_shadow,
                  total_shadow
                FROM stg.itenscomprovantes
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_comprovante = 9101
                  AND id_itemcomprovante = 401
                """,
                (tenant_id,),
            ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(int(row["id_empresa"]), tenant_id)
        self.assertEqual(int(row["id_filial"]), 1)
        self.assertEqual(int(row["id_db"]), 1)
        self.assertEqual(int(row["id_comprovante"]), 9101)
        self.assertEqual(int(row["id_itemcomprovante"]), 401)
        self.assertEqual(int(row["id_produto_shadow"]), 9901)
        self.assertEqual(int(row["cfop_shadow"]), 5102)
        self.assertAlmostEqual(float(row["total_shadow"]), 43.0, places=2)

    def test_itenscomprovantes_accepts_id_itemcomprovante_alias_and_persists(self) -> None:
        tenant_id, ingest_key = self._create_tenant("Tenant Itenscomprovantes Alias Alternate")
        ref_date = self._current_date().isoformat()

        response = self._post_ndjson(
            "itenscomprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 9102,
                    "ID_ITEMCOMPROVANTE": 402,
                    "ID_PRODUTOS": 9902,
                    "CFOP": 5102,
                    "QTDE": 1,
                    "VLRUNITARIO": 55.0,
                    "TOTAL": 55.0,
                    "DATA": ref_date,
                }
            ],
        )

        self.assertEqual(int(response["inserted_or_updated"]), 1)
        self.assertEqual(int(response["inserted"]), 1)
        self.assertEqual(int(response["updated"]), 0)
        self.assertEqual(int(response["rejected_invalid"]), 0)

        self.assertEqual(
            self._scalar(
                """
                SELECT COUNT(*)
                FROM stg.itenscomprovantes
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_comprovante = 9102
                  AND id_itemcomprovante = 402
                """,
                (tenant_id,),
            ),
            1,
        )

    def test_itenscomprovantes_rejects_missing_item_pk(self) -> None:
        _tenant_id, ingest_key = self._create_tenant("Tenant Itenscomprovantes Missing PK")
        ref_date = self._current_date().isoformat()

        response = self._post_ndjson(
            "itenscomprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 9103,
                    "ID_PRODUTOS": 9903,
                    "CFOP": 5102,
                    "QTDE": 3,
                    "TOTAL": 60.0,
                    "DATA": ref_date,
                }
            ],
        )

        self.assertEqual(int(response["inserted_or_updated"]), 0)
        self.assertEqual(int(response["inserted"]), 0)
        self.assertEqual(int(response["updated"]), 0)
        self.assertEqual(int(response["rejected_invalid"]), 1)
        self.assertEqual(int(response["rejected_by_retention"]), 0)
        self.assertIn("Missing/invalid PK fields", response["sample_rejections"][0]["reason"])

    def test_itenscomprovantes_rejects_conflicting_item_pk_aliases(self) -> None:
        tenant_id, ingest_key = self._create_tenant("Tenant Itenscomprovantes Conflicting PK")
        ref_date = self._current_date().isoformat()

        response = self._post_ndjson(
            "itenscomprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 9104,
                    "ID_ITENSCOMPROVANTE": 501,
                    "ID_ITEMCOMPROVANTE": 502,
                    "ID_PRODUTOS": 9904,
                    "CFOP": 5102,
                    "QTDE": 4,
                    "TOTAL": 80.0,
                    "DATA": ref_date,
                }
            ],
        )

        self.assertEqual(int(response["inserted_or_updated"]), 0)
        self.assertEqual(int(response["rejected_invalid"]), 1)
        self.assertIn("Conflicting PK aliases", response["sample_rejections"][0]["reason"])
        self.assertEqual(
            self._scalar("SELECT COUNT(*) FROM stg.itenscomprovantes WHERE id_empresa = %s", (tenant_id,)),
            0,
        )

    def test_itenscomprovantes_dedupes_same_effective_pk_across_alias_names(self) -> None:
        tenant_id, ingest_key = self._create_tenant("Tenant Itenscomprovantes Alias Dedupe")
        ref_date = self._current_date().isoformat()

        response = self._post_ndjson(
            "itenscomprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 9105,
                    "ID_ITENSCOMPROVANTE": 601,
                    "ID_PRODUTOS": 9905,
                    "CFOP": 5102,
                    "QTDE": 1,
                    "TOTAL": 41.0,
                    "DATA": ref_date,
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 9105,
                    "ID_ITEMCOMPROVANTE": 601,
                    "ID_PRODUTOS": 9905,
                    "CFOP": 5102,
                    "QTDE": 1,
                    "TOTAL": 42.0,
                    "DATA": ref_date,
                },
            ],
        )

        self.assertEqual(int(response["inserted_or_updated"]), 1)
        self.assertEqual(int(response["inserted"]), 1)
        self.assertEqual(int(response["updated"]), 0)
        self.assertEqual(int(response["duplicates_in_batch"]), 1)
        self.assertEqual(int(response["rejected_invalid"]), 0)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total, MAX(total_shadow) AS total_shadow
                FROM stg.itenscomprovantes
                WHERE id_empresa = %s
                  AND id_filial = 1
                  AND id_db = 1
                  AND id_comprovante = 9105
                  AND id_itemcomprovante = 601
                """,
                (tenant_id,),
            ).fetchone()

        self.assertEqual(int(row["total"]), 1)
        self.assertAlmostEqual(float(row["total_shadow"]), 42.0, places=2)

    def test_canonical_sales_loaders_keep_legacy_bridge_optional_and_off_hot_path(self) -> None:
        tenant_id, _ingest_key = self._create_tenant("Tenant Canonical Hot Path")
        same_day = self._current_date().isoformat()

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_produto (id_empresa, id_filial, id_produto, nome, custo_medio)
                VALUES (%s, 1, 501, 'Produto Hot Path', 12)
                ON CONFLICT (id_empresa, id_filial, id_produto)
                DO UPDATE SET nome = EXCLUDED.nome, custo_medio = EXCLUDED.custo_medio
                """,
                (tenant_id,),
            )
            conn.execute(
                """
                INSERT INTO stg.comprovantes (
                  id_empresa,
                  id_filial,
                  id_db,
                  id_comprovante,
                  payload,
                  ingested_at,
                  dt_evento,
                  received_at,
                  id_usuario_shadow,
                  id_turno_shadow,
                  id_cliente_shadow,
                  valor_total_shadow,
                  cancelado_shadow,
                  situacao_shadow
                )
                VALUES
                  (%s, 1, 1, 1201, %s::jsonb, now(), %s::timestamptz, now(), 11, 71, 901, 40, false, 1),
                  (%s, 1, 1, 1202, %s::jsonb, now(), %s::timestamptz, now(), 12, 72, 902, 55, false, 1)
                """,
                (
                    tenant_id,
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_COMPROVANTE": 1201,
                            "ID_USUARIOS": 11,
                            "ID_TURNOS": 71,
                            "ID_ENTIDADE": 901,
                            "VLRTOTAL": 40,
                            "SITUACAO": 1,
                            "SAIDAS_ENTRADAS": 1,
                            "DATA": f"{same_day} 09:00:00",
                        }
                    ),
                    f"{same_day} 09:00:00",
                    tenant_id,
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_COMPROVANTE": 1202,
                            "ID_USUARIOS": 12,
                            "ID_TURNOS": 72,
                            "ID_ENTIDADE": 902,
                            "VLRTOTAL": 55,
                            "SITUACAO": 1,
                            "SAIDAS_ENTRADAS": 1,
                            "DATA": f"{same_day} 10:00:00",
                        }
                    ),
                    f"{same_day} 10:00:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO stg.itenscomprovantes (
                  id_empresa,
                  id_filial,
                  id_db,
                  id_comprovante,
                  id_itemcomprovante,
                  payload,
                  ingested_at,
                  dt_evento,
                  received_at,
                  id_produto_shadow,
                  cfop_shadow,
                  qtd_shadow,
                  valor_unitario_shadow,
                  total_shadow,
                  desconto_shadow,
                  custo_unitario_shadow
                )
                VALUES
                  (%s, 1, 1, 1201, 33, %s::jsonb, now(), %s::timestamptz, now(), 501, 5102, 2, 20, 40, 0, 12),
                  (%s, 1, 1, 1202, 34, %s::jsonb, now(), %s::timestamptz, now(), 501, 5102, 1, 55, 55, 0, 12)
                """,
                (
                    tenant_id,
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_COMPROVANTE": 1201,
                            "ID_ITENSCOMPROVANTE": 33,
                            "ID_PRODUTOS": 501,
                            "CFOP": 5102,
                            "QTDE": 2,
                            "VLRUNITARIO": 20,
                            "TOTAL": 40,
                            "DATA": f"{same_day} 09:00:00",
                        }
                    ),
                    f"{same_day} 09:00:00",
                    tenant_id,
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_COMPROVANTE": 1202,
                            "ID_ITENSCOMPROVANTE": 34,
                            "ID_PRODUTOS": 501,
                            "CFOP": 5102,
                            "QTDE": 1,
                            "VLRUNITARIO": 55,
                            "TOTAL": 55,
                            "DATA": f"{same_day} 10:00:00",
                        }
                    ),
                    f"{same_day} 10:00:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO stg.movprodutos (
                  id_empresa,
                  id_filial,
                  id_db,
                  id_movprodutos,
                  payload,
                  ingested_at,
                  dt_evento,
                  received_at,
                  id_comprovante_shadow,
                  total_venda_shadow,
                  situacao_shadow
                )
                VALUES (
                  %s,
                  1,
                  1,
                  8801,
                  %s::jsonb,
                  now(),
                  %s::timestamptz,
                  now(),
                  1201,
                  40,
                  1
                )
                """,
                (
                    tenant_id,
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_MOVPRODUTOS": 8801,
                            "ID_COMPROVANTE": 1201,
                            "TOTALVENDA": 40,
                            "SITUACAO": 1,
                            "DATA": f"{same_day} 09:00:00",
                        }
                    ),
                    f"{same_day} 09:00:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO stg.itensmovprodutos (
                  id_empresa,
                  id_filial,
                  id_db,
                  id_movprodutos,
                  id_itensmovprodutos,
                  payload,
                  ingested_at,
                  dt_evento,
                  received_at,
                  id_produto_shadow,
                  cfop_shadow,
                  qtd_shadow,
                  valor_unitario_shadow,
                  total_shadow,
                  desconto_shadow,
                  custo_unitario_shadow
                )
                VALUES (
                  %s,
                  1,
                  1,
                  8801,
                  9901,
                  %s::jsonb,
                  now(),
                  %s::timestamptz,
                  now(),
                  501,
                  5102,
                  2,
                  20,
                  40,
                  0,
                  12
                )
                """,
                (
                    tenant_id,
                    json.dumps(
                        {
                            "ID_FILIAL": 1,
                            "ID_DB": 1,
                            "ID_MOVPRODUTOS": 8801,
                            "ID_ITENS_COMPROVANTE": 33,
                            "ID_PRODUTOS": 501,
                            "CFOP": 5102,
                            "QTDE": 2,
                            "VLRUNITARIO": 20,
                            "TOTAL": 40,
                            "DATA": f"{same_day} 09:00:00",
                        }
                    ),
                    f"{same_day} 09:00:00",
                ),
            )
            conn.commit()

        venda_def = self._function_def("load_fact_venda").lower()
        item_def = self._function_def("load_fact_venda_item").lower()
        self.assertNotIn("perform etl.sync_legacy_sales_bridge", venda_def)
        self.assertNotIn("perform etl.backfill_itenscomprovantes_from_legacy", item_def)
        self.assertNotIn("stg.movprodutos", venda_def)
        self.assertNotIn("stg.movprodutos", item_def)
        self.assertNotIn("stg.itensmovprodutos", item_def)
        self.assertNotIn("etl.comprovante_sales_bridge", venda_def)
        self.assertNotIn("etl.comprovante_item_bridge", item_def)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            comprovante_rows = conn.execute("SELECT etl.load_fact_comprovante(%s) AS total", (tenant_id,)).fetchone()
            venda_rows = conn.execute("SELECT etl.load_fact_venda(%s) AS total", (tenant_id,)).fetchone()
            item_rows = conn.execute("SELECT etl.load_fact_venda_item(%s) AS total", (tenant_id,)).fetchone()
            conn.commit()

        self.assertEqual(int(comprovante_rows["total"]), 2)
        self.assertEqual(int(venda_rows["total"]), 2)
        self.assertEqual(int(item_rows["total"]), 2)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            venda_rows = conn.execute(
                """
                SELECT id_comprovante, id_movprodutos
                FROM dw.fact_venda
                WHERE id_empresa = %s
                  AND id_comprovante IN (1201, 1202)
                ORDER BY id_comprovante
                """,
                (tenant_id,),
            ).fetchall()
            item_rows = conn.execute(
                """
                SELECT
                  i.id_comprovante,
                  i.id_movprodutos,
                  i.id_itemcomprovante,
                  i.id_itensmovprodutos
                FROM dw.fact_venda_item i
                WHERE i.id_empresa = %s
                  AND i.id_comprovante IN (1201, 1202)
                ORDER BY i.id_comprovante
                """,
                (tenant_id,),
            ).fetchall()

        venda_map = {int(row["id_comprovante"]): int(row["id_movprodutos"]) for row in venda_rows}
        item_map = {
            int(row["id_comprovante"]): (
                int(row["id_movprodutos"]),
                int(row["id_itemcomprovante"]),
                int(row["id_itensmovprodutos"]),
            )
            for row in item_rows
        }

        self.assertEqual(venda_map[1201], 1201)
        self.assertEqual(venda_map[1202], 1202)
        self.assertEqual(item_map[1201], (1201, 33, 33))
        self.assertEqual(item_map[1202], (1202, 34, 34))

    def test_canonical_comprovante_chain_populates_stg_and_dw_sales_backbone(self) -> None:
        tenant_id, ingest_key = self._create_tenant("Tenant Canonical Comprovante Chain")
        day_one = self._current_date() - timedelta(days=1)
        day_two = self._current_date()

        self._post_ndjson(
            "turnos",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_TURNOS": 71,
                    "ID_DB": 1,
                    "ID_USUARIOS": 501,
                    "TURNO": "1",
                    "DATAABERTURA": f"{day_one.isoformat()} 08:00:00",
                    "DATAFECHAMENTO": f"{day_one.isoformat()} 18:00:00",
                    "ENCERRANTEFECHAMENTO": 1,
                    "STATUS": "CLOSED",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_TURNOS": 72,
                    "ID_DB": 1,
                    "ID_USUARIOS": 501,
                    "TURNO": "2",
                    "DATAABERTURA": f"{day_two.isoformat()} 08:00:00",
                    "DATAFECHAMENTO": f"{day_two.isoformat()} 18:00:00",
                    "ENCERRANTEFECHAMENTO": 1,
                    "STATUS": "CLOSED",
                },
            ],
        )
        self._post_ndjson(
            "comprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 970011,
                    "ID_USUARIOS": 501,
                    "ID_TURNOS": 71,
                    "ID_ENTIDADE": 1001,
                    "VLRTOTAL": 100,
                    "REFERENCIA": 70011,
                    "CANCELADO": False,
                    "SITUACAO": 1,
                    "SAIDAS_ENTRADAS": 1,
                    "CFOP": "5102",
                    "DTACONTA": day_one.isoformat(),
                    "DATA": f"{day_one.isoformat()} 10:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 970012,
                    "ID_USUARIOS": 501,
                    "ID_TURNOS": 72,
                    "ID_ENTIDADE": 1001,
                    "VLRTOTAL": 150,
                    "REFERENCIA": 70012,
                    "CANCELADO": False,
                    "SITUACAO": 1,
                    "SAIDAS_ENTRADAS": 1,
                    "CFOP": "5102",
                    "DTACONTA": day_two.isoformat(),
                    "DATA": f"{day_two.isoformat()} 10:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 970013,
                    "ID_USUARIOS": 501,
                    "ID_TURNOS": 72,
                    "ID_ENTIDADE": 1001,
                    "VLRTOTAL": 30,
                    "REFERENCIA": 70013,
                    "CANCELADO": True,
                    "SITUACAO": 2,
                    "SAIDAS_ENTRADAS": 1,
                    "CFOP": "5102",
                    "DTACONTA": day_two.isoformat(),
                    "DATA": f"{day_two.isoformat()} 11:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 970014,
                    "ID_USUARIOS": 501,
                    "ID_TURNOS": 72,
                    "ID_ENTIDADE": 1001,
                    "VLRTOTAL": 40,
                    "REFERENCIA": 70014,
                    "CANCELADO": False,
                    "SITUACAO": 1,
                    "SAIDAS_ENTRADAS": 0,
                    "CFOP": "1202",
                    "DTACONTA": day_two.isoformat(),
                    "DATA": f"{day_two.isoformat()} 12:00:00",
                },
            ],
        )
        self._post_ndjson(
            "itenscomprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 970011,
                    "ID_ITENS_COMPROVANTE": 1,
                    "ID_PRODUTOS": 5010,
                    "ID_GRUPOPRODUTOS": 1,
                    "ID_FUNCIONARIOS": 601,
                    "CFOP": "5102",
                    "QTDE": 10,
                    "VLRUNITARIO": 10,
                    "TOTAL": 100,
                    "VLRDESCONTO": 0,
                    "VLRCUSTOCOMICMS": 6,
                    "DATA": f"{day_one.isoformat()} 10:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 970012,
                    "ID_ITENS_COMPROVANTE": 1,
                    "ID_PRODUTOS": 5010,
                    "ID_GRUPOPRODUTOS": 1,
                    "ID_FUNCIONARIOS": 601,
                    "CFOP": "5102",
                    "QTDE": 15,
                    "VLRUNITARIO": 10,
                    "TOTAL": 150,
                    "VLRDESCONTO": 0,
                    "VLRCUSTOCOMICMS": 6,
                    "DATA": f"{day_two.isoformat()} 10:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 970013,
                    "ID_ITENS_COMPROVANTE": 1,
                    "ID_PRODUTOS": 5010,
                    "ID_GRUPOPRODUTOS": 1,
                    "ID_FUNCIONARIOS": 601,
                    "CFOP": "5102",
                    "QTDE": 3,
                    "VLRUNITARIO": 10,
                    "TOTAL": 30,
                    "VLRDESCONTO": 0,
                    "VLRCUSTOCOMICMS": 6,
                    "DATA": f"{day_two.isoformat()} 11:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 970014,
                    "ID_ITENS_COMPROVANTE": 1,
                    "ID_PRODUTOS": 5011,
                    "ID_GRUPOPRODUTOS": 2,
                    "ID_FUNCIONARIOS": 601,
                    "CFOP": "1202",
                    "QTDE": 4,
                    "VLRUNITARIO": 10,
                    "TOTAL": 40,
                    "VLRDESCONTO": 0,
                    "VLRCUSTOCOMICMS": 6,
                    "DATA": f"{day_two.isoformat()} 12:00:00",
                },
            ],
        )
        self._post_ndjson(
            "formas_pgto_comprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_REFERENCIA": 70011,
                    "REFERENCIA": 70011,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 970011,
                    "ID_TURNOS": 71,
                    "ID_USUARIOS": 501,
                    "TIPO_FORMA": 1,
                    "VALOR": 100,
                    "DATA": f"{day_one.isoformat()} 10:05:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_REFERENCIA": 70012,
                    "REFERENCIA": 70012,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 970012,
                    "ID_TURNOS": 72,
                    "ID_USUARIOS": 501,
                    "TIPO_FORMA": 1,
                    "VALOR": 150,
                    "DATA": f"{day_two.isoformat()} 10:05:00",
                },
            ],
        )

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute("SELECT etl.load_fact_comprovante(%s)", (tenant_id,))
            conn.execute("SELECT etl.load_fact_caixa_turno(%s)", (tenant_id,))
            conn.execute("SELECT etl.load_fact_pagamento_comprovante(%s)", (tenant_id,))
            conn.execute("SELECT etl.load_fact_venda(%s)", (tenant_id,))
            conn.execute("SELECT etl.load_fact_venda_item(%s)", (tenant_id,))
            conn.execute(
                """
                SELECT etl.refresh_marts(%s::jsonb, %s::date)
                """,
                (
                    json.dumps(
                        {
                            "fact_comprovante": 1,
                            "fact_caixa_turno": 1,
                            "fact_pagamento_comprovante": 1,
                            "fact_venda": 1,
                            "fact_venda_item": 1,
                        }
                    ),
                    day_two.isoformat(),
                ),
            )
            stg_counts = conn.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM stg.comprovantes WHERE id_empresa = %s) AS comprovantes,
                  (SELECT COUNT(*) FROM stg.itenscomprovantes WHERE id_empresa = %s) AS itenscomprovantes,
                  (SELECT COUNT(*) FROM stg.formas_pgto_comprovantes WHERE id_empresa = %s) AS pagamentos,
                  (SELECT COUNT(*) FROM stg.turnos WHERE id_empresa = %s) AS turnos
                """,
                (tenant_id, tenant_id, tenant_id, tenant_id),
            ).fetchone()
            dw_counts = conn.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM dw.fact_comprovante WHERE id_empresa = %s) AS comprovantes,
                  (SELECT COUNT(*) FROM dw.fact_venda WHERE id_empresa = %s) AS vendas,
                  (SELECT COUNT(*) FROM dw.fact_venda_item WHERE id_empresa = %s) AS itens,
                  (SELECT COUNT(*) FROM dw.fact_pagamento_comprovante WHERE id_empresa = %s) AS pagamentos,
                  (SELECT COUNT(*) FROM dw.fact_caixa_turno WHERE id_empresa = %s) AS turnos
                """,
                (tenant_id, tenant_id, tenant_id, tenant_id, tenant_id),
            ).fetchone()
            comercial = conn.execute(
                """
                SELECT
                  COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND payload->>'CFOP' = '5102'), 0)::numeric(18,2) AS saidas,
                  COALESCE(SUM(valor_total) FILTER (WHERE cancelado = true AND payload->>'CFOP' = '5102'), 0)::numeric(18,2) AS cancelamentos,
                  COALESCE(SUM(valor_total) FILTER (WHERE cancelado = false AND payload->>'CFOP' = '1202'), 0)::numeric(18,2) AS entradas
                FROM dw.fact_comprovante
                WHERE id_empresa = %s
                """,
                (tenant_id,),
            ).fetchone()
            pagamentos = conn.execute(
                """
                SELECT COALESCE(SUM(valor), 0)::numeric(18,2) AS total_pagamentos
                FROM dw.fact_pagamento_comprovante
                WHERE id_empresa = %s
                  AND cash_eligible = true
                """,
                (tenant_id,),
            ).fetchone()
            analytical = conn.execute(
                """
                SELECT
                  COALESCE(SUM(i.total) FILTER (WHERE COALESCE(v.situacao, 0) = 1 AND COALESCE(i.cfop, 0) > 5000), 0)::numeric(18,2) AS faturamento,
                  COALESCE(SUM(i.total) FILTER (WHERE COALESCE(v.situacao, 0) = 2 AND COALESCE(i.cfop, 0) > 5000), 0)::numeric(18,2) AS cancelamentos,
                  COALESCE(SUM(i.total) FILTER (WHERE COALESCE(v.situacao, 0) = 1 AND COALESCE(i.cfop, 0) <= 3999), 0)::numeric(18,2) AS entradas
                FROM dw.fact_venda v
                JOIN dw.fact_venda_item i
                  ON i.id_empresa = v.id_empresa
                 AND i.id_filial = v.id_filial
                 AND i.id_db = v.id_db
                 AND i.id_comprovante = v.id_comprovante
                WHERE v.id_empresa = %s
                """,
                (tenant_id,),
            ).fetchone()
            conn.commit()

        self.assertEqual(int(stg_counts["comprovantes"]), 4)
        self.assertEqual(int(stg_counts["itenscomprovantes"]), 4)
        self.assertEqual(int(stg_counts["pagamentos"]), 2)
        self.assertEqual(int(stg_counts["turnos"]), 2)

        self.assertEqual(int(dw_counts["comprovantes"]), 4)
        self.assertEqual(int(dw_counts["vendas"]), 4)
        self.assertEqual(int(dw_counts["itens"]), 4)
        self.assertEqual(int(dw_counts["pagamentos"]), 2)
        self.assertEqual(int(dw_counts["turnos"]), 2)

        self.assertAlmostEqual(float(comercial["saidas"]), 250.0, places=2)
        self.assertAlmostEqual(float(comercial["cancelamentos"]), 30.0, places=2)
        self.assertAlmostEqual(float(comercial["entradas"]), 40.0, places=2)
        self.assertAlmostEqual(float(pagamentos["total_pagamentos"]), 250.0, places=2)
        self.assertAlmostEqual(float(analytical["faturamento"]), 250.0, places=2)
        self.assertAlmostEqual(float(analytical["cancelamentos"]), 30.0, places=2)
        self.assertAlmostEqual(float(analytical["entradas"]), 40.0, places=2)

    def test_ingest_sanitizes_null_bytes_before_bulk_upsert(self) -> None:
        tenant_id, ingest_key = self._create_tenant("Tenant Null Byte Sanitization")
        ingest_result = self._post_ndjson(
            "usuarios",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_USUARIO": 501,
                    "NOME": "Ana\x00 Maria",
                    "OBS": {"apelido": "A\x00na"},
                    "TAGS": ["ativo", "vip\x00"],
                }
            ],
        )

        self.assertTrue(bool(ingest_result["ok"]))
        self.assertEqual(int(ingest_result["inserted"]), 1)
        self.assertEqual(int(ingest_result["updated"]), 0)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            usuario = conn.execute(
                """
                SELECT payload
                FROM stg.usuarios
                WHERE id_empresa = %s AND id_filial = 1 AND id_usuario = 501
                """,
                (tenant_id,),
            ).fetchone()

        payload = dict(usuario["payload"])
        self.assertEqual(payload["NOME"], "Ana Maria")
        self.assertEqual(payload["OBS"]["apelido"], "Ana")
        self.assertEqual(payload["TAGS"], ["ativo", "vip"])

    def test_sales_loaders_apply_icms_cost_and_cash_semantics_for_status_turn_and_account_date(self) -> None:
        tenant_id, ingest_key = self._create_tenant("Tenant Cash Financial Semantics")
        ref_date = self._current_date()
        same_day = ref_date.isoformat()
        next_day = (ref_date + timedelta(days=1)).isoformat()

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_produto (id_empresa, id_filial, id_produto, nome, custo_medio)
                VALUES (%s, 1, 501, 'Produto Semântica', 99)
                ON CONFLICT (id_empresa, id_filial, id_produto)
                DO UPDATE SET nome = EXCLUDED.nome, custo_medio = EXCLUDED.custo_medio
                """,
                (tenant_id,),
            )
            conn.commit()

        self._post_ndjson(
            "comprovantes",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 9001,
                    "ID_USUARIOS": 11,
                    "ID_TURNOS": 3,
                    "VLRTOTAL": 60,
                    "CANCELADO": True,
                    "SITUACAO": 3,
                    "REFERENCIA": 99001,
                    "DATA": f"{same_day} 10:00:00",
                    "DTACONTA": same_day,
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 9002,
                    "ID_USUARIOS": 12,
                    "ID_TURNOS": 4,
                    "VLRTOTAL": 40,
                    "CANCELADO": False,
                    "SITUACAO": 2,
                    "REFERENCIA": 99002,
                    "DATA": f"{same_day} 11:00:00",
                    "DTACONTA": same_day,
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 9003,
                    "ID_USUARIOS": 13,
                    "ID_TURNOS": 1,
                    "VLRTOTAL": 25,
                    "CANCELADO": False,
                    "SITUACAO": 1,
                    "REFERENCIA": 99003,
                    "DATA": f"{same_day} 12:00:00",
                    "DTACONTA": same_day,
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_COMPROVANTE": 9004,
                    "ID_USUARIOS": 14,
                    "ID_TURNOS": 5,
                    "VLRTOTAL": 35,
                    "CANCELADO": False,
                    "SITUACAO": 1,
                    "REFERENCIA": 99004,
                    "DATA": f"{same_day} 13:00:00",
                    "DTACONTA": next_day,
                },
            ],
        )
        self._post_ndjson(
            "movprodutos",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 9101,
                    "ID_COMPROVANTE": 9001,
                    "ID_USUARIOS": 11,
                    "ID_TURNOS": 3,
                    "SAIDAS_ENTRADAS": 1,
                    "TOTALVENDA": 60,
                    "SITUACAO": 3,
                    "DATA": f"{same_day} 10:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 9102,
                    "ID_COMPROVANTE": 9002,
                    "ID_USUARIOS": 12,
                    "ID_TURNOS": 4,
                    "SAIDAS_ENTRADAS": 1,
                    "TOTALVENDA": 40,
                    "SITUACAO": 2,
                    "DATA": f"{same_day} 11:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 9103,
                    "ID_COMPROVANTE": 9003,
                    "ID_USUARIOS": 13,
                    "ID_TURNOS": 1,
                    "SAIDAS_ENTRADAS": 1,
                    "TOTALVENDA": 25,
                    "SITUACAO": 1,
                    "DATA": f"{same_day} 12:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 9104,
                    "ID_COMPROVANTE": 9004,
                    "ID_USUARIOS": 14,
                    "ID_TURNOS": 5,
                    "SAIDAS_ENTRADAS": 1,
                    "TOTALVENDA": 35,
                    "SITUACAO": 1,
                    "DATA": f"{same_day} 13:00:00",
                },
            ],
        )
        self._post_ndjson(
            "itensmovprodutos",
            ingest_key,
            [
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 9101,
                    "ID_ITENSMOVPRODUTOS": 1,
                    "ID_PRODUTOS": 501,
                    "CFOP": 5102,
                    "QTDE": 2,
                    "VLRUNITARIO": 30,
                    "TOTAL": 60,
                    "VLRCUSTOCOMICMS": 18,
                    "VLRCUSTO": 10,
                    "DATA": f"{same_day} 10:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 9102,
                    "ID_ITENSMOVPRODUTOS": 1,
                    "ID_PRODUTOS": 501,
                    "CFOP": 5102,
                    "QTDE": 1,
                    "VLRUNITARIO": 40,
                    "TOTAL": 40,
                    "VLRCUSTO": 10,
                    "DATA": f"{same_day} 11:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 9103,
                    "ID_ITENSMOVPRODUTOS": 1,
                    "ID_PRODUTOS": 501,
                    "CFOP": 5102,
                    "QTDE": 1,
                    "VLRUNITARIO": 25,
                    "TOTAL": 25,
                    "VLRCUSTO": 10,
                    "DATA": f"{same_day} 12:00:00",
                },
                {
                    "ID_FILIAL": 1,
                    "ID_DB": 1,
                    "ID_MOVPRODUTOS": 9104,
                    "ID_ITENSMOVPRODUTOS": 1,
                    "ID_PRODUTOS": 501,
                    "CFOP": 5102,
                    "QTDE": 1,
                    "VLRUNITARIO": 35,
                    "TOTAL": 35,
                    "VLRCUSTO": 10,
                    "DATA": f"{same_day} 13:00:00",
                },
            ],
        )
        self._post_ndjson(
            "formas_pgto_comprovantes",
            ingest_key,
            [
                {"ID_FILIAL": 1, "ID_REFERENCIA": 99001, "TIPO_FORMA": 28, "VALOR": 60, "DATA": f"{same_day} 10:05:00"},
                {"ID_FILIAL": 1, "ID_REFERENCIA": 99002, "TIPO_FORMA": 6, "VALOR": 40, "DATA": f"{same_day} 11:05:00"},
                {"ID_FILIAL": 1, "ID_REFERENCIA": 99003, "TIPO_FORMA": 1, "VALOR": 25, "DATA": f"{same_day} 12:05:00"},
                {"ID_FILIAL": 1, "ID_REFERENCIA": 99004, "TIPO_FORMA": 3, "VALOR": 35, "DATA": f"{same_day} 13:05:00"},
            ],
        )

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            comp_rows = conn.execute("SELECT etl.load_fact_comprovante(%s) AS total", (tenant_id,)).fetchone()
            venda_rows = conn.execute("SELECT etl.load_fact_venda(%s) AS total", (tenant_id,)).fetchone()
            item_rows = conn.execute("SELECT etl.load_fact_venda_item(%s) AS total", (tenant_id,)).fetchone()
            pgto_rows = conn.execute("SELECT etl.load_fact_pagamento_comprovante(%s) AS total", (tenant_id,)).fetchone()
            conn.commit()

        self.assertEqual(int(comp_rows["total"]), 4)
        self.assertEqual(int(venda_rows["total"]), 4)
        self.assertEqual(int(item_rows["total"]), 4)
        self.assertEqual(int(pgto_rows["total"]), 4)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            comprovantes = conn.execute(
                """
                SELECT id_comprovante, cancelado, situacao, data_conta, cash_eligible
                FROM dw.fact_comprovante
                WHERE id_empresa = %s
                  AND id_comprovante BETWEEN 9001 AND 9004
                ORDER BY id_comprovante
                """,
                (tenant_id,),
            ).fetchall()
            vendas = conn.execute(
                """
                SELECT id_comprovante, id_movprodutos, cancelado, situacao
                FROM dw.fact_venda
                WHERE id_empresa = %s
                  AND id_comprovante BETWEEN 9001 AND 9004
                ORDER BY id_comprovante
                """,
                (tenant_id,),
            ).fetchall()
            item = conn.execute(
                """
                SELECT id_comprovante, id_itemcomprovante, id_movprodutos, id_itensmovprodutos, custo_total, margem
                FROM dw.fact_venda_item
                WHERE id_empresa = %s
                  AND id_comprovante = 9001
                  AND id_itemcomprovante = 1
                """,
                (tenant_id,),
            ).fetchone()
            pagamentos = conn.execute(
                """
                SELECT referencia, data_conta, cash_eligible
                FROM dw.fact_pagamento_comprovante
                WHERE id_empresa = %s
                  AND referencia BETWEEN 99001 AND 99004
                ORDER BY referencia
                """,
                (tenant_id,),
            ).fetchall()

        comp_map = {int(row["id_comprovante"]): row for row in comprovantes}
        venda_map = {int(row["id_comprovante"]): row for row in vendas}
        pagamento_map = {int(row["referencia"]): row for row in pagamentos}

        self.assertFalse(bool(comp_map[9001]["cancelado"]))
        self.assertEqual(int(comp_map[9001]["situacao"]), 3)
        self.assertEqual(comp_map[9001]["data_conta"].isoformat(), same_day)
        self.assertTrue(bool(comp_map[9001]["cash_eligible"]))

        self.assertTrue(bool(comp_map[9002]["cancelado"]))
        self.assertEqual(int(comp_map[9002]["situacao"]), 2)
        self.assertTrue(bool(comp_map[9002]["cash_eligible"]))

        self.assertFalse(bool(comp_map[9003]["cancelado"]))
        self.assertFalse(bool(comp_map[9003]["cash_eligible"]))

        self.assertFalse(bool(comp_map[9004]["cancelado"]))
        self.assertEqual(comp_map[9004]["data_conta"].isoformat(), next_day)
        self.assertFalse(bool(comp_map[9004]["cash_eligible"]))

        self.assertEqual(int(venda_map[9001]["id_movprodutos"]), 9001)
        self.assertFalse(bool(venda_map[9001]["cancelado"]))
        self.assertEqual(int(venda_map[9001]["situacao"]), 3)
        self.assertEqual(int(venda_map[9002]["id_movprodutos"]), 9002)
        self.assertTrue(bool(venda_map[9002]["cancelado"]))
        self.assertEqual(int(venda_map[9002]["situacao"]), 2)
        self.assertEqual(int(venda_map[9003]["id_movprodutos"]), 9003)
        self.assertFalse(bool(venda_map[9003]["cancelado"]))
        self.assertEqual(int(venda_map[9003]["situacao"]), 1)
        self.assertEqual(int(venda_map[9004]["id_movprodutos"]), 9004)
        self.assertFalse(bool(venda_map[9004]["cancelado"]))
        self.assertEqual(int(venda_map[9004]["situacao"]), 1)

        self.assertEqual(int(item["id_movprodutos"]), 9001)
        self.assertEqual(int(item["id_itensmovprodutos"]), 1)
        self.assertAlmostEqual(float(item["custo_total"]), 36, places=2)
        self.assertAlmostEqual(float(item["margem"]), 24, places=2)

        self.assertTrue(bool(pagamento_map[99001]["cash_eligible"]))
        self.assertEqual(pagamento_map[99001]["data_conta"].isoformat(), same_day)
        self.assertTrue(bool(pagamento_map[99002]["cash_eligible"]))
        self.assertFalse(bool(pagamento_map[99003]["cash_eligible"]))
        self.assertFalse(bool(pagamento_map[99004]["cash_eligible"]))

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
