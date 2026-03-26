from __future__ import annotations

import json
import unittest
from datetime import date, timedelta
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from fastapi.testclient import TestClient

from app.cli import reconcile_sales as reconcile_sales_cli
from app.cli import seed as seed_cli
from app.db import get_conn
from app.main import app
from app.security import hash_password


class PlatformBackofficeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def _unique_email(self, prefix: str) -> str:
        return f"{prefix}.{uuid4().hex[:10]}@torqmind.test"

    def _create_channel(self, name: str) -> int:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                "INSERT INTO app.channels (name, is_enabled) VALUES (%s, true) RETURNING id",
                (name,),
            ).fetchone()
            conn.commit()
            return int(row["id"])

    def _create_tenant(self, name: str, channel_id: int | None = None, is_active: bool = True, status: str = "active") -> int:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                """
                INSERT INTO app.tenants (
                  nome,
                  is_active,
                  status,
                  billing_status,
                  valid_from,
                  channel_id
                )
                VALUES (%s, %s, %s, 'current', CURRENT_DATE, %s)
                RETURNING id_empresa
                """,
                (name, is_active, status, channel_id),
            ).fetchone()
            conn.commit()
            return int(row["id_empresa"])

    def _create_branch(self, tenant_id: int, branch_id: int, name: str, is_active: bool = True) -> None:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO auth.filiais (id_empresa, id_filial, nome, is_active, valid_from)
                VALUES (%s, %s, %s, %s, CURRENT_DATE)
                ON CONFLICT (id_empresa, id_filial)
                DO UPDATE SET nome = EXCLUDED.nome, is_active = EXCLUDED.is_active
                """,
                (tenant_id, branch_id, name, is_active),
            )
            conn.commit()

    def _upsert_stg_branch(self, tenant_id: int, branch_id: int, name: str, cnpj: str | None = None) -> None:
        payload = json.dumps(
            {
                "ID_FILIAL": branch_id,
                "NOMEFILIAL": name,
                "CNPJ": cnpj,
            },
            ensure_ascii=False,
        )
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO stg.filiais (id_empresa, id_filial, payload)
                VALUES (%s, %s, %s::jsonb)
                ON CONFLICT (id_empresa, id_filial)
                DO UPDATE SET payload = EXCLUDED.payload, ingested_at = now()
                """,
                (tenant_id, branch_id, payload),
            )
            conn.commit()

    def _deactivate_active_contracts(self, tenant_id: int) -> None:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                UPDATE billing.contracts
                SET
                  is_enabled = false,
                  end_date = COALESCE(end_date, CURRENT_DATE),
                  updated_at = now()
                WHERE tenant_id = %s
                  AND is_enabled = true
                """,
                (tenant_id,),
            )
            conn.commit()

    def _refresh_sales_marts(self) -> None:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_vendas_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_vendas_hora")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_produtos_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_grupos_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_funcionarios_diaria")
            conn.commit()

    def _create_user(
        self,
        role: str,
        password: str,
        *,
        email: str | None = None,
        tenant_id: int | None = None,
        branch_id: int | None = None,
        channel_id: int | None = None,
        is_active: bool = True,
    ) -> str:
        email = email or self._unique_email(role)
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            user = conn.execute(
                """
                INSERT INTO auth.users (email, password_hash, nome, role, is_active, valid_from)
                VALUES (%s, %s, %s, %s, %s, CURRENT_DATE)
                RETURNING id
                """,
                (email, hash_password(password), role, role, is_active),
            ).fetchone()
            conn.execute(
                """
                INSERT INTO auth.user_tenants (
                  user_id,
                  role,
                  channel_id,
                  id_empresa,
                  id_filial,
                  is_enabled,
                  valid_from
                )
                VALUES (%s::uuid, %s, %s, %s, %s, true, CURRENT_DATE)
                """,
                (user["id"], role, channel_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO app.user_notification_settings (user_id, email)
                VALUES (%s::uuid, %s)
                ON CONFLICT (user_id) DO NOTHING
                """,
                (user["id"], email),
            )
            conn.commit()
            return email

    def _login(self, email: str, password: str, expected_status: int = 200):
        response = self.client.post("/auth/login", json={"email": email, "password": password})
        self.assertEqual(response.status_code, expected_status, response.text)
        return response

    def _auth_headers(self, email: str, password: str) -> dict[str, str]:
        token = self._login(email, password).json()["access_token"]
        return {"Authorization": f"Bearer {token}"}

    def _user_id_by_email(self, email: str) -> str:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                "SELECT id::text AS id FROM auth.users WHERE lower(email) = lower(%s)",
                (email,),
            ).fetchone()
            conn.commit()
        self.assertIsNotNone(row, email)
        return str(row["id"])

    def _ensure_sovereign_user(self) -> str:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            row = conn.execute(
                """
                INSERT INTO auth.users (
                  email,
                  password_hash,
                  nome,
                  role,
                  is_active,
                  valid_from,
                  must_change_password,
                  failed_login_count,
                  locked_until
                )
                VALUES (%s, %s, %s, 'platform_master', true, CURRENT_DATE, false, 0, NULL)
                ON CONFLICT (email)
                DO UPDATE SET
                  password_hash = EXCLUDED.password_hash,
                  nome = EXCLUDED.nome,
                  role = 'platform_master',
                  is_active = true,
                  must_change_password = false,
                  failed_login_count = 0,
                  locked_until = NULL,
                  valid_from = COALESCE(auth.users.valid_from, CURRENT_DATE)
                RETURNING id::text AS id
                """,
                (
                    seed_cli.PLATFORM_MASTER_EMAIL,
                    hash_password(seed_cli.PLATFORM_MASTER_PASSWORD),
                    "TorqMind Sovereign Master",
                ),
            ).fetchone()
            conn.execute("DELETE FROM auth.user_tenants WHERE user_id = %s::uuid", (row["id"],))
            conn.execute(
                """
                INSERT INTO auth.user_tenants (
                  user_id,
                  role,
                  channel_id,
                  id_empresa,
                  id_filial,
                  is_enabled,
                  valid_from,
                  valid_until
                )
                VALUES (%s::uuid, 'platform_master', NULL, NULL, NULL, true, CURRENT_DATE, NULL)
                """,
                (row["id"],),
            )
            conn.execute(
                """
                INSERT INTO app.user_notification_settings (user_id, email)
                VALUES (%s::uuid, %s)
                ON CONFLICT (user_id) DO UPDATE SET email = EXCLUDED.email
                """,
                (row["id"], seed_cli.PLATFORM_MASTER_EMAIL),
            )
            conn.commit()
        return seed_cli.PLATFORM_MASTER_EMAIL

    def test_login_blocks_inactive_user_company_and_branch(self) -> None:
        tenant_id = self._create_tenant("Tenant login inactive", is_active=True)
        self._create_branch(tenant_id, 991, "Filial 991", is_active=True)

        disabled_user_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id, is_active=False)
        response_user = self._login(disabled_user_email, "Senha@123", expected_status=403)
        self.assertEqual(response_user.json()["error"], "user_disabled")

        inactive_tenant_id = self._create_tenant("Tenant disabled", is_active=False)
        inactive_tenant_email = self._create_user("tenant_admin", "Senha@123", tenant_id=inactive_tenant_id)
        response_tenant = self._login(inactive_tenant_email, "Senha@123", expected_status=403)
        self.assertEqual(response_tenant.json()["error"], "tenant_disabled")

        branch_tenant_id = self._create_tenant("Tenant branch disabled", is_active=True)
        self._create_branch(branch_tenant_id, 992, "Filial 992", is_active=False)
        branch_user_email = self._create_user("tenant_manager", "Senha@123", tenant_id=branch_tenant_id, branch_id=992)
        response_branch = self._login(branch_user_email, "Senha@123", expected_status=403)
        self.assertEqual(response_branch.json()["error"], "branch_disabled")

    def test_channel_admin_cannot_access_platform_finance(self) -> None:
        channel_id = self._create_channel("Canal Restrito")
        tenant_id = self._create_tenant("Tenant Canal", channel_id=channel_id)
        self._create_branch(tenant_id, 993, "Filial 993", is_active=True)
        email = self._create_user("channel_admin", "Senha@123", channel_id=channel_id)

        headers = self._auth_headers(email, "Senha@123")
        companies_response = self.client.get("/platform/companies?limit=10", headers=headers)
        self.assertEqual(companies_response.status_code, 200, companies_response.text)
        finance_response = self.client.get("/platform/receivables?limit=10", headers=headers)
        self.assertEqual(finance_response.status_code, 403, finance_response.text)
        self.assertEqual(finance_response.json()["error"], "platform_finance_forbidden")

    def test_channel_admin_gets_product_access_limited_to_channel_portfolio(self) -> None:
        channel_id = self._create_channel("Canal Produto")
        tenant_id = self._create_tenant("Tenant Canal Produto", channel_id=channel_id)
        other_tenant_id = self._create_tenant("Tenant Outro Canal", channel_id=self._create_channel("Canal Secundário"))
        branch_id = 991
        self._create_branch(tenant_id, branch_id, "Filial 991", is_active=True)
        self._create_branch(other_tenant_id, 992, "Filial 992", is_active=True)
        email = self._create_user("channel_admin", "Senha@123", channel_id=channel_id)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_grupo_produto (id_empresa, id_filial, id_grupo_produto, nome)
                VALUES (%s, %s, 1, 'COMBUSTIVEIS')
                ON CONFLICT (id_empresa, id_filial, id_grupo_produto)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.dim_produto (id_empresa, id_filial, id_produto, nome, id_grupo_produto, unidade)
                VALUES (%s, %s, 101, 'GASOLINA COMUM', 1, 'LT')
                ON CONFLICT (id_empresa, id_filial, id_produto)
                DO UPDATE SET nome = EXCLUDED.nome, id_grupo_produto = EXCLUDED.id_grupo_produto, unidade = EXCLUDED.unidade
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  id_comprovante, saidas_entradas, total_venda, cancelado, payload
                )
                VALUES (%s, %s, 1, 501, %s, %s, 601, 1, 150, false, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos)
                DO UPDATE SET data = EXCLUDED.data, data_key = EXCLUDED.data_key, total_venda = EXCLUDED.total_venda
                """,
                (tenant_id, branch_id, "2026-03-20 09:00:00", 20260320),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, id_grupo_produto, cfop, qtd, valor_unitario, total, desconto, custo_total, margem, payload
                )
                VALUES (%s, %s, 1, 501, 1, %s, 101, 1, 5102, 10, 15, 150, 0, 120, 30, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
                DO UPDATE SET total = EXCLUDED.total, margem = EXCLUDED.margem
                """,
                (tenant_id, branch_id, 20260320),
            )
            conn.commit()
        self._refresh_sales_marts()

        login_response = self._login(email, "Senha@123")
        self.assertTrue(login_response.json()["home_path"].startswith("/dashboard?"), login_response.json()["home_path"])

        headers = {"Authorization": f"Bearer {login_response.json()['access_token']}"}
        me_response = self.client.get("/auth/me", headers=headers)
        self.assertEqual(me_response.status_code, 200, me_response.text)
        me_body = me_response.json()
        self.assertEqual(me_body["user_role"], "channel_admin")
        self.assertTrue(bool(me_body["access"]["platform"]))
        self.assertTrue(bool(me_body["access"]["product"]))
        self.assertFalse(bool(me_body["access"]["platform_finance"]))
        self.assertIn(tenant_id, me_body["tenant_ids"])
        self.assertNotIn(other_tenant_id, me_body["tenant_ids"])
        self.assertTrue(any(int(company["id_empresa"]) == tenant_id for company in me_body["product_companies"]))

        filiais_ok = self.client.get(f"/bi/filiais?id_empresa={tenant_id}", headers=headers)
        self.assertEqual(filiais_ok.status_code, 200, filiais_ok.text)
        filiais_forbidden = self.client.get(f"/bi/filiais?id_empresa={other_tenant_id}", headers=headers)
        self.assertEqual(filiais_forbidden.status_code, 403, filiais_forbidden.text)

        sales_response = self.client.get(
            f"/bi/sales/overview?dt_ini=2026-03-20&dt_fim=2026-03-20&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(sales_response.status_code, 200, sales_response.text)
        self.assertEqual(float(sales_response.json()["kpis"]["faturamento"]), 150.0)

    def test_sales_top_groups_keep_operational_group_without_combustiveis_bucket_leak(self) -> None:
        tenant_id = self._create_tenant("Tenant Sales Reconcile")
        branch_id = 997
        self._create_branch(tenant_id, branch_id, "Filial 997", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_grupo_produto (id_empresa, id_filial, id_grupo_produto, nome)
                VALUES
                  (%s, %s, 10, 'COMBUSTIVEIS'),
                  (%s, %s, 11, 'FILTROS DE COMBUSTIVEIS')
                ON CONFLICT (id_empresa, id_filial, id_grupo_produto)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.dim_produto (id_empresa, id_filial, id_produto, nome, id_grupo_produto, unidade)
                VALUES
                  (%s, %s, 201, 'GASOLINA COMUM', 10, 'LT'),
                  (%s, %s, 202, 'FILTRO DE COMBUSTIVEL', 11, 'UN')
                ON CONFLICT (id_empresa, id_filial, id_produto)
                DO UPDATE SET nome = EXCLUDED.nome, id_grupo_produto = EXCLUDED.id_grupo_produto, unidade = EXCLUDED.unidade
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  id_comprovante, saidas_entradas, total_venda, cancelado, payload
                )
                VALUES
                  (%s, %s, 1, 701, %s, %s, 801, 1, 115336.56, false, '{}'::jsonb),
                  (%s, %s, 1, 702, %s, %s, 802, 1, 89.00, false, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos)
                DO UPDATE SET data = EXCLUDED.data, data_key = EXCLUDED.data_key, total_venda = EXCLUDED.total_venda
                """,
                (tenant_id, branch_id, "2026-03-07 08:00:00", 20260307, tenant_id, branch_id, "2026-03-07 09:00:00", 20260307),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, id_grupo_produto, cfop, qtd, valor_unitario, total, desconto, custo_total, margem, payload
                )
                VALUES
                  (%s, %s, 1, 701, 1, %s, 201, 10, 5102, 368, 313.414565, 115336.56, 0, 100000, 15336.56, '{}'::jsonb),
                  (%s, %s, 1, 702, 1, %s, 202, 11, 5102, 1, 89, 89.00, 0, 40, 49, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
                DO UPDATE SET total = EXCLUDED.total, margem = EXCLUDED.margem
                """,
                (tenant_id, branch_id, 20260307, tenant_id, branch_id, 20260307),
            )
            conn.commit()
        self._refresh_sales_marts()

        headers = self._auth_headers(owner_email, "Senha@123")
        response = self.client.get(
            f"/bi/sales/overview?dt_ini=2026-03-07&dt_fim=2026-03-07&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        top_groups = {row["grupo_nome"]: float(row["faturamento"]) for row in response.json()["top_groups"]}
        self.assertEqual(top_groups["COMBUSTIVEIS"], 115336.56)
        self.assertEqual(top_groups["FILTROS DE COMBUSTIVEIS"], 89.0)
        self.assertNotIn("Combustíveis", top_groups)

        reconciliation = reconcile_sales_cli.reconcile_sales(
            tenant_id=tenant_id,
            target_date=date(2026, 3, 7),
            branch_id=branch_id,
            group="COMBUSTIVEIS",
            detail_limit=5,
        )
        self.assertEqual(reconciliation["totals"]["source_operational"], 0.0)
        self.assertEqual(reconciliation["totals"]["dw"], 115336.56)
        self.assertEqual(reconciliation["totals"]["mart"], 115336.56)
        self.assertEqual(reconciliation["totals"]["endpoint"], 115336.56)
        self.assertEqual(reconciliation["legacy_bucket"]["total"], 115425.56)
        self.assertEqual(reconciliation["deltas"]["legacy_bucket_extra"], 89.0)
        self.assertEqual(reconciliation["legacy_bucket"]["extra_groups"][0]["grupo_nome"], "FILTROS DE COMBUSTIVEIS")

    def test_platform_master_scope_and_product_user_auto_scope_use_latest_operational_date(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        master_login = self._login(master_email, "Senha@123")
        self.assertNotEqual(master_login.json()["home_path"], "/scope")
        self.assertTrue(
            master_login.json()["home_path"] == "/platform"
            or master_login.json()["home_path"].startswith("/dashboard?"),
            master_login.json()["home_path"],
        )

        tenant_id = self._create_tenant("Tenant Auto Scope")
        branch_id = 994
        self._create_branch(tenant_id, branch_id, "Filial 994", is_active=True)

        manager_email = self._create_user(
            "tenant_manager",
            "Senha@123",
            tenant_id=tenant_id,
            branch_id=branch_id,
        )

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                UPDATE app.tenants
                SET default_product_scope_days = %s
                WHERE id_empresa = %s
                """,
                (30, tenant_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  total_venda, payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)
                """,
                (tenant_id, branch_id, 1, 1001, "2026-03-18 12:00:00", 20260318, 150),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_comprovante (
                  id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                  valor_total, payload
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, '{}'::jsonb)
                """,
                (tenant_id, branch_id, 1, 2001, "2026-03-20 08:00:00", 20260320, 180),
            )
            conn.commit()

        login_response = self._login(manager_email, "Senha@123")
        body = login_response.json()
        self.assertTrue(body["home_path"].startswith("/dashboard?"), body["home_path"])

        qs = parse_qs(urlparse(body["home_path"]).query)
        self.assertEqual(qs["dt_ini"][0], "2026-02-19")
        self.assertEqual(qs["dt_fim"][0], "2026-03-20")
        self.assertNotIn("dt_ref", qs)
        self.assertEqual(qs["id_empresa"][0], str(tenant_id))
        self.assertEqual(qs["id_filial"][0], str(branch_id))

        token = body["access_token"]
        me_response = self.client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(me_response.status_code, 200, me_response.text)
        me_body = me_response.json()
        self.assertEqual(me_body["home_path"], body["home_path"])
        self.assertEqual(me_body["default_scope"]["dt_ini"], "2026-02-19")
        self.assertEqual(me_body["default_scope"]["dt_fim"], "2026-03-20")
        self.assertEqual(me_body["default_scope"]["dt_ref"], str(date.today()))
        self.assertEqual(me_body["default_scope"]["latest_operational_dt"], "2026-03-20")
        self.assertEqual(int(me_body["default_scope"]["days"]), 30)

    def test_product_user_auto_scope_falls_back_to_branch_operational_facts_beyond_sales(self) -> None:
        tenant_id = self._create_tenant("Tenant Auto Scope Finance")
        branch_id = 995
        other_branch_id = 996
        self._create_branch(tenant_id, branch_id, "Filial 995", is_active=True)
        self._create_branch(tenant_id, other_branch_id, "Filial 996", is_active=True)

        manager_email = self._create_user(
            "tenant_manager",
            "Senha@123",
            tenant_id=tenant_id,
            branch_id=branch_id,
        )

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                UPDATE app.tenants
                SET default_product_scope_days = %s
                WHERE id_empresa = %s
                """,
                (14, tenant_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  total_venda, payload
                )
                VALUES (%s, %s, 1, 3001, %s, %s, 250, '{}'::jsonb)
                """,
                (tenant_id, other_branch_id, "2026-03-20 10:00:00", 20260320),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_financeiro (
                  id_empresa, id_filial, id_db, tipo_titulo, id_titulo, id_entidade,
                  data_emissao, data_key_emissao, vencimento, data_key_venc,
                  data_pagamento, data_key_pgto, valor, valor_pago, payload
                )
                VALUES (%s, %s, 1, 1, 4001, 10, %s, %s, %s, %s, NULL, NULL, 120, 0, '{}'::jsonb)
                """,
                (tenant_id, branch_id, "2026-03-02", 20260302, "2026-03-15", 20260315),
            )
            conn.commit()

        login_response = self._login(manager_email, "Senha@123")
        body = login_response.json()
        qs = parse_qs(urlparse(body["home_path"]).query)

        self.assertEqual(qs["dt_ini"][0], "2026-03-02")
        self.assertEqual(qs["dt_fim"][0], "2026-03-15")
        self.assertNotIn("dt_ref", qs)
        self.assertEqual(qs["id_empresa"][0], str(tenant_id))
        self.assertEqual(qs["id_filial"][0], str(branch_id))

        token = body["access_token"]
        me_response = self.client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(me_response.status_code, 200, me_response.text)
        me_body = me_response.json()
        self.assertEqual(me_body["default_scope"]["dt_ini"], "2026-03-02")
        self.assertEqual(me_body["default_scope"]["dt_fim"], "2026-03-15")
        self.assertEqual(me_body["default_scope"]["dt_ref"], str(date.today()))
        self.assertEqual(me_body["default_scope"]["latest_operational_dt"], "2026-03-15")
        self.assertEqual(me_body["default_scope"]["source"], "latest_operational_date")
        self.assertEqual(int(me_body["default_scope"]["days"]), 14)

    def test_global_product_user_has_product_wide_scope_without_platform_access(self) -> None:
        tenant_id = self._create_tenant("Tenant Global Product")
        self._create_branch(tenant_id, 901, "Filial 901", is_active=True)
        email = self._create_user("product_global", "Senha@123")

        login_response = self._login(email, "Senha@123")
        self.assertTrue(login_response.json()["home_path"].startswith("/dashboard?"), login_response.json()["home_path"])
        self.assertNotIn("/scope", login_response.json()["home_path"])

        headers = {"Authorization": f"Bearer {login_response.json()['access_token']}"}
        me_response = self.client.get("/auth/me", headers=headers)
        self.assertEqual(me_response.status_code, 200, me_response.text)
        me_body = me_response.json()
        self.assertEqual(me_body["user_role"], "product_global")
        self.assertTrue(bool(me_body["access"]["product"]))
        self.assertFalse(bool(me_body["access"]["platform"]))

        platform_response = self.client.get("/platform/companies?limit=10", headers=headers)
        self.assertEqual(platform_response.status_code, 403, platform_response.text)
        self.assertEqual(platform_response.json()["error"], "platform_forbidden")

        filiais_response = self.client.get(f"/bi/filiais?id_empresa={tenant_id}", headers=headers)
        self.assertEqual(filiais_response.status_code, 200, filiais_response.text)
        self.assertTrue(any(int(item["id_filial"]) == 901 for item in filiais_response.json()["items"]))

    def test_dashboard_home_uses_operational_fallbacks_when_exact_snapshots_are_missing(self) -> None:
        tenant_id = self._create_tenant("Tenant Home Fallback")
        branch_id = 997
        self._create_branch(tenant_id, branch_id, "Filial 997", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(owner_email, "Senha@123")

        ref_today = date.today()
        churn_sale_date = ref_today - timedelta(days=45)
        fraud_date = ref_today - timedelta(days=5)
        finance_due_date = ref_today - timedelta(days=6)
        comp_data_key = int(churn_sale_date.strftime("%Y%m%d"))

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_cliente (id_empresa, id_filial, id_cliente, nome, documento)
                VALUES (%s, %s, 7001, 'Cliente Fallback', NULL)
                ON CONFLICT (id_empresa, id_filial, id_cliente) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  id_cliente, id_comprovante, id_turno, saidas_entradas, total_venda, cancelado, payload
                )
                VALUES (%s, %s, 1, 7001, %s, %s, 7001, 97001, 31, 1, 180, false, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos) DO NOTHING
                """,
                (tenant_id, branch_id, f"{churn_sale_date.isoformat()} 10:00:00", comp_data_key),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, qtd, valor_unitario, total, desconto, custo_total, margem, cfop, payload
                )
                VALUES (%s, %s, 1, 7001, 70011, %s, 500, 1, 180, 180, 0, 120, 60, 5102, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos) DO NOTHING
                """,
                (tenant_id, branch_id, comp_data_key),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_comprovante (
                  id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                  id_turno, id_cliente, valor_total, cancelado, situacao, payload
                )
                VALUES (%s, %s, 1, 97001, %s, %s, 31, 7001, 180, true, 1, '{"CFOP":"5102"}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante) DO NOTHING
                """,
                (tenant_id, branch_id, f"{churn_sale_date.isoformat()} 10:00:00", comp_data_key),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_comprovante (
                  id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                  id_turno, id_cliente, valor_total, cancelado, situacao, payload
                )
                VALUES (%s, %s, 1, 97002, %s, %s, 32, 7001, 260, true, 1, '{"CFOP":"5102"}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante) DO NOTHING
                """,
                (tenant_id, branch_id, f"{fraud_date.isoformat()} 11:00:00", int(fraud_date.strftime("%Y%m%d"))),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_financeiro (
                  id_empresa, id_filial, id_db, tipo_titulo, id_titulo, id_entidade,
                  data_emissao, data_key_emissao, vencimento, data_key_venc,
                  data_pagamento, data_key_pgto, valor, valor_pago, payload
                )
                VALUES (%s, %s, 1, 1, 8801, 1, %s, %s, %s, %s, NULL, NULL, 320, 0, '{}'::jsonb)
                """,
                (
                    tenant_id,
                    branch_id,
                    (finance_due_date - timedelta(days=5)).isoformat(),
                    int((finance_due_date - timedelta(days=5)).strftime("%Y%m%d")),
                    finance_due_date.isoformat(),
                    int(finance_due_date.strftime("%Y%m%d")),
                ),
            )
            conn.execute("REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.clientes_churn_risco")
            conn.commit()

        response = self.client.get(
            f"/bi/dashboard/home?dt_ini={(ref_today - timedelta(days=30)).isoformat()}&dt_fim={ref_today.isoformat()}&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()

        self.assertGreater(float(body["overview"]["fraud"]["operational"]["kpis"]["valor_cancelado"]), 0)
        self.assertEqual(float(body["overview"]["fraud"]["modeled_risk"]["kpis"]["impacto_total"] or 0), 0.0)
        self.assertEqual(body["churn"]["snapshot_meta"]["snapshot_status"], "operational_current")
        self.assertEqual(body["finance"]["aging"]["snapshot_status"], "operational")
        self.assertGreater(float(body["overview"]["jarvis"]["impact_value"] or 0), 0)

    def test_finance_aging_prefers_exact_then_best_effort_snapshot(self) -> None:
        tenant_id = self._create_tenant("Tenant Finance Snapshot")
        branch_id = 998
        self._create_branch(tenant_id, branch_id, "Filial 998", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(owner_email, "Senha@123")

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO mart.finance_aging_daily (
                  dt_ref, id_empresa, id_filial, receber_total_aberto, receber_total_vencido,
                  pagar_total_aberto, pagar_total_vencido, bucket_0_7, bucket_8_15,
                  bucket_16_30, bucket_31_60, bucket_60_plus, top5_concentration_pct, data_gaps
                )
                VALUES
                  ('2026-03-20', %s, %s, 100, 40, 50, 10, 10, 10, 10, 5, 5, 30, false),
                  ('2026-03-22', %s, %s, 120, 55, 60, 12, 12, 12, 12, 6, 6, 35, false)
                ON CONFLICT (dt_ref, id_empresa, id_filial) DO UPDATE SET
                  receber_total_aberto = EXCLUDED.receber_total_aberto,
                  receber_total_vencido = EXCLUDED.receber_total_vencido,
                  pagar_total_aberto = EXCLUDED.pagar_total_aberto,
                  pagar_total_vencido = EXCLUDED.pagar_total_vencido,
                  bucket_0_7 = EXCLUDED.bucket_0_7,
                  bucket_8_15 = EXCLUDED.bucket_8_15,
                  bucket_16_30 = EXCLUDED.bucket_16_30,
                  bucket_31_60 = EXCLUDED.bucket_31_60,
                  bucket_60_plus = EXCLUDED.bucket_60_plus,
                  top5_concentration_pct = EXCLUDED.top5_concentration_pct,
                  data_gaps = EXCLUDED.data_gaps,
                  updated_at = now()
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.commit()

        exact = self.client.get(
            f"/bi/finance/overview?dt_ini=2026-03-01&dt_fim=2026-03-31&dt_ref=2026-03-22&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(exact.status_code, 200, exact.text)
        exact_body = exact.json()["aging"]
        self.assertEqual(exact_body["snapshot_status"], "exact")
        self.assertEqual(str(exact_body["effective_dt_ref"]), "2026-03-22")

        best_effort = self.client.get(
            f"/bi/finance/overview?dt_ini=2026-03-01&dt_fim=2026-03-31&dt_ref=2026-03-24&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(best_effort.status_code, 200, best_effort.text)
        best_effort_body = best_effort.json()["aging"]
        self.assertEqual(best_effort_body["snapshot_status"], "best_effort")
        self.assertEqual(best_effort_body["precision_mode"], "latest_leq_ref")
        self.assertEqual(str(best_effort_body["effective_dt_ref"]), "2026-03-22")

    def test_cash_overview_splits_historical_window_from_live_monitor(self) -> None:
        tenant_id = self._create_tenant("Tenant Cash Split")
        branch_id = 999
        self._create_branch(tenant_id, branch_id, "Filial 999", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(owner_email, "Senha@123")

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_usuario_caixa (id_empresa, id_filial, id_usuario, nome, payload)
                VALUES (%s, %s, 910, 'Operador Histórico', '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_usuario) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_caixa_turno (
                  id_empresa, id_filial, id_turno, id_db, id_usuario, abertura_ts,
                  fechamento_ts, data_key_abertura, data_key_fechamento,
                  encerrante_fechamento, is_aberto, status_raw, payload
                )
                VALUES
                  (%s, %s, 41, 1, 910, TIMESTAMPTZ '2026-03-05 08:00:00+00', TIMESTAMPTZ '2026-03-05 18:00:00+00', 20260305, 20260305, NULL, false, 'CLOSED', '{}'::jsonb),
                  (%s, %s, 42, 1, 910, now() - interval '4 hour', NULL, %s, NULL, NULL, true, 'OPEN', '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_turno) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id, int(date.today().strftime("%Y%m%d"))),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_comprovante (
                  id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                  id_usuario, id_turno, valor_total, cancelado, situacao, payload
                )
                VALUES (%s, %s, 1, 41001, TIMESTAMPTZ '2026-03-05 10:00:00+00', 20260305, 910, 41, 250, false, 1, '{"CFOP":"5102"}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_pagamento_comprovante (
                  id_empresa, id_filial, referencia, id_db, id_comprovante, id_turno, id_usuario,
                  tipo_forma, valor, dt_evento, data_key, payload
                )
                VALUES (%s, %s, 41001, 1, 41001, 41, 910, 1, 250, TIMESTAMPTZ '2026-03-05 10:05:00+00', 20260305, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, referencia, tipo_forma) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto")
            conn.execute("REFRESH MATERIALIZED VIEW mart.alerta_caixa_aberto")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_pagamentos_turno")
            conn.commit()

        response = self.client.get(
            f"/bi/cash/overview?dt_ini=2026-03-01&dt_fim=2026-03-10&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()

        self.assertEqual(body["historical"]["requested_window"]["dt_ini"], "2026-03-01")
        self.assertEqual(body["historical"]["requested_window"]["dt_fim"], "2026-03-10")
        self.assertEqual(int(body["historical"]["kpis"]["caixas_periodo"]), 1)
        self.assertEqual(int(body["live_now"]["kpis"]["caixas_abertos"]), 1)
        self.assertTrue(body["historical"]["top_turnos"])
        self.assertEqual(body["source_status"], body["historical"]["source_status"])

    def test_cash_overview_uses_turn_user_identity_without_employee_fallback(self) -> None:
        tenant_id = self._create_tenant("Tenant Cash User Truth")
        branch_id = 1000
        self._create_branch(tenant_id, branch_id, "Filial 1000", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(owner_email, "Senha@123")
        today_key = int(date.today().strftime("%Y%m%d"))

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_funcionario (id_empresa, id_filial, id_funcionario, nome)
                VALUES (%s, %s, 777, 'Maria Operadora')
                ON CONFLICT (id_empresa, id_filial, id_funcionario) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_caixa_turno (
                  id_empresa, id_filial, id_turno, id_db, id_usuario, abertura_ts,
                  fechamento_ts, data_key_abertura, data_key_fechamento,
                  encerrante_fechamento, is_aberto, status_raw, payload
                )
                VALUES
                  (%s, %s, 51, 1, 910, TIMESTAMPTZ '2026-03-05 08:00:00+00', TIMESTAMPTZ '2026-03-05 18:00:00+00', 20260305, 20260305, NULL, false, 'CLOSED', '{}'::jsonb),
                  (%s, %s, 52, 1, 910, now() - interval '2 hour', NULL, %s, NULL, NULL, true, 'OPEN', '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_turno) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id, today_key),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_comprovante (
                  id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                  id_usuario, id_turno, valor_total, cancelado, situacao, payload
                )
                VALUES
                  (%s, %s, 1, 51001, TIMESTAMPTZ '2026-03-05 10:00:00+00', 20260305, 910, 51, 250, false, 1, '{"CFOP":"5102"}'::jsonb),
                  (%s, %s, 1, 52001, now() - interval '90 minute', %s, 910, 52, 180, false, 1, '{"CFOP":"5102"}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id, today_key),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  id_usuario, id_cliente, id_comprovante, id_turno, saidas_entradas,
                  total_venda, cancelado, payload
                )
                VALUES
                  (%s, %s, 1, 51001, TIMESTAMPTZ '2026-03-05 10:00:00+00', 20260305, 910, NULL, 51001, 51, 1, 250, false, '{}'::jsonb),
                  (%s, %s, 1, 52001, now() - interval '90 minute', %s, 910, NULL, 52001, 52, 1, 180, false, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id, today_key),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, id_grupo_produto, id_local_venda, id_funcionario, qtd,
                  valor_unitario, total, desconto, custo_total, margem, cfop, payload
                )
                VALUES
                  (%s, %s, 1, 51001, 1, 20260305, 1, NULL, NULL, 777, 50, 5, 250, 0, 0, 0, 5102, '{}'::jsonb),
                  (%s, %s, 1, 52001, 1, %s, 1, NULL, NULL, 777, 36, 5, 180, 0, 0, 0, 5102, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id, today_key),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_pagamento_comprovante (
                  id_empresa, id_filial, referencia, id_db, id_comprovante, id_turno, id_usuario,
                  tipo_forma, valor, dt_evento, data_key, payload
                )
                VALUES
                  (%s, %s, 51001, 1, 51001, 51, 910, 1, 250, TIMESTAMPTZ '2026-03-05 10:05:00+00', 20260305, '{}'::jsonb),
                  (%s, %s, 52001, 1, 52001, 52, 910, 1, 180, now() - interval '80 minute', %s, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, referencia, tipo_forma) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id, today_key),
            )
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto")
            conn.execute("REFRESH MATERIALIZED VIEW mart.alerta_caixa_aberto")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_pagamentos_turno")
            conn.commit()

        response = self.client.get(
            f"/bi/cash/overview?dt_ini=2026-03-01&dt_fim={date.today().isoformat()}&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()

        historical_labels = [
            item["usuario_label"]
            for item in (body.get("historical", {}).get("top_turnos") or [])
            if int(item.get("id_turno") or 0) == 51
        ]
        live_labels = [
            item["usuario_label"]
            for item in (body.get("live_now", {}).get("open_boxes") or [])
            if int(item.get("id_turno") or 0) == 52
        ]

        self.assertIn("Operador 910", historical_labels)
        self.assertIn("Operador 910", live_labels)
        self.assertIn("TURNOS.ID_USUARIOS", body["definitions"]["operator"])

    def test_fraud_overview_aligns_cancelamento_with_cashier_operator_from_turn(self) -> None:
        tenant_id = self._create_tenant("Tenant Fraud Cashier Truth")
        branch_id = 1003
        self._create_branch(tenant_id, branch_id, "Filial 1003", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(owner_email, "Senha@123")

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_usuario_caixa (id_empresa, id_filial, id_usuario, nome, payload)
                VALUES (%s, %s, 910, 'Operadora do Caixa', '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_usuario) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.dim_funcionario (id_empresa, id_filial, id_funcionario, nome)
                VALUES (%s, %s, 777, 'Frentista da Venda')
                ON CONFLICT (id_empresa, id_filial, id_funcionario) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_caixa_turno (
                  id_empresa, id_filial, id_turno, id_db, id_usuario, abertura_ts,
                  fechamento_ts, data_key_abertura, data_key_fechamento,
                  encerrante_fechamento, is_aberto, status_raw, payload
                )
                VALUES (%s, %s, 61, 1, 910, TIMESTAMPTZ '2026-03-05 08:00:00+00', TIMESTAMPTZ '2026-03-05 19:00:00+00', 20260305, 20260305, 901, false, 'CLOSED', '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_turno) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_comprovante (
                  id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                  id_usuario, id_turno, valor_total, cancelado, situacao, payload
                )
                VALUES (%s, %s, 1, 61001, TIMESTAMPTZ '2026-03-05 12:00:00+00', 20260305, 111, 61, 300, true, 1, '{"CFOP":"5102"}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  id_usuario, id_cliente, id_comprovante, id_turno, saidas_entradas,
                  total_venda, cancelado, payload
                )
                VALUES (%s, %s, 1, 61001, TIMESTAMPTZ '2026-03-05 11:50:00+00', 20260305, 111, NULL, 61001, 61, 1, 300, true, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, id_grupo_produto, id_local_venda, id_funcionario, qtd,
                  valor_unitario, total, desconto, custo_total, margem, cfop, payload
                )
                VALUES (%s, %s, 1, 61001, 1, 20260305, 1, NULL, NULL, 777, 60, 5, 300, 0, 0, 0, 5102, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute("REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_eventos")
            conn.commit()

        fraud_response = self.client.get(
            f"/bi/fraud/overview?dt_ini=2026-03-01&dt_fim=2026-03-10&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(fraud_response.status_code, 200, fraud_response.text)
        fraud_body = fraud_response.json()

        cash_response = self.client.get(
            f"/bi/cash/overview?dt_ini=2026-03-01&dt_fim=2026-03-10&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(cash_response.status_code, 200, cash_response.text)
        cash_body = cash_response.json()

        self.assertEqual(int(fraud_body["kpis"]["cancelamentos"]), 1)
        self.assertEqual(float(fraud_body["kpis"]["valor_cancelado"]), 300.0)
        self.assertEqual(fraud_body["top_users"][0]["usuario_label"], "Operadora do Caixa")
        self.assertEqual(fraud_body["last_events"][0]["usuario_label"], "Operadora do Caixa")
        self.assertEqual(fraud_body["last_events"][0]["usuario_source"], "turno")
        self.assertIn("TURNOS.ID_USUARIOS", fraud_body["definitions"]["cashier_operator"])

        self.assertEqual(float(cash_body["historical"]["kpis"]["total_cancelamentos"]), 300.0)
        self.assertEqual(int(cash_body["historical"]["kpis"]["qtd_cancelamentos"]), 1)
        self.assertEqual(cash_body["historical"]["cancelamentos"][0]["usuario_label"], "Operadora do Caixa")

    def test_branch_visibility_respects_platform_master_owner_and_manager(self) -> None:
        tenant_id = self._create_tenant("Tenant Branch Visibility")
        self._create_branch(tenant_id, 1001, "Filial 1001", is_active=True)
        self._create_branch(tenant_id, 1002, "Filial 1002", is_active=True)

        master_email = self._create_user("platform_master", "Senha@123")
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        manager_email = self._create_user("tenant_manager", "Senha@123", tenant_id=tenant_id, branch_id=1001)

        master_headers = self._auth_headers(master_email, "Senha@123")
        owner_headers = self._auth_headers(owner_email, "Senha@123")
        manager_headers = self._auth_headers(manager_email, "Senha@123")

        master_filiais = self.client.get(f"/bi/filiais?id_empresa={tenant_id}", headers=master_headers)
        owner_filiais = self.client.get(f"/bi/filiais?id_empresa={tenant_id}", headers=owner_headers)
        manager_filiais = self.client.get(f"/bi/filiais?id_empresa={tenant_id}", headers=manager_headers)

        self.assertEqual(master_filiais.status_code, 200, master_filiais.text)
        self.assertEqual(owner_filiais.status_code, 200, owner_filiais.text)
        self.assertEqual(manager_filiais.status_code, 200, manager_filiais.text)

        self.assertEqual(len(master_filiais.json()["items"]), 2)
        self.assertEqual(len(owner_filiais.json()["items"]), 2)
        self.assertEqual([int(item["id_filial"]) for item in manager_filiais.json()["items"]], [1001])

        owner_platform = self.client.get("/platform/companies?limit=10", headers=owner_headers)
        self.assertEqual(owner_platform.status_code, 403, owner_platform.text)

    def test_multi_branch_scope_allows_owner_and_blocks_manager_cross_branch(self) -> None:
        tenant_id = self._create_tenant("Tenant Multi Branch Scope")
        self._create_branch(tenant_id, 1101, "Filial 1101", is_active=True)
        self._create_branch(tenant_id, 1102, "Filial 1102", is_active=True)

        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        manager_email = self._create_user("tenant_manager", "Senha@123", tenant_id=tenant_id, branch_id=1101)

        owner_headers = self._auth_headers(owner_email, "Senha@123")
        manager_headers = self._auth_headers(manager_email, "Senha@123")

        owner_response = self.client.get(
            f"/bi/notifications/unread-count?id_empresa={tenant_id}&id_filiais=1101&id_filiais=1102",
            headers=owner_headers,
        )
        self.assertEqual(owner_response.status_code, 200, owner_response.text)
        self.assertIn("unread", owner_response.json())

        manager_allowed = self.client.get(
            f"/bi/notifications/unread-count?id_empresa={tenant_id}&id_filiais=1101",
            headers=manager_headers,
        )
        self.assertEqual(manager_allowed.status_code, 200, manager_allowed.text)

        manager_forbidden = self.client.get(
            f"/bi/notifications/unread-count?id_empresa={tenant_id}&id_filiais=1101&id_filiais=1102",
            headers=manager_headers,
        )
        self.assertEqual(manager_forbidden.status_code, 403, manager_forbidden.text)
        self.assertEqual(manager_forbidden.json()["detail"]["error"], "branch_access_denied")

    def test_user_update_can_clear_valid_from_and_keep_global_product_scope(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        headers = self._auth_headers(master_email, "Senha@123")

        create_response = self.client.post(
            "/platform/users",
            json={
                "nome": "Produto Global",
                "email": self._unique_email("product-global"),
                "password": "Senha@123",
                "role": "product_global",
                "is_enabled": True,
                "valid_from": "2025-01-01",
                "valid_until": None,
                "must_change_password": False,
                "accesses": [{"role": "product_global", "is_enabled": True}],
            },
            headers=headers,
        )
        self.assertEqual(create_response.status_code, 200, create_response.text)
        user_id = create_response.json()["id"]

        update_response = self.client.patch(
            f"/platform/users/{user_id}",
            json={
                "nome": "Produto Global Editado",
                "email": create_response.json()["email"],
                "password": None,
                "role": "product_global",
                "is_enabled": True,
                "valid_from": None,
                "valid_until": None,
                "must_change_password": False,
                "locked_until": None,
                "reset_failed_login": True,
                "accesses": [{"role": "product_global", "is_enabled": True, "valid_from": None, "valid_until": None}],
            },
            headers=headers,
        )
        self.assertEqual(update_response.status_code, 200, update_response.text)
        self.assertIsNone(update_response.json()["valid_from"])
        self.assertEqual(update_response.json()["role"], "product_global")
        self.assertEqual(len(update_response.json()["accesses"]), 1)
        self.assertIsNone(update_response.json()["accesses"][0]["id_empresa"])

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            user_row = conn.execute(
                """
                SELECT valid_from, failed_login_count
                FROM auth.users
                WHERE id = %s::uuid
                """,
                (user_id,),
            ).fetchone()
            access_row = conn.execute(
                """
                SELECT role, channel_id, id_empresa, id_filial, valid_from
                FROM auth.user_tenants
                WHERE user_id = %s::uuid
                """,
                (user_id,),
            ).fetchone()
            conn.commit()

        self.assertIsNone(user_row["valid_from"])
        self.assertEqual(int(user_row["failed_login_count"] or 0), 0)
        self.assertEqual(access_row["role"], "product_global")
        self.assertIsNone(access_row["channel_id"])
        self.assertIsNone(access_row["id_empresa"])
        self.assertIsNone(access_row["id_filial"])
        self.assertIsNone(access_row["valid_from"])

    def test_company_operational_defaults_appear_and_can_be_updated(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        headers = self._auth_headers(master_email, "Senha@123")

        create_response = self.client.post(
            "/platform/companies",
            json={
                "nome": "Tenant Operacional",
                "is_enabled": True,
                "sales_history_days": 365,
                "default_product_scope_days": 30,
            },
            headers=headers,
        )
        self.assertEqual(create_response.status_code, 200, create_response.text)
        tenant_id = int(create_response.json()["id_empresa"])
        self.assertEqual(int(create_response.json()["sales_history_days"]), 365)
        self.assertEqual(int(create_response.json()["default_product_scope_days"]), 30)

        update_response = self.client.patch(
            f"/platform/companies/{tenant_id}",
            json={
                "nome": "Tenant Operacional",
                "is_enabled": True,
                "sales_history_days": 180,
                "default_product_scope_days": 45,
            },
            headers=headers,
        )
        self.assertEqual(update_response.status_code, 200, update_response.text)
        body = update_response.json()
        self.assertEqual(int(body["sales_history_days"]), 180)
        self.assertEqual(int(body["default_product_scope_days"]), 45)

        detail_response = self.client.get(f"/platform/companies/{tenant_id}", headers=headers)
        self.assertEqual(detail_response.status_code, 200, detail_response.text)
        detail = detail_response.json()
        self.assertEqual(int(detail["sales_history_days"]), 180)
        self.assertEqual(int(detail["default_product_scope_days"]), 45)

    def test_platform_admin_and_tenant_admin_cannot_cross_platform_finance_boundaries(self) -> None:
        platform_admin_email = self._create_user("platform_admin", "Senha@123")
        platform_admin_headers = self._auth_headers(platform_admin_email, "Senha@123")
        platform_admin_ops = self.client.get("/platform/companies?limit=10", headers=platform_admin_headers)
        self.assertEqual(platform_admin_ops.status_code, 200, platform_admin_ops.text)
        platform_admin_finance = self.client.get("/platform/receivables?limit=10", headers=platform_admin_headers)
        self.assertEqual(platform_admin_finance.status_code, 403, platform_admin_finance.text)
        self.assertEqual(platform_admin_finance.json()["error"], "platform_finance_forbidden")

        tenant_id = self._create_tenant("Tenant sem platform")
        tenant_admin_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        tenant_admin_headers = self._auth_headers(tenant_admin_email, "Senha@123")
        tenant_platform = self.client.get("/platform/companies?limit=10", headers=tenant_admin_headers)
        self.assertEqual(tenant_platform.status_code, 403, tenant_platform.text)
        self.assertEqual(tenant_platform.json()["error"], "platform_forbidden")

    def test_existing_branches_can_be_updated_while_manual_creation_stays_blocked(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        headers = self._auth_headers(master_email, "Senha@123")
        tenant_id = self._create_tenant("Tenant Sync Branches")
        self._create_branch(tenant_id, 997, "Filial Sincronizada", is_active=True)

        create_response = self.client.post(
            f"/platform/companies/{tenant_id}/branches",
            json={"nome": "Filial Manual", "is_enabled": True},
            headers=headers,
        )
        self.assertEqual(create_response.status_code, 409, create_response.text)
        self.assertEqual(create_response.json()["error"], "branch_sync_managed")

        update_response = self.client.patch(
            f"/platform/companies/{tenant_id}/branches/997",
            json={
                "nome": "Filial Editada",
                "cnpj": "12.345.678/0001-90",
                "is_enabled": False,
                "valid_from": "2025-01-01",
                "valid_until": "2025-12-31",
                "blocked_reason": "manutencao_operacional",
            },
            headers=headers,
        )
        self.assertEqual(update_response.status_code, 200, update_response.text)

        body = update_response.json()
        self.assertEqual(body["nome"], "Filial Editada")
        self.assertEqual(body["cnpj"], "12.345.678/0001-90")
        self.assertFalse(bool(body["is_active"]))
        self.assertEqual(str(body["valid_from"]), "2025-01-01")
        self.assertEqual(str(body["valid_until"]), "2025-12-31")
        self.assertEqual(body["blocked_reason"], "manutencao_operacional")

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            branch_row = conn.execute(
                """
                SELECT nome, cnpj, is_active, valid_from, valid_until, blocked_reason
                FROM auth.filiais
                WHERE id_empresa = %s
                  AND id_filial = %s
                """,
                (tenant_id, 997),
            ).fetchone()
            audit_row = conn.execute(
                """
                SELECT
                  action,
                  entity_type,
                  entity_id,
                  old_values->>'nome' AS old_nome,
                  new_values->>'nome' AS new_nome,
                  new_values->>'blocked_reason' AS new_blocked_reason
                FROM audit.audit_log
                WHERE entity_type = 'branch'
                  AND entity_id = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (f"{tenant_id}:997",),
            ).fetchone()
            conn.commit()

        self.assertEqual(branch_row["nome"], "Filial Editada")
        self.assertEqual(branch_row["cnpj"], "12.345.678/0001-90")
        self.assertFalse(bool(branch_row["is_active"]))
        self.assertEqual(str(branch_row["valid_from"]), "2025-01-01")
        self.assertEqual(str(branch_row["valid_until"]), "2025-12-31")
        self.assertEqual(branch_row["blocked_reason"], "manutencao_operacional")
        self.assertIsNotNone(audit_row)
        self.assertEqual(audit_row["action"], "branch.update")
        self.assertEqual(audit_row["entity_type"], "branch")
        self.assertEqual(audit_row["entity_id"], f"{tenant_id}:997")
        self.assertEqual(audit_row["old_nome"], "Filial Sincronizada")
        self.assertEqual(audit_row["new_nome"], "Filial Editada")
        self.assertEqual(audit_row["new_blocked_reason"], "manutencao_operacional")

    def test_branch_admin_state_survives_incremental_filial_sync(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        headers = self._auth_headers(master_email, "Senha@123")
        tenant_id = self._create_tenant("Tenant Branch ETL Preserve")
        self._create_branch(tenant_id, 998, "Filial Original", is_active=True)
        self._upsert_stg_branch(tenant_id, 998, "Filial Origem ETL", cnpj="98.765.432/0001-10")

        update_response = self.client.patch(
            f"/platform/companies/{tenant_id}/branches/998",
            json={
                "nome": "Filial Administrada",
                "cnpj": "11.222.333/0001-44",
                "is_enabled": False,
                "valid_from": "2025-02-01",
                "valid_until": None,
                "blocked_reason": "manter_desabilitada",
            },
            headers=headers,
        )
        self.assertEqual(update_response.status_code, 200, update_response.text)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            run_row = conn.execute(
                "SELECT etl.run_tenant_phase(%s, %s, %s) AS result",
                (tenant_id, False, date.today()),
            ).fetchone()
            branch_row = conn.execute(
                """
                SELECT nome, cnpj, is_active, valid_from, valid_until, blocked_reason
                FROM auth.filiais
                WHERE id_empresa = %s
                  AND id_filial = %s
                """,
                (tenant_id, 998),
            ).fetchone()
            dw_row = conn.execute(
                """
                SELECT nome, cnpj
                FROM dw.dim_filial
                WHERE id_empresa = %s
                  AND id_filial = %s
                """,
                (tenant_id, 998),
            ).fetchone()
            conn.commit()

        result = run_row["result"]
        self.assertTrue(result["ok"], result)
        self.assertGreaterEqual(int((result.get("meta") or {}).get("dim_filial", 0)), 1)
        self.assertEqual(branch_row["nome"], "Filial Administrada")
        self.assertEqual(branch_row["cnpj"], "11.222.333/0001-44")
        self.assertFalse(bool(branch_row["is_active"]))
        self.assertEqual(str(branch_row["valid_from"]), "2025-02-01")
        self.assertIsNone(branch_row["valid_until"])
        self.assertEqual(branch_row["blocked_reason"], "manter_desabilitada")
        self.assertIsNotNone(dw_row)
        self.assertEqual(dw_row["nome"], "Filial Origem ETL")
        self.assertEqual(dw_row["cnpj"], "98.765.432/0001-10")

    def test_user_ids_from_api_work_for_contacts_and_notification_subscriptions(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        headers = self._auth_headers(master_email, "Senha@123")
        tenant_id = self._create_tenant("Tenant Notification Runtime")

        user_response = self.client.post(
            "/platform/users",
            json={
                "nome": "Tenant Notification User",
                "email": self._unique_email("notify"),
                "password": "Senha@123",
                "role": "tenant_admin",
                "is_enabled": True,
                "must_change_password": False,
                "accesses": [{"role": "tenant_admin", "id_empresa": tenant_id, "is_enabled": True}],
            },
            headers=headers,
        )
        self.assertEqual(user_response.status_code, 200, user_response.text)
        user_id = user_response.json()["id"]

        contacts_response = self.client.put(
            f"/platform/users/{user_id}/contacts",
            json={
                "telegram_chat_id": "123456",
                "telegram_username": "tenantnotify",
                "telegram_enabled": True,
                "email": "notify@tenant.test",
                "phone": "11999999999",
            },
            headers=headers,
        )
        self.assertEqual(contacts_response.status_code, 200, contacts_response.text)
        self.assertEqual(contacts_response.json()["email"], "notify@tenant.test")

        subscription_response = self.client.post(
            "/platform/notifications/subscriptions",
            json={
                "user_id": user_id,
                "tenant_id": tenant_id,
                "event_type": "billing.receivable_due",
                "channel": "email",
                "severity_min": "WARN",
                "is_enabled": True,
            },
            headers=headers,
        )
        self.assertEqual(subscription_response.status_code, 200, subscription_response.text)
        self.assertEqual(subscription_response.json()["user_id"], user_id)

        subscriptions_list = self.client.get(
            f"/platform/notifications/subscriptions?tenant_id={tenant_id}&limit=20",
            headers=headers,
        )
        self.assertEqual(subscriptions_list.status_code, 200, subscriptions_list.text)
        items = subscriptions_list.json()["items"]
        self.assertTrue(any(item["user_id"] == user_id for item in items))

    def test_access_validity_and_session_revalidation_enforced(self) -> None:
        tenant_id = self._create_tenant("Tenant Vigência", is_active=True)
        expired_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                UPDATE auth.user_tenants
                SET valid_until = CURRENT_DATE - interval '1 day'
                WHERE user_id = (SELECT id FROM auth.users WHERE lower(email) = lower(%s))
                """,
                (expired_email,),
            )
            conn.commit()
        expired_response = self._login(expired_email, "Senha@123", expected_status=403)
        self.assertEqual(expired_response.json()["error"], "access_unavailable")

        tenant_live_id = self._create_tenant("Tenant Revalidação", is_active=True)
        live_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_live_id)
        live_headers = self._auth_headers(live_email, "Senha@123")
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                UPDATE app.tenants
                SET status = 'suspended_total',
                    suspended_reason = 'inadimplencia',
                    suspended_at = now()
                WHERE id_empresa = %s
                """,
                (tenant_live_id,),
            )
            conn.commit()

        me_response = self.client.get("/auth/me", headers=live_headers)
        self.assertEqual(me_response.status_code, 403, me_response.text)
        self.assertEqual(me_response.json()["error"], "tenant_suspended_total")
        suspended_login = self._login(live_email, "Senha@123", expected_status=403)
        self.assertEqual(suspended_login.json()["error"], "tenant_suspended_total")

    def test_suspended_readonly_allows_login_and_blocks_product_write(self) -> None:
        tenant_id = self._create_tenant("Tenant Readonly", is_active=True, status="suspended_readonly")
        readonly_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(readonly_email, "Senha@123")

        me_response = self.client.get("/auth/me", headers=headers)
        self.assertEqual(me_response.status_code, 200, me_response.text)
        self.assertTrue(bool(me_response.json()["access"]["product_readonly"]))

        write_response = self.client.post("/etl/run?refresh_mart=false", headers=headers)
        self.assertEqual(write_response.status_code, 403, write_response.text)
        self.assertEqual(write_response.json()["error"], "product_readonly")

    def test_receivables_and_channel_payables_are_idempotent(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        channel_id = self._create_channel("Canal Idempotente")
        tenant_id = self._create_tenant("Tenant Billing Idempotente", channel_id=channel_id)
        self._create_branch(tenant_id, 994, "Filial 994", is_active=True)
        self._deactivate_active_contracts(tenant_id)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            contract = conn.execute(
                """
                INSERT INTO billing.contracts (
                  tenant_id,
                  channel_id,
                  plan_name,
                  monthly_amount,
                  billing_day,
                  issue_day,
                  start_date,
                  is_enabled,
                  commission_first_year_pct,
                  commission_recurring_pct
                )
                VALUES (%s, %s, 'Plano A', 1000, 10, 5, DATE '2025-01-01', true, 10, 5)
                RETURNING id
                """,
                (tenant_id, channel_id),
            ).fetchone()
            conn.commit()

        headers = self._auth_headers(master_email, "Senha@123")
        payload = {"competence_month": "2025-03-01", "tenant_id": tenant_id}
        first = self.client.post("/platform/receivables/generate", json=payload, headers=headers)
        second = self.client.post("/platform/receivables/generate", json=payload, headers=headers)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(second.status_code, 200, second.text)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            receivable_count = conn.execute(
                "SELECT COUNT(*) AS total FROM billing.receivables WHERE tenant_id = %s AND contract_id = %s",
                (tenant_id, contract["id"]),
            ).fetchone()
            receivable = conn.execute(
                "SELECT id FROM billing.receivables WHERE tenant_id = %s AND contract_id = %s LIMIT 1",
                (tenant_id, contract["id"]),
            ).fetchone()
            conn.commit()

        self.assertEqual(int(receivable_count["total"]), 1)

        pay_response = self.client.post(
            f"/platform/receivables/{receivable['id']}/pay",
            json={"received_amount": 1000, "payment_method": "manual"},
            headers=headers,
        )
        pay_response_repeat = self.client.post(
            f"/platform/receivables/{receivable['id']}/pay",
            json={"received_amount": 1000, "payment_method": "manual"},
            headers=headers,
        )
        self.assertEqual(pay_response.status_code, 200, pay_response.text)
        self.assertEqual(pay_response_repeat.status_code, 200, pay_response_repeat.text)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            payable_count = conn.execute(
                "SELECT COUNT(*) AS total FROM billing.channel_payables WHERE receivable_id = %s",
                (receivable["id"],),
            ).fetchone()
            conn.commit()
        self.assertEqual(int(payable_count["total"]), 1)

    def test_commission_switches_between_first_year_and_recurring(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        channel_id = self._create_channel("Canal Comissão")
        tenant_id = self._create_tenant("Tenant Comissão", channel_id=channel_id)
        self._create_branch(tenant_id, 995, "Filial 995", is_active=True)
        self._deactivate_active_contracts(tenant_id)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            contract = conn.execute(
                """
                INSERT INTO billing.contracts (
                  tenant_id,
                  channel_id,
                  plan_name,
                  monthly_amount,
                  billing_day,
                  issue_day,
                  start_date,
                  is_enabled,
                  commission_first_year_pct,
                  commission_recurring_pct
                )
                VALUES (%s, %s, 'Plano Comissão', 2000, 15, 5, DATE '2024-01-15', true, 12, 6)
                RETURNING id
                """,
                (tenant_id, channel_id),
            ).fetchone()

            first_year_receivable = conn.execute(
                """
                INSERT INTO billing.receivables (
                  tenant_id, contract_id, competence_month, issue_date, due_date, amount, status, is_emitted
                )
                VALUES (%s, %s, DATE '2024-12-01', DATE '2024-12-05', DATE '2024-12-15', 2000, 'issued', true)
                RETURNING id
                """,
                (tenant_id, contract["id"]),
            ).fetchone()
            recurring_receivable = conn.execute(
                """
                INSERT INTO billing.receivables (
                  tenant_id, contract_id, competence_month, issue_date, due_date, amount, status, is_emitted
                )
                VALUES (%s, %s, DATE '2025-02-01', DATE '2025-02-05', DATE '2025-02-15', 2000, 'issued', true)
                RETURNING id
                """,
                (tenant_id, contract["id"]),
            ).fetchone()
            conn.commit()

        headers = self._auth_headers(master_email, "Senha@123")
        self.client.post(
            f"/platform/receivables/{first_year_receivable['id']}/pay",
            json={"received_amount": 2000},
            headers=headers,
        )
        self.client.post(
            f"/platform/receivables/{recurring_receivable['id']}/pay",
            json={"received_amount": 2000},
            headers=headers,
        )

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            rows = conn.execute(
                """
                SELECT receivable_id, commission_pct
                FROM billing.channel_payables
                WHERE receivable_id IN (%s, %s)
                ORDER BY receivable_id
                """,
                (first_year_receivable["id"], recurring_receivable["id"]),
            ).fetchall()
            conn.commit()

        pct_map = {int(row["receivable_id"]): float(row["commission_pct"]) for row in rows}
        self.assertEqual(pct_map[int(first_year_receivable["id"])], 12.0)
        self.assertEqual(pct_map[int(recurring_receivable["id"])], 6.0)

    def test_receivable_reversals_and_received_amount_drive_payable(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        channel_id = self._create_channel("Canal Reversão")
        tenant_id = self._create_tenant("Tenant Reversão", channel_id=channel_id)
        self._create_branch(tenant_id, 996, "Filial 996", is_active=True)
        self._deactivate_active_contracts(tenant_id)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            contract = conn.execute(
                """
                INSERT INTO billing.contracts (
                  tenant_id,
                  channel_id,
                  plan_name,
                  monthly_amount,
                  billing_day,
                  issue_day,
                  start_date,
                  is_enabled,
                  commission_first_year_pct,
                  commission_recurring_pct
                )
                VALUES (%s, %s, 'Plano Reversão', 1000, 10, 5, DATE '2025-01-01', true, 10, 4)
                RETURNING id
                """,
                (tenant_id, channel_id),
            ).fetchone()
            receivable = conn.execute(
                """
                INSERT INTO billing.receivables (
                  tenant_id, contract_id, competence_month, issue_date, due_date, amount, status, is_emitted
                )
                VALUES (%s, %s, DATE '2025-01-01', DATE '2025-01-05', DATE '2025-01-10', 1000, 'issued', true)
                RETURNING id
                """,
                (tenant_id, contract["id"]),
            ).fetchone()
            conn.commit()

        headers = self._auth_headers(master_email, "Senha@123")
        unemit_response = self.client.post(f"/platform/receivables/{receivable['id']}/unemit", json={}, headers=headers)
        self.assertEqual(unemit_response.status_code, 200, unemit_response.text)
        self.assertFalse(bool(unemit_response.json()["is_emitted"]))

        emit_response = self.client.post(f"/platform/receivables/{receivable['id']}/emit", json={}, headers=headers)
        self.assertEqual(emit_response.status_code, 200, emit_response.text)
        pay_response = self.client.post(
            f"/platform/receivables/{receivable['id']}/pay",
            json={"received_amount": 850, "payment_method": "pix"},
            headers=headers,
        )
        self.assertEqual(pay_response.status_code, 200, pay_response.text)
        self.assertEqual(float(pay_response.json()["channel_payable"]["gross_amount"]), 850.0)
        self.assertEqual(float(pay_response.json()["channel_payable"]["payable_amount"]), 85.0)

        undo_response = self.client.post(
            f"/platform/receivables/{receivable['id']}/undo-payment",
            json={"notes": "estorno manual"},
            headers=headers,
        )
        self.assertEqual(undo_response.status_code, 200, undo_response.text)
        self.assertNotEqual(undo_response.json()["receivable"]["status"], "paid")
        self.assertEqual(undo_response.json()["channel_payable"]["status"], "cancelled")

        cancel_response = self.client.post(
            f"/platform/receivables/{receivable['id']}/cancel",
            json={"notes": "cancelado"},
            headers=headers,
        )
        self.assertEqual(cancel_response.status_code, 200, cancel_response.text)
        reopen_response = self.client.post(
            f"/platform/receivables/{receivable['id']}/reopen",
            json={"notes": "reaberto"},
            headers=headers,
        )
        self.assertEqual(reopen_response.status_code, 200, reopen_response.text)
        self.assertIn(reopen_response.json()["status"], {"open", "issued", "overdue", "planned"})

    def test_calendar_clamp_and_contract_history_rollover(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        headers = self._auth_headers(master_email, "Senha@123")
        channel_a = self._create_channel("Canal Clamp A")
        channel_b = self._create_channel("Canal Clamp B")
        tenant_id = self._create_tenant("Tenant Clamp", channel_id=channel_a)

        contract_response = self.client.post(
            "/platform/contracts",
            json={
                "tenant_id": tenant_id,
                "channel_id": channel_a,
                "plan_name": "Plano 31",
                "monthly_amount": 1200,
                "billing_day": 31,
                "issue_day": 31,
                "start_date": "2025-01-01",
                "commission_first_year_pct": 10,
                "commission_recurring_pct": 5,
                "is_enabled": True,
            },
            headers=headers,
        )
        self.assertEqual(contract_response.status_code, 200, contract_response.text)
        contract_id = int(contract_response.json()["id"])

        generate_response = self.client.post(
            "/platform/receivables/generate",
            json={"tenant_id": tenant_id, "competence_month": "2025-02-01"},
            headers=headers,
        )
        self.assertEqual(generate_response.status_code, 200, generate_response.text)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            receivable = conn.execute(
                """
                SELECT issue_date, due_date
                FROM billing.receivables
                WHERE tenant_id = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (tenant_id,),
            ).fetchone()
            conn.commit()

        self.assertEqual(str(receivable["issue_date"]), "2025-02-28")
        self.assertEqual(str(receivable["due_date"]), "2025-02-28")

        rollover_response = self.client.patch(
            f"/platform/contracts/{contract_id}",
            json={
                "tenant_id": tenant_id,
                "channel_id": channel_b,
                "plan_name": "Plano 31 Plus",
                "monthly_amount": 1500,
                "billing_day": 15,
                "issue_day": 7,
                "start_date": "2025-04-01",
                "commission_first_year_pct": 12,
                "commission_recurring_pct": 6,
                "is_enabled": True,
            },
            headers=headers,
        )
        self.assertEqual(rollover_response.status_code, 200, rollover_response.text)
        new_contract_id = int(rollover_response.json()["id"])
        self.assertNotEqual(new_contract_id, contract_id)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            rows = conn.execute(
                """
                SELECT id, is_enabled, start_date, end_date, channel_id
                FROM billing.contracts
                WHERE tenant_id = %s
                  AND id IN (%s, %s)
                ORDER BY id
                """,
                (tenant_id, contract_id, new_contract_id),
            ).fetchall()
            conn.commit()

        self.assertEqual(len(rows), 2)
        first_contract = dict(rows[0])
        second_contract = dict(rows[1])
        self.assertFalse(bool(first_contract["is_enabled"]))
        self.assertEqual(str(first_contract["end_date"]), "2025-03-31")
        self.assertTrue(bool(second_contract["is_enabled"]))
        self.assertEqual(int(second_contract["channel_id"]), channel_b)

        disable_response = self.client.patch(
            f"/platform/contracts/{new_contract_id}",
            json={
                "tenant_id": tenant_id,
                "channel_id": channel_b,
                "plan_name": "Plano 31 Plus",
                "monthly_amount": 1500,
                "billing_day": 15,
                "issue_day": 7,
                "start_date": "2025-04-01",
                "commission_first_year_pct": 12,
                "commission_recurring_pct": 6,
                "is_enabled": False,
            },
            headers=headers,
        )
        self.assertEqual(disable_response.status_code, 200, disable_response.text)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            tenant_summary = conn.execute(
                """
                SELECT channel_id, plan_name, monthly_amount, billing_day, issue_day
                FROM app.tenants
                WHERE id_empresa = %s
                """,
                (tenant_id,),
            ).fetchone()
            conn.commit()

        self.assertIsNone(tenant_summary["channel_id"])
        self.assertIsNone(tenant_summary["plan_name"])
        self.assertIsNone(tenant_summary["monthly_amount"])
        self.assertIsNone(tenant_summary["billing_day"])
        self.assertIsNone(tenant_summary["issue_day"])

    def test_audit_log_records_core_backoffice_actions(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        headers = self._auth_headers(master_email, "Senha@123")

        company_response = self.client.post(
            "/platform/companies",
            json={"nome": "Tenant Auditoria", "is_enabled": True},
            headers=headers,
        )
        self.assertEqual(company_response.status_code, 200, company_response.text)
        tenant_id = int(company_response.json()["id_empresa"])

        channel_id = self._create_channel("Canal Auditoria")
        contract_response = self.client.post(
            "/platform/contracts",
            json={
                "tenant_id": tenant_id,
                "channel_id": channel_id,
                "plan_name": "Plano Audit",
                "monthly_amount": 500,
                "billing_day": 20,
                "issue_day": 10,
                "start_date": "2025-01-01",
                "commission_first_year_pct": 8,
                "commission_recurring_pct": 4,
                "is_enabled": True,
            },
            headers=headers,
        )
        self.assertEqual(contract_response.status_code, 200, contract_response.text)

        generate_response = self.client.post(
            "/platform/receivables/generate",
            json={"tenant_id": tenant_id, "competence_month": "2025-05-01"},
            headers=headers,
        )
        self.assertEqual(generate_response.status_code, 200, generate_response.text)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            receivable = conn.execute(
                "SELECT id FROM billing.receivables WHERE tenant_id = %s ORDER BY id DESC LIMIT 1",
                (tenant_id,),
            ).fetchone()
            audit_rows = conn.execute(
                """
                SELECT action
                FROM audit.audit_log
                WHERE entity_id IN (%s, %s)
                ORDER BY id DESC
                """,
                (str(tenant_id), str(receivable["id"])),
            ).fetchall()
            conn.commit()

        actions = {row["action"] for row in audit_rows}
        self.assertIn("tenant.create", actions)
        self.assertIn("receivable.generate", actions)

    def test_customers_overview_excludes_supplier_only_entities_from_top_customers(self) -> None:
        tenant_id = self._create_tenant("Tenant Customers Semantics")
        branch_id = 1201
        self._create_branch(tenant_id, branch_id, "Filial 1201", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(owner_email, "Senha@123")

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_cliente (id_empresa, id_filial, id_cliente, nome)
                VALUES
                  (%s, %s, 9101, 'Cliente Válido'),
                  (%s, %s, 9102, 'Fornecedor Exclusivo')
                ON CONFLICT (id_empresa, id_filial, id_cliente)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  id_cliente, id_comprovante, id_turno, saidas_entradas, total_venda, cancelado, payload
                )
                VALUES
                  (%s, %s, 1, 91001, '2026-03-10 10:00:00', 20260310, 9101, 991001, 1, 1, 240, false, '{}'::jsonb),
                  (%s, %s, 1, 91002, '2026-03-11 11:00:00', 20260311, 9102, 991002, 1, 0, 900, false, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, qtd, valor_unitario, total, desconto, custo_total, margem, cfop, payload
                )
                VALUES
                  (%s, %s, 1, 91001, 910011, 20260310, 501, 1, 240, 240, 0, 160, 80, 5102, '{}'::jsonb),
                  (%s, %s, 1, 91002, 910021, 20260311, 502, 1, 900, 900, 0, 780, 120, 1102, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                "SELECT etl.backfill_customer_sales_daily_range(%s, %s::date, %s::date)",
                (tenant_id, "2026-03-01", "2026-03-31"),
            )
            conn.commit()

        response = self.client.get(
            f"/bi/customers/overview?dt_ini=2026-03-01&dt_fim=2026-03-31&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        top_customers = response.json()["top_customers"]
        names = [item["cliente_nome"] for item in top_customers]
        self.assertIn("Cliente Válido", names)
        self.assertNotIn("Fornecedor Exclusivo", names)

    def test_sovereign_platform_master_can_change_any_user_and_any_role(self) -> None:
        sovereign_email = self._ensure_sovereign_user()
        headers = self._auth_headers(sovereign_email, seed_cli.PLATFORM_MASTER_PASSWORD)
        me_response = self.client.get("/auth/me", headers=headers)
        self.assertEqual(me_response.status_code, 200, me_response.text)
        self.assertTrue(bool(me_response.json()["access"]["platform_superuser"]))

        tenant_id = self._create_tenant("Tenant Sovereign Target")
        branch_id = 1931
        self._create_branch(tenant_id, branch_id, "Filial Sovereign Target")
        target_email = self._create_user("platform_admin", "Senha@123")
        target_user_id = self._user_id_by_email(target_email)

        promote_response = self.client.patch(
            f"/platform/users/{target_user_id}",
            json={
                "nome": "Target Sovereign",
                "email": target_email,
                "password": None,
                "role": "platform_master",
                "is_enabled": True,
                "valid_from": None,
                "valid_until": None,
                "must_change_password": False,
                "locked_until": None,
                "reset_failed_login": True,
                "accesses": [
                    {
                        "role": "platform_master",
                        "channel_id": None,
                        "id_empresa": None,
                        "id_filial": None,
                        "is_enabled": True,
                        "valid_from": None,
                        "valid_until": None,
                    }
                ],
            },
            headers=headers,
        )
        self.assertEqual(promote_response.status_code, 200, promote_response.text)
        self.assertEqual(promote_response.json()["role"], "platform_master")

        demote_response = self.client.patch(
            f"/platform/users/{target_user_id}",
            json={
                "nome": "Target Sovereign",
                "email": target_email,
                "password": None,
                "role": "tenant_viewer",
                "is_enabled": True,
                "valid_from": None,
                "valid_until": None,
                "must_change_password": False,
                "locked_until": None,
                "reset_failed_login": True,
                "accesses": [
                    {
                        "role": "tenant_viewer",
                        "channel_id": None,
                        "id_empresa": tenant_id,
                        "id_filial": branch_id,
                        "is_enabled": True,
                        "valid_from": None,
                        "valid_until": None,
                    }
                ],
            },
            headers=headers,
        )
        self.assertEqual(demote_response.status_code, 200, demote_response.text)
        self.assertEqual(demote_response.json()["role"], "tenant_viewer")
        self.assertEqual(demote_response.json()["accesses"][0]["id_empresa"], tenant_id)
        self.assertEqual(demote_response.json()["accesses"][0]["id_filial"], branch_id)

    def test_tenant_viewer_requires_explicit_branch_scope(self) -> None:
        master_email = self._ensure_sovereign_user()
        headers = self._auth_headers(master_email, seed_cli.PLATFORM_MASTER_PASSWORD)
        tenant_id = self._create_tenant("Tenant Viewer Scope Validation")
        target_email = self._create_user("platform_admin", "Senha@123")
        target_user_id = self._user_id_by_email(target_email)

        response = self.client.patch(
            f"/platform/users/{target_user_id}",
            json={
                "nome": "Target Viewer Scope",
                "email": target_email,
                "password": None,
                "role": "tenant_viewer",
                "is_enabled": True,
                "valid_from": None,
                "valid_until": None,
                "must_change_password": False,
                "locked_until": None,
                "reset_failed_login": False,
                "accesses": [
                    {
                        "role": "tenant_viewer",
                        "channel_id": None,
                        "id_empresa": tenant_id,
                        "id_filial": None,
                        "is_enabled": True,
                        "valid_from": None,
                        "valid_until": None,
                    }
                ],
            },
            headers=headers,
        )
        self.assertEqual(response.status_code, 422, response.text)
        payload = response.json()
        detail = payload.get("detail") if isinstance(payload, dict) else None
        error_payload = detail if isinstance(detail, dict) else payload
        self.assertEqual(error_payload["error"], "validation_error")
        self.assertIn("id_filial", error_payload["message"])

    def test_platform_admin_cannot_edit_sovereign_user_or_promote_platform_master(self) -> None:
        sovereign_email = self._ensure_sovereign_user()
        sovereign_user_id = self._user_id_by_email(sovereign_email)
        platform_admin_email = self._create_user("platform_admin", "Senha@123")
        headers = self._auth_headers(platform_admin_email, "Senha@123")

        protected_response = self.client.patch(
            f"/platform/users/{sovereign_user_id}",
            json={
                "nome": "TorqMind Sovereign Master",
                "email": sovereign_email,
                "password": None,
                "role": "platform_master",
                "is_enabled": True,
                "valid_from": None,
                "valid_until": None,
                "must_change_password": False,
                "locked_until": None,
                "reset_failed_login": False,
                "accesses": [
                    {
                        "role": "platform_master",
                        "channel_id": None,
                        "id_empresa": None,
                        "id_filial": None,
                        "is_enabled": True,
                        "valid_from": None,
                        "valid_until": None,
                    }
                ],
            },
            headers=headers,
        )
        self.assertEqual(protected_response.status_code, 403, protected_response.text)
        self.assertEqual(protected_response.json()["error"], "sovereign_user_protected")

        tenant_id = self._create_tenant("Tenant Platform Admin Target")
        target_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        target_user_id = self._user_id_by_email(target_email)

        promote_response = self.client.patch(
            f"/platform/users/{target_user_id}",
            json={
                "nome": "Target Platform Admin",
                "email": target_email,
                "password": None,
                "role": "platform_master",
                "is_enabled": True,
                "valid_from": None,
                "valid_until": None,
                "must_change_password": False,
                "locked_until": None,
                "reset_failed_login": False,
                "accesses": [
                    {
                        "role": "platform_master",
                        "channel_id": None,
                        "id_empresa": None,
                        "id_filial": None,
                        "is_enabled": True,
                        "valid_from": None,
                        "valid_until": None,
                    }
                ],
            },
            headers=headers,
        )
        self.assertEqual(promote_response.status_code, 403, promote_response.text)
        self.assertEqual(promote_response.json()["error"], "role_escalation_forbidden")
