from __future__ import annotations

import unittest
from datetime import date, timedelta
from uuid import uuid4

from fastapi.testclient import TestClient

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

    def test_branches_are_sync_managed_and_cannot_be_changed_manually(self) -> None:
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
            json={"nome": "Filial Editada", "is_enabled": True},
            headers=headers,
        )
        self.assertEqual(update_response.status_code, 409, update_response.text)
        self.assertEqual(update_response.json()["error"], "branch_sync_managed")

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
                ORDER BY id
                """,
                (tenant_id,),
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
