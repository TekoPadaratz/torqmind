from __future__ import annotations

import json
import time
import unittest
from datetime import date, timedelta
from typing import Any
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse
from uuid import uuid4

from fastapi.testclient import TestClient

from app.cli import reconcile_sales as reconcile_sales_cli
from app.cli import seed as seed_cli
from app.business_time import business_today
from app import repos_mart, routes_bi
from app.db import get_conn
from app.main import app
from app.security import hash_password
from app.services import snapshot_cache
from app.usernames import username_from_email_candidate, validate_username


class PlatformBackofficeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app)

    def _unique_email(self, prefix: str) -> str:
        return f"{prefix}.{uuid4().hex[:10]}@torqmind.test"

    def _unique_username(self, prefix: str) -> str:
        safe_prefix = "".join(ch for ch in prefix.lower() if ch.isalnum() or ch in "._-").strip("._-") or "user"
        return f"{safe_prefix[:21]}.{uuid4().hex[:10]}"

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

    def _refresh_risk_marts(self) -> None:
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_risco_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.risco_top_funcionarios_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.risco_turno_local_diaria")
            conn.commit()

    def _create_user(
        self,
        role: str,
        password: str,
        *,
        email: str | None = None,
        username: str | None = None,
        tenant_id: int | None = None,
        branch_id: int | None = None,
        channel_id: int | None = None,
        is_active: bool = True,
    ) -> str:
        email = email or self._unique_email(role)
        normalized_username = validate_username(username or username_from_email_candidate(email))
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            user = conn.execute(
                """
                INSERT INTO auth.users (email, username, password_hash, nome, role, is_active, valid_from)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE - 1)
                RETURNING id
                """,
                (email, normalized_username, hash_password(password), role, role, is_active),
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
                VALUES (%s::uuid, %s, %s, %s, %s, true, CURRENT_DATE - 1)
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

    def _login(self, identifier: str, password: str, expected_status: int = 200):
        response = self.client.post("/auth/login", json={"identifier": identifier, "password": password})
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
                  username,
                  password_hash,
                  nome,
                  role,
                  is_active,
                  valid_from,
                  must_change_password,
                  failed_login_count,
                  locked_until
                )
                VALUES (%s, %s, %s, %s, 'platform_master', true, CURRENT_DATE, false, 0, NULL)
                ON CONFLICT (email)
                DO UPDATE SET
                  username = EXCLUDED.username,
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
                    validate_username(username_from_email_candidate(seed_cli.PLATFORM_MASTER_EMAIL)),
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

    def _get_hot_route_json(
        self,
        path: str,
        *,
        headers: dict[str, str],
        attempts: int = 40,
        sleep_seconds: float = 0.25,
    ) -> tuple[Any, dict[str, Any]]:
        primed = self._prime_hot_route_snapshot(path)
        last_response = None
        last_body: dict[str, Any] = {}
        for _ in range(attempts):
            response = self.client.get(path, headers=headers)
            self.assertEqual(response.status_code, 200, response.text)
            body = response.json()
            cache_meta = body.get("_snapshot_cache") or {}
            if cache_meta.get("source") != "fallback" or primed:
                return response, body
            last_response = response
            last_body = body
            time.sleep(sleep_seconds)
        assert last_response is not None
        return last_response, last_body

    def _prime_hot_route_snapshot(self, path: str) -> bool:
        parsed = urlparse(path)
        params = parse_qs(parsed.query)
        tenant_values = params.get("id_empresa")
        if not tenant_values:
            return False

        tenant_id = int(tenant_values[0])
        dt_ini = date.fromisoformat(params["dt_ini"][0])
        dt_fim = date.fromisoformat(params["dt_fim"][0])
        dt_ref = date.fromisoformat(params["dt_ref"][0]) if params.get("dt_ref") else None
        branch_ids = [int(value) for value in params.get("id_filiais", [])]
        branch_scope: int | list[int] | None = branch_ids or None
        if params.get("id_filial"):
            branch_scope = int(params["id_filial"][0])
        filial = branch_scope
        role = "MASTER"
        extra_context: dict[str, Any] | None = None

        def _parse_bool(name: str, default: bool) -> bool:
            raw = params.get(name, [str(default).lower()])[0]
            return str(raw).lower() not in {"0", "false", "no", "off"}

        if parsed.path == "/bi/dashboard/home":
            as_of = dt_ref or business_today(tenant_id)
            scope_key = "dashboard_home"
            payload = repos_mart.dashboard_home_bundle(role, tenant_id, filial, dt_ini=dt_ini, dt_fim=dt_fim, dt_ref=as_of)
            effective_dt_ref = as_of
        elif parsed.path == "/bi/sales/overview":
            as_of = dt_ref or business_today(tenant_id)
            scope_key = "sales_overview"
            payload = repos_mart.sales_overview_bundle(role, tenant_id, filial, dt_ini, dt_fim, as_of=as_of)
            extra_context = {"module": "sales"}
            effective_dt_ref = as_of
        elif parsed.path == "/bi/fraud/overview":
            as_of = dt_ref or business_today(tenant_id)
            risk_window = repos_mart.risk_data_window(role, tenant_id, filial)
            model_coverage = repos_mart.risk_model_coverage(dt_ini, dt_fim, risk_window)
            operational_kpis = repos_mart.fraud_kpis(role, tenant_id, filial, dt_ini, dt_fim)
            operational_series = repos_mart.fraud_series(role, tenant_id, filial, dt_ini, dt_fim)
            top_users = repos_mart.fraud_top_users(role, tenant_id, filial, dt_ini, dt_fim, limit=10)
            last_events = repos_mart.fraud_last_events(role, tenant_id, filial, dt_ini, dt_fim, limit=30)
            risk_kpis = repos_mart.risk_kpis(role, tenant_id, filial, dt_ini, dt_fim)
            risk_series = repos_mart.risk_series(role, tenant_id, filial, dt_ini, dt_fim)
            risk_top_employees = repos_mart.risk_top_employees(role, tenant_id, filial, dt_ini, dt_fim, limit=10)
            risk_by_turn_local = repos_mart.risk_by_turn_local(role, tenant_id, filial, dt_ini, dt_fim, limit=10)
            risk_last_events = repos_mart.risk_last_events(role, tenant_id, filial, dt_ini, dt_fim, limit=30)
            payments_risk = repos_mart.payments_anomalies(role, tenant_id, filial, dt_ini, dt_fim, limit=20)
            open_cash = repos_mart.open_cash_monitor(role, tenant_id, filial)
            scope_key = "fraud_overview"
            payload = {
                "requested_window": {
                    "dt_ini": dt_ini.isoformat(),
                    "dt_fim": dt_fim.isoformat(),
                    "dt_ref": as_of.isoformat(),
                },
                "business_clock": repos_mart.business_clock_payload(tenant_id),
                "kpis": operational_kpis,
                "by_day": operational_series,
                "top_users": top_users,
                "last_events": last_events,
                "definitions": repos_mart.fraud_definitions(),
                "operational": {
                    "kind": "operational",
                    "kpis": operational_kpis,
                    "by_day": operational_series,
                    "top_users": top_users,
                    "last_events": last_events,
                    "open_cash": open_cash,
                },
                "risk_kpis": risk_kpis,
                "risk_by_day": risk_series,
                "risk_window": risk_window,
                "model_coverage": model_coverage,
                "modeled_risk": {
                    "kind": "modeled",
                    "kpis": risk_kpis,
                    "by_day": risk_series,
                    "window": risk_window,
                    "coverage": model_coverage,
                    "top_employees": risk_top_employees,
                    "by_turn_local": risk_by_turn_local,
                    "last_events": risk_last_events,
                    "payments_risk": payments_risk,
                },
                "risk_top_employees": risk_top_employees,
                "risk_by_turn_local": risk_by_turn_local,
                "risk_last_events": risk_last_events,
                "insights": repos_mart.risk_insights(role, tenant_id, filial, dt_ini, dt_fim, limit=15),
                "payments_risk": payments_risk,
                "open_cash": open_cash,
            }
            extra_context = {"module": "fraud"}
            effective_dt_ref = as_of
        elif parsed.path == "/bi/customers/overview":
            as_of = dt_ref or business_today(tenant_id)
            scope_key = "customers_overview"
            churn_bundle = repos_mart.customers_churn_bundle(role, tenant_id, filial, as_of=as_of, min_score=40, limit=10)
            churn_top = []
            for customer in churn_bundle.get("top_risk") or []:
                freq_30 = int(customer.get("frequency_30") or 0)
                freq_90 = int(customer.get("frequency_90") or 0)
                mon_30 = float(customer.get("monetary_30") or 0)
                mon_90 = float(customer.get("monetary_90") or 0)
                churn_top.append(
                    {
                        "id_cliente": customer.get("id_cliente"),
                        "cliente_nome": customer.get("cliente_nome"),
                        "churn_score": customer.get("churn_score"),
                        "last_purchase": customer.get("last_purchase"),
                        "compras_30d": freq_30,
                        "compras_60_30": max(0, freq_90 - freq_30),
                        "faturamento_30d": mon_30,
                        "faturamento_60_30": max(0.0, mon_90 - mon_30),
                        "revenue_at_risk_30d": customer.get("revenue_at_risk_30d"),
                        "reasons": customer.get("reasons"),
                        "recommendation": customer.get("recommendation"),
                    }
                )
            payload = {
                "top_customers": repos_mart.customers_top(role, tenant_id, filial, dt_ini, dt_fim, limit=15),
                "rfm": repos_mart.customers_rfm_snapshot(role, tenant_id, filial, as_of=as_of),
                "churn_top": churn_top,
                "churn_snapshot": churn_bundle.get("snapshot_meta") or repos_mart.customers_churn_snapshot_meta(role, tenant_id, filial, as_of),
                "anonymous_retention": repos_mart.anonymous_retention_overview(role, tenant_id, filial, dt_ini, dt_fim),
            }
            extra_context = {"feature": "churn"}
            effective_dt_ref = as_of
        elif parsed.path == "/bi/finance/overview":
            as_of = dt_ref or business_today(tenant_id)
            include_series = _parse_bool("include_series", True)
            include_payments = _parse_bool("include_payments", True)
            include_operational = _parse_bool("include_operational", True)
            scope_key = "finance_overview"
            payload = {
                "kpis": repos_mart.finance_kpis(role, tenant_id, filial, dt_ini, dt_fim),
                "aging": repos_mart.finance_aging_overview(role, tenant_id, filial, as_of=as_of),
                "definitions": repos_mart.finance_definitions(),
                "business_clock": repos_mart.business_clock_payload(tenant_id),
            }
            if include_series:
                payload["by_day"] = repos_mart.finance_series(role, tenant_id, filial, dt_ini, dt_fim)
            if include_payments:
                payload["payments"] = repos_mart.payments_overview(role, tenant_id, filial, dt_ini, dt_fim, anomaly_limit=10)
            if include_operational:
                payload["open_cash"] = repos_mart.open_cash_monitor(role, tenant_id, filial)
            extra_context = {
                "include_series": include_series,
                "include_payments": include_payments,
                "include_operational": include_operational,
            }
            effective_dt_ref = as_of
        elif parsed.path == "/bi/goals/overview":
            as_of = dt_ref or business_today(tenant_id)
            scope_key = "goals_overview"
            payload = {
                "business_clock": repos_mart.business_clock_payload(tenant_id),
                "leaderboard": repos_mart.leaderboard_employees(role, tenant_id, filial, dt_ini, dt_fim, limit=15),
                "goals_today": repos_mart.goals_today(role, tenant_id, filial, goal_date=as_of),
                "risk_top_employees": repos_mart.risk_top_employees(role, tenant_id, filial, dt_ini, dt_fim, limit=15),
                "monthly_projection": repos_mart.monthly_goal_projection(role, tenant_id, filial, as_of=as_of),
            }
            extra_context = {"goal_date": as_of.isoformat()}
            effective_dt_ref = as_of
        elif parsed.path == "/bi/cash/overview":
            scope_key = "cash_overview"
            payload = repos_mart.cash_overview(role, tenant_id, filial, dt_ini=dt_ini, dt_fim=dt_fim)
            effective_dt_ref = None
        elif parsed.path == "/bi/pricing/competitor/overview":
            days_simulation = int(params.get("days_simulation", ["10"])[0])
            pricing_filial = routes_bi.primary_branch_id(branch_scope)
            if pricing_filial is None:
                return False
            scope_key = "pricing_competitor_overview"
            payload = repos_mart.competitor_pricing_overview(
                role,
                tenant_id,
                pricing_filial,
                dt_ini=dt_ini,
                dt_fim=dt_fim,
                days_simulation=days_simulation,
            )
            extra_context = {"days_simulation": days_simulation}
            effective_dt_ref = None
        else:
            return False

        context = routes_bi._build_snapshot_context(dt_ini, dt_fim, effective_dt_ref, branch_scope, extra_context)
        scope_signature = snapshot_cache.build_scope_signature(context)
        snapshot_cache.write_snapshot(
            role,
            tenant_id,
            routes_bi.primary_branch_id(branch_scope),
            scope_key,
            scope_signature,
            context,
            payload,
        )
        return True

    def _write_hot_route_snapshot(
        self,
        *,
        scope_key: str,
        tenant_id: int,
        branch_scope: int | list[int] | None,
        dt_ini: date,
        dt_fim: date,
        payload: dict[str, Any],
        dt_ref: date | None = None,
        extra_context: dict[str, Any] | None = None,
    ) -> None:
        context = routes_bi._build_snapshot_context(dt_ini, dt_fim, dt_ref, branch_scope, extra_context)
        scope_signature = snapshot_cache.build_scope_signature(context)
        snapshot_cache.write_snapshot(
            "MASTER",
            tenant_id,
            routes_bi.primary_branch_id(branch_scope),
            scope_key,
            scope_signature,
            context,
            payload,
        )

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

    def test_existing_email_login_keeps_working_after_username_support(self) -> None:
        tenant_id = self._create_tenant("Tenant Existing Email Login")
        username = self._unique_username("legacy-login")
        email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id, username=username)

        response = self._login(email, "Senha@123")

        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["user_role"], "tenant_admin")
        self.assertTrue(body["home_path"].startswith("/dashboard?"), body["home_path"])

    def test_login_accepts_case_insensitive_username(self) -> None:
        tenant_id = self._create_tenant("Tenant Username Login")
        username = self._unique_username("tenant-owner")
        self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id, username=username)

        response = self._login(username.upper(), "Senha@123")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["user_role"], "tenant_admin")

    def test_login_keeps_generic_error_for_missing_username_and_wrong_password(self) -> None:
        tenant_id = self._create_tenant("Tenant Generic Login Error")
        username = self._unique_username("generic-user")
        email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id, username=username)

        missing_username = self._login(self._unique_username("missing-user"), "Senha@123", expected_status=401)
        wrong_password = self._login(email, "Senha@999", expected_status=401)

        self.assertEqual(missing_username.json(), wrong_password.json())
        self.assertEqual(missing_username.json()["error"], "invalid_credentials")
        self.assertEqual(missing_username.json()["detail"]["message"], "Credenciais inválidas.")

    def test_user_create_rejects_invalid_username(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        headers = self._auth_headers(master_email, "Senha@123")
        tenant_id = self._create_tenant("Tenant Invalid Username")

        response = self.client.post(
            "/platform/users",
            json={
                "nome": "Usuario Invalido",
                "email": self._unique_email("invalid-username"),
                "username": "João Silva",
                "password": "Senha@123",
                "role": "tenant_admin",
                "is_enabled": True,
                "must_change_password": False,
                "accesses": [{"role": "tenant_admin", "id_empresa": tenant_id, "is_enabled": True}],
            },
            headers=headers,
        )

        self.assertEqual(response.status_code, 422, response.text)
        self.assertEqual(response.json()["error"], "validation_error")
        self.assertIn("Nome de usuário", response.json()["detail"]["message"])

    def test_user_create_rejects_duplicate_username_case_insensitively(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        headers = self._auth_headers(master_email, "Senha@123")
        tenant_id = self._create_tenant("Tenant Duplicate Username")
        duplicate_username = self._unique_username("dup-user")
        self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id, username=duplicate_username)

        response = self.client.post(
            "/platform/users",
            json={
                "nome": "Usuario Duplicado",
                "email": self._unique_email("duplicate-username"),
                "username": duplicate_username.upper(),
                "password": "Senha@123",
                "role": "tenant_admin",
                "is_enabled": True,
                "must_change_password": False,
                "accesses": [{"role": "tenant_admin", "id_empresa": tenant_id, "is_enabled": True}],
            },
            headers=headers,
        )

        self.assertEqual(response.status_code, 409, response.text)
        self.assertEqual(response.json()["error"], "username_conflict")

    def test_user_create_persists_username_lists_it_and_keeps_username_login_working(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        headers = self._auth_headers(master_email, "Senha@123")
        tenant_id = self._create_tenant("Tenant Username Persistence")
        requested_username = self._unique_username("Persist-User").upper()

        create_response = self.client.post(
            "/platform/users",
            json={
                "nome": "Usuario Username Persistido",
                "email": self._unique_email("persist-username"),
                "username": requested_username,
                "password": "Senha@123",
                "role": "tenant_admin",
                "is_enabled": True,
                "must_change_password": False,
                "accesses": [{"role": "tenant_admin", "id_empresa": tenant_id, "is_enabled": True}],
            },
            headers=headers,
        )
        self.assertEqual(create_response.status_code, 200, create_response.text)
        created_body = create_response.json()
        normalized_username = requested_username.lower()
        self.assertEqual(created_body["username"], normalized_username)

        list_response = self.client.get("/platform/users?limit=200", headers=headers)
        self.assertEqual(list_response.status_code, 200, list_response.text)
        listed = next((item for item in list_response.json()["items"] if item["id"] == created_body["id"]), None)
        self.assertIsNotNone(listed)
        self.assertEqual(listed["username"], normalized_username)

        login_response = self._login(normalized_username.upper(), "Senha@123")
        self.assertEqual(login_response.status_code, 200, login_response.text)
        self.assertEqual(login_response.json()["user_role"], "tenant_admin")

    def test_user_update_can_change_username_and_login_with_the_new_value(self) -> None:
        master_email = self._create_user("platform_master", "Senha@123")
        headers = self._auth_headers(master_email, "Senha@123")
        tenant_id = self._create_tenant("Tenant Username Update")
        original_username = self._unique_username("before-update")
        email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id, username=original_username)
        user_id = self._user_id_by_email(email)
        updated_username = self._unique_username("after-update").upper()

        update_response = self.client.patch(
            f"/platform/users/{user_id}",
            json={
                "nome": "Usuario Username Alterado",
                "email": email,
                "username": updated_username,
                "password": None,
                "role": "tenant_admin",
                "is_enabled": True,
                "valid_from": None,
                "valid_until": None,
                "must_change_password": False,
                "locked_until": None,
                "reset_failed_login": True,
                "accesses": [
                    {
                        "role": "tenant_admin",
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
        self.assertEqual(update_response.status_code, 200, update_response.text)
        normalized_username = updated_username.lower()
        self.assertEqual(update_response.json()["username"], normalized_username)

        old_login = self._login(original_username, "Senha@123", expected_status=401)
        self.assertEqual(old_login.json()["error"], "invalid_credentials")

        new_login = self._login(normalized_username.upper(), "Senha@123")
        self.assertEqual(new_login.status_code, 200, new_login.text)
        self.assertEqual(new_login.json()["user_role"], "tenant_admin")

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
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_vendas_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_vendas_hora")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_produtos_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_grupos_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_funcionarios_diaria")
            conn.commit()

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
        sales_body = sales_response.json()
        self.assertEqual(float(sales_body["kpis"]["faturamento"]), 150.0)

    def test_sales_overview_falls_back_to_dw_when_historical_marts_are_missing(self) -> None:
        tenant_id = self._create_tenant("Tenant Sales Historical DW Fallback")
        branch_id = 998
        self._create_branch(tenant_id, branch_id, "Filial 998", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)

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
                INSERT INTO dw.dim_funcionario (id_empresa, id_filial, id_funcionario, nome)
                VALUES (%s, %s, 1, 'Equipe Teste')
                ON CONFLICT (id_empresa, id_filial, id_funcionario)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  id_comprovante, saidas_entradas, total_venda, cancelado, payload
                )
                VALUES
                  (%s, %s, 1, 801, %s, %s, 901, 1, 100.00, false, '{}'::jsonb),
                  (%s, %s, 1, 802, %s, %s, 902, 1, 200.00, false, '{}'::jsonb),
                  (%s, %s, 1, 803, %s, %s, 903, 1, 300.00, false, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos)
                DO UPDATE SET data = EXCLUDED.data, data_key = EXCLUDED.data_key, total_venda = EXCLUDED.total_venda
                """,
                (
                    tenant_id,
                    branch_id,
                    "2026-03-31 08:00:00",
                    20260331,
                    tenant_id,
                    branch_id,
                    "2026-04-01 08:00:00",
                    20260401,
                    tenant_id,
                    branch_id,
                    "2026-04-02 08:00:00",
                    20260402,
                ),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, id_grupo_produto, id_funcionario, cfop, qtd, valor_unitario, total,
                  desconto, custo_total, margem, payload
                )
                VALUES
                  (%s, %s, 1, 801, 1, %s, 101, 1, 1, 5102, 10, 10, 100.00, 0, 70.00, 30.00, '{}'::jsonb),
                  (%s, %s, 1, 802, 1, %s, 101, 1, 1, 5102, 20, 10, 200.00, 0, 140.00, 60.00, '{}'::jsonb),
                  (%s, %s, 1, 803, 1, %s, 101, 1, 1, 5102, 30, 10, 300.00, 0, 210.00, 90.00, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
                DO UPDATE SET total = EXCLUDED.total, margem = EXCLUDED.margem
                """,
                (
                    tenant_id,
                    branch_id,
                    20260331,
                    tenant_id,
                    branch_id,
                    20260401,
                    tenant_id,
                    branch_id,
                    20260402,
                ),
            )
            conn.commit()

        headers = self._auth_headers(owner_email, "Senha@123")
        with patch.object(routes_bi, "resolve_business_date", side_effect=lambda dt_ref, tenant=None: dt_ref or date(2026, 4, 2)), patch.object(
            repos_mart,
            "business_today",
            return_value=date(2026, 4, 2),
        ):
            response = self.client.get(
                f"/bi/sales/overview?dt_ini=2026-03-31&dt_fim=2026-04-02&id_empresa={tenant_id}&id_filial={branch_id}",
                headers=headers,
            )

        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(body["reading_status"], "operational_overlay")
        self.assertEqual([int(row["data_key"]) for row in body["by_day"]], [20260331, 20260401, 20260402])
        self.assertEqual(round(float(body["kpis"]["faturamento"]), 2), 600.0)
        self.assertEqual(round(float(body["kpis"]["margem"]), 2), 180.0)
        self.assertEqual(body["freshness"]["source"], "dw.fact_venda")
        self.assertEqual(body["operational_sync"]["dt_ref"], "2026-04-02")

    def test_sales_overview_separates_sales_returns_and_canceled_rows(self) -> None:
        tenant_id = self._create_tenant("Tenant Sales Status Semantics")
        branch_id = 996
        self._create_branch(tenant_id, branch_id, "Filial 996", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)

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
                INSERT INTO dw.dim_funcionario (id_empresa, id_filial, id_funcionario, nome)
                VALUES (%s, %s, 901, 'Equipe Venda')
                ON CONFLICT (id_empresa, id_filial, id_funcionario)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key, id_comprovante,
                  saidas_entradas, total_venda, cancelado, situacao, payload
                )
                VALUES
                  (%s, %s, 1, 9101, TIMESTAMPTZ '2026-04-01 09:00:00+00', 20260401, 9901, 1, 100, false, 1, '{}'::jsonb),
                  (%s, %s, 1, 9102, TIMESTAMPTZ '2026-04-01 10:00:00+00', 20260401, 9902, 1, 200, false, 1, '{}'::jsonb),
                  (%s, %s, 1, 9103, TIMESTAMPTZ '2026-04-01 11:00:00+00', 20260401, 9903, 1, 40, false, 3, '{}'::jsonb),
                  (%s, %s, 1, 9104, TIMESTAMPTZ '2026-04-01 12:00:00+00', 20260401, 9904, 1, 999, true, 2, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos)
                DO UPDATE SET
                  data = EXCLUDED.data,
                  data_key = EXCLUDED.data_key,
                  total_venda = EXCLUDED.total_venda,
                  cancelado = EXCLUDED.cancelado,
                  situacao = EXCLUDED.situacao
                """,
                (
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                ),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, id_grupo_produto, id_funcionario, cfop, qtd, valor_unitario, total,
                  desconto, custo_total, margem, payload
                )
                VALUES
                  (%s, %s, 1, 9101, 1, 20260401, 101, 1, 901, 5102, 10, 10, 100, 0, 70, 30, '{}'::jsonb),
                  (%s, %s, 1, 9102, 1, 20260401, 101, 1, 901, 5102, 20, 10, 200, 0, 140, 60, '{}'::jsonb),
                  (%s, %s, 1, 9103, 1, 20260401, 101, 1, 901, 5102, 4, 10, 40, 0, 28, 12, '{}'::jsonb),
                  (%s, %s, 1, 9104, 1, 20260401, 101, 1, 901, 5102, 99, 10.0909, 999, 0, 700, 299, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
                DO UPDATE SET total = EXCLUDED.total, margem = EXCLUDED.margem
                """,
                (
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                ),
            )
            conn.commit()

        headers = self._auth_headers(owner_email, "Senha@123")
        response = self.client.get(
            f"/bi/sales/overview?dt_ini=2026-04-01&dt_fim=2026-04-01&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertEqual(round(float(body["kpis"]["faturamento"]), 2), 300.0)
        self.assertEqual(round(float(body["kpis"]["margem"]), 2), 90.0)
        self.assertEqual(round(float(body["kpis"]["ticket_medio"]), 2), 150.0)
        self.assertEqual(round(float(body["kpis"]["devolucoes"]), 2), 40.0)
        self.assertNotIn("itens", body["kpis"])
        self.assertEqual(round(float(body["top_products"][0]["faturamento"]), 2), 300.0)
        self.assertEqual(round(float(body["top_groups"][0]["faturamento"]), 2), 300.0)
        self.assertEqual(body["top_employees"], [])
        self.assertEqual(sorted(int(row["hora"]) for row in body["by_hour"]), [9, 10])

    def test_sales_and_cash_reconcile_with_exact_status_and_cfop_rules(self) -> None:
        tenant_id = self._create_tenant("Tenant Sales Cash Reconcile")
        branch_id = 998
        self._create_branch(tenant_id, branch_id, "Filial 998", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)

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
                INSERT INTO dw.dim_funcionario (id_empresa, id_filial, id_funcionario, nome)
                VALUES (%s, %s, 901, 'Equipe Venda')
                ON CONFLICT (id_empresa, id_filial, id_funcionario)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.dim_usuario_caixa (id_empresa, id_filial, id_usuario, nome, payload)
                VALUES (%s, %s, 910, 'Operador Caixa', '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_usuario)
                DO UPDATE SET nome = EXCLUDED.nome, payload = EXCLUDED.payload
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
                  (%s, %s, 61, 1, 910, TIMESTAMPTZ '2026-04-01 08:00:00+00', TIMESTAMPTZ '2026-04-01 18:00:00+00', 20260401, 20260401, NULL, false, 'CLOSED', '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_turno)
                DO UPDATE SET
                  id_usuario = EXCLUDED.id_usuario,
                  abertura_ts = EXCLUDED.abertura_ts,
                  fechamento_ts = EXCLUDED.fechamento_ts,
                  data_key_abertura = EXCLUDED.data_key_abertura,
                  data_key_fechamento = EXCLUDED.data_key_fechamento,
                  is_aberto = EXCLUDED.is_aberto,
                  status_raw = EXCLUDED.status_raw,
                  payload = EXCLUDED.payload
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_comprovante (
                  id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                  id_usuario, id_turno, valor_total, cancelado, situacao, data_conta, cash_eligible, payload
                )
                VALUES
                  (%s, %s, 1, 9801, TIMESTAMPTZ '2026-04-01 09:00:00+00', 20260401, 910, 61, 100, false, 1, DATE '2026-04-01', true, '{"CFOP":"5102"}'::jsonb),
                  (%s, %s, 1, 9802, TIMESTAMPTZ '2026-04-01 10:00:00+00', 20260401, 910, 61, 200, false, 1, DATE '2026-04-01', true, '{"CFOP":"5102"}'::jsonb),
                  (%s, %s, 1, 9803, TIMESTAMPTZ '2026-04-01 11:00:00+00', 20260401, 910, 61, 40, false, 3, DATE '2026-04-01', true, '{"CFOP":"5102"}'::jsonb),
                  (%s, %s, 1, 9804, TIMESTAMPTZ '2026-04-01 12:00:00+00', 20260401, 910, 61, 60, true, 2, DATE '2026-04-01', true, '{"CFOP":"5102"}'::jsonb),
                  (%s, %s, 1, 9805, TIMESTAMPTZ '2026-04-01 13:00:00+00', 20260401, 910, 61, 30, false, 1, DATE '2026-04-01', true, '{"CFOP":"4999"}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante)
                DO UPDATE SET
                  data = EXCLUDED.data,
                  data_key = EXCLUDED.data_key,
                  id_usuario = EXCLUDED.id_usuario,
                  id_turno = EXCLUDED.id_turno,
                  valor_total = EXCLUDED.valor_total,
                  cancelado = EXCLUDED.cancelado,
                  situacao = EXCLUDED.situacao,
                  data_conta = EXCLUDED.data_conta,
                  cash_eligible = EXCLUDED.cash_eligible,
                  payload = EXCLUDED.payload
                """,
                (
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                ),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key, id_usuario, id_cliente,
                  id_comprovante, id_turno, saidas_entradas, total_venda, cancelado, situacao, payload
                )
                VALUES
                  (%s, %s, 1, 9101, TIMESTAMPTZ '2026-04-01 09:00:00+00', 20260401, 910, NULL, 9801, 61, 1, 100, false, 1, '{}'::jsonb),
                  (%s, %s, 1, 9102, TIMESTAMPTZ '2026-04-01 10:00:00+00', 20260401, 910, NULL, 9802, 61, 1, 200, false, 1, '{}'::jsonb),
                  (%s, %s, 1, 9103, TIMESTAMPTZ '2026-04-01 11:00:00+00', 20260401, 910, NULL, 9803, 61, 1, 40, false, 3, '{}'::jsonb),
                  (%s, %s, 1, 9104, TIMESTAMPTZ '2026-04-01 12:00:00+00', 20260401, 910, NULL, 9804, 61, 1, 60, true, 2, '{}'::jsonb),
                  (%s, %s, 1, 9105, TIMESTAMPTZ '2026-04-01 13:00:00+00', 20260401, 910, NULL, 9805, 61, 1, 30, false, 1, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos)
                DO UPDATE SET
                  data = EXCLUDED.data,
                  data_key = EXCLUDED.data_key,
                  id_usuario = EXCLUDED.id_usuario,
                  id_comprovante = EXCLUDED.id_comprovante,
                  id_turno = EXCLUDED.id_turno,
                  total_venda = EXCLUDED.total_venda,
                  cancelado = EXCLUDED.cancelado,
                  situacao = EXCLUDED.situacao,
                  payload = EXCLUDED.payload
                """,
                (
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                ),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, id_grupo_produto, id_funcionario, cfop, qtd, valor_unitario, total,
                  desconto, custo_total, margem, payload
                )
                VALUES
                  (%s, %s, 1, 9101, 1, 20260401, 101, 1, 901, 5102, 10, 10, 100, 0, 70, 30, '{}'::jsonb),
                  (%s, %s, 1, 9102, 1, 20260401, 101, 1, 901, 5102, 20, 10, 200, 0, 140, 60, '{}'::jsonb),
                  (%s, %s, 1, 9103, 1, 20260401, 101, 1, 901, 5102, 4, 10, 40, 0, 28, 12, '{}'::jsonb),
                  (%s, %s, 1, 9104, 1, 20260401, 101, 1, 901, 5102, 6, 10, 60, 0, 42, 18, '{}'::jsonb),
                  (%s, %s, 1, 9105, 1, 20260401, 101, 1, 901, 4999, 3, 10, 30, 0, 21, 9, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
                DO UPDATE SET
                  cfop = EXCLUDED.cfop,
                  total = EXCLUDED.total,
                  margem = EXCLUDED.margem,
                  payload = EXCLUDED.payload
                """,
                (
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                    tenant_id, branch_id,
                ),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_pagamento_comprovante (
                  id_empresa, id_filial, referencia, id_db, id_comprovante, id_turno, id_usuario,
                  tipo_forma, valor, dt_evento, data_key, data_conta, cash_eligible, payload
                )
                VALUES
                  (%s, %s, 9801, 1, 9801, 61, 910, 1, 100, TIMESTAMPTZ '2026-04-01 09:05:00+00', 20260401, DATE '2026-04-01', true, '{}'::jsonb),
                  (%s, %s, 9802, 1, 9802, 61, 910, 1, 200, TIMESTAMPTZ '2026-04-01 10:05:00+00', 20260401, DATE '2026-04-01', true, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, referencia, tipo_forma)
                DO UPDATE SET
                  valor = EXCLUDED.valor,
                  dt_evento = EXCLUDED.dt_evento,
                  data_key = EXCLUDED.data_key,
                  data_conta = EXCLUDED.data_conta,
                  cash_eligible = EXCLUDED.cash_eligible,
                  payload = EXCLUDED.payload
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.commit()

        headers = self._auth_headers(owner_email, "Senha@123")
        sales_response = self.client.get(
            f"/bi/sales/overview?dt_ini=2026-04-01&dt_fim=2026-04-01&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        cash_response = self.client.get(
            f"/bi/cash/overview?dt_ini=2026-04-01&dt_fim=2026-04-01&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(sales_response.status_code, 200, sales_response.text)
        self.assertEqual(cash_response.status_code, 200, cash_response.text)

        sales_body = sales_response.json()
        cash_body = cash_response.json()
        cash_kpis = cash_body["historical"]["kpis"]
        turno = cash_body["historical"]["top_turnos"][0]
        daily = cash_body["historical"]["by_day"][0]

        self.assertEqual(round(float(sales_body["kpis"]["faturamento"]), 2), 300.0)
        self.assertEqual(round(float(sales_body["kpis"]["margem"]), 2), 90.0)
        self.assertEqual(round(float(sales_body["kpis"]["ticket_medio"]), 2), 150.0)
        self.assertEqual(round(float(sales_body["kpis"]["devolucoes"]), 2), 40.0)

        self.assertEqual(round(float(cash_kpis["total_vendas"]), 2), 300.0)
        self.assertEqual(round(float(cash_kpis["total_cancelamentos"]), 2), 60.0)
        self.assertEqual(round(float(cash_kpis["total_devolucoes"]), 2), 40.0)
        self.assertEqual(round(float(cash_kpis["caixa_liquido"]), 2), 200.0)
        self.assertEqual(round(float(cash_kpis["ticket_medio"]), 2), 150.0)
        self.assertEqual(round(float(cash_kpis["total_pagamentos"]), 2), 300.0)
        self.assertEqual(int(cash_kpis["qtd_cancelamentos"]), 1)
        self.assertEqual(int(cash_kpis["qtd_devolucoes"]), 1)

        self.assertEqual(round(float(daily["total_vendas"]), 2), 300.0)
        self.assertEqual(round(float(daily["total_cancelamentos"]), 2), 60.0)
        self.assertEqual(round(float(daily["total_devolucoes"]), 2), 40.0)
        self.assertEqual(round(float(daily["caixa_liquido"]), 2), 200.0)

        self.assertEqual(round(float(turno["total_vendas"]), 2), 300.0)
        self.assertEqual(round(float(turno["total_cancelamentos"]), 2), 60.0)
        self.assertEqual(round(float(turno["total_devolucoes"]), 2), 40.0)
        self.assertEqual(round(float(turno["caixa_liquido"]), 2), 200.0)
        self.assertEqual(round(float(turno["total_pagamentos"]), 2), 300.0)
        self.assertEqual(cash_body["historical"]["cancelamentos"][0]["usuario_label"], "Operador Caixa")

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
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_vendas_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_vendas_hora")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_produtos_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_grupos_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_funcionarios_diaria")
            conn.commit()

        headers = self._auth_headers(owner_email, "Senha@123")
        response = self.client.get(
            f"/bi/sales/overview?dt_ini=2026-03-07&dt_fim=2026-03-07&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        top_groups = {row["grupo_nome"]: float(row["faturamento"]) for row in body["top_groups"]}
        self.assertEqual(top_groups["Combustíveis"], 115336.56)
        self.assertEqual(top_groups["FILTROS DE COMBUSTIVEIS"], 89.0)
        self.assertNotIn("COMBUSTIVEIS", top_groups)

        with patch.object(
            reconcile_sales_cli,
            "_fetch_mart_groups",
            return_value=[
                {"grupo_nome": "COMBUSTIVEIS", "faturamento": 115336.56},
                {"grupo_nome": "FILTROS DE COMBUSTIVEIS", "faturamento": 89.0},
            ],
        ), patch.object(
            reconcile_sales_cli.repos_mart,
            "sales_top_groups",
            return_value=[
                {"grupo_nome": "COMBUSTIVEIS", "faturamento": 115336.56},
                {"grupo_nome": "FILTROS DE COMBUSTIVEIS", "faturamento": 89.0},
            ],
        ):
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

    def test_sales_top_groups_collapse_convenience_bucket_once(self) -> None:
        tenant_id = self._create_tenant("Tenant Convenience Bucket")
        branch_id = 996
        self._create_branch(tenant_id, branch_id, "Filial 996", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_grupo_produto (id_empresa, id_filial, id_grupo_produto, nome)
                VALUES
                  (%s, %s, 10, 'CIGARROS'),
                  (%s, %s, 14, 'BEBIDAS ALCOOLICAS'),
                  (%s, %s, 15, 'BEBIDAS NAO ALCOOLICAS'),
                  (%s, %s, 1, 'COMBUSTIVEIS')
                ON CONFLICT (id_empresa, id_filial, id_grupo_produto)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (tenant_id, branch_id, tenant_id, branch_id, tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.dim_produto (id_empresa, id_filial, id_produto, nome, id_grupo_produto, unidade)
                VALUES
                  (%s, %s, 301, 'CIGARRO TESTE', 10, 'UN'),
                  (%s, %s, 302, 'VINHO TESTE', 14, 'UN'),
                  (%s, %s, 303, 'REFRIGERANTE TESTE', 15, 'UN'),
                  (%s, %s, 304, 'GASOLINA TESTE', 1, 'LT')
                ON CONFLICT (id_empresa, id_filial, id_produto)
                DO UPDATE SET nome = EXCLUDED.nome, id_grupo_produto = EXCLUDED.id_grupo_produto, unidade = EXCLUDED.unidade
                """,
                (tenant_id, branch_id, tenant_id, branch_id, tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  id_comprovante, saidas_entradas, total_venda, cancelado, payload
                )
                VALUES
                  (%s, %s, 1, 801, %s, %s, 901, 1, 100.00, false, '{}'::jsonb),
                  (%s, %s, 1, 802, %s, %s, 902, 1, 80.00, false, '{}'::jsonb),
                  (%s, %s, 1, 803, %s, %s, 903, 1, 70.00, false, '{}'::jsonb),
                  (%s, %s, 1, 804, %s, %s, 904, 1, 400.00, false, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos)
                DO UPDATE SET data = EXCLUDED.data, data_key = EXCLUDED.data_key, total_venda = EXCLUDED.total_venda
                """,
                (
                    tenant_id, branch_id, "2026-03-08 08:00:00", 20260308,
                    tenant_id, branch_id, "2026-03-08 09:00:00", 20260308,
                    tenant_id, branch_id, "2026-03-08 10:00:00", 20260308,
                    tenant_id, branch_id, "2026-03-08 11:00:00", 20260308,
                ),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, id_grupo_produto, cfop, qtd, valor_unitario, total, desconto, custo_total, margem, payload
                )
                VALUES
                  (%s, %s, 1, 801, 1, %s, 301, 10, 5102, 1, 100, 100.00, 0, 50, 50, '{}'::jsonb),
                  (%s, %s, 1, 802, 1, %s, 302, 14, 5102, 1, 80, 80.00, 0, 40, 40, '{}'::jsonb),
                  (%s, %s, 1, 803, 1, %s, 303, 15, 5102, 1, 70, 70.00, 0, 35, 35, '{}'::jsonb),
                  (%s, %s, 1, 804, 1, %s, 304, 1, 5102, 1, 400, 400.00, 0, 300, 100, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
                DO UPDATE SET total = EXCLUDED.total, margem = EXCLUDED.margem
                """,
                (
                    tenant_id, branch_id, 20260308,
                    tenant_id, branch_id, 20260308,
                    tenant_id, branch_id, 20260308,
                    tenant_id, branch_id, 20260308,
                ),
            )
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_vendas_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_vendas_hora")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_produtos_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_grupos_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_funcionarios_diaria")
            conn.commit()

        headers = self._auth_headers(owner_email, "Senha@123")
        response = self.client.get(
            f"/bi/sales/overview?dt_ini=2026-03-08&dt_fim=2026-03-08&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        top_groups = {row["grupo_nome"]: float(row["faturamento"]) for row in body["top_groups"]}
        self.assertEqual(top_groups["Combustíveis"], 400.0)
        self.assertEqual(top_groups["Conveniência"], 250.0)
        self.assertEqual(sum(1 for row in body["top_groups"] if row["grupo_nome"] == "Conveniência"), 1)

    def test_competitor_pricing_overview_keeps_only_real_fuels(self) -> None:
        tenant_id = self._create_tenant("Tenant Strict Fuel Pricing")
        branch_id = 998
        self._create_branch(tenant_id, branch_id, "Filial 998", is_active=True)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_grupo_produto (id_empresa, id_filial, id_grupo_produto, nome)
                VALUES
                  (%s, %s, 10, 'COMBUSTIVEIS'),
                  (%s, %s, 11, 'ACESSORIOS'),
                  (%s, %s, 12, 'COMBUSTIVEIS ESPECIAIS')
                ON CONFLICT (id_empresa, id_filial, id_grupo_produto)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (tenant_id, branch_id, tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.dim_produto (id_empresa, id_filial, id_produto, nome, id_grupo_produto, unidade, custo_medio, situacao)
                VALUES
                  (%s, %s, 301, 'GASOLINA ADITIVADA', 10, 'LT', 5.20, 1),
                  (%s, %s, 302, 'FILTRO DE COMBUSTIVEL', 11, 'UN', 12.00, 1),
                  (%s, %s, 303, 'ARLA 32', 10, 'LT', 3.10, 1),
                  (%s, %s, 304, 'DIESEL S10', 12, 'LT', 4.80, 1)
                ON CONFLICT (id_empresa, id_filial, id_produto)
                DO UPDATE SET
                  nome = EXCLUDED.nome,
                  id_grupo_produto = EXCLUDED.id_grupo_produto,
                  unidade = EXCLUDED.unidade,
                  custo_medio = EXCLUDED.custo_medio,
                  situacao = EXCLUDED.situacao
                """,
                (
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                ),
            )
            conn.commit()

        body = repos_mart.competitor_pricing_overview(
            "MASTER",
            tenant_id,
            branch_id,
            date(2026, 3, 1),
            date(2026, 3, 10),
            days_simulation=10,
        )
        items = body["items"]
        returned_ids = {int(item["id_produto"]) for item in items}

        self.assertIn(301, returned_ids)
        self.assertIn(304, returned_ids)
        self.assertNotIn(302, returned_ids)
        self.assertNotIn(303, returned_ids)
        self.assertTrue(all((item.get("familia_combustivel") or "") in {"GASOLINA", "ETANOL", "DIESEL S10", "DIESEL S500", "GNV"} for item in items))

    def test_competitor_pricing_filters_active_fuels_and_reloads_saved_value(self) -> None:
        tenant_id = self._create_tenant("Tenant Competitor Active Fuel")
        branch_id = 995
        self._create_branch(tenant_id, branch_id, "Filial 995", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_grupo_produto (id_empresa, id_filial, id_grupo_produto, nome)
                VALUES
                  (%s, %s, 10, 'COMBUSTIVEIS'),
                  (%s, %s, 11, 'ACESSORIOS')
                ON CONFLICT (id_empresa, id_filial, id_grupo_produto)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.dim_produto (
                  id_empresa, id_filial, id_produto, nome, id_grupo_produto, unidade, custo_medio, situacao
                )
                VALUES
                  (%s, %s, 401, 'GASOLINA ADITIVADA', 10, 'LT', 5.20, 1),
                  (%s, %s, 402, 'DIESEL S10', 10, 'LT', 4.80, 2),
                  (%s, %s, 403, 'FILTRO DE COMBUSTIVEL', 11, 'UN', 12.00, 1)
                ON CONFLICT (id_empresa, id_filial, id_produto)
                DO UPDATE SET
                  nome = EXCLUDED.nome,
                  id_grupo_produto = EXCLUDED.id_grupo_produto,
                  unidade = EXCLUDED.unidade,
                  custo_medio = EXCLUDED.custo_medio,
                  situacao = EXCLUDED.situacao
                """,
                (
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                ),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key, id_comprovante,
                  saidas_entradas, total_venda, cancelado, payload
                )
                VALUES
                  (%s, %s, 1, 6401, TIMESTAMPTZ '2026-03-05 09:00:00+00', 20260305, 7401, 1, 600, false, '{}'::jsonb),
                  (%s, %s, 1, 6402, TIMESTAMPTZ '2026-03-05 10:00:00+00', 20260305, 7402, 1, 400, false, '{}'::jsonb),
                  (%s, %s, 1, 6403, TIMESTAMPTZ '2026-03-05 11:00:00+00', 20260305, 7403, 1, 120, false, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos)
                DO UPDATE SET total_venda = EXCLUDED.total_venda
                """,
                (
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                ),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, id_grupo_produto, cfop, qtd, valor_unitario, total, desconto, custo_total, margem, payload
                )
                VALUES
                  (%s, %s, 1, 6401, 1, 20260305, 401, 10, 5102, 100, 6, 600, 0, 520, 80, '{}'::jsonb),
                  (%s, %s, 1, 6402, 1, 20260305, 402, 10, 5102, 80, 5, 400, 0, 320, 80, '{}'::jsonb),
                  (%s, %s, 1, 6403, 1, 20260305, 403, 11, 5102, 10, 12, 120, 0, 90, 30, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
                DO UPDATE SET total = EXCLUDED.total, margem = EXCLUDED.margem
                """,
                (
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                    tenant_id,
                    branch_id,
                ),
            )
            conn.commit()
        self._refresh_sales_marts()

        headers = self._auth_headers(owner_email, "Senha@123")
        overview_response = self.client.get(
            f"/bi/pricing/competitor/overview?dt_ini=2026-03-01&dt_fim=2026-03-10&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(overview_response.status_code, 200, overview_response.text)
        overview_body = overview_response.json()
        item_ids = [int(item["id_produto"]) for item in overview_body["items"]]
        self.assertEqual(item_ids, [401])
        self.assertEqual(round(float(overview_body["items"][0]["competitor_price"]), 3), 0.0)

        invalid_save_response = self.client.post(
            f"/bi/pricing/competitor/prices?id_empresa={tenant_id}&id_filial={branch_id}",
            json={
                "items": [
                    {"id_produto": 402, "competitor_price": 5.499},
                    {"id_produto": 403, "competitor_price": 11.900},
                ]
            },
            headers=headers,
        )
        self.assertEqual(invalid_save_response.status_code, 400, invalid_save_response.text)
        invalid_detail = invalid_save_response.json()["detail"]
        self.assertEqual(invalid_detail["error"], "competitor_invalid_product")
        self.assertEqual(sorted(invalid_detail["ids"]), [402, 403])

        save_response = self.client.post(
            f"/bi/pricing/competitor/prices?id_empresa={tenant_id}&id_filial={branch_id}",
            json={"items": [{"id_produto": 401, "competitor_price": 5.799}]},
            headers=headers,
        )
        self.assertEqual(save_response.status_code, 200, save_response.text)
        self.assertEqual(int(save_response.json()["saved"]), 1)

        reloaded_response = self.client.get(
            f"/bi/pricing/competitor/overview?dt_ini=2026-03-01&dt_fim=2026-03-10&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(reloaded_response.status_code, 200, reloaded_response.text)
        reloaded_item = reloaded_response.json()["items"][0]
        self.assertEqual(int(reloaded_item["id_produto"]), 401)
        self.assertAlmostEqual(float(reloaded_item["competitor_price"]), 5.799, places=3)

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
        today = business_today()
        expected_start = (today - timedelta(days=29)).isoformat()
        self.assertTrue(body["home_path"].startswith("/dashboard?"), body["home_path"])
        self.assertEqual(body["home_path"], body["session"]["home_path"])
        self.assertEqual(body["session"]["default_scope"]["dt_ini"], expected_start)
        self.assertEqual(body["session"]["default_scope"]["dt_fim"], today.isoformat())

        token = body["access_token"]
        me_response = self.client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(me_response.status_code, 200, me_response.text)
        me_body = me_response.json()
        self.assertTrue(me_body["home_path"].startswith("/dashboard?"), me_body["home_path"])
        qs = parse_qs(urlparse(me_body["home_path"]).query)
        self.assertEqual(qs["dt_ini"][0], expected_start)
        self.assertEqual(qs["dt_fim"][0], today.isoformat())
        self.assertEqual(qs["dt_ref"][0], today.isoformat())
        self.assertEqual(qs["id_empresa"][0], str(tenant_id))
        self.assertEqual(qs["id_filial"][0], str(branch_id))
        self.assertEqual(me_body["default_scope"]["dt_ini"], expected_start)
        self.assertEqual(me_body["default_scope"]["dt_fim"], today.isoformat())
        self.assertEqual(me_body["default_scope"]["dt_ref"], today.isoformat())
        self.assertEqual(me_body["default_scope"]["latest_operational_dt"], "2026-03-20")
        self.assertEqual(me_body["default_scope"]["source"], "business_today_default")
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
        today = business_today()
        expected_start = (today - timedelta(days=13)).isoformat()
        self.assertTrue(body["home_path"].startswith("/dashboard?"), body["home_path"])
        self.assertEqual(body["home_path"], body["session"]["home_path"])
        self.assertEqual(body["session"]["default_scope"]["dt_ini"], expected_start)
        self.assertEqual(body["session"]["default_scope"]["dt_fim"], today.isoformat())

        token = body["access_token"]
        me_response = self.client.get("/auth/me", headers={"Authorization": f"Bearer {token}"})
        self.assertEqual(me_response.status_code, 200, me_response.text)
        me_body = me_response.json()
        qs = parse_qs(urlparse(me_body["home_path"]).query)
        self.assertEqual(qs["dt_ini"][0], expected_start)
        self.assertEqual(qs["dt_fim"][0], today.isoformat())
        self.assertEqual(qs["dt_ref"][0], today.isoformat())
        self.assertEqual(qs["id_empresa"][0], str(tenant_id))
        self.assertEqual(qs["id_filial"][0], str(branch_id))
        self.assertEqual(me_body["default_scope"]["dt_ini"], expected_start)
        self.assertEqual(me_body["default_scope"]["dt_fim"], today.isoformat())
        self.assertEqual(me_body["default_scope"]["dt_ref"], today.isoformat())
        self.assertEqual(me_body["default_scope"]["latest_operational_dt"], "2026-03-15")
        self.assertEqual(me_body["default_scope"]["source"], "business_today_default")
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

        ref_today = business_today(tenant_id)
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

        dashboard_payload = {
            "scope": {
                "id_empresa": tenant_id,
                "id_filial": branch_id,
                "id_filiais": [branch_id],
                "filial_label": "Filial 997",
                "dt_ini": (ref_today - timedelta(days=30)).isoformat(),
                "dt_fim": ref_today.isoformat(),
                "requested_dt_ref": ref_today.isoformat(),
            },
            "overview": {
                "sales": {"faturamento": 180.0, "margem": 60.0, "ticket_medio": 180.0, "itens": 1},
                "insights_generated": [],
                "fraud": {
                    "operational": {"kpis": {"cancelamentos": 1, "valor_cancelado": 260.0}, "window": {"rows": 1}},
                    "modeled_risk": {"kpis": {"total_eventos": 0, "eventos_alto_risco": 0, "impacto_total": 0.0, "score_medio": 0.0}, "window": {"rows": 0}},
                },
                "risk": {"kpis": {"total_eventos": 0, "eventos_alto_risco": 0, "impacto_total": 0.0, "score_medio": 0.0}, "window": {"rows": 0}},
                "cash": {"historical": {"source_status": "ok"}, "live_now": {"source_status": "unavailable"}},
                "jarvis": {"impact_value": 180.0},
            },
            "churn": {
                "top_risk": [],
                "summary": {"total_top_risk": 1, "avg_churn_score": 85.0, "revenue_at_risk_30d": 180.0},
                "snapshot_meta": {"snapshot_status": "operational_current"},
            },
            "finance": {"aging": {"snapshot_status": "operational"}},
            "cash": {
                "source_status": "ok",
                "summary": "Leitura consolidada disponível.",
                "definitions": repos_mart.cash_definitions(),
                "historical": {"source_status": "ok", "summary": "Histórico consolidado disponível.", "kpis": {}, "payment_mix": [], "top_turnos": [], "cancelamentos": [], "by_day": []},
                "live_now": {"source_status": "unavailable", "summary": "Monitor operacional indisponível.", "kpis": {}, "open_boxes": [], "stale_boxes": [], "payment_mix": [], "cancelamentos": [], "alerts": []},
                "open_boxes": [],
                "stale_boxes": [],
                "payment_mix": [],
                "cancelamentos": [],
                "alerts": [],
            },
            "notifications_unread": 0,
        }
        self._write_hot_route_snapshot(
            scope_key="dashboard_home",
            tenant_id=tenant_id,
            branch_scope=branch_id,
            dt_ini=ref_today - timedelta(days=30),
            dt_fim=ref_today,
            dt_ref=ref_today,
            payload=dashboard_payload,
        )
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

        _, exact = self._get_hot_route_json(
            f"/bi/finance/overview?dt_ini=2026-03-01&dt_fim=2026-03-31&dt_ref=2026-03-22&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        exact_body = exact["aging"]
        self.assertEqual(exact_body["snapshot_status"], "exact")
        self.assertEqual(str(exact_body["effective_dt_ref"]), "2026-03-22")
        self.assertEqual(exact["definitions"]["receber_vencido"]["label"], "Receber vencido")
        self.assertEqual(exact["business_clock"]["timezone"], "America/Sao_Paulo")

        _, best_effort = self._get_hot_route_json(
            f"/bi/finance/overview?dt_ini=2026-03-01&dt_fim=2026-03-31&dt_ref=2026-03-24&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        best_effort_body = best_effort["aging"]
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
            today_key = int(business_today(tenant_id).strftime("%Y%m%d"))
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
                (tenant_id, branch_id, tenant_id, branch_id, today_key),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_comprovante (
                  id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                  id_usuario, id_turno, valor_total, cancelado, situacao, payload
                )
                VALUES
                  (%s, %s, 1, 41001, TIMESTAMPTZ '2026-03-05 10:00:00+00', 20260305, 910, 41, 250, false, 1, '{"CFOP":"5102"}'::jsonb),
                  (%s, %s, 1, 42001, now() - interval '90 minute', %s, 910, 42, 180, false, 1, '{"CFOP":"5102"}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id, today_key),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                  id_usuario, id_cliente, id_comprovante, id_turno, saidas_entradas,
                  total_venda, cancelado, situacao, payload
                )
                VALUES
                  (%s, %s, 1, 41001, TIMESTAMPTZ '2026-03-05 10:00:00+00', 20260305, 910, NULL, 41001, 41, 1, 250, false, 1, '{}'::jsonb),
                  (%s, %s, 1, 42001, now() - interval '90 minute', %s, 910, NULL, 42001, 42, 1, 180, false, 1, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id, today_key),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, id_produto,
                  qtd, valor_unitario, total, custo_total, margem, cfop, payload
                )
                VALUES
                  (%s, %s, 1, 41001, 1, 5001, 1, 250, 250, 175, 75, 5102, '{}'::jsonb),
                  (%s, %s, 1, 42001, 1, 5001, 1, 180, 180, 126, 54, 5102, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_pagamento_comprovante (
                  id_empresa, id_filial, referencia, id_db, id_comprovante, id_turno, id_usuario,
                  tipo_forma, valor, dt_evento, data_key, payload
                )
                VALUES
                  (%s, %s, 41001, 1, 41001, 41, 910, 1, 250, TIMESTAMPTZ '2026-03-05 10:05:00+00', 20260305, '{}'::jsonb),
                  (%s, %s, 42001, 1, 42001, 42, 910, 1, 180, now() - interval '80 minute', %s, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, referencia, tipo_forma) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id, today_key),
            )
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto")
            conn.execute("REFRESH MATERIALIZED VIEW mart.alerta_caixa_aberto")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_caixa_forma_pagamento")
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
        today_key = int(business_today(tenant_id).strftime("%Y%m%d"))

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
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_caixa_forma_pagamento")
            conn.commit()
        response = self.client.get(
            f"/bi/cash/overview?dt_ini=2026-03-01&dt_fim={business_today(tenant_id).isoformat()}&id_empresa={tenant_id}&id_filial={branch_id}",
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
        self.assertIn("operador logado", body["definitions"]["operator"].lower())
        self.assertNotIn("turnos.id_usuarios", body["definitions"]["operator"].lower())

    def test_cash_overview_exposes_all_payment_methods_and_bank_deposit_label_without_turno_mart_refresh(self) -> None:
        tenant_id = self._create_tenant("Tenant Cash Payment Mix")
        branch_id = 1001
        self._create_branch(tenant_id, branch_id, "Filial 1001", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(owner_email, "Senha@123")

        payment_types = [
            (0, 120.0),
            (1, 80.0),
            (3, 200.0),
            (4, 160.0),
            (5, 90.0),
            (6, 110.0),
            (7, 70.0),
        ]

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.fact_caixa_turno (
                  id_empresa, id_filial, id_turno, id_db, id_usuario, abertura_ts,
                  fechamento_ts, data_key_abertura, data_key_fechamento,
                  encerrante_fechamento, is_aberto, status_raw, payload
                )
                VALUES (%s, %s, 81, 1, 910, TIMESTAMPTZ '2026-03-08 08:00:00+00', TIMESTAMPTZ '2026-03-08 18:00:00+00', 20260308, 20260308, 1, false, 'CLOSED', '{"TURNO":"2"}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_turno) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            for idx, (tipo_forma, valor) in enumerate(payment_types, start=1):
                comprovante_id = 81000 + idx
                referencia = 91000 + idx
                conn.execute(
                    """
                    INSERT INTO dw.fact_comprovante (
                      id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                      id_usuario, id_turno, valor_total, cancelado, situacao, payload
                    )
                    VALUES (%s, %s, 1, %s, TIMESTAMPTZ '2026-03-08 10:00:00+00', 20260308, 910, 81, %s, false, 1, '{"CFOP":"5102"}'::jsonb)
                    ON CONFLICT (id_empresa, id_filial, id_db, id_comprovante) DO NOTHING
                    """,
                    (tenant_id, branch_id, comprovante_id, valor),
                )
                conn.execute(
                    """
                    INSERT INTO dw.fact_pagamento_comprovante (
                      id_empresa, id_filial, referencia, id_db, id_comprovante, id_turno, id_usuario,
                      tipo_forma, valor, dt_evento, data_key, payload
                    )
                    VALUES (%s, %s, %s, 1, %s, 81, 910, %s, %s, TIMESTAMPTZ '2026-03-08 10:05:00+00', 20260308, '{}'::jsonb)
                    ON CONFLICT (id_empresa, id_filial, referencia, tipo_forma) DO NOTHING
                    """,
                    (tenant_id, branch_id, referencia, comprovante_id, tipo_forma, valor),
                )
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_caixa_turno_aberto")
            conn.execute("REFRESH MATERIALIZED VIEW mart.agg_caixa_forma_pagamento")
            conn.commit()

        _, body = self._get_hot_route_json(
            f"/bi/cash/overview?dt_ini=2026-03-01&dt_fim=2026-03-31&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )

        payment_mix = body["historical"]["payment_mix"]
        labels = {str(item["label"]) for item in payment_mix}
        self.assertEqual(len(payment_mix), len(payment_types))
        self.assertIn("Depósito Bancário", labels)

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
                VALUES (%s, %s, 61, 1, 910, TIMESTAMPTZ '2026-03-05 08:00:00+00', TIMESTAMPTZ '2026-03-05 19:00:00+00', 20260305, 20260305, 901, false, 'CLOSED', '{"TURNO":"3"}'::jsonb)
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

        _, fraud_body = self._get_hot_route_json(
            f"/bi/fraud/overview?dt_ini=2026-03-01&dt_fim=2026-03-10&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )

        cash_payload = {
            "source_status": "ok",
            "summary": "Leitura consolidada do caixa disponível.",
            "kpis": {
                "caixas_periodo": 1,
                "dias_com_movimento": 1,
                "ticket_medio": 300.0,
                "total_vendas": 300.0,
                "total_pagamentos": 0.0,
                "total_cancelamentos": 300.0,
                "qtd_cancelamentos": 1,
                "caixas_com_cancelamento": 1,
            },
            "definitions": repos_mart.cash_definitions(),
            "historical": {
                "source_status": "ok",
                "summary": "Histórico consolidado disponível.",
                "requested_window": {"dt_ini": "2026-03-01", "dt_fim": "2026-03-10"},
                "coverage": {"min_data_key": 20260305, "max_data_key": 20260305},
                "kpis": {
                    "caixas_periodo": 1,
                    "dias_com_movimento": 1,
                    "ticket_medio": 300.0,
                    "total_vendas": 300.0,
                    "total_pagamentos": 0.0,
                    "total_cancelamentos": 300.0,
                    "qtd_cancelamentos": 1,
                    "caixas_com_cancelamento": 1,
                },
                "by_day": [],
                "payment_mix": [],
                "top_turnos": [{"id_turno": 61, "usuario_label": "Operadora do Caixa"}],
                "cancelamentos": [{"id_turno": 61, "usuario_label": "Operadora do Caixa", "total_cancelamentos": 300.0, "qtd_cancelamentos": 1}],
            },
            "live_now": {
                "source_status": "unavailable",
                "summary": "Monitor operacional indisponível.",
                "kpis": {
                    "total_turnos": 1,
                    "caixas_abertos_fonte": 0,
                    "caixas_abertos": 0,
                    "caixas_stale": 0,
                    "caixas_criticos": 0,
                    "caixas_alto_risco": 0,
                    "caixas_em_monitoramento": 0,
                    "total_vendas_abertas": 0.0,
                    "total_cancelamentos_abertos": 0.0,
                    "snapshot_ts": None,
                    "latest_activity_ts": None,
                    "stale_window_hours": 96,
                    "schema_mode": "rich",
                },
                "open_boxes": [],
                "stale_boxes": [],
                "payment_mix": [],
                "cancelamentos": [],
                "alerts": [],
            },
            "open_boxes": [],
            "stale_boxes": [],
            "payment_mix": [],
            "cancelamentos": [{"id_turno": 61, "usuario_label": "Operadora do Caixa", "total_cancelamentos": 300.0, "qtd_cancelamentos": 1}],
            "alerts": [],
        }
        self._write_hot_route_snapshot(
            scope_key="cash_overview",
            tenant_id=tenant_id,
            branch_scope=branch_id,
            dt_ini=date(2026, 3, 1),
            dt_fim=date(2026, 3, 10),
            payload=cash_payload,
        )
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
        self.assertEqual(fraud_body["last_events"][0]["turno_label"], "3")
        self.assertEqual(fraud_body["last_events"][0]["filial_label"], "Filial 1003")
        self.assertIn("operador logado", fraud_body["definitions"]["cashier_operator"].lower())
        self.assertNotIn("turnos.id_usuarios", fraud_body["definitions"]["cashier_operator"].lower())

        self.assertEqual(float(cash_body["historical"]["kpis"]["total_cancelamentos"]), 300.0)
        self.assertEqual(int(cash_body["historical"]["kpis"]["qtd_cancelamentos"]), 1)
        self.assertEqual(cash_body["historical"]["cancelamentos"][0]["usuario_label"], "Operadora do Caixa")

    def test_fraud_overview_daily_and_monthly_filters_keep_operational_truth_separate_from_model_coverage(self) -> None:
        tenant_id = self._create_tenant("Tenant Fraud Window Semantics")
        branch_id = 1004
        self._create_branch(tenant_id, branch_id, "Posto Centro", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(owner_email, "Senha@123")

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_usuario_caixa (id_empresa, id_filial, id_usuario, nome, payload)
                VALUES (%s, %s, 915, 'Operadora Janela', '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_usuario) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.dim_funcionario (id_empresa, id_filial, id_funcionario, nome)
                VALUES (%s, %s, 780, 'Frentista Dia 15')
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
                  (%s, %s, 70, 1, 915, TIMESTAMPTZ '2026-03-10 07:00:00+00', TIMESTAMPTZ '2026-03-10 18:00:00+00', 20260310, 20260310, 1, false, 'CLOSED', '{"TURNO":"2"}'::jsonb),
                  (%s, %s, 71, 1, 915, TIMESTAMPTZ '2026-03-15 07:00:00+00', TIMESTAMPTZ '2026-03-15 18:00:00+00', 20260315, 20260315, 1, false, 'CLOSED', '{"TURNO":"3"}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_turno) DO NOTHING
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_comprovante (
                  id_empresa, id_filial, id_db, id_comprovante, data, data_key,
                  id_usuario, id_turno, valor_total, cancelado, situacao, payload
                )
                VALUES (%s, %s, 1, 71001, TIMESTAMPTZ '2026-03-15 12:00:00+00', 20260315, 201, 71, 320, true, 1, '{"CFOP":"5102"}'::jsonb)
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
                VALUES (%s, %s, 1, 71001, TIMESTAMPTZ '2026-03-15 11:55:00+00', 20260315, 201, NULL, 71001, 71, 1, 320, true, '{}'::jsonb)
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
                VALUES (%s, %s, 1, 71001, 1, 20260315, 1, NULL, NULL, 780, 64, 5, 320, 0, 0, 0, 5102, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_risco_evento (
                  id_empresa, id_filial, data_key, data, event_type, id_db, id_movprodutos,
                  id_comprovante, id_turno, id_usuario, id_funcionario, score_risco,
                  score_level, impacto_estimado, valor_total, reasons
                )
                VALUES (%s, %s, 20260310, TIMESTAMPTZ '2026-03-10 14:00:00+00', 'CANCELAMENTO', 1, 71010, 71010, 70, 915, 780, 91, 'ALTO', 84, 120, '["janela_modelada"]'::jsonb)
                """,
                (tenant_id, branch_id),
            )
            conn.execute("REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_diaria")
            conn.execute("REFRESH MATERIALIZED VIEW mart.fraude_cancelamentos_eventos")
            conn.commit()
        self._refresh_risk_marts()

        _, one_day = self._get_hot_route_json(
            "/bi/fraud/overview?dt_ini=2026-03-15&dt_fim=2026-03-15&id_empresa="
            f"{tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(one_day["model_coverage"]["status"], "not_covered")
        self.assertEqual(int(one_day["kpis"]["cancelamentos"]), 1)
        self.assertEqual(one_day["last_events"][0]["data_key"], 20260315)
        self.assertEqual(one_day["last_events"][0]["turno_label"], "3")
        self.assertEqual(one_day["last_events"][0]["filial_label"], "Posto Centro")
        self.assertEqual(len(one_day["risk_last_events"]), 0)
        self.assertIn("operacion", one_day["model_coverage"]["message"].lower())
        self.assertEqual(one_day["business_clock"]["timezone"], "America/Sao_Paulo")

        _, month = self._get_hot_route_json(
            "/bi/fraud/overview?dt_ini=2026-03-01&dt_fim=2026-03-31&id_empresa="
            f"{tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(month["model_coverage"]["status"], "partial")
        self.assertEqual(int(month["model_coverage"]["covered_days"]), 1)
        self.assertEqual(month["risk_last_events"][0]["data_key"], 20260310)
        self.assertEqual(month["risk_last_events"][0]["turno_label"], "2")
        self.assertEqual(month["last_events"][0]["data_key"], 20260315)
        self.assertEqual(month["modeled_risk"]["coverage"]["status"], "partial")

    def test_goals_overview_exposes_monthly_projection_with_goal_and_history(self) -> None:
        tenant_id = self._create_tenant("Tenant Goals Projection")
        branch_id = 1005
        self._create_branch(tenant_id, branch_id, "Posto Projecao", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(owner_email, "Senha@123")

        sales_rows = [
            ("2025-12-10 12:00:00+00", 20251210, 900.0, 91001),
            ("2026-01-10 12:00:00+00", 20260110, 1000.0, 91002),
            ("2026-02-10 12:00:00+00", 20260210, 1200.0, 91003),
            ("2026-03-05 12:00:00+00", 20260305, 500.0, 91004),
            ("2026-03-10 12:00:00+00", 20260310, 700.0, 91005),
            ("2026-03-20 12:00:00+00", 20260320, 800.0, 91006),
        ]
        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_funcionario (id_empresa, id_filial, id_funcionario, nome)
                VALUES (%s, %s, 805, 'Vendedor Projecao')
                ON CONFLICT (id_empresa, id_filial, id_funcionario) DO NOTHING
                """,
                (tenant_id, branch_id),
            )
            for data_ts, data_key, total_venda, mov_id in sales_rows:
                conn.execute(
                    """
                    INSERT INTO dw.fact_venda (
                      id_empresa, id_filial, id_db, id_movprodutos, data, data_key,
                      id_usuario, id_cliente, id_comprovante, id_turno, saidas_entradas,
                      total_venda, cancelado, payload
                    )
                    VALUES (%s, %s, 1, %s, %s, %s, 501, NULL, %s, 80, 1, %s, false, '{}'::jsonb)
                    ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos) DO NOTHING
                    """,
                    (tenant_id, branch_id, mov_id, data_ts, data_key, mov_id, total_venda),
                )
                conn.execute(
                    """
                    INSERT INTO dw.fact_venda_item (
                      id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                      id_produto, id_grupo_produto, id_local_venda, id_funcionario, qtd,
                      valor_unitario, total, desconto, custo_total, margem, cfop, payload
                    )
                    VALUES (%s, %s, 1, %s, 1, %s, 1, NULL, NULL, 805, 1, %s, %s, 0, 0, %s, 5102, '{}'::jsonb)
                    ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos) DO NOTHING
                    """,
                    (tenant_id, branch_id, mov_id, data_key, total_venda, total_venda, total_venda * 0.2),
                )
            conn.execute(
                """
                INSERT INTO app.goals (id_empresa, id_filial, goal_date, goal_type, target_value)
                VALUES (%s, %s, '2026-03-01', 'FATURAMENTO', 4000)
                ON CONFLICT (id_empresa, id_filial, goal_date, goal_type)
                DO UPDATE SET target_value = EXCLUDED.target_value
                """,
                (tenant_id, branch_id),
            )
            conn.commit()
        self._refresh_sales_marts()

        _, body = self._get_hot_route_json(
            f"/bi/goals/overview?dt_ini=2026-03-01&dt_fim=2026-03-20&dt_ref=2026-03-20&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        projection = body["monthly_projection"]
        self.assertEqual(body["business_clock"]["timezone"], "America/Sao_Paulo")
        self.assertEqual(projection["status"], "below_goal")
        self.assertEqual(float(projection["summary"]["mtd_actual"]), 2000.0)
        self.assertEqual(float(projection["summary"]["projection_adjusted"]), 3100.0)
        self.assertTrue(bool(projection["goal"]["configured"]))
        self.assertEqual(float(projection["goal"]["target_value"]), 4000.0)
        self.assertAlmostEqual(float(projection["goal"]["required_daily_to_goal"]), 181.82, places=2)
        self.assertEqual(projection["forecast"]["method"], "mtd_average")
        self.assertEqual(len(projection["history"]["last_3_months"]), 3)
        self.assertEqual(projection["history"]["last_3_months"][0]["month_ref"], "2026-02-01")

    def test_goals_overview_leaderboard_reads_current_sales_without_waiting_for_mart_refresh(self) -> None:
        tenant_id = self._create_tenant("Tenant Goals Live Leaderboard")
        branch_id = 1008
        self._create_branch(tenant_id, branch_id, "Posto Leaderboard", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(owner_email, "Senha@123")

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_funcionario (id_empresa, id_filial, id_funcionario, nome)
                VALUES (%s, %s, 915, 'Vendedor Operacional')
                ON CONFLICT (id_empresa, id_filial, id_funcionario)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda (
                  id_empresa, id_filial, id_db, id_movprodutos, data, data_key, id_comprovante,
                  saidas_entradas, total_venda, cancelado, situacao, payload
                )
                VALUES (%s, %s, 1, 9801, TIMESTAMPTZ '2026-03-20 12:00:00+00', 20260320, 8801, 1, 350, false, 1, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos)
                DO UPDATE SET total_venda = EXCLUDED.total_venda, situacao = EXCLUDED.situacao
                """,
                (tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_venda_item (
                  id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos, data_key,
                  id_produto, id_grupo_produto, id_funcionario, cfop, qtd, valor_unitario, total,
                  desconto, custo_total, margem, payload
                )
                VALUES (%s, %s, 1, 9801, 1, 20260320, 1, 1, 915, 5102, 1, 350, 350, 0, 250, 100, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, id_movprodutos, id_itensmovprodutos)
                DO UPDATE SET total = EXCLUDED.total, margem = EXCLUDED.margem
                """,
                (tenant_id, branch_id),
            )
            conn.commit()

        response = self.client.get(
            f"/bi/goals/overview?dt_ini=2026-03-20&dt_fim=2026-03-20&dt_ref=2026-03-20&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        self.assertEqual(response.status_code, 200, response.text)
        leaderboard = response.json()["leaderboard"]
        self.assertTrue(leaderboard, response.text)
        self.assertEqual(leaderboard[0]["funcionario_nome"], "Vendedor Operacional")
        self.assertEqual(round(float(leaderboard[0]["faturamento"]), 2), 350.0)

    def test_goals_target_requires_product_write_access_and_single_branch_scope(self) -> None:
        tenant_id = self._create_tenant("Tenant Goals Target Write")
        branch_id = 1006
        second_branch_id = 1007
        self._create_branch(tenant_id, branch_id, "Posto Meta 1006", is_active=True)
        self._create_branch(tenant_id, second_branch_id, "Posto Meta 1007", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        viewer_email = self._create_user("tenant_viewer", "Senha@123", tenant_id=tenant_id, branch_id=branch_id)
        owner_headers = self._auth_headers(owner_email, "Senha@123")
        viewer_headers = self._auth_headers(viewer_email, "Senha@123")

        denied_response = self.client.post(
            f"/bi/goals/target?id_empresa={tenant_id}&id_filial={branch_id}",
            json={"target_value": 5500, "goal_month": "2026-03-01"},
            headers=viewer_headers,
        )
        self.assertEqual(denied_response.status_code, 403, denied_response.text)
        denied_body = denied_response.json()
        detail = denied_body.get("detail") if isinstance(denied_body, dict) else None
        self.assertEqual((detail or {}).get("error"), "product_readonly")

        multi_branch_response = self.client.post(
            f"/bi/goals/target?id_empresa={tenant_id}&id_filiais={branch_id}&id_filiais={second_branch_id}",
            json={"target_value": 5600, "goal_month": "2026-03-01"},
            headers=owner_headers,
        )
        self.assertEqual(multi_branch_response.status_code, 400, multi_branch_response.text)

        allowed_response = self.client.post(
            f"/bi/goals/target?id_empresa={tenant_id}&id_filial={branch_id}",
            json={"target_value": 5700.25, "goal_month": "2026-03-18"},
            headers=owner_headers,
        )
        self.assertEqual(allowed_response.status_code, 200, allowed_response.text)
        goal = allowed_response.json()["goal"]
        self.assertEqual(goal["id_empresa"], tenant_id)
        self.assertEqual(goal["id_filial"], branch_id)
        self.assertEqual(goal["month_ref"], "2026-03-01")
        self.assertAlmostEqual(float(goal["target_value"]), 5700.25, places=2)

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            stored = conn.execute(
                """
                SELECT target_value
                FROM app.goals
                WHERE id_empresa = %s
                  AND id_filial = %s
                  AND goal_date = %s
                  AND goal_type = 'FATURAMENTO'
                """,
                (tenant_id, branch_id, "2026-03-01"),
            ).fetchone()
            conn.commit()
        self.assertIsNotNone(stored)
        self.assertAlmostEqual(float(stored["target_value"]), 5700.25, places=2)

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
        username = self._unique_username("product-global")

        create_response = self.client.post(
            "/platform/users",
            json={
                "nome": "Produto Global",
                "email": self._unique_email("product-global"),
                "username": username,
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
                "username": create_response.json()["username"],
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
            },
            headers=headers,
        )
        self.assertEqual(create_response.status_code, 200, create_response.text)
        tenant_id = int(create_response.json()["id_empresa"])
        self.assertEqual(int(create_response.json()["sales_history_days"]), 365)
        self.assertEqual(int(create_response.json()["default_product_scope_days"]), 1)

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
                (tenant_id, False, business_today(tenant_id)),
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
        username = self._unique_username("tenant-notify")

        user_response = self.client.post(
            "/platform/users",
            json={
                "nome": "Tenant Notification User",
                "email": self._unique_email("notify"),
                "username": username,
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

        _, body = self._get_hot_route_json(
            f"/bi/customers/overview?dt_ini=2026-03-01&dt_fim=2026-03-31&id_empresa={tenant_id}&id_filial={branch_id}",
            headers=headers,
        )
        top_customers = body["top_customers"]
        names = [item["cliente_nome"] for item in top_customers]
        self.assertIn("Cliente Válido", names)
        self.assertNotIn("Fornecedor Exclusivo", names)

    def test_customers_overview_exposes_delinquency_summary_and_priority_grid(self) -> None:
        tenant_id = self._create_tenant("Tenant Customers Delinquency")
        branch_id = 1202
        self._create_branch(tenant_id, branch_id, "Filial 1202", is_active=True)
        owner_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        headers = self._auth_headers(owner_email, "Senha@123")

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            conn.execute(
                """
                INSERT INTO dw.dim_cliente (id_empresa, id_filial, id_cliente, nome)
                VALUES
                  (%s, %s, 9201, 'Cliente Cobrança 1'),
                  (%s, %s, 9202, 'Cliente Cobrança 2')
                ON CONFLICT (id_empresa, id_filial, id_cliente)
                DO UPDATE SET nome = EXCLUDED.nome
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.execute(
                """
                INSERT INTO dw.fact_financeiro (
                  id_empresa, id_filial, id_db, tipo_titulo, id_titulo, id_entidade,
                  data_emissao, data_key_emissao, vencimento, data_key_venc,
                  data_pagamento, data_key_pgto, valor, valor_pago, payload
                )
                VALUES
                  (%s, %s, 1, 1, 92001, 9201, DATE '2026-03-01', 20260301, DATE '2026-03-10', 20260310, NULL, NULL, 300, 100, '{}'::jsonb),
                  (%s, %s, 1, 1, 92002, 9202, DATE '2026-02-10', 20260210, DATE '2026-02-20', 20260220, NULL, NULL, 500, 0, '{}'::jsonb)
                ON CONFLICT (id_empresa, id_filial, id_db, tipo_titulo, id_titulo)
                DO UPDATE SET
                  id_entidade = EXCLUDED.id_entidade,
                  data_emissao = EXCLUDED.data_emissao,
                  data_key_emissao = EXCLUDED.data_key_emissao,
                  vencimento = EXCLUDED.vencimento,
                  data_key_venc = EXCLUDED.data_key_venc,
                  valor = EXCLUDED.valor,
                  valor_pago = EXCLUDED.valor_pago
                """,
                (tenant_id, branch_id, tenant_id, branch_id),
            )
            conn.commit()

        _, body = self._get_hot_route_json(
            (
                f"/bi/customers/overview?dt_ini=2026-03-01&dt_fim=2026-03-31"
                f"&dt_ref=2026-03-31&id_empresa={tenant_id}&id_filial={branch_id}"
            ),
            headers=headers,
        )

        delinquency = body["delinquency"]
        self.assertEqual(delinquency["dt_ref"], "2026-03-31")
        self.assertEqual(int(delinquency["summary"]["clientes_em_aberto"]), 2)
        self.assertEqual(int(delinquency["summary"]["titulos_em_aberto"]), 2)
        self.assertEqual(round(float(delinquency["summary"]["valor_total"]), 2), 700.0)
        self.assertEqual(int(delinquency["summary"]["max_dias_atraso"]), 39)

        buckets = {item["bucket"]: round(float(item["valor"]), 2) for item in delinquency["buckets"]}
        self.assertEqual(buckets["16_30"], 200.0)
        self.assertEqual(buckets["31_60"], 500.0)

        self.assertEqual(delinquency["customers"][0]["cliente_nome"], "Cliente Cobrança 2")
        self.assertEqual(round(float(delinquency["customers"][0]["valor_aberto"]), 2), 500.0)
        self.assertEqual(delinquency["customers"][0]["bucket_label"], "31-60 dias")

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
                "username": validate_username(username_from_email_candidate(target_email)),
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
                "username": validate_username(username_from_email_candidate(target_email)),
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

    def test_sovereign_platform_master_can_save_existing_user_name_change_without_password(self) -> None:
        sovereign_email = self._ensure_sovereign_user()
        headers = self._auth_headers(sovereign_email, seed_cli.PLATFORM_MASTER_PASSWORD)
        tenant_id = self._create_tenant("Tenant Sovereign Simple Save")
        self._create_branch(tenant_id, 1932, "Filial Sovereign Simple Save")
        target_email = self._create_user("tenant_admin", "Senha@123", tenant_id=tenant_id)
        target_user_id = self._user_id_by_email(target_email)
        normalized_username = validate_username(username_from_email_candidate(target_email))

        update_response = self.client.patch(
            f"/platform/users/{target_user_id}",
            json={
                "nome": "Nome Ajustado Platform Master",
                "email": target_email,
                "username": normalized_username,
                "password": None,
                "role": "tenant_admin",
                "is_enabled": True,
                "valid_from": None,
                "valid_until": None,
                "must_change_password": False,
                "locked_until": None,
                "reset_failed_login": False,
                "accesses": [
                    {
                        "role": "tenant_admin",
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
        self.assertEqual(update_response.status_code, 200, update_response.text)
        self.assertEqual(update_response.json()["nome"], "Nome Ajustado Platform Master")
        self.assertEqual(update_response.json()["email"], target_email)
        self.assertEqual(update_response.json()["username"], normalized_username)
        self.assertEqual(update_response.json()["role"], "tenant_admin")
        self.assertIsNone(update_response.json()["locked_until"])

        with get_conn(role="MASTER", tenant_id=None, branch_id=None) as conn:
            saved_row = conn.execute(
                """
                SELECT nome, email, username, role, locked_until
                FROM auth.users
                WHERE id = %s::uuid
                """,
                (target_user_id,),
            ).fetchone()
            conn.commit()
        self.assertIsNotNone(saved_row)
        self.assertEqual(saved_row["nome"], "Nome Ajustado Platform Master")
        self.assertEqual(saved_row["email"], target_email)
        self.assertEqual(saved_row["username"], normalized_username)
        self.assertEqual(saved_row["role"], "tenant_admin")
        self.assertIsNone(saved_row["locked_until"])

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
                "username": validate_username(username_from_email_candidate(target_email)),
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
                "username": validate_username(username_from_email_candidate(sovereign_email)),
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
                "username": validate_username(username_from_email_candidate(target_email)),
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
