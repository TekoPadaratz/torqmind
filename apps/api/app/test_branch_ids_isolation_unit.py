"""Isolamento de filiais: _branch_ids([]) ≠ todas + cache por escopo efetivo.

Dados sintéticos: empresas T1/T2, filiais A/B/C. Sem Hom/Prod.
TestClient/app.main só importados nos testes de rota (evita hang de pool no collect).
"""
from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from fastapi import HTTPException

from app import routes_bi
from app import scope
from app.repos_mart import _branch_scope_clause as pg_branch_clause
from app.repos_mart import _branch_ids as pg_branch_ids
from app.repos_mart_clickhouse import _branch_clause as ch_branch_clause
from app.repos_mart_clickhouse import _branch_ids as ch_branch_ids
from app.repos_mart_realtime import _branch_clause as rt_branch_clause
from app.repos_mart_realtime import _branch_ids as rt_branch_ids
from app.services import snapshot_cache
from app.test_scope_filters_unit import _ConnStub


T1, T2 = 1, 2
FILIAL_A, FILIAL_B, FILIAL_C = 101, 102, 103


class BranchIdsHelperIsolationTest(unittest.TestCase):
    def test_empty_list_never_widens_to_all_on_any_layer(self) -> None:
        self.assertEqual(pg_branch_ids([]), [])
        self.assertEqual(ch_branch_ids([]), [])
        self.assertEqual(rt_branch_ids([]), [])

        pg_sql, pg_params = pg_branch_clause("id_filial", [])
        self.assertIn("1 = 0", pg_sql)
        self.assertEqual(pg_params, [])

        for name, clause in (("ch", ch_branch_clause), ("rt", rt_branch_clause)):
            with self.subTest(layer=name):
                sql = clause("id_filial", [])
                self.assertIn("0", sql.replace(" ", ""))
                self.assertNotEqual(sql.strip(), "")

    def test_none_and_minus_one_remain_tenant_wide_marker(self) -> None:
        self.assertIsNone(rt_branch_ids(None))
        self.assertIsNone(rt_branch_ids(-1))
        self.assertEqual(rt_branch_clause("id_filial", None), "")
        self.assertEqual(ch_branch_clause("id_filial", -1), "")

    def test_single_and_multi(self) -> None:
        self.assertEqual(rt_branch_ids(FILIAL_A), [FILIAL_A])
        self.assertEqual(rt_branch_ids([FILIAL_B, FILIAL_A]), [FILIAL_A, FILIAL_B])
        self.assertIn(str(FILIAL_A), rt_branch_clause("id_filial", FILIAL_A))
        multi = rt_branch_clause("id_filial", [FILIAL_A, FILIAL_B])
        self.assertIn("IN", multi)
        self.assertIn(str(FILIAL_A), multi)
        self.assertIn(str(FILIAL_B), multi)
        self.assertNotIn(str(FILIAL_C), multi)

    def test_historical_bug_empty_became_none(self) -> None:
        """Documenta a regressão: `values if values else None` amplificava o SQL."""

        def old_realtime(id_filial):
            if id_filial is None or id_filial == -1:
                return None
            if isinstance(id_filial, (list, tuple, set)):
                values = sorted({int(v) for v in id_filial if v is not None and int(v) != -1})
                return values if values else None
            value = int(id_filial)
            return None if value == -1 else [value]

        self.assertIsNone(old_realtime([]))
        self.assertEqual(rt_branch_ids([]), [])


class ResolveScopeEmptyVsGlobalTest(unittest.TestCase):
    def _restricted_ab(self) -> dict:
        return {
            "role": "MANAGER",
            "user_role": "tenant_manager",
            "id_empresa": T1,
            "id_filial": FILIAL_A,
            "tenant_ids": [T1],
            "access": {"product": True},
            "accesses": [
                {"id_empresa": T1, "id_filial": FILIAL_A, "role": "tenant_manager"},
                {"id_empresa": T1, "id_filial": FILIAL_B, "role": "tenant_manager"},
            ],
        }

    def _company_wide(self) -> dict:
        return {
            "role": "OWNER",
            "user_role": "tenant_admin",
            "id_empresa": T1,
            "id_filial": None,
            "tenant_ids": [T1],
            "access": {"product": True},
            "accesses": [{"id_empresa": T1, "id_filial": None, "role": "tenant_admin"}],
        }

    def test_ab_user_cannot_request_c(self) -> None:
        claims = self._restricted_ab()
        with (
            patch("app.scope.set_apelido_scope"),
            patch(
                "app.scope.get_conn",
                return_value=_ConnStub(
                    [{"id_filial": FILIAL_A}, {"id_filial": FILIAL_B}, {"id_filial": FILIAL_C}]
                ),
            ),
        ):
            with self.assertRaises(HTTPException) as exc:
                scope.resolve_scope_filters(claims, id_empresa_q=T1, id_filiais_q=[FILIAL_A, FILIAL_C])
        self.assertEqual(exc.exception.status_code, 403)

    def test_ab_union_when_explicitly_requested(self) -> None:
        claims = self._restricted_ab()
        with (
            patch("app.scope.set_apelido_scope"),
            patch(
                "app.scope.get_conn",
                return_value=_ConnStub(
                    [{"id_filial": FILIAL_A}, {"id_filial": FILIAL_B}, {"id_filial": FILIAL_C}]
                ),
            ),
        ):
            tenant, filial, requested = scope.resolve_scope_filters(
                claims, id_empresa_q=T1, id_filiais_q=[FILIAL_A, FILIAL_B]
            )
        self.assertEqual(tenant, T1)
        self.assertEqual(filial, [FILIAL_A, FILIAL_B])
        self.assertEqual(requested, [FILIAL_A, FILIAL_B])
        self.assertNotIn(FILIAL_C, filial if isinstance(filial, list) else [filial])

    def test_empty_authorized_returns_empty_effective_not_none(self) -> None:
        claims = self._restricted_ab()
        with patch("app.scope.set_apelido_scope"), patch("app.scope.get_conn", return_value=_ConnStub([])):
            tenant, filial, requested = scope.resolve_scope_filters(claims, id_empresa_q=T1)
        self.assertEqual(tenant, T1)
        self.assertEqual(filial, [])
        self.assertIsNone(requested)
        self.assertEqual(rt_branch_ids(filial), [])
        self.assertNotEqual(rt_branch_clause("id_filial", filial).strip(), "")

    def test_company_wide_materializes_active_union_not_sql_none(self) -> None:
        claims = self._company_wide()
        with (
            patch("app.scope.set_apelido_scope"),
            patch(
                "app.scope.get_conn",
                return_value=_ConnStub([{"id_filial": FILIAL_A}, {"id_filial": FILIAL_B}]),
            ),
        ):
            tenant, filial, requested = scope.resolve_scope_filters(claims, id_empresa_q=T1)
        self.assertEqual(tenant, T1)
        self.assertEqual(filial, [FILIAL_A, FILIAL_B])
        self.assertIsNone(requested)
        self.assertEqual(rt_branch_ids(filial), [FILIAL_A, FILIAL_B])

    def test_materialize_empty_is_not_ambiguous_none_none(self) -> None:
        filial_q, filiais_q = scope.materialize_branch_query_targets([], None)
        self.assertIsNone(filial_q)
        self.assertEqual(filiais_q, [])
        self.assertIsNotNone(filiais_q)

    def test_single_branch_still_works(self) -> None:
        claims = self._restricted_ab()
        with (
            patch("app.scope.set_apelido_scope"),
            patch(
                "app.scope.get_conn",
                return_value=_ConnStub(
                    [{"id_filial": FILIAL_A}, {"id_filial": FILIAL_B}, {"id_filial": FILIAL_C}]
                ),
            ),
        ):
            tenant, filial, requested = scope.resolve_scope_filters(
                claims, id_empresa_q=T1, id_filial_q=FILIAL_A
            )
        self.assertEqual(tenant, T1)
        self.assertEqual(filial, FILIAL_A)
        self.assertEqual(requested, [FILIAL_A])


class CacheSignatureIsolationTest(unittest.TestCase):
    def test_empty_and_global_effective_scopes_do_not_share_signature(self) -> None:
        dt = date(2026, 9, 10)
        empty_ctx = routes_bi._build_snapshot_context(dt, dt, dt, [])
        global_ctx = routes_bi._build_snapshot_context(dt, dt, dt, [FILIAL_A, FILIAL_B])
        single_ctx = routes_bi._build_snapshot_context(dt, dt, dt, FILIAL_A)

        empty_sig = snapshot_cache.build_scope_signature(empty_ctx)
        global_sig = snapshot_cache.build_scope_signature(global_ctx)
        single_sig = snapshot_cache.build_scope_signature(single_ctx)
        self.assertNotEqual(empty_sig, global_sig)
        self.assertNotEqual(empty_sig, single_sig)
        self.assertNotEqual(global_sig, single_sig)
        none_ctx = routes_bi._build_snapshot_context(dt, dt, dt, None)
        self.assertEqual(
            snapshot_cache.build_scope_signature(none_ctx),
            empty_sig,
            "None still normalizes to [] — callers must pass effective filial, not requested=None",
        )


class DashboardHomeCacheUsesEffectiveScopeTest(unittest.TestCase):
    """Chama o handler diretamente — evita TestClient/startup PG no host sem banco."""

    def test_cached_route_receives_effective_filial_not_requested_none(self) -> None:
        from datetime import date

        captured: dict = {}

        def _capture(*, branch_scope, compute, **kwargs):
            captured["branch_scope"] = branch_scope
            return compute()

        claims = {
            "role": "OWNER",
            "user_role": "tenant_admin",
            "id_empresa": T1,
            "allowed_screens": ["dashboard_home"],
            "access": {"product": True},
        }
        with (
            patch.object(
                routes_bi,
                "resolve_scope_filters",
                return_value=(T1, [FILIAL_A, FILIAL_B], None),
            ),
            patch.object(routes_bi, "_with_cached_response", side_effect=_capture),
            patch.object(routes_bi.repos_mart, "dashboard_home_bundle", return_value={"ok": True}) as bundle,
            patch.object(routes_bi, "resolve_business_date", return_value=date(2026, 9, 10)),
            patch.object(routes_bi, "redact_sensitive", side_effect=lambda payload, _c: payload),
        ):
            out = routes_bi.dashboard_home(
                dt_ini=date(2026, 9, 1),
                dt_fim=date(2026, 9, 10),
                dt_ref=None,
                id_filial=None,
                id_filiais=None,
                id_empresa=T1,
                claims=claims,
                _screen=None,
            )
        self.assertTrue(out["ok"])
        self.assertEqual(captured["branch_scope"], [FILIAL_A, FILIAL_B])
        self.assertEqual(bundle.call_args.args[2], [FILIAL_A, FILIAL_B])

    def test_empty_effective_scope_cached_separately_and_passed_through(self) -> None:
        from datetime import date

        captured: dict = {}

        def _capture(*, branch_scope, compute, **kwargs):
            captured["branch_scope"] = branch_scope
            return compute()

        claims = {
            "role": "OWNER",
            "user_role": "tenant_admin",
            "id_empresa": T1,
            "allowed_screens": ["dashboard_home"],
            "access": {"product": True},
        }
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(T1, [], None)),
            patch.object(routes_bi, "_with_cached_response", side_effect=_capture),
            patch.object(
                routes_bi.repos_mart, "dashboard_home_bundle", return_value={"ok": True, "empty": True}
            ) as bundle,
            patch.object(routes_bi, "resolve_business_date", return_value=date(2026, 9, 10)),
            patch.object(routes_bi, "redact_sensitive", side_effect=lambda payload, _c: payload),
        ):
            out = routes_bi.dashboard_home(
                dt_ini=date(2026, 9, 1),
                dt_fim=date(2026, 9, 10),
                dt_ref=None,
                id_filial=None,
                id_filiais=None,
                id_empresa=T1,
                claims=claims,
                _screen=None,
            )
        self.assertTrue(out.get("ok"))
        self.assertEqual(captured["branch_scope"], [])
        self.assertEqual(bundle.call_args.args[2], [])


class ProductStockIdleEmptyScopeTest(unittest.TestCase):
    def test_empty_scope_rejects_without_widening(self) -> None:
        claims = {
            "role": "OWNER",
            "user_role": "tenant_admin",
            "allowed_screens": ["product_management"],
            "access": {"product": True},
        }
        with patch.object(routes_bi, "resolve_scope_filters", return_value=(T1, [], None)):
            with self.assertRaises(HTTPException) as exc:
                routes_bi.product_stock_idle_overview(
                    min_dias_sem_venda=7,
                    setor=None,
                    limit=50,
                    offset=0,
                    id_filial=None,
                    id_filiais=None,
                    id_empresa=T1,
                    claims=claims,
                    _screen=None,
                )
        self.assertEqual(exc.exception.status_code, 422)


if __name__ == "__main__":
    unittest.main()
