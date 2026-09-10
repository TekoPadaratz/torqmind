"""Regressões de isolamento de filiais do Jarvis (Prompt 1 hardening).

Não chama OpenAI/Telegram reais — mocks obrigatórios.
"""
from __future__ import annotations

import unittest
from datetime import date
from unittest.mock import patch

from fastapi import HTTPException

from app import scope
from app import routes_bi
from app.permissions import redact_sensitive
from app.services import jarvis_ai


T1 = 1
T2 = 2
FILIAL_A = 101
FILIAL_B = 102
FILIAL_C = 103  # não autorizada para o usuário T1 restrito


class BranchScopeAsIdsUnitTest(unittest.TestCase):
    def test_none_and_empty_never_mean_all(self) -> None:
        self.assertEqual(scope.branch_scope_as_ids(None), [])
        self.assertEqual(scope.branch_scope_as_ids([]), [])

    def test_single_and_multi(self) -> None:
        self.assertEqual(scope.branch_scope_as_ids(FILIAL_A), [FILIAL_A])
        self.assertEqual(scope.branch_scope_as_ids([FILIAL_B, FILIAL_A]), [FILIAL_A, FILIAL_B])

    def test_primary_branch_id_still_collapses_multi_to_none(self) -> None:
        # Documenta a armadilha histórica — Jarvis não deve mais usar isso.
        self.assertIsNone(scope.primary_branch_id([FILIAL_A, FILIAL_B]))
        self.assertIsNone(scope.primary_branch_id([]))


class JarvisSqlScopeUnitTest(unittest.TestCase):
    def test_candidate_insights_uses_any_for_multi_and_excludes_unauthorized(self) -> None:
        captured: dict = {}

        class _Cur:
            def fetchall(self):
                return [
                    {"id": 1, "id_empresa": T1, "id_filial": FILIAL_A, "ai_plan": None},
                    {"id": 2, "id_empresa": T1, "id_filial": FILIAL_B, "ai_plan": None},
                ]

        class _Conn:
            def execute(self, sql, params):
                captured["sql"] = sql
                captured["params"] = list(params)
                return _Cur()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        with patch.object(jarvis_ai, "get_conn", return_value=_Conn()):
            rows = jarvis_ai._candidate_insights("OWNER", T1, [FILIAL_A, FILIAL_B], date(2026, 9, 10), 10)

        self.assertIn("ANY(%s)", captured["sql"])
        self.assertEqual(captured["params"][0], T1)
        self.assertEqual(captured["params"][2], [FILIAL_A, FILIAL_B])
        self.assertEqual(len(rows), 2)

    def test_candidate_insights_empty_scope_denies_with_false_predicate(self) -> None:
        captured: dict = {}

        class _Cur:
            def fetchall(self):
                return [{"id": 99, "id_filial": FILIAL_C}]  # não deve ser usado se SQL negar

        class _Conn:
            def execute(self, sql, params):
                captured["sql"] = sql
                captured["params"] = list(params)
                return _Cur()

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        with patch.object(jarvis_ai, "get_conn", return_value=_Conn()):
            jarvis_ai._candidate_insights("OWNER", T1, [], date(2026, 9, 10), 10)

        self.assertIn("1 = 0", captured["sql"])
        self.assertEqual(captured["params"], [T1, date(2026, 9, 10), 10])

    def test_generate_empty_scope_skips_openai_and_notifications(self) -> None:
        with (
            patch.object(jarvis_ai, "_candidate_insights") as cand,
            patch.object(jarvis_ai, "_call_openai_structured") as openai,
            patch.object(jarvis_ai, "_create_notification_for_critical") as notify,
            patch.object(jarvis_ai, "send_telegram_alert") as tg,
        ):
            stats = jarvis_ai.generate_jarvis_ai_plans("OWNER", T1, [], date(2026, 9, 10), limit=5)

        cand.assert_not_called()
        openai.assert_not_called()
        notify.assert_not_called()
        tg.assert_not_called()
        self.assertTrue(stats["scope_denied"])
        self.assertEqual(stats["candidates"], 0)
        self.assertEqual(stats["openai_calls"], 0)

    def test_generate_multi_branch_skips_insight_outside_authorized_set(self) -> None:
        insights = [
            {
                "id": 1,
                "id_empresa": T1,
                "id_filial": FILIAL_A,
                "insight_type": "CANCEL",
                "severity": "WARN",
                "dt_ref": date(2026, 9, 10),
                "impacto_estimado": 10,
                "title": "A",
                "message": "A",
                "recommendation": "ok",
                "meta": {},
                "ai_plan": None,
            },
            {
                "id": 2,
                "id_empresa": T1,
                "id_filial": FILIAL_C,
                "insight_type": "CANCEL",
                "severity": "CRITICAL",
                "dt_ref": date(2026, 9, 10),
                "impacto_estimado": 99,
                "title": "C leak",
                "message": "C",
                "recommendation": "no",
                "meta": {},
                "ai_plan": None,
            },
        ]
        notified: list[int] = []

        with (
            patch.object(jarvis_ai, "_candidate_insights", return_value=insights),
            patch.object(jarvis_ai, "_read_cache", return_value=None),
            patch.object(jarvis_ai, "_write_cache"),
            patch.object(jarvis_ai, "_attach_plan_to_insight"),
            patch.object(
                jarvis_ai,
                "_call_openai_structured",
                return_value=({"priority": "HIGH", "diagnosis": "x"}, 1, 1),
            ) as openai,
            patch.object(
                jarvis_ai,
                "_create_notification_for_critical",
                side_effect=lambda *a, **k: notified.append(int(a[2])),
            ),
            patch.object(jarvis_ai, "send_telegram_alert"),
            patch.object(jarvis_ai.settings, "openai_api_key", "test-key"),
            patch.object(jarvis_ai.settings, "jarvis_model_fast", "gpt-test"),
        ):
            stats = jarvis_ai.generate_jarvis_ai_plans(
                "OWNER", T1, [FILIAL_A, FILIAL_B], date(2026, 9, 10), limit=10, force=True
            )

        self.assertEqual(stats["openai_calls"], 1)
        self.assertEqual(stats["processed"], 1)
        self.assertEqual(notified, [FILIAL_A])
        self.assertNotIn(FILIAL_C, notified)

    def test_cache_read_never_uses_null_filial_wildcard(self) -> None:
        self.assertIsNone(jarvis_ai._read_cache("OWNER", T1, None, "hash", "model"))

    def test_ai_usage_empty_scope_returns_zeros_without_query(self) -> None:
        with patch.object(jarvis_ai, "get_conn") as get_conn:
            out = jarvis_ai.ai_usage_summary("OWNER", T1, [], days=7)
        get_conn.assert_not_called()
        self.assertTrue(out["scope_denied"])
        self.assertEqual(out["totals"]["cache_rows"], 0)


class JarvisRouteScopeUnitTest(unittest.TestCase):
    """Chama handlers diretamente (sem TestClient/lifespan/PG) com mocks."""

    def _owner_claims(self, *, empresa: int = T1) -> dict:
        return {
            "role": "OWNER",
            "user_role": "tenant_admin",
            "id_empresa": empresa,
            "id_filial": None,
            "tenant_ids": [empresa],
            "access": {"product": True},
            "accesses": [{"id_empresa": empresa, "id_filial": None, "role": "tenant_admin"}],
            "can_view_sensitive_financials": True,
        }

    def test_briefing_multi_passes_list_not_primary_none(self) -> None:
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(T1, [FILIAL_A, FILIAL_B], [FILIAL_A, FILIAL_B])),
            patch.object(
                routes_bi.repos_mart,
                "jarvis_briefing",
                return_value={"status": "ok", "margem": 12.5, "headline": "x"},
            ) as briefing,
        ):
            body = routes_bi.jarvis_briefing(
                dt_ref=date(2026, 9, 10),
                id_filial=None,
                id_filiais=[FILIAL_A, FILIAL_B],
                id_empresa=T1,
                claims=self._owner_claims(),
                _screen=None,
            )

        self.assertEqual(briefing.call_args.args[2], [FILIAL_A, FILIAL_B])
        self.assertEqual(body["id_filiais"], [FILIAL_A, FILIAL_B])

    def test_briefing_empty_scope_does_not_call_repo(self) -> None:
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(T1, [], None)),
            patch.object(routes_bi.repos_mart, "jarvis_briefing") as briefing,
        ):
            body = routes_bi.jarvis_briefing(
                dt_ref=date(2026, 9, 10),
                id_filial=None,
                id_filiais=None,
                id_empresa=T1,
                claims=self._owner_claims(),
                _screen=None,
            )

        briefing.assert_not_called()
        self.assertTrue(body.get("scope_denied"))
        self.assertEqual(body.get("id_filiais"), [])

    def test_briefing_unauthorized_branch_is_forbidden(self) -> None:
        def _deny(*args, **kwargs):
            raise HTTPException(
                status_code=403,
                detail={"error": "branch_access_denied", "message": "Acesso não permitido à filial."},
            )

        with (
            patch.object(routes_bi, "resolve_scope_filters", side_effect=_deny),
            patch.object(routes_bi.repos_mart, "jarvis_briefing") as briefing,
        ):
            with self.assertRaises(HTTPException) as exc:
                routes_bi.jarvis_briefing(
                    dt_ref=date(2026, 9, 10),
                    id_filial=FILIAL_C,
                    id_filiais=None,
                    id_empresa=T1,
                    claims=self._owner_claims(),
                    _screen=None,
                )

        self.assertEqual(exc.exception.status_code, 403)
        briefing.assert_not_called()

    def test_generate_single_branch_passes_int(self) -> None:
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(T1, FILIAL_A, [FILIAL_A])),
            patch.object(routes_bi.repos_auth, "assert_product_write_allowed"),
            patch.object(
                routes_bi,
                "generate_jarvis_ai_plans",
                return_value={"processed": 0, "openai_calls": 0, "scope_denied": False},
            ) as gen,
        ):
            body = routes_bi.jarvis_generate(
                dt_ref=date(2026, 9, 10),
                id_filial=FILIAL_A,
                id_filiais=None,
                limit=10,
                force=False,
                id_empresa=T1,
                claims=self._owner_claims(),
                _screen=None,
            )

        self.assertEqual(gen.call_args.args[2], FILIAL_A)
        self.assertEqual(body["id_filiais"], [FILIAL_A])

    def test_generate_global_authorized_passes_all_active_branches(self) -> None:
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(T1, [FILIAL_A, FILIAL_B], None)),
            patch.object(routes_bi.repos_auth, "assert_product_write_allowed"),
            patch.object(
                routes_bi,
                "generate_jarvis_ai_plans",
                return_value={"processed": 0, "openai_calls": 0, "scope_denied": False},
            ) as gen,
        ):
            body = routes_bi.jarvis_generate(
                dt_ref=date(2026, 9, 10),
                id_filial=None,
                id_filiais=None,
                limit=10,
                force=False,
                id_empresa=T1,
                claims=self._owner_claims(),
                _screen=None,
            )

        self.assertEqual(gen.call_args.args[2], [FILIAL_A, FILIAL_B])
        self.assertEqual(body["id_filiais"], [FILIAL_A, FILIAL_B])

    def test_generate_tenant_isolation_keeps_t2_out_of_t1_call(self) -> None:
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(T1, FILIAL_A, [FILIAL_A])),
            patch.object(routes_bi.repos_auth, "assert_product_write_allowed"),
            patch.object(
                routes_bi,
                "generate_jarvis_ai_plans",
                return_value={"processed": 0, "openai_calls": 0},
            ) as gen,
        ):
            routes_bi.jarvis_generate(
                dt_ref=date(2026, 9, 10),
                id_filial=FILIAL_A,
                id_filiais=None,
                limit=10,
                force=False,
                id_empresa=T1,
                claims=self._owner_claims(empresa=T1),
                _screen=None,
            )

        self.assertEqual(gen.call_args.args[1], T1)
        self.assertNotEqual(gen.call_args.args[1], T2)

    def test_ai_usage_empty_scope(self) -> None:
        with (
            patch.object(routes_bi, "resolve_scope_filters", return_value=(T1, [], None)),
            patch.object(routes_bi.repos_auth, "assert_product_write_allowed"),
        ):
            body = routes_bi.admin_ai_usage(
                days=7,
                id_filial=None,
                id_filiais=None,
                id_empresa=T1,
                claims=self._owner_claims(),
            )

        self.assertTrue(body.get("scope_denied"))
        self.assertEqual(body.get("id_filiais"), [])


class JarvisRedactionUnitTest(unittest.TestCase):
    def test_manager_redacts_margin_fields_in_briefing_payload(self) -> None:
        claims = {
            "role": "MANAGER",
            "user_role": "tenant_manager",
            "access": {"product": True},
            "can_view_sensitive_financials": False,
        }
        payload = {
            "headline": "ok",
            "margem": 33.3,
            "lucro": 10.0,
            "custo": 5.0,
            "impact_value": 100.0,
        }
        redacted = redact_sensitive(dict(payload), claims)
        self.assertIsNone(redacted.get("margem"))
        self.assertIsNone(redacted.get("lucro"))
        self.assertIsNone(redacted.get("custo"))
        self.assertEqual(redacted.get("headline"), "ok")
        self.assertEqual(redacted.get("impact_value"), 100.0)


if __name__ == "__main__":
    unittest.main()
