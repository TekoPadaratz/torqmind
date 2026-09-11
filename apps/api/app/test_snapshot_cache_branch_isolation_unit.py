"""Isolamento do cache BI: escopo efetivo em exact + compatible fallback.

Exercita ``_with_cached_response`` real (sem mock do wrapper). Fronteira de
banco/serviços mockada. Sem Postgres neste host.
"""
from __future__ import annotations

import unittest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

from app import routes_bi
from app.services import snapshot_cache


FILIAL_A, FILIAL_B, FILIAL_C = 101, 102, 103
DT_INI = date(2026, 9, 1)
DT_FIM = date(2026, 9, 10)
DT_REF = date(2026, 9, 10)


def _abc_context(**overrides):
    base = {
        "scope_v": 2,
        "branch_scope_kind": "multi",
        "branch_ids": [FILIAL_A, FILIAL_B, FILIAL_C],
        "dt_ini": DT_INI.isoformat(),
        "dt_fim": "2026-09-05",
        "dt_ref": DT_REF.isoformat(),
    }
    base.update(overrides)
    return base


def _record(payload: dict, context: dict, *, sig: str = "sig-abc") -> dict:
    return {
        "snapshot_data": payload,
        "scope_context": context,
        "scope_signature": sig,
        "updated_at": datetime(2026, 9, 5, 12, 0, tzinfo=timezone.utc),
        "branch_id": None,
    }


class SnapshotContextVersionUnitTest(unittest.TestCase):
    def test_empty_and_legacy_unscoped_signatures_differ(self) -> None:
        empty = routes_bi._build_snapshot_context(DT_INI, DT_FIM, DT_REF, [])
        unscoped = routes_bi._build_snapshot_context(DT_INI, DT_FIM, DT_REF, None)
        multi = routes_bi._build_snapshot_context(DT_INI, DT_FIM, DT_REF, [FILIAL_A, FILIAL_B])
        self.assertEqual(empty["branch_scope_kind"], "empty")
        self.assertEqual(unscoped["branch_scope_kind"], "unscoped")
        self.assertEqual(multi["branch_scope_kind"], "multi")
        self.assertNotEqual(
            snapshot_cache.build_scope_signature(empty),
            snapshot_cache.build_scope_signature(unscoped),
        )
        # Historical ambiguous shape (no kind) must not extract as empty.
        self.assertIsNone(
            snapshot_cache.extract_branch_ids_from_context({"branch_ids": []})
        )

    def test_legacy_ambiguous_not_compatible_with_empty_or_multi(self) -> None:
        expected_empty = routes_bi._build_snapshot_context(DT_INI, DT_FIM, DT_REF, [])
        expected_ab = routes_bi._build_snapshot_context(DT_INI, DT_FIM, DT_REF, [FILIAL_A, FILIAL_B])
        legacy = {"branch_ids": [], "dt_ini": DT_INI.isoformat()}
        self.assertFalse(snapshot_cache.contexts_share_effective_branches(expected_empty, legacy))
        self.assertFalse(snapshot_cache.contexts_share_effective_branches(expected_ab, legacy))


class CachedResponseBranchIsolationTest(unittest.TestCase):
    """_with_cached_response real + protect_reads + exact miss."""

    def _run_protected(self, *, branch_scope, rows_for_compatible, safe_fallback=None):
        compute = MagicMock(side_effect=AssertionError("compute must not run under protect_reads"))
        captured_kwargs: dict = {}

        def fake_compatible(role, tenant_id, branch_id, snapshot_key, expected_context):
            captured_kwargs["tenant_id"] = tenant_id
            captured_kwargs["expected_context"] = expected_context
            for row in rows_for_compatible:
                if snapshot_cache.record_is_compatible_for_fallback(row, expected_context):
                    return row
            return None

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=None),
            patch.object(
                routes_bi.snapshot_cache,
                "read_latest_compatible_snapshot_record",
                side_effect=fake_compatible,
            ),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": True, "reasons": ["etl_running"], "etl_running": True},
            ),
            patch.object(routes_bi.snapshot_cache, "route_snapshot_is_bypassed", return_value=False),
        ):
            payload = routes_bi._with_cached_response(
                scope_key="fraud_probe",
                role="MASTER",
                tenant_id=1,
                branch_scope=branch_scope,
                dt_ini=DT_INI,
                dt_fim=DT_FIM,
                dt_ref=DT_REF,
                compute=compute,
                safe_fallback=safe_fallback or (lambda: {"kpis": {"leaked": False}, "source": "fallback"}),
            )
        compute.assert_not_called()
        return payload, captured_kwargs

    def test_empty_scope_never_reads_or_returns_abc_snapshot(self) -> None:
        abc = _record({"kpis": {"leaked": True, "from": "ABC"}}, _abc_context())
        # Even if the reader were to return ABC, empty short-circuits before reads.
        with (
            patch.object(
                routes_bi.snapshot_cache,
                "read_snapshot_record",
                side_effect=AssertionError("empty must not read exact snapshot"),
            ),
            patch.object(
                routes_bi.snapshot_cache,
                "read_latest_compatible_snapshot_record",
                side_effect=AssertionError("empty must not read compatible snapshot"),
            ),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                side_effect=AssertionError("empty must not consult guard for snapshots"),
            ),
            patch.object(routes_bi.snapshot_cache, "route_snapshot_is_bypassed", return_value=False),
        ):
            payload = routes_bi._with_cached_response(
                scope_key="fraud_probe",
                role="MASTER",
                tenant_id=1,
                branch_scope=[],
                dt_ini=DT_INI,
                dt_fim=DT_FIM,
                dt_ref=DT_REF,
                compute=MagicMock(side_effect=AssertionError("empty must not compute")),
                safe_fallback=lambda: {"kpis": {"leaked": False}},
            )
        self.assertFalse(payload["kpis"]["leaked"])
        self.assertEqual(payload["_snapshot_cache"]["mode"], "empty_branch_scope")
        self.assertNotEqual(payload.get("kpis"), abc["snapshot_data"]["kpis"])

    def test_ab_scope_never_receives_abc_compatible_snapshot(self) -> None:
        abc = _record({"kpis": {"filiais": "A+B+C", "secret": 1}}, _abc_context())
        payload, captured = self._run_protected(
            branch_scope=[FILIAL_A, FILIAL_B],
            rows_for_compatible=[abc],
            safe_fallback=lambda: {"kpis": {"filiais": "fallback"}},
        )
        self.assertEqual(payload["kpis"]["filiais"], "fallback")
        self.assertNotEqual(payload["_snapshot_cache"]["source"], "snapshot")
        self.assertEqual(captured["expected_context"]["branch_ids"], [FILIAL_A, FILIAL_B])
        self.assertEqual(captured["tenant_id"], 1)

    def test_legacy_ambiguous_context_rejected_for_empty_and_ab(self) -> None:
        legacy = _record(
            {"kpis": {"legacy": True}},
            {"branch_ids": [], "dt_ini": DT_INI.isoformat()},  # no kind / no scope_v
            sig="legacy-empty-shape",
        )
        payload_empty, _ = self._run_protected(branch_scope=[], rows_for_compatible=[legacy])
        self.assertEqual(payload_empty["_snapshot_cache"]["mode"], "empty_branch_scope")

        payload_ab, _ = self._run_protected(
            branch_scope=[FILIAL_A, FILIAL_B],
            rows_for_compatible=[legacy],
            safe_fallback=lambda: {"kpis": {"ok": True}},
        )
        self.assertNotEqual(payload_ab["_snapshot_cache"].get("source"), "snapshot")
        self.assertTrue(payload_ab["kpis"].get("ok"))

    def test_legitimately_compatible_same_branches_different_dates(self) -> None:
        stored = _record(
            {"kpis": {"cancelamentos": 7}},
            {
                "scope_v": 2,
                "branch_scope_kind": "multi",
                "branch_ids": [FILIAL_A, FILIAL_B],
                "dt_ini": DT_INI.isoformat(),
                "dt_fim": "2026-09-05",
                "dt_ref": DT_REF.isoformat(),
            },
            sig="older-dates",
        )
        payload, _ = self._run_protected(
            branch_scope=[FILIAL_A, FILIAL_B],
            rows_for_compatible=[stored],
        )
        self.assertEqual(payload["kpis"]["cancelamentos"], 7)
        self.assertEqual(payload["_snapshot_cache"]["mode"], "protected_stale_snapshot")
        self.assertFalse(payload["_snapshot_cache"]["exact_scope_match"])

    def test_different_tenant_not_served(self) -> None:
        other_tenant_row = _record(
            {"kpis": {"tenant": 2}},
            {
                "scope_v": 2,
                "branch_scope_kind": "multi",
                "branch_ids": [FILIAL_A, FILIAL_B],
                "dt_ini": DT_INI.isoformat(),
                "dt_fim": DT_FIM.isoformat(),
            },
        )
        seen_tenants: list[int] = []

        def fake_compatible(role, tenant_id, branch_id, snapshot_key, expected_context):
            seen_tenants.append(tenant_id)
            # Simulate DB already filtered by id_empresa — wrong tenant never returned.
            if tenant_id != 1:
                return other_tenant_row
            return None

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=None),
            patch.object(
                routes_bi.snapshot_cache,
                "read_latest_compatible_snapshot_record",
                side_effect=fake_compatible,
            ),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": True, "reasons": ["etl_running"], "etl_running": True},
            ),
            patch.object(routes_bi.snapshot_cache, "route_snapshot_is_bypassed", return_value=False),
        ):
            payload = routes_bi._with_cached_response(
                scope_key="fraud_probe",
                role="MASTER",
                tenant_id=1,
                branch_scope=[FILIAL_A, FILIAL_B],
                dt_ini=DT_INI,
                dt_fim=DT_FIM,
                dt_ref=DT_REF,
                compute=MagicMock(side_effect=AssertionError("no compute")),
                safe_fallback=lambda: {"kpis": {"tenant": 1}},
            )
        self.assertEqual(seen_tenants, [1])
        self.assertEqual(payload["kpis"]["tenant"], 1)

    def test_realtime_bypass_still_computes_live(self) -> None:
        compute = MagicMock(return_value={"kpis": {"live": True}})
        with (
            patch.object(routes_bi.snapshot_cache, "route_snapshot_is_bypassed", return_value=True),
            patch.object(
                routes_bi.snapshot_cache,
                "read_snapshot_record",
                side_effect=AssertionError("bypass must skip snapshot reads"),
            ),
            patch.object(
                routes_bi.snapshot_cache,
                "read_latest_compatible_snapshot_record",
                side_effect=AssertionError("bypass must skip compatible reads"),
            ),
        ):
            payload = routes_bi._with_cached_response(
                scope_key="dashboard_home",
                role="MASTER",
                tenant_id=1,
                branch_scope=[FILIAL_A, FILIAL_B],
                dt_ini=DT_INI,
                dt_fim=DT_FIM,
                dt_ref=DT_REF,
                compute=compute,
            )
        self.assertTrue(payload["kpis"]["live"])
        self.assertEqual(payload["_snapshot_cache"]["mode"], "cache_bypassed")
        compute.assert_called_once()

    def test_compatible_reader_filters_sql_candidates(self) -> None:
        """Real reader filters rows — id_filial NULL alone is not enough."""
        rows = [
            {
                "snapshot_data": {"kpis": {"bad": True}},
                "scope_context": _abc_context(),
                "updated_at": datetime(2026, 9, 8, tzinfo=timezone.utc),
                "scope_signature": "abc",
                "id_filial": None,
            },
            {
                "snapshot_data": {"kpis": {"good": True}},
                "scope_context": {
                    "scope_v": 2,
                    "branch_scope_kind": "multi",
                    "branch_ids": [FILIAL_A, FILIAL_B],
                    "dt_ini": DT_INI.isoformat(),
                    "dt_fim": "2026-09-05",
                },
                "updated_at": datetime(2026, 9, 7, tzinfo=timezone.utc),
                "scope_signature": "ab",
                "id_filial": None,
            },
        ]

        class _Cur:
            def fetchall(self):
                return rows

        class _Conn:
            def execute(self, sql, params):
                self.sql = sql
                self.params = params
                return _Cur()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        expected = routes_bi._build_snapshot_context(DT_INI, DT_FIM, DT_REF, [FILIAL_A, FILIAL_B])
        with patch.object(snapshot_cache, "get_conn", return_value=_Conn()):
            hit = snapshot_cache.read_latest_compatible_snapshot_record(
                "MASTER",
                1,
                None,
                "fraud_probe",
                expected_context=expected,
            )
        self.assertIsNotNone(hit)
        self.assertTrue(hit["snapshot_data"]["kpis"].get("good"))
        self.assertFalse(hit["snapshot_data"]["kpis"].get("bad"))


class CachedResponseBusinessFilterCompatTest(unittest.TestCase):
    """Compatible fallback must match business filters, not only branches."""

    def _run_sales_protected(self, *, id_grupos, rows_for_compatible, safe_fallback=None):
        compute = MagicMock(side_effect=AssertionError("compute must not run under protect_reads"))

        def fake_compatible(role, tenant_id, branch_id, snapshot_key, expected_context):
            for row in rows_for_compatible:
                if snapshot_cache.record_is_compatible_for_fallback(row, expected_context):
                    return row
            return None

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=None),
            patch.object(
                routes_bi.snapshot_cache,
                "read_latest_compatible_snapshot_record",
                side_effect=fake_compatible,
            ),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": True, "reasons": ["etl_running"], "etl_running": True},
            ),
            patch.object(routes_bi.snapshot_cache, "route_snapshot_is_bypassed", return_value=False),
        ):
            payload = routes_bi._with_cached_response(
                scope_key="sales_overview",
                role="MASTER",
                tenant_id=1,
                branch_scope=[FILIAL_A, FILIAL_B],
                dt_ini=DT_INI,
                dt_fim=DT_FIM,
                dt_ref=DT_REF,
                compute=compute,
                extra_context={"module": "sales", "id_grupos": list(id_grupos)},
                safe_fallback=safe_fallback
                or (lambda: {"produtos": [], "source": "fallback", "id_grupos_meta": list(id_grupos)}),
            )
        compute.assert_not_called()
        return payload

    def test_same_branches_different_id_grupos_rejected(self) -> None:
        stored = _record(
            {"produtos": [{"grupo": 20, "nome": "Grupo20"}], "kpis": {"from": "g20"}},
            {
                "scope_v": 2,
                "branch_scope_kind": "multi",
                "branch_ids": [FILIAL_A, FILIAL_B],
                "dt_ini": DT_INI.isoformat(),
                "dt_fim": "2026-09-05",
                "dt_ref": DT_REF.isoformat(),
                "module": "sales",
                "id_grupos": [20],
            },
            sig="sales-g20",
        )
        payload = self._run_sales_protected(
            id_grupos=[10],
            rows_for_compatible=[stored],
            safe_fallback=lambda: {"produtos": [], "kpis": {"from": "fallback"}, "requested_grupos": [10]},
        )
        self.assertEqual(payload["kpis"]["from"], "fallback")
        self.assertEqual(payload.get("requested_grupos"), [10])
        self.assertNotEqual(payload["_snapshot_cache"].get("source"), "snapshot")
        self.assertNotIn({"grupo": 20, "nome": "Grupo20"}, payload.get("produtos") or [])

    def test_same_branches_same_id_grupos_different_dates_accepted(self) -> None:
        stored = _record(
            {"produtos": [{"grupo": 10}], "kpis": {"from": "g10-stale"}},
            {
                "scope_v": 2,
                "branch_scope_kind": "multi",
                "branch_ids": [FILIAL_A, FILIAL_B],
                "dt_ini": DT_INI.isoformat(),
                "dt_fim": "2026-09-05",
                "dt_ref": "2026-09-05",
                "module": "sales",
                "id_grupos": [10],
            },
            sig="sales-g10-older",
        )
        payload = self._run_sales_protected(id_grupos=[10], rows_for_compatible=[stored])
        self.assertEqual(payload["kpis"]["from"], "g10-stale")
        self.assertEqual(payload["_snapshot_cache"]["mode"], "protected_stale_snapshot")
        self.assertFalse(payload["_snapshot_cache"]["exact_scope_match"])

    def test_legacy_sales_without_id_grupos_rejected(self) -> None:
        legacy = _record(
            {"produtos": [{"grupo": 99}], "kpis": {"from": "legacy"}},
            {
                "scope_v": 2,
                "branch_scope_kind": "multi",
                "branch_ids": [FILIAL_A, FILIAL_B],
                "module": "sales",
                # missing id_grupos — cannot prove filter compatibility
            },
            sig="sales-legacy",
        )
        payload = self._run_sales_protected(
            id_grupos=[10],
            rows_for_compatible=[legacy],
            safe_fallback=lambda: {"kpis": {"from": "fallback"}},
        )
        self.assertEqual(payload["kpis"]["from"], "fallback")

    def test_finance_include_flags_must_match(self) -> None:
        stored = _record(
            {"kpis": {"from": "no-series"}},
            {
                "scope_v": 2,
                "branch_scope_kind": "multi",
                "branch_ids": [FILIAL_A, FILIAL_B],
                "include_series": False,
                "include_payments": True,
                "include_operational": True,
            },
            sig="fin-no-series",
        )
        compute = MagicMock(side_effect=AssertionError("no compute"))

        def fake_compatible(role, tenant_id, branch_id, snapshot_key, expected_context):
            if snapshot_cache.record_is_compatible_for_fallback(stored, expected_context):
                return stored
            return None

        with (
            patch.object(routes_bi.snapshot_cache, "read_snapshot_record", return_value=None),
            patch.object(
                routes_bi.snapshot_cache,
                "read_latest_compatible_snapshot_record",
                side_effect=fake_compatible,
            ),
            patch.object(
                routes_bi.snapshot_cache,
                "get_hot_route_guard",
                return_value={"protect_reads": True, "reasons": ["etl_running"], "etl_running": True},
            ),
            patch.object(routes_bi.snapshot_cache, "route_snapshot_is_bypassed", return_value=False),
        ):
            payload = routes_bi._with_cached_response(
                scope_key="finance_overview",
                role="MASTER",
                tenant_id=1,
                branch_scope=[FILIAL_A, FILIAL_B],
                dt_ini=DT_INI,
                dt_fim=DT_FIM,
                dt_ref=DT_REF,
                compute=compute,
                extra_context={
                    "include_series": True,
                    "include_payments": True,
                    "include_operational": True,
                },
                safe_fallback=lambda: {"kpis": {"from": "fallback"}},
            )
        self.assertEqual(payload["kpis"]["from"], "fallback")

    def test_fraud_contract_version_must_match(self) -> None:
        stored = _record(
            {"kpis": {"from": "v2"}},
            {
                "scope_v": 2,
                "branch_scope_kind": "multi",
                "branch_ids": [FILIAL_A, FILIAL_B],
                "module": "fraud",
                "contract_version": 2,
                "sections": ["risco"],
            },
            sig="fraud-v2",
        )
        expected = routes_bi._build_snapshot_context(
            DT_INI,
            DT_FIM,
            DT_REF,
            [FILIAL_A, FILIAL_B],
            {"module": "fraud", "contract_version": 3, "sections": ["risco"]},
        )
        self.assertFalse(snapshot_cache.contexts_are_compatible_for_fallback(expected, stored["scope_context"]))
        expected_ok = dict(expected)
        expected_ok["contract_version"] = 2
        self.assertTrue(
            snapshot_cache.contexts_are_compatible_for_fallback(expected_ok, stored["scope_context"])
        )

    def test_missing_scope_v_rejected_even_with_same_branches(self) -> None:
        expected = routes_bi._build_snapshot_context(DT_INI, DT_FIM, DT_REF, [FILIAL_A, FILIAL_B])
        legacy = {
            "branch_scope_kind": "multi",
            "branch_ids": [FILIAL_A, FILIAL_B],
            "dt_ini": DT_INI.isoformat(),
        }
        self.assertFalse(snapshot_cache.contexts_are_compatible_for_fallback(expected, legacy))


if __name__ == "__main__":
    unittest.main()
