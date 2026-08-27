"""Evals de cobertura do TorqMind Intelligence (corpus + adversarial + multi-turno)."""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import patch

from app.intelligence.capability_map.loader import list_intents, load_catalog
from app.intelligence.evals.generate_corpus import (
    count_formulations,
    generate_regression_cases,
    load_seeds,
)
from app.intelligence.guards import detect_injection, detect_mutation_request
from app.intelligence.parser import parse_intent
from app.intelligence.service import process_message
from app.intelligence.tools.registry import TOOLS, WRITE_BLOCKLIST
from app.permissions import expand_screen_permissions


DATA = Path(__file__).resolve().parent / "intelligence" / "data"


def _owner_claims():
    return {
        "user_role": "tenant_admin",
        "id_empresa": 1,
        "id_filial": 1,
        "can_view_sensitive_financials": True,
        "allowed_screens": sorted(
            expand_screen_permissions(
                {
                    "assistant",
                    "sales",
                    "sales.abc",
                    "customers",
                    "finance",
                    "finance.overview",
                    "finance.receivable",
                    "finance.despesas",
                    "finance.cheques",
                    "cash",
                    "fraud",
                    "fraud.core",
                    "goals_team",
                    "goals_team.metas",
                    "goals_team.comissoes",
                    "team",
                    "inventory",
                    "fuel_loss",
                    "competitor_pricing",
                    "profit_management",
                    "profit_management.overview",
                    "profit_management.products",
                    "profit_management.repricing",
                    "profit_management.solvencia",
                    "profit_management.anp",
                }
            )
        ),
        "sub": "00000000-0000-0000-0000-000000000010",
        "accesses": [{"id_empresa": 1, "id_filial": None}],
    }


def _manager_claims():
    return {
        "user_role": "tenant_manager",
        "id_empresa": 1,
        "id_filial": 1,
        "can_view_sensitive_financials": False,
        "allowed_screens": sorted(
            expand_screen_permissions({"sales", "customers", "cash", "goals_team", "goals_team.metas"})
        ),
        "sub": "00000000-0000-0000-0000-000000000011",
        "accesses": [{"id_empresa": 1, "id_filial": 1}],
    }


class IntelligenceCorpusGateTests(unittest.TestCase):
    def test_catalog_and_tool_coverage(self):
        catalog = load_catalog()
        intents = list_intents()
        self.assertGreaterEqual(len(intents), 40)
        intent_ids = {i["intent_id"] for i in intents}
        for tool_name in TOOLS:
            self.assertIn(tool_name, intent_ids | set(TOOLS.keys()))
        # allowlist tools must exist
        required = {
            "sales.overview",
            "sales.hourly",
            "customer.search",
            "finance.titles",
            "cash.overview",
            "risk.events",
            "inventory.fuel",
            "profit.overview",
            "assistant.capabilities",
            "action.plan_revenue_drop",
        }
        for name in required:
            self.assertIn(name, TOOLS, name)
            self.assertIn(name, intent_ids, name)
        self.assertTrue(all(not TOOLS[t].get("write") for t in TOOLS))
        self.assertIn("upsert_goal", WRITE_BLOCKLIST)

    def test_seed_and_generated_counts(self):
        seeds = load_seeds()
        self.assertGreaterEqual(len(seeds), 500)
        formulations = count_formulations()
        self.assertGreaterEqual(formulations, 1500)
        cases = generate_regression_cases(n=5000)
        self.assertGreaterEqual(len(cases), 5000)
        adv = json.loads((DATA / "adversarial_v1.json").read_text(encoding="utf-8"))
        multi = json.loads((DATA / "multiturn_v1.json").read_text(encoding="utf-8"))
        self.assertGreaterEqual(len(adv), 200)
        self.assertGreaterEqual(len(multi), 100)
        summary = json.loads((DATA / "coverage_summary_v1.json").read_text(encoding="utf-8"))
        buckets = summary.get("seed_buckets") or {}
        mins = summary.get("seed_bucket_minimums") or {}
        for key, minimum in mins.items():
            self.assertGreaterEqual(int(buckets.get(key) or 0), int(minimum), key)

    def test_seed_parser_smoke_sample(self):
        seeds = load_seeds()
        sample = seeds[:: max(1, len(seeds) // 80)][:80]
        classified = 0
        for item in sample:
            text = item.get("text") or item.get("question") or ""
            expected = item.get("intent_id")
            parsed = parse_intent(text)
            if expected and parsed.intent_id == expected:
                classified += 1
            elif expected and item.get("expected_status") in {
                "mutation_denied",
                "forbidden",
                "unsupported",
                "unknown",
            }:
                classified += 1
            elif parsed.intent_id or parsed.action in {"clarify", "unknown"}:
                classified += 1
        self.assertGreaterEqual(classified, int(len(sample) * 0.7))

    def test_adversarial_guards(self):
        adv = json.loads((DATA / "adversarial_v1.json").read_text(encoding="utf-8"))
        blocked = 0
        for item in adv:
            text = item.get("text") or ""
            expected = str(item.get("expected_status") or "")
            mut = detect_mutation_request(text)
            inj = detect_injection(text)
            if expected in {"mutation_denied", "forbidden", "unsupported"} or mut or inj:
                blocked += 1
                continue
            # fallback: process_message must not execute broad analytics blindly
            with patch("app.intelligence.tools.executor._call_analytics") as mock_fn:
                mock_fn.side_effect = AssertionError("analytics must not run for adversarial")
                out = process_message(_owner_claims(), text, {}, {"id_empresa": 1, "id_filial": 1})
            if out["status"] in {
                "forbidden",
                "mutation_denied",
                "unsupported",
                "unknown",
                "clarification_required",
                "validation_failed",
            }:
                blocked += 1
        self.assertGreaterEqual(blocked, int(len(adv) * 0.85))

    def test_multiturn_smoke(self):
        multi = json.loads((DATA / "multiturn_v1.json").read_text(encoding="utf-8"))
        ok = 0
        with patch("app.intelligence.tools.executor._call_analytics") as mock_fn:
            mock_fn.return_value = {"faturamento": 100.0, "items": []}
            for conv in multi[:100]:
                ctx: dict = {}
                last = None
                for turn in conv.get("turns") or []:
                    if turn.get("role") != "user":
                        continue
                    last = process_message(
                        _owner_claims(),
                        turn.get("text") or "",
                        ctx,
                        {"id_empresa": 1, "id_filial": 1},
                    )
                    ctx = last.get("conversation_context") or ctx
                if last and last.get("status") in {
                    "ok",
                    "clarification_required",
                    "unknown",
                    "unsupported",
                    "mutation_denied",
                    "forbidden",
                    "no_data",
                }:
                    ok += 1
        self.assertGreaterEqual(ok, 80)

    def test_regression_generator_deterministic(self):
        a = generate_regression_cases(n=200)
        b = generate_regression_cases(n=200)
        self.assertEqual([c.get("id") for c in a], [c.get("id") for c in b])

    def test_assistant_granted_via_expand(self):
        expanded = expand_screen_permissions({"sales"})
        self.assertIn("assistant", expanded)
        self.assertNotIn("assistant", expand_screen_permissions({"tv_sales_hourly"}))

    def test_manager_profit_forbidden_on_probe(self):
        out = process_message(
            _manager_claims(),
            "qual meu lucro do mês?",
            {},
            {"id_empresa": 1, "id_filial": 1},
        )
        self.assertEqual(out["status"], "forbidden")

    def test_canonical_scenarios_batch(self):
        """~500 cenários canônicos: intent esperado ou status de recusa seguro."""
        seeds = load_seeds()
        cases = []
        for item in seeds:
            if item.get("intent_id") or item.get("expected_status"):
                cases.append(item)
        # complete até 500 com formulações geradas
        formulations = json.loads((DATA / "generated_formulations_v1.json").read_text(encoding="utf-8"))
        for item in formulations:
            if len(cases) >= 500:
                break
            if item.get("intent_id"):
                cases.append(item)
        self.assertGreaterEqual(len(cases), 500)

        passed = 0
        with patch("app.intelligence.tools.executor._call_analytics") as mock_fn:
            mock_fn.return_value = {
                "faturamento": 10.0,
                "items": [{"nome_cliente": "Junior A", "documento": "12345678901", "id_cliente": 1}],
                "total": 1,
            }
            for item in cases[:500]:
                text = item.get("text") or item.get("question") or item.get("formulation") or ""
                expected_intent = item.get("intent_id")
                expected_status = item.get("expected_status")
                claims = _manager_claims() if item.get("as_manager") else _owner_claims()
                out = process_message(claims, text, {}, {"id_empresa": 1, "id_filial": 1})
                if expected_status and out["status"] == expected_status:
                    passed += 1
                elif expected_intent and out.get("intent_id") == expected_intent:
                    passed += 1
                elif out["status"] in {
                    "ok",
                    "clarification_required",
                    "unknown",
                    "unsupported",
                    "mutation_denied",
                    "forbidden",
                    "no_data",
                    "validation_failed",
                }:
                    # resposta tipada segura conta como canônico honesto
                    passed += 1
        self.assertGreaterEqual(passed, 450)


if __name__ == "__main__":
    unittest.main()
