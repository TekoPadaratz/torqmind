"""Resolução de filial por apelido (VR 01) no assistente."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from app.intelligence.branch_resolve import _score_hint, resolve_branch_hint
from app.intelligence.parser import parse_intent


class IntelligenceBranchResolveTests(unittest.TestCase):
    def test_parser_extracts_vr_hint(self):
        out = parse_intent("Quanto faturou hoje na VR 01?")
        self.assertEqual(out.intent_id, "sales.overview")
        self.assertEqual(out.slots.get("filial_label"), "VR 01")

    def test_score_hint_vr_variants(self):
        self.assertGreaterEqual(_score_hint("VR 01", "VR01"), 0.9)
        self.assertGreaterEqual(_score_hint("vr01", "VR 01"), 0.9)

    @patch("app.intelligence.branch_resolve._branch_catalog")
    def test_resolve_single_branch(self, mock_catalog):
        mock_catalog.return_value = {101: "VR 01", 102: "VR 02"}
        scope = {"id_empresa": 1, "id_filial": [101, 102], "branch_scope": "all"}
        claims = {"id_empresa": 1, "accesses": [{"id_empresa": 1, "id_filial": 101}, {"id_empresa": 1, "id_filial": 102}]}
        result = resolve_branch_hint("VR 01", scope, claims)
        self.assertEqual(result.status, "resolved")
        self.assertEqual(result.id_filial, 101)
        self.assertEqual(result.label, "VR 01")

    @patch("app.intelligence.branch_resolve._branch_catalog")
    def test_resolve_ambiguous_same_prefix(self, mock_catalog):
        mock_catalog.return_value = {101: "VR 01", 102: "VR 02"}
        scope = {"id_empresa": 1, "id_filial": [101, 102]}
        claims = {"id_empresa": 1}
        result = resolve_branch_hint("VR", scope, claims)
        self.assertEqual(result.status, "ambiguous")
        self.assertGreaterEqual(len(result.candidates), 2)


if __name__ == "__main__":
    unittest.main()
