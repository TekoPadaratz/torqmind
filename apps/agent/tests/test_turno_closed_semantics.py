"""TURNOS close/revisit semantics — ENCERRANTEFECHAMENTO is NOT a close flag."""

from __future__ import annotations

import os
import sys
import types
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

# Host runners may lack libodbc; stub before importing agent.runner.
sys.modules.setdefault("pyodbc", types.ModuleType("pyodbc"))

from agent.runner import AgentRunner  # noqa: E402
from agent.config import DEFAULT_DATASETS  # noqa: E402


class TurnoClosedSemanticsTests(unittest.TestCase):
    def test_encerrante_nonzero_while_open_is_still_open(self):
        # Pump meter reading mid-shift — must NOT mark as closed.
        self.assertFalse(
            AgentRunner._turno_is_closed(
                {
                    "ENCERRANTEFECHAMENTO": 6069003,
                    "DATAFECHAMENTO": None,
                    "STATUSTURNO": 0,
                }
            )
        )

    def test_statusturno_5_is_closed(self):
        self.assertTrue(
            AgentRunner._turno_is_closed(
                {
                    "ENCERRANTEFECHAMENTO": 0,
                    "DATAFECHAMENTO": None,
                    "STATUSTURNO": 5,
                }
            )
        )

    def test_datafechamento_is_closed(self):
        self.assertTrue(
            AgentRunner._turno_is_closed(
                {
                    "ENCERRANTEFECHAMENTO": 0,
                    "DATAFECHAMENTO": "2026-07-28T12:00:00",
                    "STATUSTURNO": 0,
                }
            )
        )

    def test_turnos_dataset_has_revisit_for_open_and_recent(self):
        cfg = DEFAULT_DATASETS["turnos"]
        clause = str(cfg.get("revisit_open_clause") or "")
        self.assertIn("STATUSTURNO", clause)
        self.assertIn("DATEADD(day,-45", clause)
        self.assertIn("STATUSTURNO", cfg["preflight_tables"]["dbo.TURNOS"])


if __name__ == "__main__":
    unittest.main()
