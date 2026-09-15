"""Parent→child follow-up after comprovantes prevents commission orphans."""

from __future__ import annotations

import os
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

sys.modules.setdefault("pyodbc", types.ModuleType("pyodbc"))

from agent.runner import AgentRunner  # noqa: E402


class ParentChildFollowupTests(unittest.TestCase):
    def test_followup_fetches_itens_by_comprovante_keys(self):
        runner = AgentRunner.__new__(AgentRunner)
        runner.logger = MagicMock()
        runner.cfg = SimpleNamespace(
            runtime=SimpleNamespace(
                batch_size=2000,
                fetch_size=1000,
                batch_delay_seconds=0,
                revisit_max_rows=5000,
            ),
            datasets={
                "itenscomprovantes": {
                    "enabled": True,
                    "revisit_max_rows": 25000,
                    "batch_size": 100,
                }
            },
        )
        runner.extractor = MagicMock()
        runner.extractor.fetch_rows_by_keys.return_value = [
            {"ID_FILIAL": 11621, "ID_DB": 11621, "ID_COMPROVANTE": 1, "ID_ITENSCOMPROVANTE": 10},
            {"ID_FILIAL": 11621, "ID_DB": 11621, "ID_COMPROVANTE": 2, "ID_ITENSCOMPROVANTE": 11},
        ]
        runner.sink = MagicMock()
        runner.sink.send.return_value = {"inserted_or_updated": 2, "rejected": 0, "spooled": False}
        runner._validate_outgoing_batch = MagicMock(return_value=SimpleNamespace())
        runner._validate_batch_delivery = MagicMock()

        runner._followup_comprovante_children(
            [
                {"ID_FILIAL": 11621, "ID_DB": 11621, "ID_COMPROVANTE": 1},
                {"ID_FILIAL": 11621, "ID_DB": 11621, "ID_COMPROVANTE": 1},  # dup
                {"ID_FILIAL": 11621, "ID_DB": 11621, "ID_COMPROVANTE": 2},
            ]
        )

        runner.extractor.fetch_rows_by_keys.assert_called_once()
        kwargs = runner.extractor.fetch_rows_by_keys.call_args
        self.assertEqual(kwargs.args[0], "itenscomprovantes")
        self.assertEqual(len(kwargs.kwargs["keys"]), 2)
        runner.sink.send.assert_called_once()
        self.assertEqual(runner.sink.send.call_args.kwargs["dataset"], "itenscomprovantes")


if __name__ == "__main__":
    unittest.main()
