"""Upsert condicional: lote 100% unchanged não pode abortar o ciclo do agent."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

# runner importa extractors.xpert → pyodbc; no CI Linux sem ODBC usamos mock.
sys.modules.setdefault("pyodbc", MagicMock())

from agent.runner import AgentRunner  # noqa: E402


def test_validate_batch_delivery_accepts_unchanged_noop() -> None:
    runner = AgentRunner.__new__(AgentRunner)
    runner.logger = __import__("logging").getLogger("test")

    runner._validate_batch_delivery(
        dataset="clientes",
        ds_cfg={"enabled": True, "full_refresh": False},
        rows=[{"ID_ENTIDADE": 1, "ID_FILIAL": 1}] * 3,
        extracted=135,
        inserted=0,
        rejected=0,
        spooled=False,
        ingest_result={"unchanged": 135, "rejected_invalid": 0, "rejected_by_retention": 0},
    )


def test_validate_batch_delivery_accepts_partial_noop_with_rejects() -> None:
    """Overlap window: most rows unchanged, a few rejected — must NOT fail the dataset."""
    runner = AgentRunner.__new__(AgentRunner)
    runner.logger = __import__("logging").getLogger("test")

    runner._validate_batch_delivery(
        dataset="comprovantes",
        ds_cfg={"enabled": True, "full_refresh": False},
        rows=[{"ID_COMPROVANTE": 1, "ID_FILIAL": 1, "ID_DB": 1}] * 10,
        extracted=10,
        inserted=0,
        rejected=1,
        spooled=False,
        ingest_result={
            "unchanged": 9,
            "rejected": 1,
            "rejected_invalid": 1,
            "rejected_by_retention": 0,
            "details": [{"reason": "Missing id_filial"}],
        },
    )


def test_validate_batch_delivery_still_fails_when_truly_empty() -> None:
    runner = AgentRunner.__new__(AgentRunner)
    runner.logger = __import__("logging").getLogger("test")

    try:
        runner._validate_batch_delivery(
            dataset="clientes",
            ds_cfg={"enabled": True, "full_refresh": False},
            rows=[{"ID_ENTIDADE": 1}],
            extracted=10,
            inserted=0,
            rejected=0,
            spooled=False,
            ingest_result={"unchanged": 0, "rejected_invalid": 0, "rejected_by_retention": 0},
        )
    except RuntimeError as exc:
        assert "nothing inserted" in str(exc)
    else:
        raise AssertionError("expected RuntimeError for true empty delivery")


def test_allow_zero_does_not_mask_full_invalid_rejection() -> None:
    runner = AgentRunner.__new__(AgentRunner)
    runner.logger = __import__("logging").getLogger("test")

    try:
        runner._validate_batch_delivery(
            dataset="turnos",
            ds_cfg={"enabled": True, "allow_zero_inserted_batches": True},
            rows=[{"ID_TURNOS": 1, "ID_FILIAL": 1}] * 5,
            extracted=5,
            inserted=0,
            rejected=5,
            spooled=False,
            ingest_result={
                "unchanged": 0,
                "rejected_invalid": 5,
                "rejected_by_retention": 0,
                "details": [{"reason": "Missing ID_TURNOS"}],
            },
        )
    except RuntimeError as exc:
        assert "payload contract" in str(exc)
    else:
        raise AssertionError("expected RuntimeError for full invalid rejection")
