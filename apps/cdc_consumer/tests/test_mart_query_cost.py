"""Static contracts for ClickHouse mart query-cost reductions."""

from __future__ import annotations

import inspect
import re

import pytest

from torqmind_cdc_consumer.mart_builder import MartBuilder, _has_sales_exit_item_pred


@pytest.fixture(scope="module")
def builder() -> MartBuilder:
    return MartBuilder(enabled=False)


class TestNfeLatestStatusCteBatchFilter:
    def test_filtered_cte_joins_comprovantes_by_data_key(self, builder: MartBuilder):
        cte = builder._nfe_latest_status_cte(
            "nfe_latest",
            data_keys=[20260911],
            id_empresa=1,
            id_filial=2,
        )
        assert "stg_comprovantes_slim" in cte
        assert "data_key IN (20260911)" in cte
        assert "id_empresa = 1" in cte
        assert "id_filial = 2" in cte
        # Must NOT filter by NF emission date — comprovante.data_key ≠ NF date.
        assert not re.search(r"\bDATA\b", cte)
        assert "dt_emissao" not in cte.lower()
        assert "data_emissao" not in cte.lower()

    def test_unfiltered_cte_has_no_comprovantes_slim_join(self, builder: MartBuilder):
        cte = builder._nfe_latest_status_cte("nfe_latest")
        assert "stg_comprovantes_slim" not in cte
        assert "argMax(status, source_ts_ms)" in cte
        assert "stg_nfe_slim" in cte


class TestSalesExitItemDataKeyFilter:
    def test_pred_without_keys_unchanged(self):
        pred = _has_sales_exit_item_pred("torqmind_current", "c")
        assert "i.data_key IN" not in pred
        assert "stg_itenscomprovantes_slim" in pred

    def test_pred_with_keys_filters_items(self):
        pred = _has_sales_exit_item_pred(
            "torqmind_current", "c", data_keys=[20260911, 20260910]
        )
        assert "i.data_key IN (20260910,20260911)" in pred


class TestAntifraudeQueryCost:
    def test_antifraude_drops_payload_nro_join_and_filters_exit_items(
        self, builder: MartBuilder
    ):
        src = inspect.getsource(builder._refresh_antifraude_eventos_stg)
        assert "stg_comprovantes AS p" not in src
        assert "JSONExtractString(p.payload, 'NROCOMPROVANTE')" not in src
        assert "toInt64(0) AS nro_comprovante" in src
        assert "data_keys=data_keys" in src
        # Exit-item semi-join must receive the batch keys.
        assert re.search(
            r"_has_sales_exit_item\(\s*[\"']c[\"']\s*,\s*data_keys=data_keys\s*\)",
            src,
        )
        exit_pred = builder._has_sales_exit_item("c", data_keys=[20260911])
        assert "i.data_key IN (20260911)" in exit_pred
