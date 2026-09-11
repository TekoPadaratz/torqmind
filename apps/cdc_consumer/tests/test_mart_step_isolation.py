"""Falha isolada de mart no refresh incremental + publicação por empresa.

Incidente coberto: MEMORY_LIMIT_EXCEEDED em mart_antifraude_eventos abortava a
cadeia do ciclo, deixando nfe_inutilizations_rt / payments_by_type_rt /
cash_overview_rt / finance_overview_rt / mart_clientes_resumo sem atualização,
e descartava as data_keys pendentes em state.clear().

Invariantes:
- Marts independentes posteriores concluem quando uma mart falha.
- A mart que falhou fica identificada e elegível a nova tentativa.
- Nenhum dia/chave pendente é descartado.
- Sem retry contínuo dentro do ciclo (uma tentativa por ciclo, sem backoff).
- Publicação registra uma linha por (mart, empresa) realmente publicada.
- Caminho sem falha mantém o comportamento anterior.
"""

from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from torqmind_cdc_consumer.mart_builder import MartBuilder, MartRefreshResult


# mart_name -> método do caminho STG (ordem real da cadeia)
STG_CHAIN: tuple[tuple[str, str], ...] = (
    ("sales_daily_rt", "_refresh_sales_daily_stg"),
    ("sales_hourly_rt", "_refresh_sales_hourly_stg"),
    ("dashboard_home_rt", "_refresh_dashboard_home_stg"),
    ("sales_products_rt", "_refresh_sales_products_stg"),
    ("sales_groups_rt", "_refresh_sales_groups_stg"),
    ("fraud_daily_rt", "_refresh_fraud_daily_stg"),
    ("risk_recent_events_rt", "_refresh_risk_recent_events_stg"),
    ("mart_antifraude_eventos", "_refresh_antifraude_eventos_stg"),
    ("nfe_inutilizations_rt", "_refresh_nfe_inutilizations_rt_stg"),
    ("payments_by_type_rt", "_refresh_payments_by_type_stg"),
    ("mart_troca_forma_pgto_rt", "_refresh_troca_forma_pgto_stg"),
    ("cash_overview_rt", "_refresh_cash_overview_stg"),
    ("finance_overview_rt", "_refresh_finance_overview_stg"),
    ("mart_clientes_resumo", "_refresh_mart_clientes_resumo_stg"),
)

SLIM_METHODS = (
    "_populate_slim_comprovantes",
    "_populate_slim_itens",
    "_populate_slim_formas",
    "_populate_slim_nfe",
)

# Erro real observado em produção (ClickHouse code 241).
OOM_ERROR = (
    "Code: 241. DB::Exception: Memory limit (for query) exceeded: would use "
    "2.82 GiB, maximum: 2.79 GiB. While executing JoiningTransform. "
    "(MEMORY_LIMIT_EXCEEDED)"
)

TABLES_ALL_GROUPS = (
    "comprovantes",
    "itenscomprovantes",
    "nfe",
    "formas_pgto_comprovantes",
    "turnos",
    "controle_troca_pgto",
    "financeiro",
    "entidades",
)


def make_builder(**kwargs) -> MartBuilder:
    return MartBuilder(enabled=True, source="stg", **kwargs)


def mark_batch(builder: MartBuilder, data_keys=(20260910, 20260911), empresas=((1, 10), (8, 20))) -> None:
    for id_empresa, id_filial in empresas:
        for data_key in data_keys:
            for table in TABLES_ALL_GROUPS:
                builder.mark_affected(id_empresa, id_filial, data_key, table)


def run_cycle(builder: MartBuilder, failing: dict[str, Exception] | None = None):
    """Roda refresh_if_needed com slim e refreshes de mart mockados.

    Retorna (results, mocks_por_mart, sleep_mock).
    """
    failing = failing or {}
    client = MagicMock()
    mocks: dict[str, MagicMock] = {}
    with ExitStack() as stack:
        stack.enter_context(patch.object(builder, "_get_client", return_value=client))
        for name in SLIM_METHODS:
            stack.enter_context(patch.object(builder, name))
        stack.enter_context(patch.object(builder, "_log_publications"))
        stack.enter_context(patch.object(builder, "_update_source_freshness"))
        for mart_name, method in STG_CHAIN:
            if mart_name in failing:
                mock = stack.enter_context(
                    patch.object(builder, method, side_effect=failing[mart_name])
                )
            else:
                mock = stack.enter_context(
                    patch.object(
                        builder,
                        method,
                        return_value=MartRefreshResult(mart_name, 7, 11),
                    )
                )
            mocks[mart_name] = mock
        sleep_mock = stack.enter_context(patch("torqmind_cdc_consumer.mart_builder.time.sleep"))
        results = builder.refresh_if_needed()
    return results, mocks, sleep_mock


class TestAntifraudeFailureDoesNotBlockChain:
    """Falha em mart_antifraude_eventos não impede as marts independentes."""

    def test_independent_marts_after_antifraude_still_publish(self):
        builder = make_builder()
        mark_batch(builder)

        results, mocks, _ = run_cycle(builder, failing={"mart_antifraude_eventos": RuntimeError(OOM_ERROR)})

        published = {r.mart_name for r in results if r.error is None}
        # Marts que hoje ficam stale quando antifraude falha (posteriores na cadeia)
        for mart in (
            "nfe_inutilizations_rt",
            "payments_by_type_rt",
            "mart_troca_forma_pgto_rt",
            "cash_overview_rt",
            "finance_overview_rt",
            "mart_clientes_resumo",
        ):
            assert mart in published, f"{mart} devia publicar mesmo com antifraude falhando"
            assert mocks[mart].called

    def test_failed_mart_is_reported_as_error_not_success(self):
        builder = make_builder()
        mark_batch(builder)

        results, _, _ = run_cycle(builder, failing={"mart_antifraude_eventos": RuntimeError(OOM_ERROR)})

        failed = [r for r in results if r.error is not None]
        assert [r.mart_name for r in failed] == ["mart_antifraude_eventos"]
        assert "MEMORY_LIMIT_EXCEEDED" in failed[0].error
        assert "mart_antifraude_eventos" not in {r.mart_name for r in results if r.error is None}

    def test_failed_mart_stays_eligible_for_retry(self):
        builder = make_builder()
        mark_batch(builder)

        run_cycle(builder, failing={"mart_antifraude_eventos": RuntimeError(OOM_ERROR)})

        assert builder.state.retry_marts == {"mart_antifraude_eventos"}

    def test_pending_data_keys_are_not_discarded(self):
        builder = make_builder()
        mark_batch(builder, data_keys=(20260215, 20260910, 20260911))

        run_cycle(builder, failing={"mart_antifraude_eventos": RuntimeError(OOM_ERROR)})

        assert builder.state.affected_data_keys == {20260215, 20260910, 20260911}
        assert builder.state.affected_empresas == {1, 8}
        assert builder.state.has_work

    def test_retry_runs_failed_mart_without_its_trigger_table(self):
        """Próximo ciclo: evento de tabela não relacionada ainda reexecuta a pendência."""
        builder = make_builder()
        mark_batch(builder)
        run_cycle(builder, failing={"mart_antifraude_eventos": RuntimeError(OOM_ERROR)})

        # Ciclo seguinte disparado só por financeiro (não dispara marts de venda).
        builder.mark_affected(1, 10, 20260911, "financeiro")
        results, mocks, _ = run_cycle(builder)

        assert mocks["mart_antifraude_eventos"].called, "mart pendente deve ser retentada"
        assert not mocks["sales_daily_rt"].called, "mart sem gatilho e sem pendência não deve rodar"
        assert "mart_antifraude_eventos" in {r.mart_name for r in results if r.error is None}
        assert builder.state.retry_marts == set(), "pendência some após publicar"

    def test_single_attempt_per_cycle_and_no_backoff_sleep(self):
        builder = make_builder()
        mark_batch(builder)

        _, mocks, sleep_mock = run_cycle(builder, failing={"mart_antifraude_eventos": RuntimeError(OOM_ERROR)})

        assert mocks["mart_antifraude_eventos"].call_count == 1, "sem loop de retry dentro do ciclo"
        sleep_mock.assert_not_called()
        # Ciclo com publicação parcial não acumula falha → não entra em backoff.
        assert builder._consecutive_failures == 0

    def test_cycle_without_any_publication_counts_for_circuit_breaker(self):
        builder = make_builder()
        mark_batch(builder)
        failing = {mart: RuntimeError(OOM_ERROR) for mart, _ in STG_CHAIN}

        results, _, _ = run_cycle(builder, failing=failing)

        assert all(r.error is not None for r in results)
        assert builder._consecutive_failures == 1
        assert builder.state.retry_marts == {mart for mart, _ in STG_CHAIN}


class TestSlimFailureAbortsCycle:
    """Slim é dependência real: falha aborta a cadeia e preserva o lote inteiro."""

    def test_slim_failure_keeps_full_batch_and_runs_no_mart(self):
        builder = make_builder()
        mark_batch(builder)
        client = MagicMock()

        with ExitStack() as stack:
            stack.enter_context(patch.object(builder, "_get_client", return_value=client))
            stack.enter_context(
                patch.object(builder, "_populate_slim_comprovantes", side_effect=RuntimeError("slim down"))
            )
            for name in SLIM_METHODS[1:]:
                stack.enter_context(patch.object(builder, name))
            sales_daily = stack.enter_context(patch.object(builder, "_refresh_sales_daily_stg"))
            stack.enter_context(patch("torqmind_cdc_consumer.mart_builder.time.sleep"))
            results = builder.refresh_if_needed()

        assert not sales_daily.called, "mart não publica sobre slim desatualizada"
        assert [r.mart_name for r in results] == ["__global__"]
        assert results[0].error is not None
        assert builder.state.affected_data_keys == {20260910, 20260911}
        assert "comprovantes" in builder.state.affected_tables
        assert builder._consecutive_failures == 1

    def test_slim_failure_preserves_previous_retry_pendency(self):
        builder = make_builder()
        builder.state.retry_marts = {"mart_antifraude_eventos"}
        mark_batch(builder)
        client = MagicMock()

        with ExitStack() as stack:
            stack.enter_context(patch.object(builder, "_get_client", return_value=client))
            stack.enter_context(
                patch.object(builder, "_populate_slim_comprovantes", side_effect=RuntimeError("slim down"))
            )
            for name in SLIM_METHODS[1:]:
                stack.enter_context(patch.object(builder, name))
            stack.enter_context(patch("torqmind_cdc_consumer.mart_builder.time.sleep"))
            builder.refresh_if_needed()

        assert builder.state.retry_marts == {"mart_antifraude_eventos"}


class TestHappyPathUnchanged:
    """Caminho sem falhas mantém comportamento anterior."""

    def test_all_marts_run_and_state_is_cleared(self):
        builder = make_builder()
        mark_batch(builder)

        results, mocks, sleep_mock = run_cycle(builder)

        assert all(r.error is None for r in results)
        assert {r.mart_name for r in results} == {mart for mart, _ in STG_CHAIN}
        for mart, _ in STG_CHAIN:
            assert mocks[mart].call_count == 1
        assert builder.state.affected_data_keys == set()
        assert builder.state.affected_tables == set()
        assert builder.state.affected_empresas == set()
        assert builder.state.retry_marts == set()
        assert not builder.state.has_work
        assert builder._consecutive_failures == 0
        sleep_mock.assert_not_called()

    def test_chain_order_is_preserved(self):
        builder = make_builder()
        mark_batch(builder)

        results, _, _ = run_cycle(builder)

        assert [r.mart_name for r in results] == [mart for mart, _ in STG_CHAIN]

    def test_only_triggered_marts_run(self):
        builder = make_builder()
        builder.mark_affected(1, 10, 20260911, "financeiro")

        _, mocks, _ = run_cycle(builder)

        assert mocks["finance_overview_rt"].called
        for mart in ("sales_daily_rt", "mart_antifraude_eventos", "payments_by_type_rt"):
            assert not mocks[mart].called

    def test_team_fuel_mart_stays_out_of_incremental_cycle(self):
        """Publicar só os dias novos subestimaria litros na tela de Equipe.

        A API troca slim → mart quando a mart tem qualquer linha na janela pedida
        (repos_mart_realtime.team_fuel_employees_dashboard), então a mart só entra
        no ciclo depois de backfill.
        """
        builder = make_builder()
        mark_batch(builder)

        with patch.object(builder, "_refresh_team_fuel_employee_daily_stg") as team_fuel:
            run_cycle(builder)

        assert not team_fuel.called

    def test_team_fuel_refresh_remains_available_for_backfill(self):
        code = (
            Path(__file__).parent.parent / "torqmind_cdc_consumer" / "mart_builder.py"
        ).read_text()
        assert "def _refresh_team_fuel_employee_daily_stg(" in code
        backfill_idx = code.index("def backfill(")
        backfill_body = code[backfill_idx : code.index("\n    def ", backfill_idx + 10)]
        assert "_refresh_team_fuel_employee_daily_stg" in backfill_body

    def test_refresh_chain_uses_isolated_steps(self):
        """Nenhuma chamada de mart direta em results.append dentro de refresh_if_needed."""
        code = (
            Path(__file__).parent.parent / "torqmind_cdc_consumer" / "mart_builder.py"
        ).read_text()
        idx = code.index("def refresh_if_needed(")
        body = code[idx : code.index("\n    def ", idx + 10)]
        assert "results.append(self._refresh_" not in body, (
            "refresh_if_needed deve usar step()/_run_mart_step para isolar falhas"
        )
        assert "step(" in body


class TestPublicationPerEmpresa:
    """mart_publication_log: uma linha por (mart, empresa) realmente publicada."""

    def _rows(self, client: MagicMock) -> list[list]:
        assert client.insert.called
        return client.insert.call_args[0][1]

    def test_two_empresas_generate_one_row_each_with_real_counts(self):
        builder = make_builder()
        client = MagicMock()
        results = [MartRefreshResult("sales_daily_rt", 30, 500)]

        builder._log_publications(
            client,
            results,
            data_keys=[20260910, 20260911],
            empresas={1, 8},
            rows_by_empresa={"sales_daily_rt": {1: 12, 8: 18}},
        )

        rows = self._rows(client)
        assert [(r[0], r[1], r[4]) for r in rows] == [
            ("sales_daily_rt", 1, 12),
            ("sales_daily_rt", 8, 18),
        ]

    def test_failed_mart_is_not_logged(self):
        builder = make_builder()
        client = MagicMock()
        results = [
            MartRefreshResult("sales_daily_rt", 12, 500),
            MartRefreshResult("mart_antifraude_eventos", 0, 300, error=OOM_ERROR),
        ]

        builder._log_publications(
            client,
            results,
            data_keys=[20260911],
            empresas={1},
            rows_by_empresa={"sales_daily_rt": {1: 12}, "mart_antifraude_eventos": {1: 99}},
        )

        logged = {row[0] for row in self._rows(client)}
        assert logged == {"sales_daily_rt"}

    def test_empresa_without_rows_in_mart_is_not_logged(self):
        """Empresa afetada mas sem linha publicada na mart não vira sucesso."""
        builder = make_builder()
        client = MagicMock()
        results = [MartRefreshResult("sales_daily_rt", 12, 500)]

        builder._log_publications(
            client,
            results,
            data_keys=[20260911],
            empresas={1, 8},
            rows_by_empresa={"sales_daily_rt": {8: 12}},
        )

        assert [(r[0], r[1]) for r in self._rows(client)] == [("sales_daily_rt", 8)]

    def test_no_arbitrary_empresa_from_set(self):
        """Sem quebra por empresa, registra todas as afetadas — não uma arbitrária."""
        builder = make_builder()
        client = MagicMock()
        results = [MartRefreshResult("sales_daily_rt", 0, 500)]

        builder._log_publications(
            client,
            results,
            data_keys=[20260911],
            empresas={1, 8},
            rows_by_empresa={},
        )

        assert sorted(r[1] for r in self._rows(client)) == [1, 8]

    def test_no_id_empresa_zero_and_no_insert_when_unattributable(self):
        builder = make_builder()
        client = MagicMock()
        results = [MartRefreshResult("sales_daily_rt", 5, 500)]

        builder._log_publications(
            client,
            results,
            data_keys=[20260911],
            empresas={0},
            rows_by_empresa={},
        )

        assert not client.insert.called

    def test_refresh_cycle_does_not_use_next_iter_empresa(self):
        code = (
            Path(__file__).parent.parent / "torqmind_cdc_consumer" / "mart_builder.py"
        ).read_text()
        idx = code.index("def refresh_if_needed(")
        body = code[idx : code.index("\n    def ", idx + 10)]
        assert "next(iter(self.state.affected_empresas)" not in body
        assert "rows_by_empresa=" in body


class TestRowsByEmpresaCounting:
    """Contagem pós-INSERT alimenta a atribuição por empresa."""

    def test_insert_and_count_records_breakdown_and_returns_total(self):
        builder = make_builder()
        client = MagicMock()
        client.query.return_value = MagicMock(result_rows=[(1, 4), (8, 6)])

        total = builder._insert_and_count(client, "sales_daily_rt", "INSERT ...", [20260911])

        assert total == 10
        assert builder._cycle_rows_by_empresa["sales_daily_rt"] == {1: 4, 8: 6}
        query_sql = client.query.call_args[0][0]
        assert "GROUP BY id_empresa" in query_sql
        assert "data_key IN (20260911)" in query_sql

    def test_count_failure_does_not_break_refresh(self):
        builder = make_builder()
        client = MagicMock()
        client.query.side_effect = RuntimeError("count failed")

        assert builder._insert_and_count(client, "sales_daily_rt", "INSERT ...", [20260911]) == 0
        assert "sales_daily_rt" not in builder._cycle_rows_by_empresa

    def test_cycle_breakdown_is_reset_between_cycles(self):
        builder = make_builder()
        builder._cycle_rows_by_empresa = {"sales_daily_rt": {1: 99}}
        mark_batch(builder)

        run_cycle(builder)

        assert builder._cycle_rows_by_empresa == {}


class TestWorkerLogging:
    """Worker não declara ciclo completo quando houve falha parcial."""

    def test_partial_cycle_is_not_logged_as_full_success(self):
        main_code = (
            Path(__file__).parent.parent / "torqmind_cdc_consumer" / "main.py"
        ).read_text()
        idx = main_code.index("results = self._mart_builder.refresh_if_needed()")
        body = main_code[idx : idx + 1200]
        assert 'if refreshed and not errors:' in body
        assert 'marts_refreshed_partial' in body
        assert 'retry_pending' in body


if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-q"])
