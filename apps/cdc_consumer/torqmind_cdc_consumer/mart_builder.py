"""TorqMind Realtime Mart Builder.

Reads from torqmind_current (deduplicated CDC state) and populates
torqmind_mart_rt with aggregated analytics tables.

Trigger modes:
  1. After CDC consumer flush (incremental: only affected data_keys)
  2. Standalone backfill (full window rebuild)
  3. Validate (compare mart_rt vs legacy mart)

Idempotency: ReplacingMergeTree on mart_rt tables ensures re-running
the builder for the same grain produces the same final result without
duplicating financial values.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import clickhouse_connect

logger = logging.getLogger(__name__)


@dataclass
class MartRefreshResult:
    mart_name: str
    rows_written: int = 0
    duration_ms: int = 0
    error: Optional[str] = None


@dataclass
class BuilderState:
    """Tracks affected windows for incremental refresh."""
    affected_data_keys: set[int] = field(default_factory=set)
    affected_empresas: set[int] = field(default_factory=set)
    affected_filiais: set[tuple[int, int]] = field(default_factory=set)
    affected_tables: set[str] = field(default_factory=set)

    def mark(self, id_empresa: int, id_filial: int, data_key: int, table: str) -> None:
        if data_key > 0:
            self.affected_data_keys.add(data_key)
        self.affected_empresas.add(id_empresa)
        self.affected_filiais.add((id_empresa, id_filial))
        self.affected_tables.add(table)

    def clear(self) -> None:
        self.affected_data_keys.clear()
        self.affected_empresas.clear()
        self.affected_filiais.clear()
        self.affected_tables.clear()

    @property
    def has_work(self) -> bool:
        return bool(self.affected_data_keys or self.affected_tables)


class MartBuilder:
    """Builds realtime marts from torqmind_current."""

    def __init__(
        self,
        clickhouse_host: str = "clickhouse",
        clickhouse_port: int = 8123,
        clickhouse_user: str = "torqmind",
        clickhouse_password: str = "",
        mart_rt_db: str = "torqmind_mart_rt",
        current_db: str = "torqmind_current",
        ops_db: str = "torqmind_ops",
        enabled: bool = True,
    ):
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.clickhouse_user = clickhouse_user
        self.clickhouse_password = clickhouse_password
        self.mart_rt_db = mart_rt_db
        self.current_db = current_db
        self.ops_db = ops_db
        self.enabled = enabled
        self.state = BuilderState()

    def _get_client(self) -> clickhouse_connect.driver.client.Client:
        return clickhouse_connect.get_client(
            host=self.clickhouse_host,
            port=self.clickhouse_port,
            username=self.clickhouse_user,
            password=self.clickhouse_password,
            connect_timeout=10,
            send_receive_timeout=120,
        )

    def mark_affected(self, id_empresa: int, id_filial: int, data_key: int, table: str) -> None:
        """Called by CDC consumer after processing each event."""
        self.state.mark(id_empresa, id_filial, data_key, table)

    def refresh_if_needed(self) -> list[MartRefreshResult]:
        """Called after CDC consumer flush. Refreshes affected marts."""
        if not self.enabled or not self.state.has_work:
            return []

        results = []
        data_keys = list(self.state.affected_data_keys)
        tables = self.state.affected_tables

        try:
            client = self._get_client()
            try:
                # Determine which marts to refresh based on affected tables
                if tables & {"fact_venda", "fact_venda_item", "fact_comprovante"}:
                    results.append(self._refresh_sales_daily(client, data_keys))
                    results.append(self._refresh_sales_hourly(client, data_keys))
                    results.append(self._refresh_dashboard_home(client, data_keys))

                if tables & {"fact_venda_item"}:
                    results.append(self._refresh_sales_products(client, data_keys))
                    results.append(self._refresh_sales_groups(client, data_keys))

                if tables & {"fact_pagamento_comprovante"}:
                    results.append(self._refresh_payments_by_type(client, data_keys))

                if tables & {"fact_caixa_turno"}:
                    results.append(self._refresh_cash_overview(client, data_keys))

                if tables & {"fact_risco_evento"}:
                    results.append(self._refresh_fraud_daily(client, data_keys))
                    results.append(self._refresh_risk_recent_events(client))

                if tables & {"fact_financeiro"}:
                    results.append(self._refresh_finance_overview(client))

                # Log publication
                self._log_publications(client, results)
                # Update source freshness
                self._update_source_freshness(client)

            finally:
                client.close()
        except Exception as e:
            logger.error(f"Mart builder refresh failed: {e}")
            results.append(MartRefreshResult(mart_name="__global__", error=str(e)))

        self.state.clear()
        return results

    def backfill(
        self,
        from_date: str = "2025-01-01",
        to_date: Optional[str] = None,
        id_empresa: int = 1,
        id_filial: Optional[int] = None,
    ) -> list[MartRefreshResult]:
        """Full backfill of all marts from current tables."""
        logger.info(f"Mart builder backfill: from={from_date} to={to_date or 'now'} empresa={id_empresa} filial={id_filial or 'all'}")

        from_key = int(from_date.replace("-", ""))
        to_key = int(to_date.replace("-", "")) if to_date else 99999999

        # Build list of all data_keys in range
        client = self._get_client()
        results = []
        try:
            # Get all distinct data_keys in range
            filial_filter = f"AND id_filial = {id_filial}" if id_filial else ""
            data_keys_rows = client.query(
                f"SELECT DISTINCT data_key FROM {self.current_db}.fact_venda FINAL "
                f"WHERE id_empresa = {{id_empresa:Int32}} AND data_key >= {{from_key:Int32}} "
                f"AND data_key <= {{to_key:Int32}} AND is_deleted = 0 {filial_filter} "
                f"ORDER BY data_key",
                parameters={"id_empresa": id_empresa, "from_key": from_key, "to_key": to_key},
            )
            data_keys = [row[0] for row in (data_keys_rows.result_rows or [])]

            if not data_keys:
                logger.warning("No data_keys found for backfill range")
                return results

            logger.info(f"Backfill: {len(data_keys)} data_keys to process")

            # Process in chunks to avoid timeouts
            chunk_size = 60  # ~2 months at a time
            for i in range(0, len(data_keys), chunk_size):
                chunk = data_keys[i:i + chunk_size]
                results.append(self._refresh_sales_daily(client, chunk))
                results.append(self._refresh_sales_hourly(client, chunk))
                results.append(self._refresh_sales_products(client, chunk))
                results.append(self._refresh_sales_groups(client, chunk))
                results.append(self._refresh_payments_by_type(client, chunk))
                results.append(self._refresh_dashboard_home(client, chunk))
                results.append(self._refresh_fraud_daily(client, chunk))

            # These don't depend on data_key chunks
            results.append(self._refresh_cash_overview(client, data_keys))
            results.append(self._refresh_risk_recent_events(client))
            results.append(self._refresh_finance_overview(client))
            self._log_publications(client, results)
            self._update_source_freshness(client)
        finally:
            client.close()

        logger.info(f"Backfill complete: {len(results)} mart refreshes, {sum(r.rows_written for r in results)} total rows")
        return results

    # ================================================================
    # MART REFRESH IMPLEMENTATIONS
    # ================================================================

    def _refresh_sales_daily(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_daily_rt
        SELECT
            v.id_empresa,
            v.id_filial,
            v.data_key,
            toDate(toString(v.data_key), '%Y%m%d') AS dt,
            sumIf(v.total_venda, v.cancelado = 0) AS faturamento,
            if(countIf(v.cancelado = 0) > 0, sumIf(v.total_venda, v.cancelado = 0) / countIf(v.cancelado = 0), 0) AS ticket_medio,
            countIf(v.cancelado = 0) AS qtd_vendas,
            toUInt32(sumIf(vi_cnt.cnt, v.cancelado = 0)) AS qtd_itens,
            countIf(v.cancelado = 1) AS qtd_canceladas,
            sumIf(v.total_venda, v.cancelado = 1) AS valor_cancelado,
            sum(vi_agg.desc_total) AS desconto_total,
            sum(vi_agg.custo) AS custo_total,
            sum(vi_agg.margem) AS margem_total,
            now64(6) AS published_at
        FROM {self.current_db}.fact_venda FINAL AS v
        LEFT JOIN (
            SELECT id_empresa, id_filial, id_db, id_movprodutos,
                   count() AS cnt
            FROM {self.current_db}.fact_venda_item FINAL
            WHERE data_key IN ({keys_str}) AND is_deleted = 0
            GROUP BY id_empresa, id_filial, id_db, id_movprodutos
        ) AS vi_cnt ON v.id_empresa = vi_cnt.id_empresa AND v.id_filial = vi_cnt.id_filial
            AND v.id_db = vi_cnt.id_db AND v.id_movprodutos = vi_cnt.id_movprodutos
        LEFT JOIN (
            SELECT id_empresa, id_filial, id_db, id_movprodutos,
                   sum(coalesce(desconto, 0)) AS desc_total,
                   sum(coalesce(custo_total, 0)) AS custo,
                   sum(coalesce(margem, 0)) AS margem
            FROM {self.current_db}.fact_venda_item FINAL
            WHERE data_key IN ({keys_str}) AND is_deleted = 0
            GROUP BY id_empresa, id_filial, id_db, id_movprodutos
        ) AS vi_agg ON v.id_empresa = vi_agg.id_empresa AND v.id_filial = vi_agg.id_filial
            AND v.id_db = vi_agg.id_db AND v.id_movprodutos = vi_agg.id_movprodutos
        WHERE v.data_key IN ({keys_str}) AND v.is_deleted = 0
        GROUP BY v.id_empresa, v.id_filial, v.data_key
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        written = _parse_insert_count(rows)
        return MartRefreshResult(mart_name="sales_daily_rt", rows_written=written, duration_ms=duration)

    def _refresh_sales_hourly(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_hourly_rt
        SELECT
            vi.id_empresa,
            vi.id_filial,
            vi.data_key,
            toDate(toString(vi.data_key), '%Y%m%d') AS dt,
            toUInt8(toHour(vi.ingested_at)) AS hora,
            sum(coalesce(vi.total, 0)) AS faturamento,
            count(DISTINCT (vi.id_db, vi.id_movprodutos)) AS qtd_vendas,
            count() AS qtd_itens,
            now64(6) AS published_at
        FROM {self.current_db}.fact_venda_item FINAL AS vi
        WHERE vi.data_key IN ({keys_str}) AND vi.is_deleted = 0
        GROUP BY vi.id_empresa, vi.id_filial, vi.data_key, hora
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="sales_hourly_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_sales_products(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_products_rt
        SELECT
            vi.id_empresa,
            vi.id_filial,
            vi.data_key,
            toDate(toString(vi.data_key), '%Y%m%d') AS dt,
            vi.id_produto,
            coalesce(p.nome, '') AS nome_produto,
            vi.id_grupo_produto,
            coalesce(g.nome, '') AS nome_grupo,
            sum(coalesce(vi.qtd, 0)) AS qtd,
            sum(coalesce(vi.total, 0)) AS faturamento,
            sum(coalesce(vi.custo_total, 0)) AS custo_total,
            sum(coalesce(vi.margem, 0)) AS margem,
            now64(6) AS published_at
        FROM {self.current_db}.fact_venda_item FINAL AS vi
        LEFT JOIN {self.current_db}.dim_produto FINAL AS p
            ON vi.id_empresa = p.id_empresa AND vi.id_filial = p.id_filial AND vi.id_produto = p.id_produto
        LEFT JOIN {self.current_db}.dim_grupo_produto FINAL AS g
            ON vi.id_empresa = g.id_empresa AND vi.id_filial = g.id_filial
            AND vi.id_grupo_produto = g.id_grupo_produto
        WHERE vi.data_key IN ({keys_str}) AND vi.is_deleted = 0
        GROUP BY vi.id_empresa, vi.id_filial, vi.data_key, vi.id_produto, p.nome, vi.id_grupo_produto, g.nome
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="sales_products_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_sales_groups(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_groups_rt
        SELECT
            vi.id_empresa,
            vi.id_filial,
            vi.data_key,
            toDate(toString(vi.data_key), '%Y%m%d') AS dt,
            coalesce(vi.id_grupo_produto, 0) AS id_grupo_produto,
            coalesce(g.nome, '') AS nome_grupo,
            count() AS qtd_itens,
            sum(coalesce(vi.total, 0)) AS faturamento,
            sum(coalesce(vi.custo_total, 0)) AS custo_total,
            sum(coalesce(vi.margem, 0)) AS margem,
            now64(6) AS published_at
        FROM {self.current_db}.fact_venda_item FINAL AS vi
        LEFT JOIN {self.current_db}.dim_grupo_produto FINAL AS g
            ON vi.id_empresa = g.id_empresa AND vi.id_filial = g.id_filial
            AND coalesce(vi.id_grupo_produto, 0) = g.id_grupo_produto
        WHERE vi.data_key IN ({keys_str}) AND vi.is_deleted = 0
        GROUP BY vi.id_empresa, vi.id_filial, vi.data_key, vi.id_grupo_produto, g.nome
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="sales_groups_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_payments_by_type(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.payments_by_type_rt
        SELECT
            p.id_empresa,
            p.id_filial,
            p.data_key,
            toDate(toString(p.data_key), '%Y%m%d') AS dt,
            p.tipo_forma,
            coalesce(m.label, concat('Forma ', toString(p.tipo_forma))) AS label,
            coalesce(m.category, 'Outros') AS category,
            sum(p.valor) AS valor_total,
            count() AS qtd_transacoes,
            now64(6) AS published_at
        FROM {self.current_db}.fact_pagamento_comprovante FINAL AS p
        LEFT JOIN {self.current_db}.payment_type_map FINAL AS m
            ON p.tipo_forma = m.tipo_forma AND (m.id_empresa IS NULL OR m.id_empresa = p.id_empresa)
        WHERE p.data_key IN ({keys_str}) AND p.is_deleted = 0
        GROUP BY p.id_empresa, p.id_filial, p.data_key, p.tipo_forma, m.label, m.category
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="payments_by_type_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_cash_overview(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.cash_overview_rt
        SELECT
            ct.id_empresa,
            ct.id_filial,
            ct.id_turno,
            ct.id_usuario,
            coalesce(u.nome, '') AS nome_operador,
            ct.abertura_ts,
            ct.fechamento_ts,
            ct.data_key_abertura,
            ct.is_aberto,
            coalesce(vendas.faturamento, 0) AS faturamento_turno,
            coalesce(vendas.qtd, 0) AS qtd_vendas_turno,
            now64(6) AS published_at
        FROM {self.current_db}.fact_caixa_turno FINAL AS ct
        LEFT JOIN {self.current_db}.dim_usuario_caixa FINAL AS u
            ON ct.id_empresa = u.id_empresa AND ct.id_filial = u.id_filial AND ct.id_usuario = u.id_usuario
        LEFT JOIN (
            SELECT id_empresa, id_filial, id_turno,
                   sumIf(total_venda, cancelado = 0) AS faturamento,
                   toUInt32(countIf(cancelado = 0)) AS qtd
            FROM {self.current_db}.fact_venda FINAL
            WHERE is_deleted = 0
            GROUP BY id_empresa, id_filial, id_turno
        ) AS vendas ON ct.id_empresa = vendas.id_empresa AND ct.id_filial = vendas.id_filial
            AND ct.id_turno = vendas.id_turno
        WHERE ct.is_deleted = 0
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="cash_overview_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_fraud_daily(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.fraud_daily_rt
        SELECT
            r.id_empresa,
            r.id_filial,
            r.data_key,
            toDate(toString(r.data_key), '%Y%m%d') AS dt,
            r.event_type,
            count() AS qtd_eventos,
            sum(r.impacto_estimado) AS impacto_total,
            avg(r.score_risco) AS score_medio,
            now64(6) AS published_at
        FROM {self.current_db}.fact_risco_evento FINAL AS r
        WHERE r.data_key IN ({keys_str}) AND r.is_deleted = 0
        GROUP BY r.id_empresa, r.id_filial, r.data_key, r.event_type
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="fraud_daily_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_risk_recent_events(self, client: Any) -> MartRefreshResult:
        t0 = time.time()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.risk_recent_events_rt
        SELECT
            r.id,
            r.id_empresa,
            r.id_filial,
            r.data_key,
            r.event_type,
            r.source,
            r.id_usuario,
            coalesce(u.nome, '') AS nome_operador,
            r.id_funcionario,
            coalesce(f.nome, '') AS nome_funcionario,
            r.valor_total,
            r.impacto_estimado,
            r.score_risco,
            r.score_level,
            r.reasons,
            now64(6) AS published_at
        FROM {self.current_db}.fact_risco_evento FINAL AS r
        LEFT JOIN {self.current_db}.dim_usuario_caixa FINAL AS u
            ON r.id_empresa = u.id_empresa AND r.id_filial = u.id_filial AND r.id_usuario = u.id_usuario
        LEFT JOIN {self.current_db}.dim_funcionario FINAL AS f
            ON r.id_empresa = f.id_empresa AND r.id_filial = f.id_filial AND r.id_funcionario = f.id_funcionario
        WHERE r.is_deleted = 0
        ORDER BY r.id DESC
        LIMIT 1000
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="risk_recent_events_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_finance_overview(self, client: Any) -> MartRefreshResult:
        t0 = time.time()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.finance_overview_rt
        SELECT
            f.id_empresa,
            f.id_filial,
            f.tipo_titulo,
            multiIf(
                f.data_pagamento IS NOT NULL, 'pago',
                f.vencimento < today(), 'vencido',
                f.vencimento <= today() + 7, 'vence_7d',
                f.vencimento <= today() + 30, 'vence_30d',
                'futuro'
            ) AS faixa,
            count() AS qtd_titulos,
            sum(coalesce(f.valor, 0)) AS valor_total,
            sum(coalesce(f.valor_pago, 0)) AS valor_pago_total,
            sum(coalesce(f.valor, 0)) - sum(coalesce(f.valor_pago, 0)) AS valor_em_aberto,
            now64(6) AS published_at
        FROM {self.current_db}.fact_financeiro FINAL AS f
        WHERE f.is_deleted = 0
        GROUP BY f.id_empresa, f.id_filial, f.tipo_titulo, faixa
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="finance_overview_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_dashboard_home(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.dashboard_home_rt
        SELECT
            v.id_empresa,
            v.id_filial,
            v.data_key,
            toDate(toString(v.data_key), '%Y%m%d') AS dt,
            sumIf(v.total_venda, v.cancelado = 0) AS faturamento,
            if(countIf(v.cancelado = 0) > 0, sumIf(v.total_venda, v.cancelado = 0) / countIf(v.cancelado = 0), 0) AS ticket_medio,
            countIf(v.cancelado = 0) AS qtd_vendas,
            toUInt32(uniqExactIf(v.id_cliente, v.cancelado = 0 AND v.id_cliente IS NOT NULL)) AS qtd_clientes,
            countIf(v.cancelado = 1) AS qtd_cancelamentos,
            sumIf(v.total_venda, v.cancelado = 1) AS valor_cancelado,
            now64(6) AS published_at
        FROM {self.current_db}.fact_venda FINAL AS v
        WHERE v.data_key IN ({keys_str}) AND v.is_deleted = 0
        GROUP BY v.id_empresa, v.id_filial, v.data_key
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="dashboard_home_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _log_publications(self, client: Any, results: list[MartRefreshResult]) -> None:
        """Log successful publications to mart_publication_log."""
        successful = [r for r in results if r.error is None and r.rows_written > 0]
        if not successful:
            return
        try:
            rows = []
            for r in successful:
                rows.append([r.mart_name, 0, "1970-01-01", "2099-12-31", r.rows_written, r.duration_ms])
            client.insert(
                f"{self.mart_rt_db}.mart_publication_log",
                rows,
                column_names=["mart_name", "id_empresa", "window_start", "window_end", "rows_written", "duration_ms"],
            )
        except Exception as e:
            logger.warning(f"Failed to log mart publications: {e}")

    def _update_source_freshness(self, client: Any) -> None:
        """Update source freshness for platform monitoring."""
        try:
            sql = f"""
            INSERT INTO {self.mart_rt_db}.source_freshness
            SELECT
                ts.id_empresa,
                ts.table_name AS domain,
                ts.last_event_at AS last_event_ts,
                dateDiff('second', ts.last_event_at, now64(6)) AS lag_seconds,
                if(dateDiff('second', ts.last_event_at, now64(6)) > 300, 'stale', 'ok') AS status,
                now64(6) AS checked_at
            FROM {self.ops_db}.cdc_table_state FINAL AS ts
            WHERE ts.id_empresa > 0
            """
            client.command(sql)
        except Exception as e:
            logger.warning(f"Failed to update source freshness: {e}")


def _parse_insert_count(result: Any) -> int:
    """Parse row count from ClickHouse INSERT command result."""
    if result is None:
        return 0
    if isinstance(result, int):
        return result
    if isinstance(result, str):
        # ClickHouse returns empty string for INSERT
        return 0
    return 0
