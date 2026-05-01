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
        source: str = "stg",
    ):
        self.clickhouse_host = clickhouse_host
        self.clickhouse_port = clickhouse_port
        self.clickhouse_user = clickhouse_user
        self.clickhouse_password = clickhouse_password
        self.mart_rt_db = mart_rt_db
        self.current_db = current_db
        self.ops_db = ops_db
        self.enabled = enabled
        self.source = source.lower().strip()
        if self.source not in {"stg", "dw"}:
            raise ValueError("MartBuilder source must be 'stg' or 'dw'")
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
                if self.source == "stg":
                    if tables & {"comprovantes", "itenscomprovantes"}:
                        results.append(self._refresh_sales_daily(client, data_keys))
                        results.append(self._refresh_sales_hourly(client, data_keys))
                        results.append(self._refresh_dashboard_home(client, data_keys))
                        results.append(self._refresh_sales_products(client, data_keys))
                        results.append(self._refresh_sales_groups(client, data_keys))
                        results.append(self._refresh_fraud_daily(client, data_keys))
                        results.append(self._refresh_risk_recent_events(client))

                    if tables & {"formas_pgto_comprovantes", "payment_type_map"}:
                        results.append(self._refresh_payments_by_type(client, data_keys))

                    if tables & {"turnos", "usuarios", "comprovantes"}:
                        results.append(self._refresh_cash_overview(client, data_keys))

                    if tables & {"financeiro", "contaspagar", "contasreceber"}:
                        results.append(self._refresh_finance_overview(client))
                else:
                    # Determine which DW-origin marts to refresh based on affected tables.
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
            # Get all distinct data_keys in range from the selected realtime source.
            filial_filter = f"AND id_filial = {id_filial}" if id_filial else ""
            if self.source == "stg":
                data_key_expr = self._stg_data_key_expr("c")
                data_keys_rows = client.query(
                    f"SELECT DISTINCT {data_key_expr} AS data_key FROM {self.current_db}.stg_comprovantes AS c FINAL "
                    f"WHERE c.id_empresa = {{id_empresa:Int32}} AND data_key >= {{from_key:Int32}} "
                    f"AND data_key <= {{to_key:Int32}} AND c.is_deleted = 0 {filial_filter} "
                    f"ORDER BY data_key",
                    parameters={"id_empresa": id_empresa, "from_key": from_key, "to_key": to_key},
                )
            else:
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
        if self.source == "stg":
            return self._refresh_sales_daily_stg(client, data_keys)
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_daily_rt
        SELECT
            base.id_empresa,
            base.id_filial,
            base.data_key,
            toDate(toString(base.data_key), '%Y%m%d') AS dt,
            base.faturamento,
            if(base.qtd_vendas > 0, base.faturamento / base.qtd_vendas, 0) AS ticket_medio,
            base.qtd_vendas,
            base.qtd_itens,
            coalesce(cancel.qtd_canceladas, 0) AS qtd_canceladas,
            coalesce(cancel.valor_cancelado, 0) AS valor_cancelado,
            base.desconto_total,
            base.custo_total,
            base.margem_total,
            now64(6) AS published_at
        FROM (
            SELECT
                v.id_empresa,
                v.id_filial,
                v.data_key,
                sum(coalesce(vi.total, 0)) AS faturamento,
                toUInt32(uniqExactIf(v.id_comprovante, v.id_comprovante IS NOT NULL)) AS qtd_vendas,
                toUInt32(count()) AS qtd_itens,
                sum(coalesce(vi.desconto, 0)) AS desconto_total,
                sum(coalesce(vi.custo_total, 0)) AS custo_total,
                sum(coalesce(vi.margem, 0)) AS margem_total
            FROM {self.current_db}.fact_venda AS v FINAL
            INNER JOIN {self.current_db}.fact_venda_item AS vi FINAL
                ON v.id_empresa = vi.id_empresa AND v.id_filial = vi.id_filial
                AND v.id_db = vi.id_db AND v.id_movprodutos = vi.id_movprodutos
            WHERE v.data_key IN ({keys_str})
              AND v.is_deleted = 0
              AND vi.is_deleted = 0
              AND v.cancelado = 0
              AND coalesce(vi.cfop, 0) >= 5000
            GROUP BY v.id_empresa, v.id_filial, v.data_key
        ) AS base
        LEFT JOIN (
            SELECT
                id_empresa,
                id_filial,
                data_key,
                toUInt32(count()) AS qtd_canceladas,
                sum(coalesce(valor_total, 0)) AS valor_cancelado
            FROM {self.current_db}.fact_comprovante FINAL
            WHERE data_key IN ({keys_str})
              AND is_deleted = 0
              AND cancelado = 1
            GROUP BY id_empresa, id_filial, data_key
        ) AS cancel
            ON base.id_empresa = cancel.id_empresa
           AND base.id_filial = cancel.id_filial
           AND base.data_key = cancel.data_key
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        written = _parse_insert_count(rows)
        return MartRefreshResult(mart_name="sales_daily_rt", rows_written=written, duration_ms=duration)

    def _refresh_sales_hourly(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        if self.source == "stg":
            return self._refresh_sales_hourly_stg(client, data_keys)
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_hourly_rt
        SELECT
            v.id_empresa,
            v.id_filial,
            v.data_key,
            toDate(toString(v.data_key), '%Y%m%d') AS dt,
            toUInt8(toHour(coalesce(v.data, vi.ingested_at))) AS hora,
            sum(coalesce(vi.total, 0)) AS faturamento,
            toUInt32(uniqExactIf(v.id_comprovante, v.id_comprovante IS NOT NULL)) AS qtd_vendas,
            toUInt32(count()) AS qtd_itens,
            now64(6) AS published_at
        FROM {self.current_db}.fact_venda_item AS vi FINAL
        INNER JOIN {self.current_db}.fact_venda AS v FINAL
            ON v.id_empresa = vi.id_empresa AND v.id_filial = vi.id_filial
            AND v.id_db = vi.id_db AND v.id_movprodutos = vi.id_movprodutos
        WHERE v.data_key IN ({keys_str})
          AND v.is_deleted = 0
          AND vi.is_deleted = 0
          AND v.cancelado = 0
          AND coalesce(vi.cfop, 0) >= 5000
        GROUP BY v.id_empresa, v.id_filial, v.data_key, hora
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="sales_hourly_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_sales_products(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        if self.source == "stg":
            return self._refresh_sales_products_stg(client, data_keys)
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
        FROM {self.current_db}.fact_venda_item AS vi FINAL
        INNER JOIN {self.current_db}.fact_venda AS v FINAL
            ON v.id_empresa = vi.id_empresa AND v.id_filial = vi.id_filial
            AND v.id_db = vi.id_db AND v.id_movprodutos = vi.id_movprodutos
        LEFT JOIN {self.current_db}.dim_produto AS p FINAL
            ON vi.id_empresa = p.id_empresa AND vi.id_filial = p.id_filial AND vi.id_produto = p.id_produto
        LEFT JOIN {self.current_db}.dim_grupo_produto AS g FINAL
            ON vi.id_empresa = g.id_empresa AND vi.id_filial = g.id_filial
            AND vi.id_grupo_produto = g.id_grupo_produto
        WHERE vi.data_key IN ({keys_str})
          AND vi.is_deleted = 0
          AND v.is_deleted = 0
          AND v.cancelado = 0
          AND coalesce(vi.cfop, 0) >= 5000
        GROUP BY vi.id_empresa, vi.id_filial, vi.data_key, vi.id_produto, p.nome, vi.id_grupo_produto, g.nome
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="sales_products_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_sales_groups(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        if self.source == "stg":
            return self._refresh_sales_groups_stg(client, data_keys)
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
        FROM {self.current_db}.fact_venda_item AS vi FINAL
        INNER JOIN {self.current_db}.fact_venda AS v FINAL
            ON v.id_empresa = vi.id_empresa AND v.id_filial = vi.id_filial
            AND v.id_db = vi.id_db AND v.id_movprodutos = vi.id_movprodutos
        LEFT JOIN {self.current_db}.dim_grupo_produto AS g FINAL
            ON vi.id_empresa = g.id_empresa AND vi.id_filial = g.id_filial
            AND coalesce(vi.id_grupo_produto, 0) = g.id_grupo_produto
        WHERE vi.data_key IN ({keys_str})
          AND vi.is_deleted = 0
          AND v.is_deleted = 0
          AND v.cancelado = 0
          AND coalesce(vi.cfop, 0) >= 5000
        GROUP BY vi.id_empresa, vi.id_filial, vi.data_key, vi.id_grupo_produto, g.nome
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="sales_groups_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_payments_by_type(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        if self.source == "stg":
            return self._refresh_payments_by_type_stg(client, data_keys)
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
        FROM {self.current_db}.fact_pagamento_comprovante AS p FINAL
        LEFT JOIN {self.current_db}.payment_type_map AS m FINAL
            ON p.tipo_forma = m.tipo_forma AND m.id_empresa = p.id_empresa
        WHERE p.data_key IN ({keys_str}) AND p.is_deleted = 0
        GROUP BY p.id_empresa, p.id_filial, p.data_key, p.tipo_forma, m.label, m.category
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="payments_by_type_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_cash_overview(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        if self.source == "stg":
            return self._refresh_cash_overview_stg(client, data_keys)
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
        FROM {self.current_db}.fact_caixa_turno AS ct FINAL
        LEFT JOIN {self.current_db}.dim_usuario_caixa AS u FINAL
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
        if self.source == "stg":
            return self._refresh_fraud_daily_stg(client, data_keys)
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
        FROM {self.current_db}.fact_risco_evento AS r FINAL
        WHERE r.data_key IN ({keys_str}) AND r.is_deleted = 0
        GROUP BY r.id_empresa, r.id_filial, r.data_key, r.event_type
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="fraud_daily_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_risk_recent_events(self, client: Any) -> MartRefreshResult:
        if self.source == "stg":
            return self._refresh_risk_recent_events_stg(client)
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
        FROM {self.current_db}.fact_risco_evento AS r FINAL
        LEFT JOIN {self.current_db}.dim_usuario_caixa AS u FINAL
            ON r.id_empresa = u.id_empresa AND r.id_filial = u.id_filial AND r.id_usuario = u.id_usuario
        LEFT JOIN {self.current_db}.dim_funcionario AS f FINAL
            ON r.id_empresa = f.id_empresa AND r.id_filial = f.id_filial AND r.id_funcionario = f.id_funcionario
        WHERE r.is_deleted = 0
        ORDER BY r.id DESC
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="risk_recent_events_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_finance_overview(self, client: Any) -> MartRefreshResult:
        if self.source == "stg":
            return self._refresh_finance_overview_stg(client)
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
        FROM {self.current_db}.fact_financeiro AS f FINAL
        WHERE f.is_deleted = 0
        GROUP BY f.id_empresa, f.id_filial, f.tipo_titulo, faixa
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="finance_overview_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    def _refresh_dashboard_home(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        if self.source == "stg":
            return self._refresh_dashboard_home_stg(client, data_keys)
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.dashboard_home_rt
        SELECT
            base.id_empresa,
            base.id_filial,
            base.data_key,
            toDate(toString(base.data_key), '%Y%m%d') AS dt,
            base.faturamento,
            if(base.qtd_vendas > 0, base.faturamento / base.qtd_vendas, 0) AS ticket_medio,
            base.qtd_vendas,
            base.qtd_clientes,
            coalesce(cancel.qtd_cancelamentos, 0) AS qtd_cancelamentos,
            coalesce(cancel.valor_cancelado, 0) AS valor_cancelado,
            now64(6) AS published_at
        FROM (
            SELECT
                v.id_empresa,
                v.id_filial,
                v.data_key,
                sum(coalesce(vi.total, 0)) AS faturamento,
                toUInt32(uniqExactIf(v.id_comprovante, v.id_comprovante IS NOT NULL)) AS qtd_vendas,
                toUInt32(uniqExactIf(v.id_cliente, v.id_cliente IS NOT NULL)) AS qtd_clientes
            FROM {self.current_db}.fact_venda AS v FINAL
            INNER JOIN {self.current_db}.fact_venda_item AS vi FINAL
                ON v.id_empresa = vi.id_empresa AND v.id_filial = vi.id_filial
                AND v.id_db = vi.id_db AND v.id_movprodutos = vi.id_movprodutos
            WHERE v.data_key IN ({keys_str})
              AND v.is_deleted = 0
              AND vi.is_deleted = 0
              AND v.cancelado = 0
              AND coalesce(vi.cfop, 0) >= 5000
            GROUP BY v.id_empresa, v.id_filial, v.data_key
        ) AS base
        LEFT JOIN (
            SELECT
                id_empresa,
                id_filial,
                data_key,
                toUInt32(count()) AS qtd_cancelamentos,
                sum(coalesce(valor_total, 0)) AS valor_cancelado
            FROM {self.current_db}.fact_comprovante FINAL
            WHERE data_key IN ({keys_str})
              AND is_deleted = 0
              AND cancelado = 1
            GROUP BY id_empresa, id_filial, data_key
        ) AS cancel
            ON base.id_empresa = cancel.id_empresa
           AND base.id_filial = cancel.id_filial
           AND base.data_key = cancel.data_key
        """
        rows = client.command(sql)
        duration = int((time.time() - t0) * 1000)
        return MartRefreshResult(mart_name="dashboard_home_rt", rows_written=_parse_insert_count(rows), duration_ms=duration)

    # ================================================================
    # STG-DIRECT MART REFRESH IMPLEMENTATIONS
    # ================================================================

    def _stg_ts_expr(self, alias: str) -> str:
        return (
            f"coalesce({alias}.dt_evento, "
            f"parseDateTime64BestEffortOrNull(JSONExtractString({alias}.payload, 'TORQMIND_DT_EVENTO')), "
            f"parseDateTime64BestEffortOrNull(JSONExtractString({alias}.payload, 'DT_EVENTO')), "
            f"parseDateTime64BestEffortOrNull(JSONExtractString({alias}.payload, 'DATAHORA')), "
            f"parseDateTime64BestEffortOrNull(JSONExtractString({alias}.payload, 'DATA')), "
            f"{alias}.received_at, {alias}.ingested_at, now64(6))"
        )

    def _stg_data_key_expr(self, alias: str) -> str:
        return f"toInt32(formatDateTime({self._stg_ts_expr(alias)}, '%Y%m%d'))"

    def _stg_keys_filter(self, expr: str, data_keys: list[int]) -> str:
        keys = ",".join(str(int(k)) for k in sorted(set(data_keys)) if int(k) > 0)
        if not keys:
            return "1 = 1"
        return f"{expr} IN ({keys})"

    def _json_int(self, alias: str, field: str) -> str:
        return f"toInt32OrZero(JSONExtractString({alias}.payload, '{field}'))"

    def _json_int64(self, alias: str, field: str) -> str:
        return f"toInt64OrZero(JSONExtractString({alias}.payload, '{field}'))"

    def _json_dec(self, alias: str, field: str, scale: int = 2) -> str:
        return f"toDecimal64OrZero(JSONExtractString({alias}.payload, '{field}'), {scale})"

    def _stg_cancel_expr(self, alias: str = "c") -> str:
        situacao = f"ifNull({alias}.situacao_shadow, {self._json_int(alias, 'SITUACAO')})"
        raw_cancelado = (
            f"ifNull({alias}.cancelado_shadow, "
            f"if(lower(JSONExtractString({alias}.payload, 'CANCELADO')) IN ('true','t','1','s','sim','yes'), 1, 0))"
        )
        return f"multiIf({situacao} = 2, 1, {situacao} IN (3, 5), 0, {raw_cancelado})"

    def _stg_item_total_expr(self) -> str:
        return f"ifNull(i.total_shadow, {self._json_dec('i', 'TOTAL', 2)})"

    def _stg_item_qtd_expr(self) -> str:
        return f"ifNull(i.qtd_shadow, {self._json_dec('i', 'QTDE', 3)})"

    def _stg_item_desconto_expr(self) -> str:
        return f"ifNull(i.desconto_shadow, {self._json_dec('i', 'VLRDESCONTO', 2)})"

    def _stg_item_custo_total_expr(self) -> str:
        qtd = self._stg_item_qtd_expr()
        return f"ifNull(i.custo_unitario_shadow, toDecimal64(0, 6)) * {qtd}"

    def _refresh_sales_daily_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        data_key = self._stg_data_key_expr("c")
        key_filter = self._stg_keys_filter(data_key, data_keys)
        cancel = self._stg_cancel_expr("c")
        total = self._stg_item_total_expr()
        desconto = self._stg_item_desconto_expr()
        custo = self._stg_item_custo_total_expr()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_daily_rt
        SELECT
            base.id_empresa,
            base.id_filial,
            base.data_key,
            toDate(toString(base.data_key), '%Y%m%d') AS dt,
            base.faturamento,
            if(base.qtd_vendas > 0, base.faturamento / base.qtd_vendas, 0) AS ticket_medio,
            base.qtd_vendas,
            base.qtd_itens,
            coalesce(cancel.qtd_canceladas, 0) AS qtd_canceladas,
            coalesce(cancel.valor_cancelado, 0) AS valor_cancelado,
            base.desconto_total,
            base.custo_total,
            base.margem_total,
            now64(6) AS published_at
        FROM (
            SELECT
                c.id_empresa,
                c.id_filial,
                {data_key} AS data_key,
                sum({total}) AS faturamento,
                toUInt32(uniqExact(c.id_comprovante)) AS qtd_vendas,
                toUInt32(count()) AS qtd_itens,
                sum({desconto}) AS desconto_total,
                sum({custo}) AS custo_total,
                sum({total} - {custo}) AS margem_total
            FROM {self.current_db}.stg_comprovantes AS c FINAL
            INNER JOIN {self.current_db}.stg_itenscomprovantes AS i FINAL
                ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
                AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
            WHERE {key_filter}
              AND c.is_deleted = 0
              AND i.is_deleted = 0
              AND {cancel} = 0
              AND ifNull(i.cfop_shadow, {self._json_int('i', 'CFOP')}) >= 5000
            GROUP BY c.id_empresa, c.id_filial, data_key
        ) AS base
        LEFT JOIN (
            SELECT
                c.id_empresa,
                c.id_filial,
                {data_key} AS data_key,
                toUInt32(count()) AS qtd_canceladas,
                sum(ifNull(c.valor_total_shadow, {self._json_dec('c', 'VLRTOTAL', 2)})) AS valor_cancelado
            FROM {self.current_db}.stg_comprovantes AS c FINAL
            WHERE {key_filter}
              AND c.is_deleted = 0
              AND {cancel} = 1
            GROUP BY c.id_empresa, c.id_filial, data_key
        ) AS cancel
            ON base.id_empresa = cancel.id_empresa
           AND base.id_filial = cancel.id_filial
           AND base.data_key = cancel.data_key
        """
        rows = client.command(sql)
        return MartRefreshResult("sales_daily_rt", _parse_insert_count(rows), int((time.time() - t0) * 1000))

    def _refresh_sales_hourly_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        data_key = self._stg_data_key_expr("c")
        ts = self._stg_ts_expr("c")
        key_filter = self._stg_keys_filter(data_key, data_keys)
        cancel = self._stg_cancel_expr("c")
        total = self._stg_item_total_expr()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_hourly_rt
        SELECT
            c.id_empresa,
            c.id_filial,
            {data_key} AS data_key,
            toDate(toString(data_key), '%Y%m%d') AS dt,
            toUInt8(toHour({ts})) AS hora,
            sum({total}) AS faturamento,
            toUInt32(uniqExact(c.id_comprovante)) AS qtd_vendas,
            toUInt32(count()) AS qtd_itens,
            now64(6) AS published_at
        FROM {self.current_db}.stg_comprovantes AS c FINAL
        INNER JOIN {self.current_db}.stg_itenscomprovantes AS i FINAL
            ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
            AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
        WHERE {key_filter}
          AND c.is_deleted = 0
          AND i.is_deleted = 0
          AND {cancel} = 0
          AND ifNull(i.cfop_shadow, {self._json_int('i', 'CFOP')}) >= 5000
        GROUP BY c.id_empresa, c.id_filial, data_key, hora
        """
        rows = client.command(sql)
        return MartRefreshResult("sales_hourly_rt", _parse_insert_count(rows), int((time.time() - t0) * 1000))

    def _refresh_sales_products_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        data_key = self._stg_data_key_expr("c")
        key_filter = self._stg_keys_filter(data_key, data_keys)
        cancel = self._stg_cancel_expr("c")
        total = self._stg_item_total_expr()
        qtd = self._stg_item_qtd_expr()
        custo = self._stg_item_custo_total_expr()
        id_produto = f"ifNull(i.id_produto_shadow, {self._json_int('i', 'ID_PRODUTOS')})"
        id_grupo = f"ifNull(i.id_grupo_produto_shadow, {self._json_int('i', 'ID_GRUPOPRODUTOS')})"
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_products_rt
        SELECT
            i.id_empresa,
            i.id_filial,
            {data_key} AS data_key,
            toDate(toString(data_key), '%Y%m%d') AS dt,
            {id_produto} AS id_produto,
            coalesce(nullIf(JSONExtractString(p.payload, 'NOME'), ''), nullIf(JSONExtractString(p.payload, 'DESCRICAO'), ''), '') AS nome_produto,
            {id_grupo} AS id_grupo_produto,
            coalesce(nullIf(JSONExtractString(g.payload, 'NOME'), ''), nullIf(JSONExtractString(g.payload, 'DESCRICAO'), ''), '') AS nome_grupo,
            sum({qtd}) AS qtd,
            sum({total}) AS faturamento,
            sum({custo}) AS custo_total,
            sum({total} - {custo}) AS margem,
            now64(6) AS published_at
        FROM {self.current_db}.stg_itenscomprovantes AS i FINAL
        INNER JOIN {self.current_db}.stg_comprovantes AS c FINAL
            ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
            AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
        LEFT JOIN {self.current_db}.stg_produtos AS p FINAL
            ON p.id_empresa = i.id_empresa AND p.id_filial = i.id_filial AND p.id_produto = {id_produto}
        LEFT JOIN {self.current_db}.stg_grupoprodutos AS g FINAL
            ON g.id_empresa = i.id_empresa AND g.id_filial = i.id_filial AND g.id_grupoprodutos = {id_grupo}
        WHERE {key_filter}
          AND i.is_deleted = 0
          AND c.is_deleted = 0
          AND {cancel} = 0
          AND ifNull(i.cfop_shadow, {self._json_int('i', 'CFOP')}) >= 5000
        GROUP BY i.id_empresa, i.id_filial, data_key, id_produto, nome_produto, id_grupo_produto, nome_grupo
        """
        rows = client.command(sql)
        return MartRefreshResult("sales_products_rt", _parse_insert_count(rows), int((time.time() - t0) * 1000))

    def _refresh_sales_groups_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        data_key = self._stg_data_key_expr("c")
        key_filter = self._stg_keys_filter(data_key, data_keys)
        cancel = self._stg_cancel_expr("c")
        total = self._stg_item_total_expr()
        custo = self._stg_item_custo_total_expr()
        id_grupo = f"ifNull(i.id_grupo_produto_shadow, {self._json_int('i', 'ID_GRUPOPRODUTOS')})"
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_groups_rt
        SELECT
            i.id_empresa,
            i.id_filial,
            {data_key} AS data_key,
            toDate(toString(data_key), '%Y%m%d') AS dt,
            {id_grupo} AS id_grupo_produto,
            coalesce(nullIf(JSONExtractString(g.payload, 'NOME'), ''), nullIf(JSONExtractString(g.payload, 'DESCRICAO'), ''), '') AS nome_grupo,
            toUInt32(count()) AS qtd_itens,
            sum({total}) AS faturamento,
            sum({custo}) AS custo_total,
            sum({total} - {custo}) AS margem,
            now64(6) AS published_at
        FROM {self.current_db}.stg_itenscomprovantes AS i FINAL
        INNER JOIN {self.current_db}.stg_comprovantes AS c FINAL
            ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
            AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
        LEFT JOIN {self.current_db}.stg_grupoprodutos AS g FINAL
            ON g.id_empresa = i.id_empresa AND g.id_filial = i.id_filial AND g.id_grupoprodutos = {id_grupo}
        WHERE {key_filter}
          AND i.is_deleted = 0
          AND c.is_deleted = 0
          AND {cancel} = 0
          AND ifNull(i.cfop_shadow, {self._json_int('i', 'CFOP')}) >= 5000
        GROUP BY i.id_empresa, i.id_filial, data_key, id_grupo_produto, nome_grupo
        """
        rows = client.command(sql)
        return MartRefreshResult("sales_groups_rt", _parse_insert_count(rows), int((time.time() - t0) * 1000))

    def _refresh_payments_by_type_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        pay_ts = f"coalesce(c.dt_evento, p.dt_evento, c.received_at, p.received_at, c.ingested_at, p.ingested_at, now64(6))"
        data_key = f"toInt32(formatDateTime({pay_ts}, '%Y%m%d'))"
        key_filter = self._stg_keys_filter(data_key, data_keys)
        valor = f"ifNull(p.valor_shadow, {self._json_dec('p', 'VALOR', 2)})"
        ref = f"ifNull(c.referencia_shadow, {self._json_int64('c', 'REFERENCIA')})"
        sql = f"""
        INSERT INTO {self.mart_rt_db}.payments_by_type_rt
        SELECT
            p.id_empresa,
            p.id_filial,
            {data_key} AS data_key,
            toDate(toString(data_key), '%Y%m%d') AS dt,
            p.tipo_forma,
            coalesce(m.label, concat('Forma ', toString(p.tipo_forma))) AS label,
            coalesce(m.category, 'Outros') AS category,
            sum({valor}) AS valor_total,
            toUInt32(count()) AS qtd_transacoes,
            now64(6) AS published_at
        FROM {self.current_db}.stg_formas_pgto_comprovantes AS p FINAL
        LEFT JOIN {self.current_db}.stg_comprovantes AS c FINAL
            ON c.id_empresa = p.id_empresa AND c.id_filial = p.id_filial AND {ref} = p.id_referencia
        LEFT JOIN {self.current_db}.payment_type_map AS m FINAL
            ON p.tipo_forma = m.tipo_forma AND m.id_empresa = p.id_empresa
        WHERE {key_filter}
          AND p.is_deleted = 0
        GROUP BY p.id_empresa, p.id_filial, data_key, p.tipo_forma, m.label, m.category
        """
        rows = client.command(sql)
        return MartRefreshResult("payments_by_type_rt", _parse_insert_count(rows), int((time.time() - t0) * 1000))

    def _refresh_cash_overview_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        abertura = (
            "coalesce("
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DTABERTURA')), "
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DATAABERTURA')), "
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DTHRABERTURA')), "
            f"{self._stg_ts_expr('t')})"
        )
        fechamento = (
            "coalesce("
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DTFECHAMENTO')), "
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DATAFECHAMENTO')), "
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DTHRFECHAMENTO')))"
        )
        id_usuario = f"coalesce({self._json_int('t', 'ID_USUARIOS')}, {self._json_int('t', 'ID_USUARIO')})"
        is_aberto = f"if({self._json_int('t', 'ENCERRANTEFECHAMENTO')} = 0 AND fechamento_ts IS NULL, 1, 0)"
        sql = f"""
        INSERT INTO {self.mart_rt_db}.cash_overview_rt
        SELECT
            turnos.id_empresa,
            turnos.id_filial,
            turnos.id_turno,
            turnos.id_usuario,
            coalesce(nullIf(JSONExtractString(u.payload, 'NOMEUSUARIOS'), ''), nullIf(JSONExtractString(u.payload, 'NOME'), ''), '') AS nome_operador,
            turnos.abertura_ts,
            turnos.fechamento_ts,
            turnos.data_key_abertura,
            turnos.is_aberto,
            coalesce(vendas.faturamento, 0) AS faturamento_turno,
            coalesce(vendas.qtd, 0) AS qtd_vendas_turno,
            now64(6) AS published_at
        FROM (
            SELECT
                t.id_empresa,
                t.id_filial,
                t.id_turno,
                nullIf({id_usuario}, 0) AS id_usuario,
                {abertura} AS abertura_ts,
                {fechamento} AS fechamento_ts,
                toInt32(formatDateTime(abertura_ts, '%Y%m%d')) AS data_key_abertura,
                {is_aberto} AS is_aberto
            FROM {self.current_db}.stg_turnos AS t FINAL
            WHERE t.is_deleted = 0
        ) AS turnos
        LEFT JOIN {self.current_db}.stg_usuarios AS u FINAL
            ON turnos.id_empresa = u.id_empresa AND turnos.id_filial = u.id_filial AND turnos.id_usuario = u.id_usuario
        LEFT JOIN (
            SELECT
                c.id_empresa,
                c.id_filial,
                ifNull(c.id_turno_shadow, {self._json_int('c', 'ID_TURNOS')}) AS id_turno,
                sum(ifNull(c.valor_total_shadow, {self._json_dec('c', 'VLRTOTAL', 2)})) AS faturamento,
                toUInt32(count()) AS qtd
            FROM {self.current_db}.stg_comprovantes AS c FINAL
            WHERE c.is_deleted = 0 AND {self._stg_cancel_expr('c')} = 0
            GROUP BY c.id_empresa, c.id_filial, id_turno
        ) AS vendas ON turnos.id_empresa = vendas.id_empresa AND turnos.id_filial = vendas.id_filial
            AND turnos.id_turno = vendas.id_turno
        """
        rows = client.command(sql)
        return MartRefreshResult("cash_overview_rt", _parse_insert_count(rows), int((time.time() - t0) * 1000))

    def _refresh_fraud_daily_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        data_key = self._stg_data_key_expr("c")
        key_filter = self._stg_keys_filter(data_key, data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.fraud_daily_rt
        SELECT
            c.id_empresa,
            c.id_filial,
            {data_key} AS data_key,
            toDate(toString(data_key), '%Y%m%d') AS dt,
            'cancelamento' AS event_type,
            toUInt32(count()) AS qtd_eventos,
            sum(ifNull(c.valor_total_shadow, {self._json_dec('c', 'VLRTOTAL', 2)})) AS impacto_total,
            toDecimal64(80, 2) AS score_medio,
            now64(6) AS published_at
        FROM {self.current_db}.stg_comprovantes AS c FINAL
        WHERE {key_filter}
          AND c.is_deleted = 0
          AND {self._stg_cancel_expr('c')} = 1
        GROUP BY c.id_empresa, c.id_filial, data_key
        """
        rows = client.command(sql)
        return MartRefreshResult("fraud_daily_rt", _parse_insert_count(rows), int((time.time() - t0) * 1000))

    def _refresh_risk_recent_events_stg(self, client: Any) -> MartRefreshResult:
        t0 = time.time()
        data_key = self._stg_data_key_expr("c")
        sql = f"""
        INSERT INTO {self.mart_rt_db}.risk_recent_events_rt
        SELECT
            toInt64(cityHash64(concat(toString(c.id_empresa), ':', toString(c.id_filial), ':', toString(c.id_db), ':', toString(c.id_comprovante))) % 9223372036854775807) AS id,
            c.id_empresa,
            c.id_filial,
            {data_key} AS data_key,
            'cancelamento' AS event_type,
            'STG' AS source,
            nullIf(ifNull(c.id_usuario_shadow, {self._json_int('c', 'ID_USUARIOS')}), 0) AS id_usuario,
            coalesce(nullIf(JSONExtractString(u.payload, 'NOMEUSUARIOS'), ''), nullIf(JSONExtractString(u.payload, 'NOME'), ''), '') AS nome_operador,
            CAST(NULL, 'Nullable(Int32)') AS id_funcionario,
            '' AS nome_funcionario,
            ifNull(c.valor_total_shadow, {self._json_dec('c', 'VLRTOTAL', 2)}) AS valor_total,
            ifNull(c.valor_total_shadow, {self._json_dec('c', 'VLRTOTAL', 2)}) AS impacto_estimado,
            80 AS score_risco,
            'HIGH' AS score_level,
            '{{"source":"stg.comprovantes","rule":"cancelled_receipt"}}' AS reasons,
            now64(6) AS published_at
        FROM {self.current_db}.stg_comprovantes AS c FINAL
        LEFT JOIN {self.current_db}.stg_usuarios AS u FINAL
            ON c.id_empresa = u.id_empresa AND c.id_filial = u.id_filial
            AND nullIf(ifNull(c.id_usuario_shadow, {self._json_int('c', 'ID_USUARIOS')}), 0) = u.id_usuario
        WHERE c.is_deleted = 0
          AND {self._stg_cancel_expr('c')} = 1
        ORDER BY data_key DESC, id DESC
        """
        rows = client.command(sql)
        return MartRefreshResult("risk_recent_events_rt", _parse_insert_count(rows), int((time.time() - t0) * 1000))

    def _refresh_finance_overview_stg(self, client: Any) -> MartRefreshResult:
        t0 = time.time()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.finance_overview_rt
        WITH src AS (
            SELECT
                id_empresa,
                id_filial,
                tipo_titulo,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAVCTO'))) AS vencimento,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAPGTO'))) AS data_pagamento,
                toDecimal64OrZero(JSONExtractString(payload, 'VALOR'), 2) AS valor,
                toDecimal64OrZero(JSONExtractString(payload, 'VLRPAGO'), 2) AS valor_pago
            FROM {self.current_db}.stg_financeiro FINAL
            WHERE is_deleted = 0
            UNION ALL
            SELECT
                id_empresa,
                id_filial,
                0 AS tipo_titulo,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAVCTO'))) AS vencimento,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAPGTO'))) AS data_pagamento,
                toDecimal64OrZero(JSONExtractString(payload, 'VALOR'), 2) AS valor,
                toDecimal64OrZero(JSONExtractString(payload, 'VLRPAGO'), 2) AS valor_pago
            FROM {self.current_db}.stg_contaspagar FINAL
            WHERE is_deleted = 0
            UNION ALL
            SELECT
                id_empresa,
                id_filial,
                1 AS tipo_titulo,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAVCTO'))) AS vencimento,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAPGTO'))) AS data_pagamento,
                toDecimal64OrZero(JSONExtractString(payload, 'VALOR'), 2) AS valor,
                toDecimal64OrZero(JSONExtractString(payload, 'VLRPAGO'), 2) AS valor_pago
            FROM {self.current_db}.stg_contasreceber FINAL
            WHERE is_deleted = 0
        )
        SELECT
            id_empresa,
            id_filial,
            tipo_titulo,
            multiIf(
                data_pagamento IS NOT NULL, 'pago',
                vencimento < today(), 'vencido',
                vencimento <= today() + 7, 'vence_7d',
                vencimento <= today() + 30, 'vence_30d',
                'futuro'
            ) AS faixa,
            toUInt32(count()) AS qtd_titulos,
            sum(valor) AS valor_total,
            sum(valor_pago) AS valor_pago_total,
            sum(valor) - sum(valor_pago) AS valor_em_aberto,
            now64(6) AS published_at
        FROM src
        GROUP BY id_empresa, id_filial, tipo_titulo, faixa
        """
        rows = client.command(sql)
        return MartRefreshResult("finance_overview_rt", _parse_insert_count(rows), int((time.time() - t0) * 1000))

    def _refresh_dashboard_home_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        data_key = self._stg_data_key_expr("c")
        key_filter = self._stg_keys_filter(data_key, data_keys)
        cancel = self._stg_cancel_expr("c")
        total = self._stg_item_total_expr()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.dashboard_home_rt
        SELECT
            base.id_empresa,
            base.id_filial,
            base.data_key,
            toDate(toString(base.data_key), '%Y%m%d') AS dt,
            base.faturamento,
            if(base.qtd_vendas > 0, base.faturamento / base.qtd_vendas, 0) AS ticket_medio,
            base.qtd_vendas,
            base.qtd_clientes,
            coalesce(cancel.qtd_cancelamentos, 0) AS qtd_cancelamentos,
            coalesce(cancel.valor_cancelado, 0) AS valor_cancelado,
            now64(6) AS published_at
        FROM (
            SELECT
                c.id_empresa,
                c.id_filial,
                {data_key} AS data_key,
                sum({total}) AS faturamento,
                toUInt32(uniqExact(c.id_comprovante)) AS qtd_vendas,
                toUInt32(uniqExactIf(ifNull(c.id_cliente_shadow, {self._json_int('c', 'ID_ENTIDADE')}), ifNull(c.id_cliente_shadow, {self._json_int('c', 'ID_ENTIDADE')}) > 0)) AS qtd_clientes
            FROM {self.current_db}.stg_comprovantes AS c FINAL
            INNER JOIN {self.current_db}.stg_itenscomprovantes AS i FINAL
                ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
                AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
            WHERE {key_filter}
              AND c.is_deleted = 0
              AND i.is_deleted = 0
              AND {cancel} = 0
              AND ifNull(i.cfop_shadow, {self._json_int('i', 'CFOP')}) >= 5000
            GROUP BY c.id_empresa, c.id_filial, data_key
        ) AS base
        LEFT JOIN (
            SELECT
                c.id_empresa,
                c.id_filial,
                {data_key} AS data_key,
                toUInt32(count()) AS qtd_cancelamentos,
                sum(ifNull(c.valor_total_shadow, {self._json_dec('c', 'VLRTOTAL', 2)})) AS valor_cancelado
            FROM {self.current_db}.stg_comprovantes AS c FINAL
            WHERE {key_filter}
              AND c.is_deleted = 0
              AND {cancel} = 1
            GROUP BY c.id_empresa, c.id_filial, data_key
        ) AS cancel
            ON base.id_empresa = cancel.id_empresa
           AND base.id_filial = cancel.id_filial
           AND base.data_key = cancel.data_key
        """
        rows = client.command(sql)
        return MartRefreshResult("dashboard_home_rt", _parse_insert_count(rows), int((time.time() - t0) * 1000))

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
            FROM {self.ops_db}.cdc_table_state AS ts FINAL
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
