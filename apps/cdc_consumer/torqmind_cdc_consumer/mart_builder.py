"""TorqMind Realtime Mart Builder.

Architecture (low-memory, production-grade):
  1. CDC/bootstrap writes raw events into torqmind_current.stg_* (with payload).
  2. MartBuilder FIRST populates slim typed tables (stg_*_slim) by extracting
     needed fields from payload + shadow columns. This is the ONLY step that
     reads the payload column.
  3. Mart aggregation queries read ONLY from slim tables (no payload, no
     JSONExtractString, much cheaper FINAL due to ~100 byte rows vs ~2KB).

Memory budget: all queries target < 4 GB peak usage on an 8 GB server.
Backfill processes 7 data_keys at a time with conservative settings.

Trigger modes:
  1. After CDC consumer flush (incremental: only affected data_keys)
  2. Standalone backfill (full window rebuild)
  3. Validate (compare mart_rt vs legacy mart)

Idempotency: ReplacingMergeTree on mart_rt and slim tables ensures re-running
the builder for the same grain produces the same final result.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import clickhouse_connect

logger = logging.getLogger(__name__)

# Conservative ClickHouse settings for mart queries on 8 GB servers.
_QUERY_SETTINGS = {
    "max_memory_usage": 3_000_000_000,  # 3 GB hard limit per query
    "max_threads": 2,
    "join_algorithm": "partial_merge",
    "max_bytes_before_external_group_by": 500_000_000,
    "max_bytes_before_external_sort": 500_000_000,
}

# Backfill batch size: number of data_keys processed per iteration.
_BACKFILL_BATCH_SIZE = 7  # ~1 week at a time


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
    """Builds realtime marts from torqmind_current via slim typed layer."""

    # All STG timestamps are stored in UTC. For Brazilian operations,
    # data_key (YYYYMMDD) and hora must be in America/Sao_Paulo.
    _BUSINESS_TZ = "America/Sao_Paulo"

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
        self._consecutive_failures = 0
        self._max_consecutive_failures = 5
        self._backoff_seconds = 2.0

    def _get_client(self) -> clickhouse_connect.driver.client.Client:
        return clickhouse_connect.get_client(
            host=self.clickhouse_host,
            port=self.clickhouse_port,
            username=self.clickhouse_user,
            password=self.clickhouse_password,
            connect_timeout=10,
            send_receive_timeout=300,
        )

    def mark_affected(self, id_empresa: int, id_filial: int, data_key: int, table: str) -> None:
        """Called by CDC consumer after processing each event."""
        self.state.mark(id_empresa, id_filial, data_key, table)

    def refresh_if_needed(self) -> list[MartRefreshResult]:
        """Called after CDC consumer flush. Refreshes affected marts with backoff."""
        if not self.enabled or not self.state.has_work:
            return []

        # Circuit breaker: skip if too many consecutive failures
        if self._consecutive_failures >= self._max_consecutive_failures:
            backoff = self._backoff_seconds * (2 ** min(self._consecutive_failures - self._max_consecutive_failures, 6))
            logger.warning(
                f"Mart builder in backoff mode ({self._consecutive_failures} failures), "
                f"waiting {backoff:.0f}s before retry"
            )
            time.sleep(min(backoff, 120))

        results = []
        data_keys = list(self.state.affected_data_keys)
        tables = self.state.affected_tables

        try:
            client = self._get_client()
            try:
                if self.source == "stg":
                    # Step 1: Populate slim tables for affected data_keys
                    if tables & {"comprovantes", "itenscomprovantes", "formas_pgto_comprovantes", "payment_type_map", "turnos"}:
                        self._populate_slim_comprovantes(client, data_keys)
                        self._populate_slim_itens(client, data_keys)
                    if tables & {"formas_pgto_comprovantes", "payment_type_map"}:
                        self._populate_slim_formas(client, data_keys)

                    # Step 2: Build marts from slim tables
                    if tables & {"comprovantes", "itenscomprovantes"}:
                        results.append(self._refresh_sales_daily_stg(client, data_keys))
                        results.append(self._refresh_sales_hourly_stg(client, data_keys))
                        results.append(self._refresh_dashboard_home_stg(client, data_keys))
                        results.append(self._refresh_sales_products_stg(client, data_keys))
                        results.append(self._refresh_sales_groups_stg(client, data_keys))
                        results.append(self._refresh_fraud_daily_stg(client, data_keys))
                        results.append(self._refresh_risk_recent_events_stg(client))

                    if tables & {"formas_pgto_comprovantes", "payment_type_map"}:
                        results.append(self._refresh_payments_by_type_stg(client, data_keys))

                    if tables & {"turnos", "usuarios", "comprovantes"}:
                        results.append(self._refresh_cash_overview_stg(client, data_keys))

                    if tables & {"financeiro", "contaspagar", "contasreceber"}:
                        results.append(self._refresh_finance_overview_stg(client))
                else:
                    # DW-origin path (already typed, no slim needed)
                    if tables & {"fact_venda", "fact_venda_item", "fact_comprovante"}:
                        results.append(self._refresh_sales_daily_dw(client, data_keys))
                        results.append(self._refresh_sales_hourly_dw(client, data_keys))
                        results.append(self._refresh_dashboard_home_dw(client, data_keys))

                    if tables & {"fact_venda_item"}:
                        results.append(self._refresh_sales_products_dw(client, data_keys))
                        results.append(self._refresh_sales_groups_dw(client, data_keys))

                    if tables & {"fact_pagamento_comprovante"}:
                        results.append(self._refresh_payments_by_type_dw(client, data_keys))

                    if tables & {"fact_caixa_turno"}:
                        results.append(self._refresh_cash_overview_dw(client, data_keys))

                    if tables & {"fact_risco_evento"}:
                        results.append(self._refresh_fraud_daily_dw(client, data_keys))
                        results.append(self._refresh_risk_recent_events_dw(client))

                    if tables & {"fact_financeiro"}:
                        results.append(self._refresh_finance_overview_dw(client))

                # Log publication
                self._log_publications(client, results)
                self._update_source_freshness(client)
                self._consecutive_failures = 0  # Reset on success

            finally:
                client.close()
        except Exception as e:
            self._consecutive_failures += 1
            logger.error(
                f"Mart builder refresh failed (attempt {self._consecutive_failures}): {e}"
            )
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
        """Full backfill of all marts from current tables via slim layer.

        Process:
        1. Discover data_keys in range.
        2. For each batch of data_keys:
           a. Populate slim tables (payload -> typed, one-time extraction).
           b. Build all mart_rt tables from slim (no payload access).
        3. Non-data_key marts (cash, finance, risk) built at end.
        """
        logger.info(
            f"Mart builder backfill: from={from_date} to={to_date or 'now'} "
            f"empresa={id_empresa} filial={id_filial or 'all'} "
            f"batch_size={_BACKFILL_BATCH_SIZE}"
        )

        from_key = int(from_date.replace("-", ""))
        if to_date:
            to_key = int(to_date.replace("-", ""))
        else:
            # Cap at today + 30 days to exclude corrupt future dates (2299xxxx)
            from datetime import date, timedelta
            cap = date.today() + timedelta(days=30)
            to_key = int(cap.strftime("%Y%m%d"))

        client = self._get_client()
        results: list[MartRefreshResult] = []
        try:
            # Ensure slim tables exist
            self._ensure_slim_ddl(client)

            filial_filter = f"AND id_filial = {id_filial}" if id_filial else ""

            if self.source == "stg":
                # Get distinct data_keys from comprovantes using dt_evento
                data_key_expr = self._stg_data_key_expr("c")
                data_keys_rows = client.query(
                    f"SELECT DISTINCT {data_key_expr} AS dk "
                    f"FROM {self.current_db}.stg_comprovantes AS c FINAL "
                    f"WHERE c.id_empresa = {{id_empresa:Int32}} "
                    f"AND {data_key_expr} >= {{from_key:Int32}} "
                    f"AND {data_key_expr} <= {{to_key:Int32}} "
                    f"AND c.is_deleted = 0 {filial_filter} "
                    f"ORDER BY dk",
                    parameters={"id_empresa": id_empresa, "from_key": from_key, "to_key": to_key},
                    settings={"max_memory_usage": 2_000_000_000, "max_threads": 2},
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

            logger.info(f"Backfill: {len(data_keys)} data_keys to process in batches of {_BACKFILL_BATCH_SIZE}")

            # Process in small batches for memory safety
            for i in range(0, len(data_keys), _BACKFILL_BATCH_SIZE):
                chunk = data_keys[i:i + _BACKFILL_BATCH_SIZE]
                batch_num = (i // _BACKFILL_BATCH_SIZE) + 1
                total_batches = (len(data_keys) + _BACKFILL_BATCH_SIZE - 1) // _BACKFILL_BATCH_SIZE
                logger.info(
                    f"Backfill batch {batch_num}/{total_batches}: "
                    f"data_keys {chunk[0]}..{chunk[-1]} ({len(chunk)} keys)"
                )

                if self.source == "stg":
                    # Step 1: Populate slim tables for this batch
                    self._populate_slim_comprovantes(client, chunk)
                    self._populate_slim_itens(client, chunk)
                    self._populate_slim_formas(client, chunk)

                    # Step 2: Build marts from slim
                    results.append(self._refresh_sales_daily_stg(client, chunk))
                    results.append(self._refresh_sales_hourly_stg(client, chunk))
                    results.append(self._refresh_sales_products_stg(client, chunk))
                    results.append(self._refresh_sales_groups_stg(client, chunk))
                    results.append(self._refresh_payments_by_type_stg(client, chunk))
                    results.append(self._refresh_dashboard_home_stg(client, chunk))
                    results.append(self._refresh_fraud_daily_stg(client, chunk))
                else:
                    results.append(self._refresh_sales_daily_dw(client, chunk))
                    results.append(self._refresh_sales_hourly_dw(client, chunk))
                    results.append(self._refresh_sales_products_dw(client, chunk))
                    results.append(self._refresh_sales_groups_dw(client, chunk))
                    results.append(self._refresh_payments_by_type_dw(client, chunk))
                    results.append(self._refresh_dashboard_home_dw(client, chunk))
                    results.append(self._refresh_fraud_daily_dw(client, chunk))

            # Non-batched marts (no data_key dependency)
            if self.source == "stg":
                results.append(self._refresh_cash_overview_stg(client, data_keys))
                results.append(self._refresh_risk_recent_events_stg(client))
                results.append(self._refresh_finance_overview_stg(client))
            else:
                results.append(self._refresh_cash_overview_dw(client, data_keys))
                results.append(self._refresh_risk_recent_events_dw(client))
                results.append(self._refresh_finance_overview_dw(client))

            self._log_publications(client, results)
            self._update_source_freshness(client)
        finally:
            client.close()

        total_rows = sum(r.rows_written for r in results)
        errors = [r for r in results if r.error]
        logger.info(
            f"Backfill complete: {len(results)} refreshes, {total_rows} rows, "
            f"{len(errors)} errors"
        )
        return results

    # ================================================================
    # SLIM TABLE DDL & POPULATION
    # ================================================================

    def _ensure_slim_ddl(self, client: Any) -> None:
        """Create slim tables if they don't exist."""
        ddls = [
            f"""CREATE TABLE IF NOT EXISTS {self.current_db}.stg_comprovantes_slim (
                id_empresa Int32 NOT NULL, id_filial Int32 NOT NULL,
                id_db Int32 NOT NULL, id_comprovante Int32 NOT NULL,
                data_key Int32 NOT NULL, hora UInt8 NOT NULL DEFAULT 0,
                dt_evento_local DateTime64(6, 'America/Sao_Paulo') NOT NULL DEFAULT '1970-01-01 00:00:00',
                valor_total Decimal(18,2) NOT NULL DEFAULT 0,
                cancelado UInt8 NOT NULL DEFAULT 0,
                situacao Int32 NOT NULL DEFAULT 0,
                id_turno Int32 NOT NULL DEFAULT 0,
                id_usuario Int32 NOT NULL DEFAULT 0,
                id_cliente Int32 NOT NULL DEFAULT 0,
                referencia Int64 NOT NULL DEFAULT 0,
                is_deleted UInt8 NOT NULL DEFAULT 0,
                source_ts_ms Int64 NOT NULL
            ) ENGINE = ReplacingMergeTree(source_ts_ms)
            ORDER BY (id_empresa, id_filial, id_db, id_comprovante)
            SETTINGS index_granularity = 8192""",
            f"""CREATE TABLE IF NOT EXISTS {self.current_db}.stg_itenscomprovantes_slim (
                id_empresa Int32 NOT NULL, id_filial Int32 NOT NULL,
                id_db Int32 NOT NULL, id_comprovante Int32 NOT NULL,
                id_itemcomprovante Int32 NOT NULL, data_key Int32 NOT NULL,
                id_produto Int32 NOT NULL DEFAULT 0,
                id_grupo_produto Int32 NOT NULL DEFAULT 0,
                cfop Int32 NOT NULL DEFAULT 0,
                qtd Decimal(18,3) NOT NULL DEFAULT 0,
                total Decimal(18,2) NOT NULL DEFAULT 0,
                desconto Decimal(18,2) NOT NULL DEFAULT 0,
                custo_total Decimal(18,6) NOT NULL DEFAULT 0,
                is_deleted UInt8 NOT NULL DEFAULT 0,
                source_ts_ms Int64 NOT NULL
            ) ENGINE = ReplacingMergeTree(source_ts_ms)
            ORDER BY (id_empresa, id_filial, id_db, id_comprovante, id_itemcomprovante)
            SETTINGS index_granularity = 8192""",
            f"""CREATE TABLE IF NOT EXISTS {self.current_db}.stg_formas_pgto_slim (
                id_empresa Int32 NOT NULL, id_filial Int32 NOT NULL,
                id_referencia Int64 NOT NULL, tipo_forma Int32 NOT NULL,
                data_key Int32 NOT NULL,
                valor Decimal(18,2) NOT NULL DEFAULT 0,
                is_deleted UInt8 NOT NULL DEFAULT 0,
                source_ts_ms Int64 NOT NULL
            ) ENGINE = ReplacingMergeTree(source_ts_ms)
            ORDER BY (id_empresa, id_filial, id_referencia, tipo_forma)
            SETTINGS index_granularity = 8192""",
        ]
        for ddl in ddls:
            client.command(ddl)

    def _populate_slim_comprovantes(self, client: Any, data_keys: list[int]) -> None:
        """Extract typed columns from stg_comprovantes payload into slim table.

        This is the ONLY query that reads the payload column for comprovantes.
        """
        if not data_keys:
            return
        t0 = time.time()
        data_key_expr = self._stg_data_key_expr("c")
        key_filter = self._stg_keys_filter(data_key_expr, data_keys)

        # Resolve all fields from shadow columns with payload fallback
        situacao = f"ifNull(c.situacao_shadow, toInt32OrZero(JSONExtractString(c.payload, 'SITUACAO')))"
        raw_cancelado = (
            f"ifNull(c.cancelado_shadow, "
            f"if(lower(JSONExtractString(c.payload, 'CANCELADO')) IN ('true','t','1','s','sim','yes'), 1, 0))"
        )
        cancelado_expr = f"toUInt8(multiIf({situacao} = 2, 1, {situacao} IN (3, 5), 0, {raw_cancelado}))"
        valor_total = f"ifNull(c.valor_total_shadow, toDecimal64OrZero(JSONExtractString(c.payload, 'VLRTOTAL'), 2))"
        id_turno = f"ifNull(c.id_turno_shadow, toInt32OrZero(JSONExtractString(c.payload, 'ID_TURNOS')))"
        id_usuario = f"coalesce(c.id_usuario_shadow, toInt32OrZero(JSONExtractString(c.payload, 'ID_USUARIOS')), toInt32OrZero(JSONExtractString(c.payload, 'ID_USUARIO')))"
        id_cliente = f"ifNull(c.id_cliente_shadow, toInt32OrZero(JSONExtractString(c.payload, 'ID_ENTIDADE')))"
        referencia = f"ifNull(c.referencia_shadow, toInt64OrZero(JSONExtractString(c.payload, 'REFERENCIA')))"
        ts_local = self._stg_ts_local_expr("c")

        sql = f"""
        INSERT INTO {self.current_db}.stg_comprovantes_slim
        SELECT
            c.id_empresa, c.id_filial, c.id_db, c.id_comprovante,
            {data_key_expr} AS data_key,
            toUInt8(toHour({ts_local})) AS hora,
            {ts_local} AS dt_evento_local,
            {valor_total} AS valor_total,
            {cancelado_expr} AS cancelado,
            {situacao} AS situacao,
            {id_turno} AS id_turno,
            {id_usuario} AS id_usuario,
            {id_cliente} AS id_cliente,
            {referencia} AS referencia,
            c.is_deleted,
            c.source_ts_ms
        FROM {self.current_db}.stg_comprovantes AS c FINAL
        WHERE {key_filter}
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        elapsed = int((time.time() - t0) * 1000)
        logger.debug(f"Populated slim comprovantes for {len(data_keys)} keys in {elapsed}ms")

    def _populate_slim_itens(self, client: Any, data_keys: list[int]) -> None:
        """Extract typed columns from stg_itenscomprovantes payload into slim table."""
        if not data_keys:
            return
        t0 = time.time()
        data_key_expr = self._stg_data_key_expr("c")
        key_filter = self._stg_keys_filter(data_key_expr, data_keys)

        id_produto = f"ifNull(i.id_produto_shadow, toInt32OrZero(JSONExtractString(i.payload, 'ID_PRODUTOS')))"
        id_grupo = f"ifNull(i.id_grupo_produto_shadow, toInt32OrZero(JSONExtractString(i.payload, 'ID_GRUPOPRODUTOS')))"
        cfop = f"ifNull(i.cfop_shadow, toInt32OrZero(replaceAll(JSONExtractString(i.payload, 'CFOP'), '.', '')))"
        qtd = f"ifNull(i.qtd_shadow, toDecimal64OrZero(JSONExtractString(i.payload, 'QTDE'), 3))"
        total = f"ifNull(i.total_shadow, toDecimal64OrZero(JSONExtractString(i.payload, 'TOTAL'), 2))"
        desconto = f"ifNull(i.desconto_shadow, toDecimal64OrZero(JSONExtractString(i.payload, 'VLRDESCONTO'), 2))"
        custo = f"ifNull(i.custo_unitario_shadow, toDecimal64(0, 6)) * {qtd}"

        sql = f"""
        INSERT INTO {self.current_db}.stg_itenscomprovantes_slim
        SELECT
            i.id_empresa, i.id_filial, i.id_db, i.id_comprovante,
            i.id_itemcomprovante,
            {data_key_expr} AS data_key,
            {id_produto} AS id_produto,
            {id_grupo} AS id_grupo_produto,
            {cfop} AS cfop,
            {qtd} AS qtd,
            {total} AS total,
            {desconto} AS desconto,
            {custo} AS custo_total,
            i.is_deleted,
            i.source_ts_ms
        FROM {self.current_db}.stg_itenscomprovantes AS i FINAL
        INNER JOIN {self.current_db}.stg_comprovantes AS c FINAL
            ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
            AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
        WHERE {key_filter}
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        elapsed = int((time.time() - t0) * 1000)
        logger.debug(f"Populated slim itens for {len(data_keys)} keys in {elapsed}ms")

    def _populate_slim_formas(self, client: Any, data_keys: list[int]) -> None:
        """Extract typed columns from stg_formas_pgto_comprovantes into slim table."""
        if not data_keys:
            return
        t0 = time.time()
        data_key_expr = self._stg_data_key_expr("c")
        key_filter = self._stg_keys_filter(data_key_expr, data_keys)
        valor = f"ifNull(p.valor_shadow, toDecimal64OrZero(JSONExtractString(p.payload, 'VALOR'), 2))"
        ref = f"ifNull(c.referencia_shadow, toInt64OrZero(JSONExtractString(c.payload, 'REFERENCIA')))"

        sql = f"""
        INSERT INTO {self.current_db}.stg_formas_pgto_slim
        SELECT
            p.id_empresa, p.id_filial, p.id_referencia, p.tipo_forma,
            {data_key_expr} AS data_key,
            {valor} AS valor,
            p.is_deleted,
            p.source_ts_ms
        FROM {self.current_db}.stg_formas_pgto_comprovantes AS p FINAL
        LEFT JOIN {self.current_db}.stg_comprovantes AS c FINAL
            ON c.id_empresa = p.id_empresa AND c.id_filial = p.id_filial
            AND {ref} = p.id_referencia
        WHERE {key_filter}
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        elapsed = int((time.time() - t0) * 1000)
        logger.debug(f"Populated slim formas for {len(data_keys)} keys in {elapsed}ms")

    # ================================================================
    # HELPER EXPRESSIONS (payload parsing for slim population only)
    # ================================================================

    def _stg_ts_expr(self, alias: str) -> str:
        """Raw UTC timestamp expression from STG comprovantes (used in slim population)."""
        return (
            f"coalesce({alias}.dt_evento, "
            f"parseDateTime64BestEffortOrNull(JSONExtractString({alias}.payload, 'TORQMIND_DT_EVENTO')), "
            f"parseDateTime64BestEffortOrNull(JSONExtractString({alias}.payload, 'DT_EVENTO')), "
            f"parseDateTime64BestEffortOrNull(JSONExtractString({alias}.payload, 'DATAHORA')), "
            f"parseDateTime64BestEffortOrNull(JSONExtractString({alias}.payload, 'DATA')), "
            f"{alias}.received_at, {alias}.ingested_at, now64(6))"
        )

    def _stg_ts_local_expr(self, alias: str) -> str:
        """Timestamp converted to local business timezone."""
        return f"toTimezone({self._stg_ts_expr(alias)}, '{self._BUSINESS_TZ}')"

    def _stg_data_key_expr(self, alias: str) -> str:
        """data_key (YYYYMMDD int) in local business timezone."""
        return f"toInt32(formatDateTime({self._stg_ts_local_expr(alias)}, '%Y%m%d'))"

    def _stg_keys_filter(self, expr: str, data_keys: list[int]) -> str:
        keys = ",".join(str(int(k)) for k in sorted(set(data_keys)) if int(k) > 0)
        if not keys:
            return "1 = 1"
        return f"{expr} IN ({keys})"

    # ================================================================
    # STG-DIRECT MART QUERIES (read from SLIM tables - no payload!)
    # ================================================================

    def _slim_keys_filter(self, data_keys: list[int], alias: str = "") -> str:
        """Filter by data_key on slim tables."""
        keys = ",".join(str(int(k)) for k in sorted(set(data_keys)) if int(k) > 0)
        prefix = f"{alias}." if alias else ""
        if not keys:
            return "1 = 1"
        return f"{prefix}data_key IN ({keys})"

    def _refresh_sales_daily_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        """Sales daily from slim tables. No payload, no JSONExtract."""
        t0 = time.time()
        kf = self._slim_keys_filter(data_keys, "c")
        kf_i = self._slim_keys_filter(data_keys, "i")
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_daily_rt
        SELECT
            base.id_empresa, base.id_filial, base.data_key,
            toDate(toString(base.data_key), '%Y%m%d') AS dt,
            base.faturamento,
            if(base.qtd_vendas > 0, base.faturamento / base.qtd_vendas, 0) AS ticket_medio,
            base.qtd_vendas, base.qtd_itens,
            coalesce(cancel_agg.qtd_canceladas, 0) AS qtd_canceladas,
            coalesce(cancel_agg.valor_cancelado, 0) AS valor_cancelado,
            base.desconto_total, base.custo_total, base.margem_total,
            now64(6) AS published_at
        FROM (
            SELECT
                c.id_empresa, c.id_filial, c.data_key,
                sum(i.total) AS faturamento,
                toUInt32(uniqExact(c.id_comprovante)) AS qtd_vendas,
                toUInt32(count()) AS qtd_itens,
                sum(i.desconto) AS desconto_total,
                sum(i.custo_total) AS custo_total,
                sum(i.total - i.custo_total) AS margem_total
            FROM {self.current_db}.stg_comprovantes_slim AS c
            INNER JOIN {self.current_db}.stg_itenscomprovantes_slim AS i
                ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
                AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
            WHERE {kf} AND c.is_deleted = 0 AND i.is_deleted = 0
              AND c.cancelado = 0 AND i.cfop >= 5000 AND {kf_i}
            GROUP BY c.id_empresa, c.id_filial, c.data_key
        ) AS base
        LEFT JOIN (
            SELECT c.id_empresa, c.id_filial, c.data_key,
                   toUInt32(count()) AS qtd_canceladas,
                   sum(c.valor_total) AS valor_cancelado
            FROM {self.current_db}.stg_comprovantes_slim AS c
            WHERE {kf} AND c.is_deleted = 0 AND c.cancelado = 1
            GROUP BY c.id_empresa, c.id_filial, c.data_key
        ) AS cancel_agg
            ON base.id_empresa = cancel_agg.id_empresa
           AND base.id_filial = cancel_agg.id_filial
           AND base.data_key = cancel_agg.data_key
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("sales_daily_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_sales_hourly_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        """Sales hourly from slim tables. No payload, no JSONExtract."""
        t0 = time.time()
        kf = self._slim_keys_filter(data_keys, "c")
        kf_i = self._slim_keys_filter(data_keys, "i")
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_hourly_rt
        SELECT
            c.id_empresa, c.id_filial, c.data_key,
            toDate(toString(c.data_key), '%Y%m%d') AS dt,
            c.hora,
            sum(i.total) AS faturamento,
            toUInt32(uniqExact(c.id_comprovante)) AS qtd_vendas,
            toUInt32(count()) AS qtd_itens,
            now64(6) AS published_at
        FROM {self.current_db}.stg_comprovantes_slim AS c
        INNER JOIN {self.current_db}.stg_itenscomprovantes_slim AS i
            ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
            AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
        WHERE {kf} AND c.is_deleted = 0 AND i.is_deleted = 0
          AND c.cancelado = 0 AND i.cfop >= 5000 AND {kf_i}
        GROUP BY c.id_empresa, c.id_filial, c.data_key, c.hora
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("sales_hourly_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_sales_products_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        """Sales by product from slim tables."""
        t0 = time.time()
        kf = self._slim_keys_filter(data_keys, "c")
        kf_i = self._slim_keys_filter(data_keys, "i")
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_products_rt
        SELECT
            i.id_empresa, i.id_filial, i.data_key,
            toDate(toString(i.data_key), '%Y%m%d') AS dt,
            i.id_produto,
            coalesce(nullIf(JSONExtractString(p.payload, 'NOME'), ''), nullIf(JSONExtractString(p.payload, 'DESCRICAO'), ''), '') AS nome_produto,
            i.id_grupo_produto,
            coalesce(nullIf(JSONExtractString(g.payload, 'NOME'), ''), nullIf(JSONExtractString(g.payload, 'DESCRICAO'), ''), '') AS nome_grupo,
            sum(i.qtd) AS qtd,
            sum(i.total) AS faturamento,
            sum(i.custo_total) AS custo_total,
            sum(i.total - i.custo_total) AS margem,
            now64(6) AS published_at
        FROM {self.current_db}.stg_itenscomprovantes_slim AS i
        INNER JOIN {self.current_db}.stg_comprovantes_slim AS c
            ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
            AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
        LEFT JOIN {self.current_db}.stg_produtos AS p FINAL
            ON p.id_empresa = i.id_empresa AND p.id_filial = i.id_filial AND p.id_produto = i.id_produto
        LEFT JOIN {self.current_db}.stg_grupoprodutos AS g FINAL
            ON g.id_empresa = i.id_empresa AND g.id_filial = i.id_filial AND g.id_grupoprodutos = i.id_grupo_produto
        WHERE {kf} AND i.is_deleted = 0 AND c.is_deleted = 0
          AND c.cancelado = 0 AND i.cfop >= 5000 AND {kf_i}
        GROUP BY i.id_empresa, i.id_filial, i.data_key, i.id_produto, nome_produto, i.id_grupo_produto, nome_grupo
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("sales_products_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_sales_groups_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        """Sales by group from slim tables."""
        t0 = time.time()
        kf = self._slim_keys_filter(data_keys, "c")
        kf_i = self._slim_keys_filter(data_keys, "i")
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_groups_rt
        SELECT
            i.id_empresa, i.id_filial, i.data_key,
            toDate(toString(i.data_key), '%Y%m%d') AS dt,
            i.id_grupo_produto,
            coalesce(nullIf(JSONExtractString(g.payload, 'NOME'), ''), nullIf(JSONExtractString(g.payload, 'DESCRICAO'), ''), '') AS nome_grupo,
            toUInt32(count()) AS qtd_itens,
            sum(i.total) AS faturamento,
            sum(i.custo_total) AS custo_total,
            sum(i.total - i.custo_total) AS margem,
            now64(6) AS published_at
        FROM {self.current_db}.stg_itenscomprovantes_slim AS i
        INNER JOIN {self.current_db}.stg_comprovantes_slim AS c
            ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
            AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
        LEFT JOIN {self.current_db}.stg_grupoprodutos AS g FINAL
            ON g.id_empresa = i.id_empresa AND g.id_filial = i.id_filial AND g.id_grupoprodutos = i.id_grupo_produto
        WHERE {kf} AND i.is_deleted = 0 AND c.is_deleted = 0
          AND c.cancelado = 0 AND i.cfop >= 5000 AND {kf_i}
        GROUP BY i.id_empresa, i.id_filial, i.data_key, i.id_grupo_produto, nome_grupo
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("sales_groups_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_payments_by_type_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        """Payments by type from slim tables."""
        t0 = time.time()
        kf = self._slim_keys_filter(data_keys, "p")
        sql = f"""
        INSERT INTO {self.mart_rt_db}.payments_by_type_rt
        SELECT
            p.id_empresa, p.id_filial, p.data_key,
            toDate(toString(p.data_key), '%Y%m%d') AS dt,
            p.tipo_forma,
            coalesce(m.label, concat('Forma ', toString(p.tipo_forma))) AS label,
            coalesce(m.category, 'Outros') AS category,
            sum(p.valor) AS valor_total,
            toUInt32(count()) AS qtd_transacoes,
            now64(6) AS published_at
        FROM {self.current_db}.stg_formas_pgto_slim AS p
        LEFT JOIN {self.current_db}.payment_type_map AS m FINAL
            ON p.tipo_forma = m.tipo_forma AND m.id_empresa = p.id_empresa
        WHERE {kf} AND p.is_deleted = 0
        GROUP BY p.id_empresa, p.id_filial, p.data_key, p.tipo_forma, m.label, m.category
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("payments_by_type_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_cash_overview_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        """Cash overview. Reads turnos payload (small table) + slim comprovantes."""
        t0 = time.time()
        tz = self._BUSINESS_TZ
        abertura = (
            f"toTimezone(coalesce("
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DTABERTURA')), "
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DATAABERTURA')), "
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DTHRABERTURA')), "
            f"t.dt_evento, t.received_at, t.ingested_at, now64(6)), '{tz}')"
        )
        fechamento = (
            f"if(coalesce("
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DTFECHAMENTO')), "
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DATAFECHAMENTO')), "
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DTHRFECHAMENTO'))) IS NOT NULL, "
            f"toTimezone(coalesce("
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DTFECHAMENTO')), "
            "parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DATAFECHAMENTO')), "
            f"parseDateTime64BestEffortOrNull(JSONExtractString(t.payload, 'DTHRFECHAMENTO'))), '{tz}'), NULL)"
        )
        id_usuario = "coalesce(toInt32OrZero(JSONExtractString(t.payload, 'ID_USUARIOS')), toInt32OrZero(JSONExtractString(t.payload, 'ID_USUARIO')))"
        is_aberto = "if(toInt32OrZero(JSONExtractString(t.payload, 'ENCERRANTEFECHAMENTO')) = 0 AND fechamento_ts IS NULL, 1, 0)"
        sql = f"""
        INSERT INTO {self.mart_rt_db}.cash_overview_rt
        SELECT
            turnos.id_empresa, turnos.id_filial, turnos.id_turno, turnos.id_usuario,
            coalesce(nullIf(JSONExtractString(u.payload, 'NOMEUSUARIOS'), ''), nullIf(JSONExtractString(u.payload, 'NOME'), ''), '') AS nome_operador,
            turnos.abertura_ts, turnos.fechamento_ts, turnos.data_key_abertura, turnos.is_aberto,
            coalesce(vendas.faturamento, 0) AS faturamento_turno,
            coalesce(vendas.qtd, 0) AS qtd_vendas_turno,
            now64(6) AS published_at
        FROM (
            SELECT
                t.id_empresa, t.id_filial, t.id_turno,
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
            SELECT c.id_empresa, c.id_filial, c.id_turno,
                   sum(c.valor_total) AS faturamento,
                   toUInt32(count()) AS qtd
            FROM {self.current_db}.stg_comprovantes_slim AS c
            WHERE c.is_deleted = 0 AND c.cancelado = 0
            GROUP BY c.id_empresa, c.id_filial, c.id_turno
        ) AS vendas ON turnos.id_empresa = vendas.id_empresa AND turnos.id_filial = vendas.id_filial
            AND turnos.id_turno = vendas.id_turno
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("cash_overview_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_fraud_daily_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        """Fraud daily from slim comprovantes (cancelled receipts)."""
        t0 = time.time()
        kf = self._slim_keys_filter(data_keys, "c")
        sql = f"""
        INSERT INTO {self.mart_rt_db}.fraud_daily_rt
        SELECT
            c.id_empresa, c.id_filial, c.data_key,
            toDate(toString(c.data_key), '%Y%m%d') AS dt,
            'cancelamento' AS event_type,
            toUInt32(count()) AS qtd_eventos,
            sum(c.valor_total) AS impacto_total,
            toDecimal64(80, 2) AS score_medio,
            now64(6) AS published_at
        FROM {self.current_db}.stg_comprovantes_slim AS c
        WHERE {kf} AND c.is_deleted = 0 AND c.cancelado = 1
        GROUP BY c.id_empresa, c.id_filial, c.data_key
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("fraud_daily_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_risk_recent_events_stg(self, client: Any) -> MartRefreshResult:
        """Risk events from slim comprovantes + usuarios (small dim)."""
        t0 = time.time()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.risk_recent_events_rt
        SELECT
            toInt64(cityHash64(concat(toString(c.id_empresa), ':', toString(c.id_filial), ':', toString(c.id_db), ':', toString(c.id_comprovante))) % 9223372036854775807) AS id,
            c.id_empresa, c.id_filial, c.data_key,
            'cancelamento' AS event_type, 'STG' AS source,
            nullIf(c.id_usuario, 0) AS id_usuario,
            coalesce(nullIf(JSONExtractString(u.payload, 'NOMEUSUARIOS'), ''), nullIf(JSONExtractString(u.payload, 'NOME'), ''), '') AS nome_operador,
            CAST(NULL, 'Nullable(Int32)') AS id_funcionario,
            '' AS nome_funcionario,
            c.valor_total, c.valor_total AS impacto_estimado,
            80 AS score_risco, 'HIGH' AS score_level,
            '{{"source":"stg.comprovantes","rule":"cancelled_receipt"}}' AS reasons,
            now64(6) AS published_at
        FROM {self.current_db}.stg_comprovantes_slim AS c
        LEFT JOIN {self.current_db}.stg_usuarios AS u FINAL
            ON c.id_empresa = u.id_empresa AND c.id_filial = u.id_filial AND nullIf(c.id_usuario, 0) = u.id_usuario
        WHERE c.is_deleted = 0 AND c.cancelado = 1
        ORDER BY c.data_key DESC, id DESC
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("risk_recent_events_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_finance_overview_stg(self, client: Any) -> MartRefreshResult:
        """Finance overview. Reads payload from finance tables (small volume)."""
        t0 = time.time()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.finance_overview_rt
        WITH src AS (
            SELECT id_empresa, id_filial, tipo_titulo,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAVCTO'))) AS vencimento,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAPGTO'))) AS data_pagamento,
                toDecimal64OrZero(JSONExtractString(payload, 'VALOR'), 2) AS valor,
                toDecimal64OrZero(JSONExtractString(payload, 'VLRPAGO'), 2) AS valor_pago
            FROM {self.current_db}.stg_financeiro FINAL WHERE is_deleted = 0
            UNION ALL
            SELECT id_empresa, id_filial, 0 AS tipo_titulo,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAVCTO'))) AS vencimento,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAPGTO'))) AS data_pagamento,
                toDecimal64OrZero(JSONExtractString(payload, 'VALOR'), 2) AS valor,
                toDecimal64OrZero(JSONExtractString(payload, 'VLRPAGO'), 2) AS valor_pago
            FROM {self.current_db}.stg_contaspagar FINAL WHERE is_deleted = 0
            UNION ALL
            SELECT id_empresa, id_filial, 1 AS tipo_titulo,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAVCTO'))) AS vencimento,
                toDate(parseDateTime64BestEffortOrNull(JSONExtractString(payload, 'DTAPGTO'))) AS data_pagamento,
                toDecimal64OrZero(JSONExtractString(payload, 'VALOR'), 2) AS valor,
                toDecimal64OrZero(JSONExtractString(payload, 'VLRPAGO'), 2) AS valor_pago
            FROM {self.current_db}.stg_contasreceber FINAL WHERE is_deleted = 0
        )
        SELECT id_empresa, id_filial, tipo_titulo,
            multiIf(data_pagamento IS NOT NULL, 'pago', vencimento < today(), 'vencido',
                    vencimento <= today() + 7, 'vence_7d', vencimento <= today() + 30, 'vence_30d', 'futuro') AS faixa,
            toUInt32(count()) AS qtd_titulos,
            sum(valor) AS valor_total, sum(valor_pago) AS valor_pago_total,
            sum(valor) - sum(valor_pago) AS valor_em_aberto,
            now64(6) AS published_at
        FROM src
        GROUP BY id_empresa, id_filial, tipo_titulo, faixa
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("finance_overview_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_dashboard_home_stg(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        """Dashboard home from slim tables."""
        t0 = time.time()
        kf = self._slim_keys_filter(data_keys, "c")
        kf_i = self._slim_keys_filter(data_keys, "i")
        sql = f"""
        INSERT INTO {self.mart_rt_db}.dashboard_home_rt
        SELECT
            base.id_empresa, base.id_filial, base.data_key,
            toDate(toString(base.data_key), '%Y%m%d') AS dt,
            base.faturamento,
            if(base.qtd_vendas > 0, base.faturamento / base.qtd_vendas, 0) AS ticket_medio,
            base.qtd_vendas, base.qtd_clientes,
            coalesce(cancel_agg.qtd_cancelamentos, 0) AS qtd_cancelamentos,
            coalesce(cancel_agg.valor_cancelado, 0) AS valor_cancelado,
            now64(6) AS published_at
        FROM (
            SELECT
                c.id_empresa, c.id_filial, c.data_key,
                sum(i.total) AS faturamento,
                toUInt32(uniqExact(c.id_comprovante)) AS qtd_vendas,
                toUInt32(uniqExactIf(c.id_cliente, c.id_cliente > 0)) AS qtd_clientes
            FROM {self.current_db}.stg_comprovantes_slim AS c
            INNER JOIN {self.current_db}.stg_itenscomprovantes_slim AS i
                ON c.id_empresa = i.id_empresa AND c.id_filial = i.id_filial
                AND c.id_db = i.id_db AND c.id_comprovante = i.id_comprovante
            WHERE {kf} AND c.is_deleted = 0 AND i.is_deleted = 0
              AND c.cancelado = 0 AND i.cfop >= 5000 AND {kf_i}
            GROUP BY c.id_empresa, c.id_filial, c.data_key
        ) AS base
        LEFT JOIN (
            SELECT c.id_empresa, c.id_filial, c.data_key,
                   toUInt32(count()) AS qtd_cancelamentos,
                   sum(c.valor_total) AS valor_cancelado
            FROM {self.current_db}.stg_comprovantes_slim AS c
            WHERE {kf} AND c.is_deleted = 0 AND c.cancelado = 1
            GROUP BY c.id_empresa, c.id_filial, c.data_key
        ) AS cancel_agg
            ON base.id_empresa = cancel_agg.id_empresa
           AND base.id_filial = cancel_agg.id_filial
           AND base.data_key = cancel_agg.data_key
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("dashboard_home_rt", 0, int((time.time() - t0) * 1000))

    # ================================================================
    # DW-ORIGIN MART QUERIES (already typed, no payload)
    # ================================================================

    def _refresh_sales_daily_dw(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_daily_rt
        SELECT
            base.id_empresa, base.id_filial, base.data_key,
            toDate(toString(base.data_key), '%Y%m%d') AS dt,
            base.faturamento,
            if(base.qtd_vendas > 0, base.faturamento / base.qtd_vendas, 0) AS ticket_medio,
            base.qtd_vendas, base.qtd_itens,
            coalesce(cancel.qtd_canceladas, 0) AS qtd_canceladas,
            coalesce(cancel.valor_cancelado, 0) AS valor_cancelado,
            base.desconto_total, base.custo_total, base.margem_total,
            now64(6) AS published_at
        FROM (
            SELECT v.id_empresa, v.id_filial, v.data_key,
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
            WHERE v.data_key IN ({keys_str}) AND v.is_deleted = 0
              AND vi.is_deleted = 0 AND v.cancelado = 0 AND coalesce(vi.cfop, 0) >= 5000
            GROUP BY v.id_empresa, v.id_filial, v.data_key
        ) AS base
        LEFT JOIN (
            SELECT id_empresa, id_filial, data_key,
                   toUInt32(count()) AS qtd_canceladas,
                   sum(coalesce(valor_total, 0)) AS valor_cancelado
            FROM {self.current_db}.fact_comprovante FINAL
            WHERE data_key IN ({keys_str}) AND is_deleted = 0 AND cancelado = 1
            GROUP BY id_empresa, id_filial, data_key
        ) AS cancel ON base.id_empresa = cancel.id_empresa
           AND base.id_filial = cancel.id_filial AND base.data_key = cancel.data_key
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("sales_daily_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_sales_hourly_dw(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_hourly_rt
        SELECT v.id_empresa, v.id_filial, v.data_key,
            toDate(toString(v.data_key), '%Y%m%d') AS dt,
            toUInt8(toHour(coalesce(v.data, vi.ingested_at))) AS hora,
            sum(coalesce(vi.total, 0)) AS faturamento,
            toUInt32(uniqExactIf(v.id_comprovante, v.id_comprovante IS NOT NULL)) AS qtd_vendas,
            toUInt32(count()) AS qtd_itens, now64(6) AS published_at
        FROM {self.current_db}.fact_venda_item AS vi FINAL
        INNER JOIN {self.current_db}.fact_venda AS v FINAL
            ON v.id_empresa = vi.id_empresa AND v.id_filial = vi.id_filial
            AND v.id_db = vi.id_db AND v.id_movprodutos = vi.id_movprodutos
        WHERE v.data_key IN ({keys_str}) AND v.is_deleted = 0
          AND vi.is_deleted = 0 AND v.cancelado = 0 AND coalesce(vi.cfop, 0) >= 5000
        GROUP BY v.id_empresa, v.id_filial, v.data_key, hora
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("sales_hourly_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_sales_products_dw(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_products_rt
        SELECT vi.id_empresa, vi.id_filial, vi.data_key,
            toDate(toString(vi.data_key), '%Y%m%d') AS dt,
            vi.id_produto, coalesce(p.nome, '') AS nome_produto,
            vi.id_grupo_produto, coalesce(g.nome, '') AS nome_grupo,
            sum(coalesce(vi.qtd, 0)) AS qtd, sum(coalesce(vi.total, 0)) AS faturamento,
            sum(coalesce(vi.custo_total, 0)) AS custo_total,
            sum(coalesce(vi.margem, 0)) AS margem, now64(6) AS published_at
        FROM {self.current_db}.fact_venda_item AS vi FINAL
        INNER JOIN {self.current_db}.fact_venda AS v FINAL
            ON v.id_empresa = vi.id_empresa AND v.id_filial = vi.id_filial
            AND v.id_db = vi.id_db AND v.id_movprodutos = vi.id_movprodutos
        LEFT JOIN {self.current_db}.dim_produto AS p FINAL
            ON vi.id_empresa = p.id_empresa AND vi.id_filial = p.id_filial AND vi.id_produto = p.id_produto
        LEFT JOIN {self.current_db}.dim_grupo_produto AS g FINAL
            ON vi.id_empresa = g.id_empresa AND vi.id_filial = g.id_filial AND vi.id_grupo_produto = g.id_grupo_produto
        WHERE vi.data_key IN ({keys_str}) AND vi.is_deleted = 0
          AND v.is_deleted = 0 AND v.cancelado = 0 AND coalesce(vi.cfop, 0) >= 5000
        GROUP BY vi.id_empresa, vi.id_filial, vi.data_key, vi.id_produto, p.nome, vi.id_grupo_produto, g.nome
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("sales_products_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_sales_groups_dw(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.sales_groups_rt
        SELECT vi.id_empresa, vi.id_filial, vi.data_key,
            toDate(toString(vi.data_key), '%Y%m%d') AS dt,
            coalesce(vi.id_grupo_produto, 0) AS id_grupo_produto,
            coalesce(g.nome, '') AS nome_grupo, count() AS qtd_itens,
            sum(coalesce(vi.total, 0)) AS faturamento,
            sum(coalesce(vi.custo_total, 0)) AS custo_total,
            sum(coalesce(vi.margem, 0)) AS margem, now64(6) AS published_at
        FROM {self.current_db}.fact_venda_item AS vi FINAL
        INNER JOIN {self.current_db}.fact_venda AS v FINAL
            ON v.id_empresa = vi.id_empresa AND v.id_filial = vi.id_filial
            AND v.id_db = vi.id_db AND v.id_movprodutos = vi.id_movprodutos
        LEFT JOIN {self.current_db}.dim_grupo_produto AS g FINAL
            ON vi.id_empresa = g.id_empresa AND vi.id_filial = g.id_filial
            AND coalesce(vi.id_grupo_produto, 0) = g.id_grupo_produto
        WHERE vi.data_key IN ({keys_str}) AND vi.is_deleted = 0
          AND v.is_deleted = 0 AND v.cancelado = 0 AND coalesce(vi.cfop, 0) >= 5000
        GROUP BY vi.id_empresa, vi.id_filial, vi.data_key, vi.id_grupo_produto, g.nome
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("sales_groups_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_payments_by_type_dw(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.payments_by_type_rt
        SELECT p.id_empresa, p.id_filial, p.data_key,
            toDate(toString(p.data_key), '%Y%m%d') AS dt, p.tipo_forma,
            coalesce(m.label, concat('Forma ', toString(p.tipo_forma))) AS label,
            coalesce(m.category, 'Outros') AS category,
            sum(p.valor) AS valor_total, count() AS qtd_transacoes, now64(6) AS published_at
        FROM {self.current_db}.fact_pagamento_comprovante AS p FINAL
        LEFT JOIN {self.current_db}.payment_type_map AS m FINAL
            ON p.tipo_forma = m.tipo_forma AND m.id_empresa = p.id_empresa
        WHERE p.data_key IN ({keys_str}) AND p.is_deleted = 0
        GROUP BY p.id_empresa, p.id_filial, p.data_key, p.tipo_forma, m.label, m.category
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("payments_by_type_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_cash_overview_dw(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.cash_overview_rt
        SELECT ct.id_empresa, ct.id_filial, ct.id_turno, ct.id_usuario,
            coalesce(u.nome, '') AS nome_operador,
            ct.abertura_ts, ct.fechamento_ts, ct.data_key_abertura, ct.is_aberto,
            coalesce(vendas.faturamento, 0) AS faturamento_turno,
            coalesce(vendas.qtd, 0) AS qtd_vendas_turno, now64(6) AS published_at
        FROM {self.current_db}.fact_caixa_turno AS ct FINAL
        LEFT JOIN {self.current_db}.dim_usuario_caixa AS u FINAL
            ON ct.id_empresa = u.id_empresa AND ct.id_filial = u.id_filial AND ct.id_usuario = u.id_usuario
        LEFT JOIN (
            SELECT id_empresa, id_filial, id_turno,
                   sumIf(total_venda, cancelado = 0) AS faturamento,
                   toUInt32(countIf(cancelado = 0)) AS qtd
            FROM {self.current_db}.fact_venda FINAL WHERE is_deleted = 0
            GROUP BY id_empresa, id_filial, id_turno
        ) AS vendas ON ct.id_empresa = vendas.id_empresa AND ct.id_filial = vendas.id_filial
            AND ct.id_turno = vendas.id_turno
        WHERE ct.is_deleted = 0
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("cash_overview_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_fraud_daily_dw(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.fraud_daily_rt
        SELECT r.id_empresa, r.id_filial, r.data_key,
            toDate(toString(r.data_key), '%Y%m%d') AS dt, r.event_type,
            count() AS qtd_eventos, sum(r.impacto_estimado) AS impacto_total,
            avg(r.score_risco) AS score_medio, now64(6) AS published_at
        FROM {self.current_db}.fact_risco_evento AS r FINAL
        WHERE r.data_key IN ({keys_str}) AND r.is_deleted = 0
        GROUP BY r.id_empresa, r.id_filial, r.data_key, r.event_type
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("fraud_daily_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_risk_recent_events_dw(self, client: Any) -> MartRefreshResult:
        t0 = time.time()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.risk_recent_events_rt
        SELECT r.id, r.id_empresa, r.id_filial, r.data_key, r.event_type, r.source,
            r.id_usuario, coalesce(u.nome, '') AS nome_operador,
            r.id_funcionario, coalesce(f.nome, '') AS nome_funcionario,
            r.valor_total, r.impacto_estimado, r.score_risco, r.score_level, r.reasons,
            now64(6) AS published_at
        FROM {self.current_db}.fact_risco_evento AS r FINAL
        LEFT JOIN {self.current_db}.dim_usuario_caixa AS u FINAL
            ON r.id_empresa = u.id_empresa AND r.id_filial = u.id_filial AND r.id_usuario = u.id_usuario
        LEFT JOIN {self.current_db}.dim_funcionario AS f FINAL
            ON r.id_empresa = f.id_empresa AND r.id_filial = f.id_filial AND r.id_funcionario = f.id_funcionario
        WHERE r.is_deleted = 0 ORDER BY r.id DESC
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("risk_recent_events_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_finance_overview_dw(self, client: Any) -> MartRefreshResult:
        t0 = time.time()
        sql = f"""
        INSERT INTO {self.mart_rt_db}.finance_overview_rt
        SELECT f.id_empresa, f.id_filial, f.tipo_titulo,
            multiIf(f.data_pagamento IS NOT NULL, 'pago', f.vencimento < today(), 'vencido',
                    f.vencimento <= today() + 7, 'vence_7d', f.vencimento <= today() + 30, 'vence_30d', 'futuro') AS faixa,
            count() AS qtd_titulos, sum(coalesce(f.valor, 0)) AS valor_total,
            sum(coalesce(f.valor_pago, 0)) AS valor_pago_total,
            sum(coalesce(f.valor, 0)) - sum(coalesce(f.valor_pago, 0)) AS valor_em_aberto,
            now64(6) AS published_at
        FROM {self.current_db}.fact_financeiro AS f FINAL WHERE f.is_deleted = 0
        GROUP BY f.id_empresa, f.id_filial, f.tipo_titulo, faixa
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("finance_overview_rt", 0, int((time.time() - t0) * 1000))

    def _refresh_dashboard_home_dw(self, client: Any, data_keys: list[int]) -> MartRefreshResult:
        t0 = time.time()
        keys_str = ",".join(str(k) for k in data_keys)
        sql = f"""
        INSERT INTO {self.mart_rt_db}.dashboard_home_rt
        SELECT base.id_empresa, base.id_filial, base.data_key,
            toDate(toString(base.data_key), '%Y%m%d') AS dt, base.faturamento,
            if(base.qtd_vendas > 0, base.faturamento / base.qtd_vendas, 0) AS ticket_medio,
            base.qtd_vendas, base.qtd_clientes,
            coalesce(cancel.qtd_cancelamentos, 0) AS qtd_cancelamentos,
            coalesce(cancel.valor_cancelado, 0) AS valor_cancelado, now64(6) AS published_at
        FROM (
            SELECT v.id_empresa, v.id_filial, v.data_key,
                sum(coalesce(vi.total, 0)) AS faturamento,
                toUInt32(uniqExactIf(v.id_comprovante, v.id_comprovante IS NOT NULL)) AS qtd_vendas,
                toUInt32(uniqExactIf(v.id_cliente, v.id_cliente IS NOT NULL)) AS qtd_clientes
            FROM {self.current_db}.fact_venda AS v FINAL
            INNER JOIN {self.current_db}.fact_venda_item AS vi FINAL
                ON v.id_empresa = vi.id_empresa AND v.id_filial = vi.id_filial
                AND v.id_db = vi.id_db AND v.id_movprodutos = vi.id_movprodutos
            WHERE v.data_key IN ({keys_str}) AND v.is_deleted = 0
              AND vi.is_deleted = 0 AND v.cancelado = 0 AND coalesce(vi.cfop, 0) >= 5000
            GROUP BY v.id_empresa, v.id_filial, v.data_key
        ) AS base
        LEFT JOIN (
            SELECT id_empresa, id_filial, data_key,
                   toUInt32(count()) AS qtd_cancelamentos, sum(coalesce(valor_total, 0)) AS valor_cancelado
            FROM {self.current_db}.fact_comprovante FINAL
            WHERE data_key IN ({keys_str}) AND is_deleted = 0 AND cancelado = 1
            GROUP BY id_empresa, id_filial, data_key
        ) AS cancel ON base.id_empresa = cancel.id_empresa
           AND base.id_filial = cancel.id_filial AND base.data_key = cancel.data_key
        """
        client.command(sql, settings=_QUERY_SETTINGS)
        return MartRefreshResult("dashboard_home_rt", 0, int((time.time() - t0) * 1000))

    # ================================================================
    # UTILITY METHODS
    # ================================================================

    def _log_publications(self, client: Any, results: list[MartRefreshResult]) -> None:
        """Log successful publications to mart_publication_log."""
        from datetime import date as _date
        successful = [r for r in results if r.error is None]
        if not successful:
            return
        try:
            rows = []
            for r in successful:
                rows.append([
                    r.mart_name,
                    0,
                    _date(1970, 1, 2),
                    _date(2099, 12, 31),
                    r.rows_written or 0,
                    r.duration_ms or 0,
                ])
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
                ts.id_empresa, ts.table_name AS domain,
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
    """Parse row count from INSERT command result (often empty string)."""
    if result is None:
        return 0
    try:
        return int(result)
    except (ValueError, TypeError):
        return 0
