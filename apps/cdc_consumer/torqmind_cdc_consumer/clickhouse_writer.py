"""ClickHouse writer for CDC events - handles raw, current and ops inserts."""

from __future__ import annotations

import time
from typing import Any

import clickhouse_connect
import orjson

from .config import settings
from .debezium import DebeziumEvent
from .logging import get_logger
from .mappings import TableMapping, get_mapping

logger = get_logger("clickhouse_writer")


class ClickHouseWriter:
    """Writes CDC events to ClickHouse raw, current and ops layers."""

    def __init__(self) -> None:
        self._raw_buffer: list[tuple] = []
        self._current_buffers: dict[str, list[tuple]] = {}
        self._state_updates: dict[str, dict[str, Any]] = {}
        self._error_buffer: list[tuple] = []
        self._last_flush = time.monotonic()

    def _get_client(self) -> clickhouse_connect.driver.Client:
        """Create a new ClickHouse client (no sharing between flushes)."""
        return clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )

    def process_event(self, event: DebeziumEvent) -> None:
        """Process a single Debezium event into write buffers."""
        # Always write to raw
        if settings.enable_raw_writes:
            self._buffer_raw(event)

        # Write to current if we have a mapping
        if settings.enable_current_writes:
            self._buffer_current(event)

        # Track table state
        if settings.enable_ops_writes:
            self._update_table_state(event)

    def _buffer_raw(self, event: DebeziumEvent) -> None:
        """Add event to raw buffer."""
        self._raw_buffer.append((
            event.topic,
            event.partition,
            event.offset,
            event.op,
            event.source_ts_ms,
            event.table_schema,
            event.table_name,
            event.id_empresa,
            event.data_key,
            orjson.dumps(event.key).decode() if event.key else "{}",
            orjson.dumps(event.before).decode() if event.before else "{}",
            orjson.dumps(event.after).decode() if event.after else "{}",
        ))

    def _buffer_current(self, event: DebeziumEvent) -> None:
        """Add event to current state buffer."""
        mapping = get_mapping(event.table_schema, event.table_name)
        if not mapping:
            return

        is_delete = event.op == "d"
        record = event.before if is_delete else event.after
        if not record:
            return

        table_key = f"{mapping.ch_database}.{mapping.ch_table}"
        if table_key not in self._current_buffers:
            self._current_buffers[table_key] = []

        row = self._build_current_row(mapping, record, event, is_delete)
        if row:
            self._current_buffers[table_key].append(row)

    def _build_current_row(
        self,
        mapping: TableMapping,
        record: dict[str, Any],
        event: DebeziumEvent,
        is_delete: bool,
    ) -> tuple | None:
        """Build a tuple for current table insert."""
        values: list[Any] = []
        for col in mapping.columns:
            val = record.get(col)
            # Handle boolean→int conversion for ClickHouse UInt8
            if col in ("cancelado", "is_aberto", "active"):
                val = 1 if val else 0
            # Handle jsonb fields stored as strings
            if col in ("payload", "reasons") and isinstance(val, dict):
                val = orjson.dumps(val).decode()
            values.append(val)

        # Append meta columns: is_deleted, source_ts_ms
        values.append(1 if is_delete else 0)
        values.append(event.source_ts_ms)

        return tuple(values)

    def _update_table_state(self, event: DebeziumEvent) -> None:
        """Track latest state per table for ops reporting."""
        key = f"{event.table_schema}.{event.table_name}.{event.id_empresa}"
        existing = self._state_updates.get(key)
        if not existing or event.source_ts_ms > existing["last_source_ts_ms"]:
            self._state_updates[key] = {
                "table_schema": event.table_schema,
                "table_name": event.table_name,
                "id_empresa": event.id_empresa,
                "last_source_ts_ms": event.source_ts_ms,
                "last_op": event.op,
            }

    def record_error(
        self,
        topic: str,
        partition: int,
        offset: int,
        table_schema: str,
        table_name: str,
        error_type: str,
        error_message: str,
        event_payload: str = "",
    ) -> None:
        """Buffer an error for writing to torqmind_ops.cdc_errors."""
        self._error_buffer.append((
            settings.cdc_consumer_group,
            topic,
            partition,
            offset,
            table_schema,
            table_name,
            error_type,
            error_message[:2000],  # Truncate long messages
            event_payload[:5000] if event_payload else "",
        ))

    def should_flush(self) -> bool:
        """Check if buffers should be flushed."""
        total_buffered = len(self._raw_buffer) + sum(
            len(b) for b in self._current_buffers.values()
        )
        elapsed = time.monotonic() - self._last_flush
        return (
            total_buffered >= settings.cdc_batch_size
            or elapsed >= settings.cdc_flush_interval_seconds
        )

    def flush(self) -> int:
        """Flush all buffers to ClickHouse. Returns total rows written."""
        total = 0
        client = self._get_client()
        try:
            total += self._flush_raw(client)
            total += self._flush_current(client)
            total += self._flush_state(client)
            total += self._flush_errors(client)
        finally:
            client.close()

        self._last_flush = time.monotonic()
        return total

    def _flush_raw(self, client: clickhouse_connect.driver.Client) -> int:
        """Flush raw event buffer."""
        if not self._raw_buffer:
            return 0

        rows = self._raw_buffer
        self._raw_buffer = []

        columns = [
            "topic", "kafka_partition", "kafka_offset", "op",
            "source_ts_ms", "table_schema", "table_name",
            "id_empresa", "data_key", "key_json", "before_json", "after_json",
        ]

        try:
            client.insert(
                f"{settings.clickhouse_raw_db}.cdc_events",
                rows,
                column_names=columns,
            )
            logger.debug("flushed_raw", rows=len(rows))
            return len(rows)
        except Exception as e:
            logger.error("flush_raw_failed", error=str(e), rows=len(rows))
            # Put rows back for retry
            self._raw_buffer = rows + self._raw_buffer
            raise

    def _flush_current(self, client: clickhouse_connect.driver.Client) -> int:
        """Flush current state buffers."""
        if not self._current_buffers:
            return 0

        buffers = self._current_buffers
        self._current_buffers = {}
        total = 0

        for table_key, rows in buffers.items():
            if not rows:
                continue

            # Get mapping to determine columns
            db, table = table_key.split(".", 1)
            mapping = self._find_mapping_by_ch_table(db, table)
            if not mapping:
                continue

            columns = list(mapping.columns) + ["is_deleted", "source_ts_ms"]

            try:
                client.insert(
                    table_key,
                    rows,
                    column_names=columns,
                )
                total += len(rows)
                logger.debug("flushed_current", table=table_key, rows=len(rows))
            except Exception as e:
                logger.error("flush_current_failed", table=table_key, error=str(e), rows=len(rows))
                # Re-buffer for retry
                if table_key not in self._current_buffers:
                    self._current_buffers[table_key] = []
                self._current_buffers[table_key] = rows + self._current_buffers[table_key]
                raise

        return total

    def _flush_state(self, client: clickhouse_connect.driver.Client) -> int:
        """Flush table state updates to ops."""
        if not self._state_updates:
            return 0

        updates = self._state_updates
        self._state_updates = {}

        rows = []
        for state in updates.values():
            rows.append((
                state["table_schema"],
                state["table_name"],
                state["id_empresa"],
                state["last_source_ts_ms"],
                state["last_op"],
                1,  # events_total increment placeholder
            ))

        columns = [
            "table_schema", "table_name", "id_empresa",
            "last_source_ts_ms", "last_op", "events_total",
        ]

        try:
            client.insert(
                f"{settings.clickhouse_ops_db}.cdc_table_state",
                rows,
                column_names=columns,
            )
            return len(rows)
        except Exception as e:
            logger.warning("flush_state_failed", error=str(e))
            return 0

    def _flush_errors(self, client: clickhouse_connect.driver.Client) -> int:
        """Flush error buffer."""
        if not self._error_buffer:
            return 0

        rows = self._error_buffer
        self._error_buffer = []

        columns = [
            "consumer_group", "topic", "kafka_partition", "kafka_offset",
            "table_schema", "table_name", "error_type", "error_message",
            "event_payload",
        ]

        try:
            client.insert(
                f"{settings.clickhouse_ops_db}.cdc_errors",
                rows,
                column_names=columns,
            )
            return len(rows)
        except Exception as e:
            logger.warning("flush_errors_failed", error=str(e))
            return 0

    def _find_mapping_by_ch_table(self, db: str, table: str) -> TableMapping | None:
        """Find mapping by ClickHouse target database.table."""
        from .mappings import TABLE_MAPPINGS
        for m in TABLE_MAPPINGS.values():
            if m.ch_database == db and m.ch_table == table:
                return m
        return None

    @property
    def buffer_size(self) -> int:
        """Total events buffered."""
        return len(self._raw_buffer) + sum(
            len(b) for b in self._current_buffers.values()
        )
