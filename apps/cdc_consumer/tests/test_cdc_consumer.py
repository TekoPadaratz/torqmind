"""Tests for TorqMind CDC Consumer - Debezium event parsing and processing."""

import json
import unittest

from torqmind_cdc_consumer.debezium import DebeziumEvent, parse_debezium_event
from torqmind_cdc_consumer.mappings import TABLE_MAPPINGS, get_mapping
from torqmind_cdc_consumer.clickhouse_writer import ClickHouseWriter
from torqmind_cdc_consumer.state import ConsumerState


class TestDebeziumParsing(unittest.TestCase):
    """Test Debezium event parsing."""

    def _make_event(self, op="c", schema="dw", table="fact_venda", after=None, before=None):
        """Helper to create a Debezium event payload."""
        default_record = {
            "id_empresa": 1,
            "id_filial": 14458,
            "id_db": 1,
            "id_movprodutos": 1000,
            "data_key": 20260430,
            "total_venda": "150.00",
            "cancelado": False,
        }
        if op == "d":
            # Delete: before has data, after is None
            effective_before = before if before is not None else default_record
            effective_after = after  # should be None for delete
        else:
            effective_before = before
            effective_after = after if after is not None else default_record

        value = {
            "payload": {
                "op": op,
                "before": effective_before,
                "after": effective_after,
                "source": {
                    "schema": schema,
                    "table": table,
                    "ts_ms": 1714500000000,
                },
            }
        }
        key = {"payload": {"id_empresa": 1, "id_filial": 14458, "id_db": 1, "id_movprodutos": 1000}}
        return json.dumps(value).encode(), json.dumps(key).encode()

    def test_parse_insert_event(self):
        value_bytes, key_bytes = self._make_event(op="c")
        event = parse_debezium_event("torqmind.dw.fact_venda", 0, 100, key_bytes, value_bytes)
        self.assertIsNotNone(event)
        self.assertEqual(event.op, "c")
        self.assertEqual(event.table_schema, "dw")
        self.assertEqual(event.table_name, "fact_venda")
        self.assertEqual(event.id_empresa, 1)
        self.assertEqual(event.data_key, 20260430)
        self.assertEqual(event.partition, 0)
        self.assertEqual(event.offset, 100)

    def test_parse_update_event(self):
        before = {"id_empresa": 1, "id_filial": 14458, "id_db": 1, "id_movprodutos": 1000, "total_venda": "100.00"}
        after = {"id_empresa": 1, "id_filial": 14458, "id_db": 1, "id_movprodutos": 1000, "total_venda": "200.00", "data_key": 20260430}
        value_bytes, key_bytes = self._make_event(op="u", before=before, after=after)
        event = parse_debezium_event("torqmind.dw.fact_venda", 0, 101, key_bytes, value_bytes)
        self.assertIsNotNone(event)
        self.assertEqual(event.op, "u")
        self.assertEqual(event.after["total_venda"], "200.00")

    def test_parse_delete_event(self):
        before = {"id_empresa": 1, "id_filial": 14458, "id_db": 1, "id_movprodutos": 1000, "data_key": 20260430}
        value_bytes, key_bytes = self._make_event(op="d", before=before, after=None)
        event = parse_debezium_event("torqmind.dw.fact_venda", 0, 102, key_bytes, value_bytes)
        self.assertIsNotNone(event)
        self.assertEqual(event.op, "d")
        self.assertIsNone(event.after)
        self.assertEqual(event.before["id_movprodutos"], 1000)

    def test_parse_snapshot_event(self):
        value_bytes, key_bytes = self._make_event(op="r")
        event = parse_debezium_event("torqmind.dw.fact_venda", 0, 0, key_bytes, value_bytes)
        self.assertIsNotNone(event)
        self.assertEqual(event.op, "r")

    def test_tombstone_returns_none(self):
        event = parse_debezium_event("torqmind.dw.fact_venda", 0, 50, b'{"payload":{}}', None)
        self.assertIsNone(event)

    def test_invalid_json_returns_none(self):
        event = parse_debezium_event("torqmind.dw.fact_venda", 0, 50, None, b"not json")
        self.assertIsNone(event)

    def test_no_op_returns_none(self):
        value = json.dumps({"payload": {"source": {}}}).encode()
        event = parse_debezium_event("torqmind.dw.fact_venda", 0, 50, None, value)
        self.assertIsNone(event)

    def test_extract_id_empresa_from_before_on_delete(self):
        before = {"id_empresa": 5, "id_filial": 100, "data_key": 20260101}
        value_bytes, _ = self._make_event(op="d", before=before, after=None)
        event = parse_debezium_event("test", 0, 0, None, value_bytes)
        self.assertEqual(event.id_empresa, 5)
        self.assertEqual(event.data_key, 20260101)


class TestMappings(unittest.TestCase):
    """Test table mappings."""

    def test_all_tables_have_mappings(self):
        expected_tables = [
            "dw.fact_venda", "dw.fact_venda_item", "dw.fact_pagamento_comprovante",
            "dw.fact_caixa_turno", "dw.fact_comprovante", "dw.fact_financeiro",
            "dw.fact_risco_evento", "dw.dim_filial", "dw.dim_produto",
            "dw.dim_grupo_produto", "dw.dim_funcionario", "dw.dim_usuario_caixa",
            "dw.dim_local_venda", "dw.dim_cliente", "app.payment_type_map",
        ]
        for table in expected_tables:
            self.assertIn(table, TABLE_MAPPINGS, f"Missing mapping for {table}")

    def test_get_mapping_found(self):
        m = get_mapping("dw", "fact_venda")
        self.assertIsNotNone(m)
        self.assertEqual(m.ch_table, "fact_venda")
        self.assertEqual(m.primary_key, ("id_empresa", "id_filial", "id_db", "id_movprodutos"))

    def test_get_mapping_not_found(self):
        m = get_mapping("public", "nonexistent")
        self.assertIsNone(m)

    def test_fact_venda_item_key(self):
        m = get_mapping("dw", "fact_venda_item")
        self.assertEqual(m.primary_key, ("id_empresa", "id_filial", "id_db", "id_movprodutos", "id_itensmovprodutos"))

    def test_payment_type_map_key(self):
        m = get_mapping("app", "payment_type_map")
        self.assertEqual(m.primary_key, ("id",))


class TestClickHouseWriter(unittest.TestCase):
    """Test ClickHouse writer buffering logic (no actual CH connection)."""

    def _make_event(self, op="c", table_schema="dw", table_name="fact_venda"):
        return DebeziumEvent(
            topic="torqmind.dw.fact_venda",
            partition=0,
            offset=100,
            op=op,
            source_ts_ms=1714500000000,
            table_schema=table_schema,
            table_name=table_name,
            before=None if op != "d" else {
                "id_empresa": 1, "id_filial": 14458, "id_db": 1,
                "id_movprodutos": 1000, "data_key": 20260430, "total_venda": "150.00", "cancelado": False,
            },
            after={
                "id_empresa": 1, "id_filial": 14458, "id_db": 1,
                "id_movprodutos": 1000, "data_key": 20260430,
                "id_usuario": None, "id_cliente": None, "id_comprovante": 50,
                "id_turno": 3, "saidas_entradas": 1, "total_venda": "150.00", "cancelado": False,
            } if op != "d" else None,
            key={"id_empresa": 1, "id_filial": 14458, "id_db": 1, "id_movprodutos": 1000},
            id_empresa=1,
            data_key=20260430,
        )

    def test_process_event_buffers_raw(self):
        writer = ClickHouseWriter()
        event = self._make_event()
        writer.process_event(event)
        self.assertEqual(len(writer._raw_buffer), 1)

    def test_process_event_buffers_current(self):
        writer = ClickHouseWriter()
        event = self._make_event()
        writer.process_event(event)
        self.assertIn("torqmind_current.fact_venda", writer._current_buffers)
        self.assertEqual(len(writer._current_buffers["torqmind_current.fact_venda"]), 1)

    def test_delete_marks_is_deleted(self):
        writer = ClickHouseWriter()
        event = self._make_event(op="d")
        writer.process_event(event)
        # Current buffer should have is_deleted=1
        rows = writer._current_buffers.get("torqmind_current.fact_venda", [])
        self.assertEqual(len(rows), 1)
        # is_deleted is second-to-last field
        self.assertEqual(rows[0][-2], 1)  # is_deleted
        self.assertEqual(rows[0][-1], 1714500000000)  # source_ts_ms

    def test_insert_marks_not_deleted(self):
        writer = ClickHouseWriter()
        event = self._make_event(op="c")
        writer.process_event(event)
        rows = writer._current_buffers["torqmind_current.fact_venda"]
        self.assertEqual(rows[0][-2], 0)  # is_deleted = 0

    def test_should_flush_by_batch_size(self):
        writer = ClickHouseWriter()
        # Override batch size for testing
        from torqmind_cdc_consumer.config import settings
        original = settings.cdc_batch_size
        # Each event creates 1 raw + 1 current = 2 buffered items
        # Set batch to 5 so 2 events (4 items) don't trigger but 3 (6 items) would
        settings.cdc_batch_size = 5
        try:
            writer.process_event(self._make_event())
            self.assertFalse(writer.should_flush())  # 2 < 5
            writer.process_event(self._make_event())
            self.assertFalse(writer.should_flush())  # 4 < 5
            writer.process_event(self._make_event())
            self.assertTrue(writer.should_flush())   # 6 >= 5
        finally:
            settings.cdc_batch_size = original

    def test_unknown_table_skips_current(self):
        writer = ClickHouseWriter()
        event = self._make_event(table_schema="public", table_name="unknown_table")
        writer.process_event(event)
        # Raw should have it, current should not
        self.assertEqual(len(writer._raw_buffer), 1)
        self.assertEqual(len(writer._current_buffers), 0)

    def test_record_error_buffered(self):
        writer = ClickHouseWriter()
        writer.record_error(
            topic="test", partition=0, offset=0,
            table_schema="dw", table_name="fact_venda",
            error_type="TEST_ERROR", error_message="something failed"
        )
        self.assertEqual(len(writer._error_buffer), 1)

    def test_state_tracking(self):
        writer = ClickHouseWriter()
        event = self._make_event()
        writer.process_event(event)
        key = "dw.fact_venda.1"
        self.assertIn(key, writer._state_updates)
        self.assertEqual(writer._state_updates[key]["last_source_ts_ms"], 1714500000000)


class TestConsumerState(unittest.TestCase):
    """Test consumer state tracking."""

    def test_record_offset(self):
        state = ConsumerState()
        state.record_offset("topic1", 0, 100)
        state.record_offset("topic1", 0, 50)  # Should not go backwards
        self.assertEqual(state.last_offsets["topic1:0"], 100)

    def test_increment_counters(self):
        state = ConsumerState()
        state.increment_processed()
        state.increment_processed()
        state.increment_errors()
        self.assertEqual(state.events_processed, 2)
        self.assertEqual(state.events_errors, 1)


class TestDDLAlignment(unittest.TestCase):
    """Verify that SQL DDL contains all columns the writer will insert."""

    @classmethod
    def setUpClass(cls):
        """Load the DDL files once."""
        import pathlib
        repo = pathlib.Path(__file__).resolve().parents[3]
        cls.raw_ddl = (repo / "sql/clickhouse/streaming/010_raw_events.sql").read_text()
        cls.current_ddl = (repo / "sql/clickhouse/streaming/020_current_tables.sql").read_text()
        cls.ops_ddl = (repo / "sql/clickhouse/streaming/030_ops_tables.sql").read_text()

    def test_raw_events_has_all_columns(self):
        """Raw DDL must have the columns the writer inserts."""
        raw_columns = [
            "topic", "kafka_partition", "kafka_offset", "op",
            "source_ts_ms", "table_schema", "table_name",
            "id_empresa", "data_key", "key_json", "before_json", "after_json",
        ]
        for col in raw_columns:
            self.assertIn(col, self.raw_ddl, f"Raw DDL missing column: {col}")
        # Also verify ingested_at with DateTime64(6)
        self.assertIn("ingested_at", self.raw_ddl)
        self.assertIn("DateTime64(6", self.raw_ddl)

    def test_raw_events_engine(self):
        """Raw table must use ReplacingMergeTree(ingested_at)."""
        self.assertIn("ReplacingMergeTree(ingested_at)", self.raw_ddl)
        self.assertIn("ORDER BY (topic, kafka_partition, kafka_offset)", self.raw_ddl)

    def test_current_tables_have_mapping_columns(self):
        """Each current table DDL must contain all columns from mappings + is_deleted + source_ts_ms."""
        for key, mapping in TABLE_MAPPINGS.items():
            with self.subTest(table=key):
                # Find the CREATE TABLE block for this table
                self.assertIn(f"torqmind_current.{mapping.ch_table}", self.current_ddl,
                              f"DDL missing table definition for {mapping.ch_table}")
                for col in mapping.columns:
                    self.assertIn(col, self.current_ddl,
                                  f"DDL for {mapping.ch_table} missing column: {col}")
                # Meta columns
                self.assertIn("is_deleted", self.current_ddl)
                self.assertIn("source_ts_ms", self.current_ddl)
                self.assertIn("ingested_at", self.current_ddl)

    def test_ops_state_has_writer_columns(self):
        """Ops cdc_table_state must have columns the writer inserts."""
        state_columns = [
            "table_schema", "table_name", "id_empresa",
            "last_source_ts_ms", "last_op", "events_total",
        ]
        for col in state_columns:
            self.assertIn(col, self.ops_ddl, f"Ops DDL missing state column: {col}")

    def test_ops_errors_has_writer_columns(self):
        """Ops cdc_errors must have columns the writer inserts."""
        error_columns = [
            "consumer_group", "topic", "kafka_partition", "kafka_offset",
            "table_schema", "table_name", "error_type", "error_message",
            "event_payload",
        ]
        for col in error_columns:
            self.assertIn(col, self.ops_ddl, f"Ops DDL missing error column: {col}")

    def test_debezium_parser_handles_no_schema_envelope(self):
        """With value.converter.schemas.enable=false, payload comes directly."""
        import json
        # Format WITHOUT schema wrapping (schemas.enable=false on value)
        value = {
            "op": "c",
            "before": None,
            "after": {"id_empresa": 1, "id_filial": 1, "id_db": 1, "id_movprodutos": 1, "data_key": 20260101},
            "source": {"schema": "dw", "table": "fact_venda", "ts_ms": 1714500000000},
        }
        # Format WITH schema wrapping on key (schemas.enable=true on key)
        key = {
            "schema": {"type": "struct", "fields": []},
            "payload": {"id_empresa": 1, "id_filial": 1, "id_db": 1, "id_movprodutos": 1},
        }
        event = parse_debezium_event(
            "torqmind.dw.fact_venda", 0, 200,
            json.dumps(key).encode(), json.dumps(value).encode(),
        )
        self.assertIsNotNone(event)
        self.assertEqual(event.op, "c")
        self.assertEqual(event.id_empresa, 1)
        self.assertEqual(event.key, {"id_empresa": 1, "id_filial": 1, "id_db": 1, "id_movprodutos": 1})


if __name__ == "__main__":
    unittest.main()
