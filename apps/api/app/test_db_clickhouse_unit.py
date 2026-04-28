from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import patch


try:
    import clickhouse_connect  # noqa: F401
except ModuleNotFoundError:
    fake_clickhouse = types.ModuleType("clickhouse_connect")
    fake_client_module = types.SimpleNamespace(Client=object)
    fake_clickhouse.driver = types.SimpleNamespace(client=fake_client_module)
    fake_clickhouse.get_client = lambda **_kwargs: None
    sys.modules["clickhouse_connect"] = fake_clickhouse

from app import db_clickhouse


class _Result:
    def __init__(self, rows, columns=None):
        self.result_rows = rows
        self.column_names = columns


class _Client:
    def __init__(self, result=None):
        self.result = result or _Result([], [])
        self.queries = []
        self.inserts = []

    def query(self, query, parameters=None):
        self.queries.append((query, parameters))
        return self.result

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, data, column_names))


class ClickHouseDbUnitTest(unittest.TestCase):
    def tearDown(self) -> None:
        db_clickhouse._pool = None

    def test_get_client_uses_supported_clickhouse_connect_timeout_args(self) -> None:
        client = object()

        with patch.object(db_clickhouse.clickhouse_connect, "get_client", return_value=client) as get_client:
            self.assertIs(db_clickhouse._get_client(), client)

        kwargs = get_client.call_args.kwargs
        self.assertEqual(kwargs["connect_timeout"], 10)
        self.assertEqual(kwargs["send_receive_timeout"], 30)
        self.assertNotIn("read_timeout", kwargs)
        self.assertNotIn("write_timeout", kwargs)

    def test_query_dict_returns_real_dicts_from_tuple_rows(self) -> None:
        client = _Client(_Result([(1, "Posto A"), (2, "Posto B")], ["id_filial", "nome"]))

        with patch.object(db_clickhouse, "_get_client", return_value=client):
            rows = db_clickhouse.query_dict(
                "SELECT id_filial, nome FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa = {id_empresa:Int32}",
                {"id_empresa": 7},
                tenant_id=7,
            )

        self.assertEqual(rows, [{"id_filial": 1, "nome": "Posto A"}, {"id_filial": 2, "nome": "Posto B"}])
        self.assertEqual(client.queries[0][1], {"id_empresa": 7})

    def test_query_dict_preserves_dict_rows(self) -> None:
        client = _Client(_Result([{"id_empresa": 7, "total": 42}], None))

        with patch.object(db_clickhouse, "_get_client", return_value=client):
            self.assertEqual(db_clickhouse.query_dict("SELECT 1"), [{"id_empresa": 7, "total": 42}])

    def test_query_dict_rejects_tuple_rows_without_columns(self) -> None:
        client = _Client(_Result([(1,)], []))

        with patch.object(db_clickhouse, "_get_client", return_value=client):
            with self.assertRaises(RuntimeError):
                db_clickhouse.query_dict("SELECT 1")

    def test_insert_batch_sends_column_names_and_rejects_unsafe_table(self) -> None:
        client = _Client()

        with patch.object(db_clickhouse, "_get_client", return_value=client):
            inserted = db_clickhouse.insert_batch(
                "torqmind_mart.agg_vendas_diaria",
                [{"id_empresa": 7, "data_key": 20260428}, {"id_empresa": 8, "data_key": 20260427}],
                order_by=["id_empresa"],
            )

        self.assertEqual(inserted, 2)
        self.assertEqual(client.inserts[0][0], "torqmind_mart.agg_vendas_diaria")
        self.assertEqual(client.inserts[0][1], [[7, 20260428], [8, 20260427]])
        self.assertEqual(client.inserts[0][2], ["id_empresa", "data_key"])

        with self.assertRaises(ValueError):
            db_clickhouse.insert_batch("torqmind_mart.agg;DROP", [{"id_empresa": 7}])


if __name__ == "__main__":
    unittest.main()
