from __future__ import annotations

import sys
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Lock
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
        self.closed = False

    def query(self, query, parameters=None):
        self.queries.append((query, parameters))
        return self.result

    def insert(self, table, data, column_names=None):
        self.inserts.append((table, data, column_names))

    def close(self):
        self.closed = True


class _ConcurrentClient(_Client):
    def __init__(self, client_id, barrier, active_lock, active_ids):
        super().__init__(_Result([(client_id,)], ["client_id"]))
        self.client_id = client_id
        self.barrier = barrier
        self.active_lock = active_lock
        self.active_ids = active_ids

    def query(self, query, parameters=None):
        self.barrier.wait(timeout=5)
        with self.active_lock:
            if self.client_id in self.active_ids:
                raise AssertionError("same ClickHouse client used concurrently")
            self.active_ids.add(self.client_id)
        try:
            return super().query(query, parameters)
        finally:
            with self.active_lock:
                self.active_ids.remove(self.client_id)


class ClickHouseDbUnitTest(unittest.TestCase):
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
        self.assertTrue(client.closed)

    def test_query_dict_preserves_dict_rows(self) -> None:
        client = _Client(_Result([{"id_empresa": 7, "total": 42}], None))

        with patch.object(db_clickhouse, "_get_client", return_value=client):
            self.assertEqual(db_clickhouse.query_dict("SELECT 1"), [{"id_empresa": 7, "total": 42}])
        self.assertTrue(client.closed)

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
        self.assertTrue(client.closed)

        with self.assertRaises(ValueError):
            db_clickhouse.insert_batch("torqmind_mart.agg;DROP", [{"id_empresa": 7}])

    def test_parallel_query_dict_uses_distinct_clients(self) -> None:
        workers = 8
        barrier = Barrier(workers)
        active_lock = Lock()
        active_ids = set()
        created_clients = []
        created_lock = Lock()

        def make_client(**_kwargs):
            with created_lock:
                client = _ConcurrentClient(len(created_clients) + 1, barrier, active_lock, active_ids)
                created_clients.append(client)
                return client

        def run_query(_index):
            return db_clickhouse.query_dict(
                "SELECT client_id FROM torqmind_mart.agg_vendas_diaria WHERE id_empresa = {id_empresa:Int32}",
                {"id_empresa": 7},
                tenant_id=7,
            )

        with patch.object(db_clickhouse.clickhouse_connect, "get_client", side_effect=make_client):
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(executor.map(run_query, range(workers)))

        self.assertEqual(len(created_clients), workers)
        self.assertEqual(sorted(row[0]["client_id"] for row in results), list(range(1, workers + 1)))
        self.assertTrue(all(client.closed for client in created_clients))


if __name__ == "__main__":
    unittest.main()
