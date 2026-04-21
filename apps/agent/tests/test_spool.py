import tempfile
import unittest

from agent_bkp.spool import SpoolQueue


class TestSpoolQueue(unittest.TestCase):
    def test_enqueue_and_remove(self):
        with tempfile.TemporaryDirectory() as td:
            queue = SpoolQueue(td)
            item = queue.enqueue(dataset="contasreceber", payload=b'{"a":1}\n', gzip_enabled=False)

            pending = list(queue.pending())
            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0].dataset, "contasreceber")
            self.assertFalse(pending[0].gzip_enabled)

            queue.remove(item)
            self.assertEqual(list(queue.pending()), [])

    def test_enqueue_gzip_filename(self):
        with tempfile.TemporaryDirectory() as td:
            queue = SpoolQueue(td)
            queue.enqueue(dataset="financeiro", payload=b"abc", gzip_enabled=True)
            pending = list(queue.pending())
            self.assertEqual(len(pending), 1)
            self.assertTrue(pending[0].gzip_enabled)
            self.assertEqual(pending[0].dataset, "financeiro")


if __name__ == "__main__":
    unittest.main()
