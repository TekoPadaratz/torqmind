from datetime import datetime
import json
import unittest

from agent_bkp.utils.ndjson import to_ndjson_bytes, to_ndjson_lines


class TestNDJSON(unittest.TestCase):
    def test_ndjson_lines(self):
        rows = [{"ID": 1, "DATA": datetime(2026, 1, 1, 10, 30, 0)}, {"ID": 2, "VAL": "x"}]
        lines = to_ndjson_lines(rows)
        self.assertEqual(len(lines), 2)
        obj0 = json.loads(lines[0])
        self.assertIn("DATA", obj0)

    def test_ndjson_bytes_endline(self):
        payload = to_ndjson_bytes([{"A": 1}, {"B": 2}])
        self.assertTrue(payload.endswith(b"\n"))
        self.assertEqual(len(payload.splitlines()), 2)


if __name__ == "__main__":
    unittest.main()
