from datetime import datetime
from decimal import Decimal
import json
import unittest

from agent.utils.ndjson import to_ndjson_bytes, to_ndjson_lines


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

    def test_integral_float_and_decimal_become_int(self):
        lines = to_ndjson_lines(
            [{"ID_CONTASPAGAR": 282384.0, "VALORBAIXA": 103205.84, "N": Decimal("12.0")}]
        )
        obj = json.loads(lines[0])
        self.assertEqual(obj["ID_CONTASPAGAR"], 282384)
        self.assertIsInstance(obj["ID_CONTASPAGAR"], int)
        self.assertEqual(obj["N"], 12)
        self.assertEqual(obj["VALORBAIXA"], 103205.84)


if __name__ == "__main__":
    unittest.main()
