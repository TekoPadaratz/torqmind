import unittest
from unittest.mock import patch

from agent.utils.retry import RetryableError, retry_with_backoff


class TestRetry(unittest.TestCase):
    def test_retry_after_is_honored(self):
        calls = {"n": 0}

        def _fn():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RetryableError("rate limit", retry_after_seconds=0.25)
            return "ok"

        with patch("time.sleep") as sleep_mock:
            got = retry_with_backoff(_fn, max_retries=2)
        self.assertEqual(got, "ok")
        sleep_mock.assert_called_once_with(0.25)


if __name__ == "__main__":
    unittest.main()
