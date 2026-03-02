from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")


def retry_with_backoff(
    fn: Callable[[], T],
    max_retries: int,
    base_sleep_seconds: float = 1.0,
    on_retry: Callable[[int, Exception], None] | None = None,
) -> T:
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as exc:  # noqa: PERF203
            if attempt >= max_retries:
                raise
            attempt += 1
            if on_retry:
                on_retry(attempt, exc)
            time.sleep(base_sleep_seconds * (2 ** (attempt - 1)))
