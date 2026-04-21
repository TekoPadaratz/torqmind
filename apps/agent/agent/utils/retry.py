from __future__ import annotations

import random
import time
from typing import Callable, TypeVar

T = TypeVar("T")


class RetryableError(Exception):
    def __init__(self, message: str, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.retry_after_seconds = retry_after_seconds


def retry_with_backoff(
    fn: Callable[[], T],
    max_retries: int,
    base_sleep_seconds: float = 1.0,
    max_sleep_seconds: float | None = None,
    jitter_seconds: float = 0.0,
    retry_exceptions: tuple[type[Exception], ...] = (RetryableError,),
    on_retry: Callable[[int, Exception], None] | None = None,
) -> T:
    attempt = 0
    while True:
        try:
            return fn()
        except retry_exceptions as exc:  # noqa: PERF203
            if attempt >= max_retries:
                raise
            attempt += 1
            if on_retry:
                on_retry(attempt, exc)
            retry_after = getattr(exc, "retry_after_seconds", None)
            sleep_seconds = float(retry_after) if retry_after else (base_sleep_seconds * (2 ** (attempt - 1)))
            if max_sleep_seconds is not None:
                sleep_seconds = min(float(max_sleep_seconds), sleep_seconds)
            if jitter_seconds > 0:
                sleep_seconds += random.uniform(0, float(jitter_seconds))
            time.sleep(sleep_seconds)
