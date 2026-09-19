from __future__ import annotations

import random
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import TypeVar

T = TypeVar("T")


class ApiRequestBudget:
    """Thread-safe rolling request budget used immediately before HTTP calls."""

    def __init__(
        self,
        calls_per_minute: int,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        on_wait: Callable[[float], None] | None = None,
    ) -> None:
        if calls_per_minute <= 0:
            raise ValueError("API request budget must be positive.")
        self._limit = calls_per_minute
        self._monotonic = monotonic
        self._sleep = sleep
        self._on_wait = on_wait or (lambda _: None)
        self._timestamps: deque[float] = deque()
        self._lock = Lock()
        self._total = 0

    @property
    def total_calls(self) -> int:
        with self._lock:
            return self._total

    def acquire(self) -> None:
        while True:
            now = self._monotonic()
            with self._lock:
                while self._timestamps and now - self._timestamps[0] >= 60.0:
                    self._timestamps.popleft()
                if len(self._timestamps) < self._limit:
                    self._timestamps.append(now)
                    self._total += 1
                    return
                wait = max(0.001, 60.0 - (now - self._timestamps[0]))
            self._on_wait(wait)
            self._sleep(wait)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    attempts: int
    base_delay: float
    max_delay: float
    jitter: float


def retry_call(
    operation: Callable[[], T],
    *,
    policy: RetryPolicy,
    sleep: Callable[[float], None] = time.sleep,
    random_value: Callable[[], float] = random.random,
    on_error: Callable[[Exception, int], None] | None = None,
) -> T:
    callback = on_error or (lambda _error, _attempt: None)
    last_error: Exception | None = None
    for attempt in range(1, policy.attempts + 1):
        try:
            return operation()
        except Exception as error:
            last_error = error
            callback(error, attempt)
            if attempt == policy.attempts:
                break
            exponential = min(policy.max_delay, policy.base_delay * 2 ** (attempt - 1))
            multiplier = 1.0 + policy.jitter * (2.0 * random_value() - 1.0)
            sleep(max(0.0, exponential * multiplier))
    assert last_error is not None
    raise last_error


def is_rate_limit_error(error: Exception) -> bool:
    message = str(error).lower()
    return any(token in message for token in ("429", "10006", "rate limit"))
