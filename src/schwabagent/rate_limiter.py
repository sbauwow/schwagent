"""API rate limiter — sliding window throttle for Schwab API calls.

Schwab's standard rate limit is ~120 requests per minute. This module
tracks call timestamps and blocks/warns when approaching the limit.
"""
from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)

_DEFAULT_MAX_CALLS = 120
_DEFAULT_WINDOW_SECONDS = 60.0
_WARN_THRESHOLD = 0.80  # warn at 80% utilization


class RateLimiter:
    """Thread-safe sliding window rate limiter."""

    def __init__(self, max_calls: int = _DEFAULT_MAX_CALLS, window: float = _DEFAULT_WINDOW_SECONDS):
        self.max_calls = max_calls
        self.window = window
        self._calls: list[float] = []
        self._lock = threading.Lock()
        self._total_calls = 0
        self._total_throttled = 0

    def acquire(self, block: bool = True, timeout: float = 30.0) -> bool:
        """Acquire a rate limit slot. Returns True if acquired.

        If block=True, waits up to timeout seconds for a slot to free up.
        If block=False, returns False immediately when at limit.
        """
        deadline = time.monotonic() + timeout

        while True:
            with self._lock:
                self._prune()

                if len(self._calls) < self.max_calls:
                    self._calls.append(time.monotonic())
                    self._total_calls += 1

                    # Warn at threshold
                    utilization = len(self._calls) / self.max_calls
                    if utilization >= _WARN_THRESHOLD:
                        logger.warning(
                            "Rate limit utilization: %d/%d (%.0f%%)",
                            len(self._calls), self.max_calls, utilization * 100,
                        )
                    return True

            if not block:
                self._total_throttled += 1
                return False

            # Wait for oldest call to expire
            with self._lock:
                if self._calls:
                    wait = self._calls[0] + self.window - time.monotonic()
                else:
                    wait = 0.1

            if time.monotonic() >= deadline:
                self._total_throttled += 1
                logger.error("Rate limiter timed out after %.0fs", timeout)
                return False

            sleep_time = min(max(wait, 0.05), 1.0)
            time.sleep(sleep_time)

    def _prune(self) -> None:
        """Remove calls outside the window."""
        cutoff = time.monotonic() - self.window
        self._calls = [t for t in self._calls if t > cutoff]

    @property
    def current_usage(self) -> int:
        with self._lock:
            self._prune()
            return len(self._calls)

    @property
    def utilization(self) -> float:
        return self.current_usage / self.max_calls

    def stats(self) -> dict:
        with self._lock:
            self._prune()
            return {
                "current": len(self._calls),
                "max": self.max_calls,
                "window_seconds": self.window,
                "utilization_pct": round(len(self._calls) / self.max_calls * 100, 1),
                "total_calls": self._total_calls,
                "total_throttled": self._total_throttled,
            }
