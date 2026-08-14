"""Async request pacing for external API clients."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable


class AsyncRateLimiter:
    """Space requests evenly at a configured maximum rate across coroutines."""

    def __init__(
        self,
        requests_per_minute: int,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if requests_per_minute <= 0:
            msg = "requests_per_minute must be positive"
            raise ValueError(msg)
        self._interval_seconds = 60 / requests_per_minute
        self._clock = clock
        self._sleep = sleep
        self._next_permitted_at = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> float:
        """Wait until one request slot is available and return the wait duration."""
        async with self._lock:
            now = self._clock()
            wait_seconds = max(0.0, self._next_permitted_at - now)
            self._next_permitted_at = max(now, self._next_permitted_at) + self._interval_seconds

        if wait_seconds > 0:
            await self._sleep(wait_seconds)
        return wait_seconds


_process_limiters: dict[int, AsyncRateLimiter] = {}


def get_process_rate_limiter(requests_per_minute: int) -> AsyncRateLimiter:
    """Return the shared in-process limiter for one provider rate budget."""
    limiter = _process_limiters.get(requests_per_minute)
    if limiter is None:
        limiter = AsyncRateLimiter(requests_per_minute)
        _process_limiters[requests_per_minute] = limiter
    return limiter
