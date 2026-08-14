from __future__ import annotations

import pytest

from pump_research.market_data.rate_limiter import AsyncRateLimiter, get_process_rate_limiter


@pytest.mark.asyncio
async def test_rate_limiter_spaces_consecutive_requests() -> None:
    sleeps: list[float] = []

    async def record_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    limiter = AsyncRateLimiter(60, clock=lambda: 100.0, sleep=record_sleep)

    assert await limiter.acquire() == 0.0
    assert await limiter.acquire() == 1.0
    assert sleeps == [1.0]


def test_process_limiter_is_shared_for_the_same_rate_budget() -> None:
    assert get_process_rate_limiter(240) is get_process_rate_limiter(240)
