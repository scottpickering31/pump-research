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


@pytest.mark.asyncio
async def test_rate_limiter_never_exceeds_configured_rolling_minute_ceiling() -> None:
    """Simulate two minutes of saturated demand without making HTTP requests."""
    current = 0.0
    request_started_at: list[float] = []

    def clock() -> float:
        return current

    async def advance_clock(seconds: float) -> None:
        nonlocal current
        current += seconds

    limiter = AsyncRateLimiter(240, clock=clock, sleep=advance_clock)
    for _ in range(480):
        await limiter.acquire()
        request_started_at.append(clock())

    rolling_counts = [
        sum(started_at - 60 < other <= started_at for other in request_started_at)
        for started_at in request_started_at
    ]
    assert max(rolling_counts) == 240
    assert request_started_at[-1] == pytest.approx(119.75)


def test_process_limiter_is_shared_for_the_same_rate_budget() -> None:
    assert get_process_rate_limiter(240) is get_process_rate_limiter(240)
