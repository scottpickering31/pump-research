from __future__ import annotations

import asyncio
import json
import math
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.persistence.models import (
    PollBatch,
    PollSchedule,
    PollScheduleDecision,
    Token,
)
from pump_research.scheduling.policy import AdaptivePollingPolicy, LifecycleState
from pump_research.scheduling.scheduler import AdaptiveScheduler, PollOutcome

NOW = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)
TOKEN_COUNT = 3_007


@dataclass(slots=True)
class FakeClock:
    """Deterministic shared wall clock for scheduler load simulation."""

    current: datetime = NOW

    def now(self) -> datetime:
        return self.current

    def advance(self, **duration: float) -> None:
        self.current += timedelta(**duration)


@dataclass(frozen=True, slots=True)
class SchedulerLoadMeasurements:
    """Measured results from a network-free scheduler simulation."""

    simulated_tokens: int
    batches: int
    mean_batch_occupancy_pct: float
    max_requests_per_minute: int
    overdue_observations: int
    p50_lateness_ms: int
    p95_lateness_ms: int
    p99_lateness_ms: int


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": (
            "postgresql+asyncpg://researcher:password@localhost:5433/pump_research"
        ),
    }
    values.update(overrides)
    return Settings.model_validate(values)


async def _seed_due_schedules(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    policy: AdaptivePollingPolicy,
    token_count: int,
    due_at: datetime,
) -> set[uuid.UUID]:
    """Bulk-load auditable due schedules without invoking any market-data client."""
    states = tuple(LifecycleState)
    token_ids: set[uuid.UUID] = set()
    tokens: list[Token] = []
    schedules: list[PollSchedule] = []
    decisions: list[PollScheduleDecision] = []
    for index in range(token_count):
        token_id = uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research-load-{index}")
        state = states[index % len(states)]
        decided_at = due_at - policy.interval_for(state)
        token_ids.add(token_id)
        tokens.append(
            Token(
                id=token_id,
                chain="solana",
                address=f"load-simulation-{index:05d}",
                first_discovered_at=decided_at,
            )
        )
        schedules.append(
            PollSchedule(
                token_id=token_id,
                lifecycle_state=state.value,
                state_decided_at=decided_at,
                priority=policy.priority_for(state),
                next_due_at=due_at,
                attempt_count=0,
                configuration_sha256=policy.sha256,
                configuration_snapshot=policy.snapshot,
                updated_at=decided_at,
            )
        )
        decisions.append(
            PollScheduleDecision(
                token_id=token_id,
                idempotency_key=f"load-simulation-{token_id}",
                previous_state=None,
                new_state=state.value,
                previous_due_at=None,
                new_due_at=due_at,
                decided_at=decided_at,
                reason_code="load_simulation_seed",
                configuration_sha256=policy.sha256,
                configuration_snapshot=policy.snapshot,
            )
        )
    async with session_factory() as session, session.begin():
        session.add_all(tokens)
        await session.flush()
        session.add_all(schedules)
        session.add_all(decisions)
    return token_ids


async def _overdue_count(
    session_factory: async_sessionmaker[AsyncSession], *, now: datetime
) -> int:
    async with session_factory() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(PollSchedule)
            .where(PollSchedule.next_due_at <= now)
        )
    return int(count or 0)


def _nearest_rank(values: list[int], percentile: float) -> int:
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _rolling_request_totals(
    batches: list[tuple[datetime, int]],
) -> list[int]:
    return [
        sum(
            capacity
            for other_claimed_at, capacity in batches
            if claimed_at - timedelta(minutes=1) < other_claimed_at <= claimed_at
        )
        for claimed_at, _ in batches
    ]


@pytest.mark.integration
async def test_concurrent_claims_cannot_overshoot_shared_api_ceiling(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Attack the budget check with concurrent workers and completed work."""
    clock = FakeClock()
    settings = _settings(
        dex_screener_requests_per_minute=6,
        dex_screener_max_attempts=3,
        scheduler_batch_size=1,
        scheduler_max_in_flight_batches=20,
    )
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    await _seed_due_schedules(
        session_factory,
        policy=scheduler.policy,
        token_count=20,
        due_at=clock.now(),
    )

    workers = [
        AdaptiveScheduler(session_factory, settings, clock=clock) for _ in range(12)
    ]
    concurrent_results = await asyncio.gather(
        *(worker.claim_next_batch() for worker in workers)
    )
    claims = [claim for claim in concurrent_results if claim is not None]

    assert len(claims) == 2
    for claim in claims:
        await scheduler.complete_batch(batch_id=claim.batch_id, outcome=PollOutcome.EMPTY)
    assert await scheduler.claim_next_batch() is None

    clock.advance(seconds=59, milliseconds=999)
    assert await scheduler.claim_next_batch() is None
    clock.advance(milliseconds=1)
    assert await scheduler.claim_next_batch() is not None

    async with session_factory() as session:
        rows = list(
            (
                await session.execute(
                    select(PollBatch.claimed_at, PollBatch.reserved_request_capacity)
                    .order_by(PollBatch.claimed_at, PollBatch.id)
                )
            ).tuples()
        )
    assert max(_rolling_request_totals(rows)) <= settings.dex_screener_requests_per_minute


@pytest.mark.integration
async def test_thousands_of_tokens_measure_scheduler_capacity_without_network_calls(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Run two saturated budget windows using only PostgreSQL and a fake clock."""
    clock = FakeClock()
    settings = _settings(
        dex_screener_requests_per_minute=240,
        dex_screener_max_attempts=3,
        scheduler_batch_size=30,
        scheduler_max_in_flight_batches=4,
    )
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    initial_token_ids = await _seed_due_schedules(
        session_factory,
        policy=scheduler.policy,
        token_count=TOKEN_COUNT,
        due_at=clock.now(),
    )
    claimed_initial_ids: set[uuid.UUID] = set()
    occupancies: list[float] = []
    lateness_values: list[int] = []

    async def consume_budget_window() -> None:
        while batch := await scheduler.claim_next_batch():
            occupancies.append(len(batch.members) / scheduler.policy.batch_size)
            lateness_values.extend(member.claim_lateness_ms for member in batch.members)
            claimed_initial_ids.update(
                member.token_id
                for member in batch.members
                if member.token_id in initial_token_ids
            )
            await scheduler.complete_batch(
                batch_id=batch.batch_id,
                outcome=PollOutcome.EMPTY,
            )

    await consume_budget_window()
    assert await _overdue_count(session_factory, now=clock.now()) == 607

    clock.advance(minutes=1)
    await consume_budget_window()

    async with session_factory() as session:
        batch_rows = list(
            (
                await session.execute(
                    select(PollBatch.claimed_at, PollBatch.reserved_request_capacity)
                    .order_by(PollBatch.claimed_at, PollBatch.id)
                )
            ).tuples()
        )
    rolling_request_totals = _rolling_request_totals(batch_rows)
    measurements = SchedulerLoadMeasurements(
        simulated_tokens=TOKEN_COUNT,
        batches=len(batch_rows),
        mean_batch_occupancy_pct=round(
            100 * sum(occupancies) / len(occupancies), 2
        ),
        max_requests_per_minute=max(rolling_request_totals),
        overdue_observations=await _overdue_count(
            session_factory,
            now=clock.now(),
        ),
        p50_lateness_ms=_nearest_rank(lateness_values, 0.50),
        p95_lateness_ms=_nearest_rank(lateness_values, 0.95),
        p99_lateness_ms=_nearest_rank(lateness_values, 0.99),
    )

    assert claimed_initial_ids == initial_token_ids
    assert measurements.batches == 160
    assert measurements.mean_batch_occupancy_pct == 100.0
    assert measurements.max_requests_per_minute == 240
    assert measurements.max_requests_per_minute <= settings.dex_screener_requests_per_minute
    assert measurements.overdue_observations == 607
    assert measurements.p50_lateness_ms == 0
    assert measurements.p50_lateness_ms <= measurements.p95_lateness_ms
    assert measurements.p95_lateness_ms <= measurements.p99_lateness_ms
    print("scheduler_load_measurements=" + json.dumps(asdict(measurements), sort_keys=True))
