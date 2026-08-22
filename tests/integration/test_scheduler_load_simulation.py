from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.persistence.models import CoveragePolicy, PollBatch, PollSchedule, Token
from pump_research.scheduling.policy import AdaptivePollingPolicy, CoverageClass
from pump_research.scheduling.scheduler import AdaptiveScheduler, PollOutcome

NOW = datetime(2026, 8, 17, 12, 0, tzinfo=UTC)


@dataclass(slots=True)
class FakeClock:
    """Deterministic shared wall clock for concurrent scheduler tests."""

    current: datetime = NOW

    def now(self) -> datetime:
        return self.current

    def advance(self, **duration: float) -> None:
        self.current += timedelta(**duration)


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://researcher:password@localhost/pump_research",
    }
    values.update(overrides)
    return Settings.model_validate(values)


async def _seed_mapped_schedules(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    policy: AdaptivePollingPolicy,
    count: int,
    coverage: CoverageClass,
    now: datetime,
) -> set[uuid.UUID]:
    """Bulk seed already-mapped operational projections, never research facts."""
    token_ids: set[uuid.UUID] = set()
    interval = policy.interval_for_coverage(coverage)
    assert interval is not None
    tokens: list[Token] = []
    schedules: list[PollSchedule] = []
    async with session_factory() as session, session.begin():
        session.add(
            CoveragePolicy(
                policy_sha256=policy.coverage_sha256,
                policy_snapshot=policy.coverage_snapshot,
            )
        )
        await session.flush()
        for index in range(count):
            token_id = uuid.uuid5(
                uuid.NAMESPACE_URL, f"v2-load:{coverage.value}:{index}"
            )
            token_ids.add(token_id)
            admitted_at = now - timedelta(minutes=3)
            tokens.append(
                Token(
                    id=token_id,
                    chain="solana",
                    address=f"v2-load-{coverage.value.lower()}-{index:05d}",
                    first_discovered_at=admitted_at,
                )
            )
            schedules.append(
                PollSchedule(
                    token_id=token_id,
                    lifecycle_state="NEW",
                    state_decided_at=admitted_at,
                    admitted_at=admitted_at,
                    coverage_class=coverage.value,
                    coverage_decided_at=now,
                    coverage_next_transition_at=now + timedelta(minutes=7),
                    coverage_policy_sha256=policy.coverage_sha256,
                    priority=policy.priority_for_coverage(coverage),
                    next_due_at=now,
                    attempt_count=0,
                    control_scan_count=0,
                    target_interval_seconds=int(interval.total_seconds()),
                    effective_interval_seconds=int(interval.total_seconds()),
                    configuration_sha256=policy.sha256,
                    configuration_snapshot=policy.snapshot,
                    updated_at=now,
                )
            )
        session.add_all(tokens)
        await session.flush()
        session.add_all(schedules)
    return token_ids


def _rolling_request_totals(rows: list[tuple[datetime, int]]) -> list[int]:
    return [
        sum(
            capacity
            for other_claimed_at, capacity in rows
            if claimed_at - timedelta(minutes=1) < other_claimed_at <= claimed_at
        )
        for claimed_at, _ in rows
    ]


@pytest.mark.integration
async def test_four_workers_cannot_overshoot_shared_scheduled_budget(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The transaction advisory lock makes the rolling request budget global."""
    clock = FakeClock()
    settings = _settings(
        dex_screener_requests_per_minute=6,
        scheduler_capacity_headroom_ratio=0.20,
        scheduler_reserved_requests_per_minute=0,
        scheduler_batch_size=1,
        scheduler_max_in_flight_batches=20,
    )
    seed = AdaptiveScheduler(session_factory, settings, clock=clock)
    await _seed_mapped_schedules(
        session_factory,
        policy=seed.policy,
        count=20,
        coverage=CoverageClass.EARLY,
        now=clock.now(),
    )
    workers = [AdaptiveScheduler(session_factory, settings, clock=clock) for _ in range(4)]
    claims = [
        claim
        for claim in await asyncio.gather(
            *(worker.claim_next_batch() for worker in workers for _ in range(3))
        )
        if claim is not None
    ]
    assert len(claims) == 4
    assert len({member.token_id for claim in claims for member in claim.members}) == 4
    for claim in claims:
        await seed.complete_batch(batch_id=claim.batch_id, outcome=PollOutcome.EMPTY)
    assert await seed.claim_next_batch() is None

    async with session_factory() as session:
        rows = list(
            (
                await session.execute(
                    select(PollBatch.claimed_at, PollBatch.reserved_request_capacity)
                )
            ).tuples()
        )
    assert max(_rolling_request_totals(rows)) <= 4


@pytest.mark.integration
@pytest.mark.parametrize("request_ceiling", (240, 40))
async def test_sustained_four_worker_normal_and_degraded_claims_are_bounded(
    session_factory: async_sessionmaker[AsyncSession],
    request_ceiling: int,
) -> None:
    """Exercise many capacity windows in both NORMAL and DEGRADED modes."""
    clock = FakeClock()
    reserve = 14 if request_ceiling == 240 else 2
    settings = _settings(
        dex_screener_requests_per_minute=request_ceiling,
        scheduler_reserved_requests_per_minute=reserve,
        scheduler_batch_size=30,
        scheduler_max_in_flight_batches=4,
    )
    seed = AdaptiveScheduler(session_factory, settings, clock=clock)
    await _seed_mapped_schedules(
        session_factory,
        policy=seed.policy,
        count=1_200,
        coverage=CoverageClass.EARLY,
        now=clock.now(),
    )
    workers = [AdaptiveScheduler(session_factory, settings, clock=clock) for _ in range(4)]
    for _ in range(20):
        claims = [
            claim
            for claim in await asyncio.gather(
                *(worker.claim_next_batch() for worker in workers)
            )
            if claim is not None
        ]
        claimed_ids = [member.token_id for claim in claims for member in claim.members]
        assert len(claimed_ids) == len(set(claimed_ids))
        for claim in claims:
            await seed.complete_batch(batch_id=claim.batch_id, outcome=PollOutcome.EMPTY)
        clock.advance(seconds=30)

    async with session_factory() as session:
        rows = list(
            (
                await session.execute(
                    select(PollBatch.claimed_at, PollBatch.reserved_request_capacity)
                )
            ).tuples()
        )
        decision_modes = set(
            (
                await session.execute(
                    select(PollBatch.capacity_decision_id).where(
                        PollBatch.capacity_decision_id.is_not(None)
                    )
                )
            ).scalars()
        )
        batch_count = int(
            await session.scalar(select(func.count()).select_from(PollBatch)) or 0
        )
    safe = int(request_ceiling * 0.8) - reserve
    assert rows
    assert max(_rolling_request_totals(rows)) <= safe
    assert batch_count == len(rows)
    assert decision_modes
