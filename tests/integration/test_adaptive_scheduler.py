from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.persistence.models import (
    PollBatchMember,
    PollBatchOutcome,
    PollSchedule,
)
from pump_research.persistence.repositories import TokenRepository
from pump_research.scheduling.policy import LifecycleState
from pump_research.scheduling.scheduler import (
    AdaptiveScheduler,
    LostPollLeaseError,
    PollOutcome,
)

NOW = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)


@dataclass(slots=True)
class FakeClock:
    """Deterministic wall clock required by every scheduler test."""

    current: datetime = NOW

    def now(self) -> datetime:
        return self.current

    def advance(self, seconds: int) -> None:
        self.current += timedelta(seconds=seconds)


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": (
            "postgresql+asyncpg://researcher:password@localhost:5433/pump_research"
        ),
    }
    values.update(overrides)
    return Settings.model_validate(values)


async def _schedule_tokens(
    session_factory: async_sessionmaker[AsyncSession],
    scheduler: AdaptiveScheduler,
    specifications: list[tuple[str, str, LifecycleState, datetime]],
) -> dict[str, object]:
    token_repository = TokenRepository()
    tokens: dict[str, object] = {}
    async with session_factory() as session, session.begin():
        for address, chain, state, decided_at in specifications:
            token = await token_repository.get_or_create(
                session,
                chain=chain,
                address=address,
                first_discovered_at=decided_at,
            )
            await scheduler.set_lifecycle_state_in_session(
                session,
                token_id=token.id,
                state=state,
                decided_at=decided_at,
                reason_code="test_state",
            )
            tokens[address] = token
    return tokens


@pytest.mark.integration
@pytest.mark.parametrize(
    ("state", "interval_seconds"),
    [
        (LifecycleState.NEW, 5),
        (LifecycleState.ACTIVE, 5),
        (LifecycleState.WATCH, 15),
        (LifecycleState.FADING, 60),
        (LifecycleState.DORMANT, 15 * 60),
        (LifecycleState.RESURRECTED, 5),
    ],
)
async def test_configured_lifecycle_cadence(
    session_factory: async_sessionmaker[AsyncSession],
    state: LifecycleState,
    interval_seconds: int,
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [(f"cadence-{state.value}", "solana", state, clock.now())],
    )
    token = tokens[f"cadence-{state.value}"]

    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token.id)  # type: ignore[attr-defined]

    assert schedule is not None
    assert schedule.next_due_at == clock.now() + timedelta(seconds=interval_seconds)


@pytest.mark.integration
async def test_lifecycle_cadence_is_configurable(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(
        session_factory,
        _settings(scheduler_watch_interval_seconds=22),
        clock=clock,
    )
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [("configured-watch", "solana", LifecycleState.WATCH, clock.now())],
    )

    async with session_factory() as session:
        schedule = await session.get(PollSchedule, tokens["configured-watch"].id)  # type: ignore[attr-defined]

    assert schedule is not None
    assert schedule.next_due_at == clock.now() + timedelta(seconds=22)


@pytest.mark.integration
async def test_thirty_one_due_tokens_form_thirty_and_one_batches(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(
        session_factory,
        _settings(scheduler_max_in_flight_batches=2),
        clock=clock,
    )
    addresses = [f"batch-{index:02d}" for index in range(31)]
    await _schedule_tokens(
        session_factory,
        scheduler,
        [
            (address, "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=5))
            for address in addresses
        ],
    )

    first = await scheduler.claim_next_batch()
    second = await scheduler.claim_next_batch()

    assert first is not None
    assert second is not None
    assert len(first.members) == 30
    assert len(second.members) == 1
    assert set(first.token_addresses).isdisjoint(second.token_addresses)
    assert set(first.token_addresses) | set(second.token_addresses) == set(addresses)


@pytest.mark.integration
async def test_equal_due_times_use_lifecycle_priority(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(
        session_factory,
        _settings(scheduler_batch_size=2),
        clock=clock,
    )
    specifications = [
        (
            f"priority-{state.value}",
            "solana",
            state,
            clock.now() - scheduler.policy.interval_for(state),
        )
        for state in LifecycleState
    ]
    await _schedule_tokens(session_factory, scheduler, specifications)

    batch = await scheduler.claim_next_batch()

    assert batch is not None
    assert [member.lifecycle_state for member in batch.members] == [
        LifecycleState.RESURRECTED,
        LifecycleState.NEW,
    ]


@pytest.mark.integration
async def test_regression_older_dormant_work_is_not_starved_by_new_work(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A strict state-priority queue starves dormant work under sustained NEW arrivals."""
    clock = FakeClock()
    scheduler = AdaptiveScheduler(
        session_factory,
        _settings(scheduler_batch_size=1),
        clock=clock,
    )
    await _schedule_tokens(
        session_factory,
        scheduler,
        [
            (
                "overdue-dormant",
                "solana",
                LifecycleState.DORMANT,
                clock.now() - timedelta(seconds=901),
            ),
            (
                "just-due-new",
                "solana",
                LifecycleState.NEW,
                clock.now() - timedelta(seconds=5),
            ),
        ],
    )

    batch = await scheduler.claim_next_batch()

    assert batch is not None
    assert batch.token_addresses == ("overdue-dormant",)


@pytest.mark.integration
async def test_regression_same_state_reapplication_does_not_postpone_due_work(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Repeated same-state evidence must not keep moving a poll into the future."""
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [("same-state", "solana", LifecycleState.NEW, clock.now())],
    )
    token = tokens["same-state"]
    original_due = clock.now() + timedelta(seconds=5)
    clock.advance(4)

    await scheduler.set_lifecycle_state(
        token_id=token.id,  # type: ignore[attr-defined]
        state=LifecycleState.NEW,
        decided_at=clock.now(),
    )

    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token.id)  # type: ignore[attr-defined]
    assert schedule is not None
    assert schedule.next_due_at == original_due


@pytest.mark.integration
async def test_concurrent_workers_cannot_claim_the_same_token(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    settings = _settings(scheduler_batch_size=1, scheduler_max_in_flight_batches=2)
    first_worker = AdaptiveScheduler(session_factory, settings, clock=clock)
    second_worker = AdaptiveScheduler(session_factory, settings, clock=clock)
    await _schedule_tokens(
        session_factory,
        first_worker,
        [("race-token", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=5))],
    )

    results = await asyncio.gather(
        first_worker.claim_next_batch(),
        second_worker.claim_next_batch(),
    )

    claims = [result for result in results if result is not None]
    assert len(claims) == 1
    async with session_factory() as session:
        membership_count = await session.scalar(
            select(func.count()).select_from(PollBatchMember)
        )
    assert membership_count == 1


@pytest.mark.integration
async def test_regression_concurrent_initial_scheduling_is_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Two lifecycle workers must not race on the first projection insert."""
    clock = FakeClock()
    settings = _settings()
    first_worker = AdaptiveScheduler(session_factory, settings, clock=clock)
    second_worker = AdaptiveScheduler(session_factory, settings, clock=clock)
    token_repository = TokenRepository()
    async with session_factory() as session, session.begin():
        token = await token_repository.get_or_create(
            session,
            chain="solana",
            address="initial-schedule-race",
            first_discovered_at=clock.now(),
        )

    results = await asyncio.gather(
        first_worker.set_lifecycle_state(
            token_id=token.id,
            state=LifecycleState.NEW,
            decided_at=clock.now(),
        ),
        second_worker.set_lifecycle_state(
            token_id=token.id,
            state=LifecycleState.NEW,
            decided_at=clock.now(),
        ),
    )

    assert results[0].token_id == results[1].token_id == token.id
    async with session_factory() as session:
        schedule_count = await session.scalar(select(func.count()).select_from(PollSchedule))
    assert schedule_count == 1


@pytest.mark.integration
async def test_regression_in_flight_batches_are_bounded(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Repeated claims must not create an unbounded external-work queue."""
    clock = FakeClock()
    scheduler = AdaptiveScheduler(
        session_factory,
        _settings(scheduler_batch_size=1, scheduler_max_in_flight_batches=1),
        clock=clock,
    )
    await _schedule_tokens(
        session_factory,
        scheduler,
        [
            (address, "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=5))
            for address in ("bounded-one", "bounded-two")
        ],
    )

    assert await scheduler.claim_next_batch() is not None
    assert await scheduler.claim_next_batch() is None


@pytest.mark.integration
async def test_regression_batch_never_mixes_chains(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A DEX batch URL has one chain segment, so mixed-chain claims are invalid."""
    clock = FakeClock()
    scheduler = AdaptiveScheduler(
        session_factory,
        _settings(scheduler_max_in_flight_batches=2),
        clock=clock,
    )
    await _schedule_tokens(
        session_factory,
        scheduler,
        [
            ("solana-token", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=5)),
            ("other-token", "other-chain", LifecycleState.NEW, clock.now() - timedelta(seconds=5)),
        ],
    )

    first = await scheduler.claim_next_batch()
    second = await scheduler.claim_next_batch()

    assert first is not None
    assert second is not None
    assert len(first.members) == len(second.members) == 1
    assert first.chain != second.chain


@pytest.mark.integration
async def test_restart_reclaims_expired_pending_batch_from_postgres(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    settings = _settings(scheduler_batch_size=1, scheduler_lease_seconds=10)
    first_process = AdaptiveScheduler(session_factory, settings, clock=clock)
    await _schedule_tokens(
        session_factory,
        first_process,
        [("restart-token", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=5))],
    )
    abandoned = await first_process.claim_next_batch()
    assert abandoned is not None
    clock.advance(11)

    restarted_process = AdaptiveScheduler(session_factory, settings, clock=clock)
    recovered = await restarted_process.claim_next_batch()

    assert recovered is not None
    assert recovered.token_addresses == ("restart-token",)
    assert recovered.members[0].previous_batch_id == abandoned.batch_id
    with pytest.raises(LostPollLeaseError):
        await first_process.complete_batch(
            batch_id=abandoned.batch_id,
            outcome=PollOutcome.SUCCEEDED,
        )


@pytest.mark.integration
async def test_observation_lateness_is_measured_from_original_due_time(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [("late-token", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=5))],
    )
    token = tokens["late-token"]
    clock.advance(12)
    batch = await scheduler.claim_next_batch()
    assert batch is not None
    assert batch.members[0].claim_lateness_ms == 12_000
    clock.advance(3)

    completion = await scheduler.complete_batch(
        batch_id=batch.batch_id,
        outcome=PollOutcome.SUCCEEDED,
    )

    assert completion.observation_lateness_min_ms == 15_000
    assert completion.observation_lateness_max_ms == 15_000
    assert completion.observation_lateness_mean_ms == Decimal("15000")
    async with session_factory() as session:
        persisted = await session.get(PollBatchOutcome, batch.batch_id)
        schedule = await session.get(PollSchedule, token.id)  # type: ignore[attr-defined]
    assert persisted is not None
    assert persisted.observation_lateness_max_ms == 15_000
    assert schedule is not None
    assert schedule.next_due_at == clock.now() + timedelta(seconds=5)


@pytest.mark.integration
async def test_regression_request_budget_reserves_retry_capacity(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Concurrent workers reserve worst-case retries before starting a DEX batch."""
    clock = FakeClock()
    scheduler = AdaptiveScheduler(
        session_factory,
        _settings(
            dex_screener_requests_per_minute=6,
            dex_screener_max_attempts=3,
            scheduler_batch_size=1,
            scheduler_max_in_flight_batches=10,
        ),
        clock=clock,
    )
    await _schedule_tokens(
        session_factory,
        scheduler,
        [
            (f"budget-{index}", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=5))
            for index in range(3)
        ],
    )

    assert await scheduler.claim_next_batch() is not None
    assert await scheduler.claim_next_batch() is not None
    assert await scheduler.claim_next_batch() is None


@pytest.mark.integration
async def test_completion_uses_state_changed_while_batch_was_in_flight(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [("transition-race", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=5))],
    )
    token = tokens["transition-race"]
    batch = await scheduler.claim_next_batch()
    assert batch is not None
    clock.advance(1)
    await scheduler.set_lifecycle_state(
        token_id=token.id,  # type: ignore[attr-defined]
        state=LifecycleState.DORMANT,
    )
    clock.advance(1)

    await scheduler.complete_batch(
        batch_id=batch.batch_id,
        outcome=PollOutcome.SUCCEEDED,
    )

    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token.id)  # type: ignore[attr-defined]
    assert schedule is not None
    assert schedule.lifecycle_state == "DORMANT"
    assert schedule.next_due_at == clock.now() + timedelta(minutes=15)
