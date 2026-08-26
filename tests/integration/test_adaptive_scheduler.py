from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.monitoring.status import read_collector_status
from pump_research.persistence.models import (
    CoverageDecision,
    LifecycleEvent,
    PollBatch,
    PollBatchMember,
    PollBatchOutcome,
    PollSchedule,
    PollScheduleDecision,
    SchedulerCapacityDecision,
    SchedulerPolicy,
    Token,
)
from pump_research.persistence.repositories import TokenRepository
from pump_research.scheduling.capacity import plan_capacity
from pump_research.scheduling.locks import lock_schedule_token_fk_path
from pump_research.scheduling.policy import CoverageClass, LifecycleState
from pump_research.scheduling.scheduler import (
    AdaptiveScheduler,
    CoverageReconstructionError,
    CoverageTransitionProgressError,
    LostPollLeaseError,
    PollOutcome,
    SchedulerCapacityDecisionIntegrityError,
    _capacity_idempotency_key,
    _legacy_admissions_statement,
    _lock_completion_schedules,
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
        "database_url": ("postgresql+asyncpg://researcher:password@localhost:5433/pump_research"),
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
                admitted_at=decided_at,
                reason_code="test_state",
            )
            tokens[address] = token
    return tokens


async def _schedule_gate_lock_count(
    session_factory: async_sessionmaker[AsyncSession],
) -> int:
    async with session_factory() as session:
        return int(
            await session.scalar(
                text(
                    """
                    SELECT count(*)
                    FROM pg_locks
                    WHERE locktype = 'advisory'
                      AND database = (
                          SELECT oid FROM pg_database WHERE datname = current_database()
                      )
                      AND classid = 1
                      AND objid = 3133933867
                      AND objsubid = 1
                    """
                )
            )
            or 0
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("state", "interval_seconds", "coverage"),
    [
        (LifecycleState.NEW, 15, CoverageClass.INITIAL),
        (LifecycleState.ACTIVE, 5, CoverageClass.PROTECTED_ACTIVE),
        (LifecycleState.WATCH, 15, CoverageClass.PROTECTED_WATCH),
        (LifecycleState.FADING, 120, CoverageClass.FADING_TAIL),
        (LifecycleState.DORMANT, None, CoverageClass.RETIRED_CONTROL),
        (LifecycleState.RESURRECTED, 5, CoverageClass.PROTECTED_RESURRECTED),
    ],
)
async def test_configured_lifecycle_cadence(
    session_factory: async_sessionmaker[AsyncSession],
    state: LifecycleState,
    interval_seconds: int | None,
    coverage: CoverageClass,
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
    assert schedule.coverage_class == coverage.value
    assert schedule.next_due_at == (
        None
        if interval_seconds is None
        else clock.now() + timedelta(seconds=interval_seconds)
    )


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
async def test_new_uses_fifteen_seconds_only_for_its_first_two_minutes(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [("new-age-window", "solana", LifecycleState.NEW, clock.now())],
    )
    clock.advance(15)
    first = await scheduler.claim_next_batch()
    assert first is not None
    await scheduler.complete_batch(batch_id=first.batch_id, outcome=PollOutcome.EMPTY)

    clock.advance(106)
    mature = await scheduler.claim_next_batch()
    assert mature is not None
    await scheduler.complete_batch(batch_id=mature.batch_id, outcome=PollOutcome.EMPTY)

    async with session_factory() as session:
        schedule = await session.get(PollSchedule, tokens["new-age-window"].id)  # type: ignore[attr-defined]
    assert schedule is not None
    assert schedule.target_interval_seconds == 30
    assert schedule.effective_interval_seconds == 30
    assert schedule.next_due_at == clock.now() + timedelta(seconds=30)


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
            (address, "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=15))
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
        _settings(scheduler_batch_size=6),
        clock=clock,
    )
    specifications = [
        (f"priority-{state.value}", "solana", state, clock.now() - timedelta(minutes=1))
        for state in LifecycleState
    ]
    await _schedule_tokens(session_factory, scheduler, specifications)

    batch = await scheduler.claim_next_batch()

    assert batch is not None
    states = [member.lifecycle_state for member in batch.members]
    assert set(states[:2]) == {LifecycleState.ACTIVE, LifecycleState.RESURRECTED}
    assert states[2:] == [LifecycleState.WATCH, LifecycleState.NEW]


@pytest.mark.integration
async def test_lifecycle_priority_precedes_cross_tier_lateness(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Capacity-sized tiers make strict priority safe while protecting NEW."""
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
                clock.now() - timedelta(seconds=15),
            ),
        ],
    )

    batch = await scheduler.claim_next_batch()

    assert batch is not None
    assert batch.token_addresses == ("just-due-new",)


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
    original_due = clock.now() + timedelta(seconds=15)
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
        [("race-token", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=15))],
    )

    results = await asyncio.gather(
        first_worker.claim_next_batch(),
        second_worker.claim_next_batch(),
    )

    claims = [result for result in results if result is not None]
    assert len(claims) == 1
    async with session_factory() as session:
        membership_count = await session.scalar(select(func.count()).select_from(PollBatchMember))
    assert membership_count == 1


@pytest.mark.integration
async def test_retired_control_rotation_is_fixed_budget_fair_and_restart_safe(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    settings = _settings(scheduler_control_scan_tokens_per_minute=2)
    seed = AdaptiveScheduler(session_factory, settings, clock=clock)
    await _schedule_tokens(
        session_factory,
        seed,
        [
            (
                f"retired-{index}",
                "solana",
                LifecycleState.DORMANT,
                clock.now() - timedelta(days=30, seconds=index),
            )
            for index in range(5)
        ],
    )

    claimed: list[str] = []
    for _ in range(3):
        workers = [AdaptiveScheduler(session_factory, settings, clock=clock) for _ in range(4)]
        results = await asyncio.gather(*(worker.claim_next_batch() for worker in workers))
        batches = [result for result in results if result is not None]
        assert len(batches) == 1
        assert batches[0].batch_kind == "retired_control"
        assert len(batches[0].members) <= 2
        claimed.extend(batches[0].token_addresses)
        await seed.complete_batch(batch_id=batches[0].batch_id, outcome=PollOutcome.EMPTY)
        clock.advance(60)

    assert set(claimed) == {f"retired-{index}" for index in range(5)}
    assert len(claimed) == 6
    async with session_factory() as session:
        control_batches = int(
            await session.scalar(
                select(func.count())
                .select_from(PollBatch)
                .where(PollBatch.batch_kind == "retired_control")
            )
            or 0
        )
    assert control_batches == 3


@pytest.mark.integration
async def test_retired_population_cannot_avalanche_after_restart(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    settings = _settings(scheduler_control_scan_tokens_per_minute=3)
    seed = AdaptiveScheduler(session_factory, settings, clock=clock)
    await _schedule_tokens(
        session_factory,
        seed,
        [
            (
                f"retired-avalanche-{index:03d}",
                "solana",
                LifecycleState.DORMANT,
                clock.now() - timedelta(days=30),
            )
            for index in range(100)
        ],
    )

    restarted = [AdaptiveScheduler(session_factory, settings, clock=clock) for _ in range(4)]
    results = await asyncio.gather(*(worker.claim_next_batch() for worker in restarted))
    claims = [result for result in results if result is not None]
    assert len(claims) == 1
    assert len(claims[0].members) == 3
    assert claims[0].batch_kind == "retired_control"


@pytest.mark.integration
async def test_fading_tail_terminates_and_preserves_transition_evidence(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [("fading-finite", "solana", LifecycleState.FADING, clock.now())],
    )
    token = tokens["fading-finite"]
    clock.advance(6 * 60 * 60)

    claim = await scheduler.claim_next_batch()
    assert claim is not None
    assert claim.batch_kind == "retired_control"
    assert claim.token_addresses == ("fading-finite",)
    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token.id)  # type: ignore[attr-defined]
        decisions = list(
            (
                await session.execute(
                    select(CoverageDecision)
                    .where(CoverageDecision.token_id == token.id)  # type: ignore[attr-defined]
                    .order_by(CoverageDecision.decided_at)
                )
            ).scalars()
        )
    assert schedule is not None
    assert schedule.lifecycle_state == LifecycleState.FADING.value
    assert schedule.coverage_class == CoverageClass.RETIRED_CONTROL.value
    assert schedule.next_due_at is None
    assert decisions[0].new_coverage_class == CoverageClass.FADING_TAIL.value
    assert decisions[-1].new_coverage_class == CoverageClass.RETIRED_CONTROL.value


@pytest.mark.integration
async def test_lifecycle_resurrection_promotes_retired_coverage_without_erasing_history(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [("coverage-resurrection", "solana", LifecycleState.DORMANT, clock.now())],
    )
    token = tokens["coverage-resurrection"]
    clock.advance(1)
    await scheduler.set_lifecycle_state(
        token_id=token.id,  # type: ignore[attr-defined]
        state=LifecycleState.RESURRECTED,
        decided_at=clock.now(),
    )

    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token.id)  # type: ignore[attr-defined]
        classes = list(
            (
                await session.execute(
                    select(CoverageDecision.new_coverage_class)
                    .where(CoverageDecision.token_id == token.id)  # type: ignore[attr-defined]
                    .order_by(CoverageDecision.decided_at)
                )
            ).scalars()
        )
    assert schedule is not None
    assert schedule.lifecycle_state == LifecycleState.RESURRECTED.value
    assert schedule.coverage_class == CoverageClass.PROTECTED_RESURRECTED.value
    assert schedule.next_due_at == clock.now() + timedelta(seconds=5)
    assert classes == [
        CoverageClass.RETIRED_CONTROL.value,
        CoverageClass.PROTECTED_RESURRECTED.value,
    ]


@pytest.mark.integration
async def test_epoch_start_reconstructs_legacy_coverage_from_immutable_admission(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    repository = TokenRepository()
    admitted_at = clock.now() - timedelta(days=8)
    async with session_factory() as session, session.begin():
        token = await repository.get_or_create(
            session,
            chain="solana",
            address="legacy-reconstruction",
            first_discovered_at=admitted_at,
        )
        session.add(
            LifecycleEvent(
                token_id=token.id,
                idempotency_key="legacy-reconstruction-admission",
                previous_state="PENDING_DEX",
                new_state="NEW",
                decided_at=admitted_at,
                input_watermark=admitted_at,
                reason_code="dex_pair_present",
                reason_detail={},
                configuration_sha256="a" * 64,
                configuration_snapshot={},
            )
        )
        session.add(
            PollSchedule(
                token_id=token.id,
                lifecycle_state=LifecycleState.NEW.value,
                state_decided_at=admitted_at,
                priority=2,
                next_due_at=clock.now() - timedelta(hours=3),
                attempt_count=10,
                configuration_sha256="b" * 64,
                configuration_snapshot={},
                updated_at=clock.now() - timedelta(hours=3),
            )
        )
        await scheduler.initialize_epoch_in_session(
            session,
            collection_epoch_id=uuid.UUID(int=0),
            epoch_number=0,
            started_at=clock.now(),
        )

    async with session_factory() as session:
        reconstructed = await session.get(PollSchedule, token.id)
        evidence = await session.scalar(
            select(CoverageDecision).where(CoverageDecision.token_id == token.id)
        )
    assert reconstructed is not None
    assert reconstructed.admitted_at == admitted_at
    assert reconstructed.coverage_class == CoverageClass.RETIRED_CONTROL.value
    assert reconstructed.next_due_at is None
    assert reconstructed.attempt_count == 10
    assert evidence is not None
    assert evidence.reason_code == "epoch_start_legacy_reconstruction"


def test_legacy_reconstruction_query_has_constant_bind_count() -> None:
    """A live-sized legacy cohort must not exceed asyncpg's argument ceiling."""
    statement = _legacy_admissions_statement()
    compiled = statement.compile()
    sql = str(compiled)

    assert len(compiled.params) == 1
    assert "JOIN poll_schedules" in sql
    assert "poll_schedules.admitted_at IS NULL" in sql
    assert " IN " not in sql


@pytest.mark.integration
async def test_epoch_start_refuses_ambiguous_legacy_admission(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    repository = TokenRepository()
    async with session_factory() as session, session.begin():
        token = await repository.get_or_create(
            session,
            chain="solana",
            address="legacy-without-admission",
            first_discovered_at=clock.now() - timedelta(days=8),
        )
        session.add(
            PollSchedule(
                token_id=token.id,
                lifecycle_state=LifecycleState.NEW.value,
                state_decided_at=clock.now() - timedelta(days=8),
                priority=2,
                next_due_at=clock.now() - timedelta(hours=3),
                attempt_count=0,
                configuration_sha256="b" * 64,
                configuration_snapshot={},
                updated_at=clock.now() - timedelta(hours=3),
            )
        )

    async with session_factory() as session, session.begin():
        with pytest.raises(CoverageReconstructionError, match="lack an immutable NEW"):
            await scheduler.initialize_epoch_in_session(
                session,
                collection_epoch_id=uuid.UUID(int=0),
                epoch_number=0,
                started_at=clock.now(),
            )


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
            (address, "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=15))
            for address in ("bounded-one", "bounded-two")
        ],
    )

    assert await scheduler.claim_next_batch() is not None
    assert await _schedule_gate_lock_count(session_factory) == 0
    assert await scheduler.claim_next_batch() is None
    assert await _schedule_gate_lock_count(session_factory) == 0


@pytest.mark.integration
async def test_non_progressing_coverage_refresh_rolls_back_and_releases_schedule_gate(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [("non-progressing-coverage", "solana", LifecycleState.NEW, clock.now())],
    )
    token_id = tokens["non-progressing-coverage"].id  # type: ignore[attr-defined]
    async with session_factory() as session, session.begin():
        schedule = await session.get(PollSchedule, token_id, with_for_update=True)
        assert schedule is not None
        original_priority = schedule.priority
        schedule.coverage_next_transition_at = clock.now()

    async def make_no_progress(
        session: AsyncSession,
        *,
        schedule: PollSchedule,
        now: datetime,
        reason_code: str,
        collector_run_id: uuid.UUID | None,
    ) -> None:
        del session, now, reason_code, collector_run_id
        schedule.priority = original_priority + 50

    scheduler._refresh_one_coverage = make_no_progress  # type: ignore[method-assign]
    with pytest.raises(CoverageTransitionProgressError, match=str(token_id)):
        await asyncio.wait_for(scheduler.claim_next_batch(), timeout=2)

    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token_id)
    assert schedule is not None
    assert schedule.priority == original_priority
    assert schedule.coverage_next_transition_at == clock.now()
    assert await _schedule_gate_lock_count(session_factory) == 0
    async with session_factory() as session, session.begin():
        await asyncio.wait_for(
            lock_schedule_token_fk_path(session, exclusive=True),
            timeout=2,
        )


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
            ("solana-token", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=15)),
            ("other-token", "other-chain", LifecycleState.NEW, clock.now() - timedelta(seconds=15)),
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
        [("restart-token", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=15))],
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
        [("late-token", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=15))],
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
    assert schedule.next_due_at == clock.now() + timedelta(seconds=15)


@pytest.mark.integration
async def test_safe_request_budget_reserves_headroom_for_retries(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Normal batches use safe capacity while the HTTP limiter governs retries."""
    clock = FakeClock()
    scheduler = AdaptiveScheduler(
        session_factory,
        _settings(
            dex_screener_requests_per_minute=6,
            dex_screener_max_attempts=3,
            scheduler_reserved_requests_per_minute=0,
            scheduler_batch_size=1,
            scheduler_max_in_flight_batches=10,
        ),
        clock=clock,
    )
    await _schedule_tokens(
        session_factory,
        scheduler,
        [
            (f"budget-{index}", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=45))
            for index in range(5)
        ],
    )

    assert await scheduler.claim_next_batch() is not None
    assert await scheduler.claim_next_batch() is not None
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
        [("transition-race", "solana", LifecycleState.NEW, clock.now() - timedelta(seconds=15))],
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
    assert schedule.coverage_class == CoverageClass.RETIRED_CONTROL.value
    assert schedule.next_due_at is None


@pytest.mark.integration
async def test_effective_cadence_decision_is_normalized_and_referenced(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    settings = _settings(
        dex_screener_requests_per_minute=2,
        scheduler_capacity_headroom_ratio=0,
        scheduler_reserved_requests_per_minute=0,
        scheduler_max_in_flight_batches=2,
    )
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [
            (
                f"capacity-evidence-{index:03d}",
                "solana",
                LifecycleState.NEW,
                clock.now() - timedelta(minutes=3),
            )
            for index in range(100)
        ],
    )

    claim = await scheduler.claim_next_batch()
    assert claim is not None
    assert len(claim.members) == 30
    assert claim.members[0].target_interval_seconds == 30
    assert claim.members[0].effective_interval_seconds == 100
    await scheduler.complete_batch(batch_id=claim.batch_id, outcome=PollOutcome.EMPTY)

    async with session_factory() as session:
        batch = await session.get(PollBatch, claim.batch_id)
        member = await session.scalar(
            select(PollBatchMember).where(PollBatchMember.batch_id == claim.batch_id)
        )
        capacity = await session.get(SchedulerCapacityDecision, claim.capacity_decision_id)
        policy_count = await session.scalar(select(func.count()).select_from(SchedulerPolicy))
        initial_decision = await session.scalar(
            select(PollScheduleDecision).where(
                PollScheduleDecision.token_id == tokens["capacity-evidence-000"].id  # type: ignore[attr-defined]
            )
        )
        schedule = await session.get(
            PollSchedule,
            claim.members[0].token_id,
        )

    assert batch is not None
    assert member is not None
    assert capacity is not None
    assert capacity.policy_sha256 == scheduler.policy.sha256
    assert capacity.decision_snapshot["effective_interval_seconds"]["EARLY"] == 100  # type: ignore[index]
    assert batch.capacity_decision_id == capacity.id
    assert member.capacity_decision_id == capacity.id
    assert initial_decision is not None
    assert initial_decision.capacity_decision_id is not None
    assert schedule is not None
    assert schedule.capacity_decision_id == capacity.id
    assert schedule.effective_interval_seconds == 100
    assert schedule.next_due_at == clock.now() + timedelta(seconds=100)
    assert policy_count is not None
    assert policy_count >= 1

    status = await read_collector_status(session_factory, settings)
    status_capacity = status["scheduler_capacity"]
    assert status_capacity["mode"] == "DEGRADED"
    assert status_capacity["requested_token_observations_per_minute"] == 200
    assert status_capacity["available_token_observations_per_minute"] == 60
    assert status_capacity["effective_requests_per_minute"] <= 2
    assert status_capacity["degraded_schedule_count"] == 100
    assert status["coverage_scheduler"]["coverage_effective_interval_seconds"]["EARLY"] == 100


@pytest.mark.integration
async def test_four_workers_survive_capacity_persistence_race_in_degraded_mode(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Reproduce the Epoch 1 claim-versus-lifecycle decision insert race."""
    clock = FakeClock()
    settings = _settings(
        dex_screener_requests_per_minute=4,
        scheduler_capacity_headroom_ratio=0,
        scheduler_reserved_requests_per_minute=0,
        scheduler_batch_size=30,
        scheduler_max_in_flight_batches=4,
    )
    seed_scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        seed_scheduler,
        [
            *[
                (
                    f"capacity-race-active-{index:03d}",
                    "solana",
                    LifecycleState.ACTIVE,
                    clock.now() - timedelta(seconds=5),
                )
                for index in range(5)
            ],
            *[
                (
                    f"capacity-race-fading-{index:03d}",
                    "solana",
                    LifecycleState.FADING,
                    clock.now() - timedelta(seconds=120),
                )
                for index in range(300)
            ],
        ],
    )
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    original_counts = scheduler._capacity_counts
    arrivals = 0
    arrivals_lock = asyncio.Lock()
    release = asyncio.Event()

    async def synchronize_capacity_read(
        session: AsyncSession, *, now: datetime
    ) -> dict[CoverageClass, int]:
        nonlocal arrivals
        counts = await original_counts(session, now=now)
        async with arrivals_lock:
            arrivals += 1
            if arrivals == 4:
                release.set()
        await asyncio.wait_for(release.wait(), timeout=5)
        return counts

    scheduler._capacity_counts = synchronize_capacity_read  # type: ignore[method-assign]
    lifecycle_races = [
        asyncio.create_task(
            scheduler.set_lifecycle_state(
                token_id=tokens[f"capacity-race-fading-{index:03d}"].id,  # type: ignore[attr-defined]
                state=LifecycleState.DORMANT,
                decided_at=clock.now(),
                reason_code="concurrent_capacity_regression",
            )
        )
        for index in range(3)
    ]
    claim_workers = [asyncio.create_task(scheduler.claim_next_batch()) for _ in range(4)]

    await asyncio.gather(*claim_workers, *lifecycle_races)
    claims = [result for task in claim_workers if (result := task.result()) is not None]

    assert arrivals == 4
    assert len(claims) == 4
    assert all(claim.members for claim in claims)
    decision_ids = {claim.capacity_decision_id for claim in claims}
    # Concurrent lifecycle commits can legitimately produce two semantically
    # distinct population snapshots in the same wall-clock bucket. Each remains
    # unique and auditable; no worker may fail or duplicate a schedule claim.
    assert 1 <= len(decision_ids) <= 2
    async with session_factory() as session:
        durable_count = await session.scalar(
            select(func.count())
            .select_from(SchedulerCapacityDecision)
            .where(SchedulerCapacityDecision.id.in_(decision_ids))
        )
        mode = await session.scalar(
            select(SchedulerCapacityDecision.capacity_mode).where(
                SchedulerCapacityDecision.id.in_(decision_ids)
            )
        )
    assert durable_count == len(decision_ids)
    assert mode == "DEGRADED"

    for claim in claims:
        await scheduler.complete_batch(batch_id=claim.batch_id, outcome=PollOutcome.EMPTY)
    clock.advance(60)
    assert await scheduler.claim_next_batch() is not None


@pytest.mark.integration
async def test_phase6_evidence_fence_waits_before_scheduler_schedule_token_fk_path(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The protocol closes Schedule -> Token KEY SHARE / Token -> Schedule."""
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [("phase6-fence-cycle", "solana", LifecycleState.NEW, clock.now())],
    )
    token_id = tokens["phase6-fence-cycle"].id  # type: ignore[attr-defined]
    schedule_locked = asyncio.Event()
    phase6_gate_attempted = asyncio.Event()

    async def scheduler_order() -> None:
        async with session_factory() as session, session.begin():
            await lock_schedule_token_fk_path(session, exclusive=False)
            await session.scalar(
                select(PollSchedule.token_id)
                .where(PollSchedule.token_id == token_id)
                .with_for_update()
            )
            schedule_locked.set()
            await phase6_gate_attempted.wait()
            await session.scalar(
                select(Token.id)
                .where(Token.id == token_id)
                .with_for_update(read=True, key_share=True)
            )

    async def phase6_order() -> None:
        await schedule_locked.wait()
        async with session_factory() as session, session.begin():
            phase6_gate_attempted.set()
            await lock_schedule_token_fk_path(session, exclusive=True)
            await session.scalar(
                select(Token.id).where(Token.id == token_id).with_for_update()
            )
            await session.scalar(
                select(PollSchedule.token_id)
                .where(PollSchedule.token_id == token_id)
                .with_for_update()
            )

    await asyncio.wait_for(
        asyncio.gather(scheduler_order(), phase6_order()),
        timeout=5,
    )


@pytest.mark.integration
async def test_reverse_overlapping_completion_sets_lock_schedules_in_token_uuid_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Expired/reclaimed batch overlap cannot reverse PollSchedule lock order."""
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    tokens = await _schedule_tokens(
        session_factory,
        scheduler,
        [
            ("completion-order-a", "solana", LifecycleState.NEW, clock.now()),
            ("completion-order-b", "solana", LifecycleState.NEW, clock.now()),
            ("completion-order-c", "solana", LifecycleState.NEW, clock.now()),
        ],
    )
    assert all(isinstance(token, Token) for token in tokens.values())
    ordered = sorted(token.id for token in tokens.values() if isinstance(token, Token))
    first_input = [ordered[0], ordered[1], ordered[2]]
    reclaimed_input = [ordered[2], ordered[1]]
    start = asyncio.Barrier(2)

    async def lock_completion(token_ids: list[uuid.UUID]) -> list[uuid.UUID]:
        await start.wait()
        async with session_factory() as session, session.begin():
            schedules = await _lock_completion_schedules(session, token_ids=token_ids)
            return [schedule.token_id for schedule in schedules]

    locked_orders = await asyncio.wait_for(
        asyncio.gather(
            lock_completion(first_input),
            lock_completion(reclaimed_input),
        ),
        timeout=5,
    )

    assert locked_orders[0] == sorted(first_input)
    assert locked_orders[1] == sorted(reclaimed_input)


@pytest.mark.integration
async def test_capacity_decision_replay_and_restart_are_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    settings = _settings()
    first_process = AdaptiveScheduler(session_factory, settings, clock=clock)
    second_process = AdaptiveScheduler(session_factory, settings, clock=clock)

    async def decide(scheduler: AdaptiveScheduler) -> uuid.UUID:
        async with session_factory() as session, session.begin():
            return (await scheduler._capacity_decision(session, now=clock.now())).id

    decision_ids = await asyncio.gather(
        decide(first_process),
        decide(first_process),
        decide(second_process),
        decide(second_process),
    )

    assert len(set(decision_ids)) == 1
    async with session_factory() as session:
        count = await session.scalar(
            select(func.count())
            .select_from(SchedulerCapacityDecision)
            .where(SchedulerCapacityDecision.id == decision_ids[0])
        )
    assert count == 1

    restarted = AdaptiveScheduler(session_factory, settings, clock=clock)
    assert await decide(restarted) == decision_ids[0]


@pytest.mark.integration
async def test_capacity_decision_is_frozen_for_root_transaction_across_cache_bucket_change(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """One root transaction cannot acquire two capacity identities."""
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)

    async with session_factory() as session:
        async with session.begin():
            first = await scheduler._capacity_decision(session, now=clock.now())
            clock.advance(30)
            scheduler._cached_capacity_bucket = None
            scheduler._cached_capacity_decision = None
            second = await scheduler._capacity_decision(session, now=clock.now())

        clock.advance(30)
        scheduler._cached_capacity_bucket = None
        scheduler._cached_capacity_decision = None
        async with session.begin():
            next_transaction = await scheduler._capacity_decision(session, now=clock.now())

    assert second is first
    assert second.id == first.id
    assert next_transaction.id != first.id
    async with session_factory() as session:
        identities = set(
            await session.scalars(
                select(SchedulerCapacityDecision.id).where(
                    SchedulerCapacityDecision.id.in_((first.id, next_transaction.id))
                )
            )
        )
    assert identities == {first.id, next_transaction.id}


@pytest.mark.integration
async def test_capacity_decision_transaction_owner_does_not_survive_rollback(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A later root transaction never inherits a rolled-back transaction's owner."""
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)

    async with session_factory() as session:
        with pytest.raises(RuntimeError, match="force capacity rollback"):
            async with session.begin():
                rolled_back = await scheduler._capacity_decision(session, now=clock.now())
                raise RuntimeError("force capacity rollback")

        clock.advance(30)
        scheduler._cached_capacity_bucket = None
        scheduler._cached_capacity_decision = None
        async with session.begin():
            committed = await scheduler._capacity_decision(session, now=clock.now())

    assert committed.id != rolled_back.id
    async with session_factory() as session:
        stored_ids = set(await session.scalars(select(SchedulerCapacityDecision.id)))
    assert rolled_back.id not in stored_ids
    assert committed.id in stored_ids


@pytest.mark.integration
async def test_capacity_decision_is_re_persisted_after_savepoint_rollback(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A savepoint rollback cannot strand the root transaction's capacity owner."""
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)

    async with session_factory() as session:
        root = await session.begin()
        savepoint = await session.begin_nested()
        first = await scheduler._capacity_decision(session, now=clock.now())
        assert await session.scalar(
            select(func.count())
            .select_from(SchedulerCapacityDecision)
            .where(SchedulerCapacityDecision.id == first.id)
        ) == 1

        await savepoint.rollback()

        assert session.get_transaction() is root
        assert root.is_active
        assert await session.scalar(
            select(func.count())
            .select_from(SchedulerCapacityDecision)
            .where(SchedulerCapacityDecision.id == first.id)
        ) == 0

        second = await scheduler._capacity_decision(session, now=clock.now())
        assert second is first
        assert second.id == first.id
        assert await session.scalar(
            select(func.count())
            .select_from(SchedulerCapacityDecision)
            .where(SchedulerCapacityDecision.id == first.id)
        ) == 1
        await root.commit()

    async with session_factory() as session:
        assert await session.scalar(
            select(func.count())
            .select_from(SchedulerCapacityDecision)
            .where(SchedulerCapacityDecision.id == first.id)
        ) == 1


@pytest.mark.integration
async def test_scheduler_instances_with_same_policy_share_transaction_capacity_owner(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Scheduler-local cache state cannot split one transaction's capacity identity."""
    clock = FakeClock()
    settings = _settings()
    first_scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    second_scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    later = clock.now() + timedelta(seconds=30)

    async with session_factory() as session:
        preparation = await session.begin()
        independently_cached = await second_scheduler._capacity_decision(
            session,
            now=later,
        )
        await preparation.rollback()

    assert second_scheduler._cached_capacity_decision is independently_cached

    async with session_factory() as session:
        root = await session.begin()
        owner = await first_scheduler._capacity_decision(session, now=clock.now())
        assert independently_cached.id != owner.id

        reused = await second_scheduler._capacity_decision(session, now=later)

        assert reused is owner
        assert reused.id == owner.id
        assert second_scheduler._cached_capacity_decision is independently_cached
        assert set(await session.scalars(select(SchedulerCapacityDecision.id))) == {owner.id}
        await root.commit()


@pytest.mark.integration
async def test_scheduler_instances_with_different_policies_reject_transaction_owner(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """A root transaction rejects a second scheduler policy and capacity identity."""
    clock = FakeClock()
    first_scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    second_scheduler = AdaptiveScheduler(
        session_factory,
        _settings(scheduler_capacity_headroom_ratio=0.25),
        clock=clock,
    )
    assert first_scheduler.policy.sha256 != second_scheduler.policy.sha256

    async with session_factory() as session:
        preparation = await session.begin()
        rejected_identity = await second_scheduler._capacity_decision(
            session,
            now=clock.now(),
        )
        await preparation.rollback()

    async with session_factory() as session:
        root = await session.begin()
        owner = await first_scheduler._capacity_decision(session, now=clock.now())
        assert rejected_identity.id != owner.id

        with pytest.raises(
            SchedulerCapacityDecisionIntegrityError,
            match="one transaction cannot use different scheduler policies",
        ):
            await second_scheduler._capacity_decision(session, now=clock.now())

        stored = list(await session.scalars(select(SchedulerCapacityDecision)))
        assert [decision.id for decision in stored] == [owner.id]
        assert stored[0].policy_sha256 == first_scheduler.policy.sha256
        assert rejected_identity.id not in {decision.id for decision in stored}
        await root.commit()


@pytest.mark.integration
@pytest.mark.parametrize(
    "collision_kind",
    ("same_both", "same_id_different_key", "different_id_same_key"),
)
async def test_capacity_identity_with_different_content_fails_explicitly(
    session_factory: async_sessionmaker[AsyncSession],
    collision_kind: str,
) -> None:
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, _settings(), clock=clock)
    counts = {tier: 0 for tier in CoverageClass}
    plan = plan_capacity(scheduler.policy, counts)
    key = _capacity_idempotency_key(
        bucket=clock.now(),
        policy_sha256=scheduler.policy.sha256,
        plan=plan,
    )
    decision_id = uuid.uuid5(uuid.NAMESPACE_URL, key)
    stored_id = (
        uuid.uuid4() if collision_kind == "different_id_same_key" else decision_id
    )
    stored_key = "f" * 64 if collision_kind == "same_id_different_key" else key
    async with session_factory() as session, session.begin():
        session.add(
            SchedulerPolicy(
                policy_sha256=scheduler.policy.sha256,
                policy_snapshot=scheduler.policy.snapshot,
            )
        )
        await session.flush()
        session.add(
            SchedulerCapacityDecision(
                id=stored_id,
                idempotency_key=stored_key,
                decided_at=clock.now(),
                capacity_mode="DEGRADED",
                policy_sha256=scheduler.policy.sha256,
                decision_snapshot={"semantically": "different"},
            )
        )

    async with session_factory() as session, session.begin():
        with pytest.raises(
            SchedulerCapacityDecisionIntegrityError,
            match="different semantic content",
        ):
            await scheduler._capacity_decision(session, now=clock.now())


@pytest.mark.integration
async def test_sustained_degraded_capacity_windows_are_unique_across_four_workers(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Exceed the 735 live windows without duplicate rows or per-window cache growth."""
    clock = FakeClock()
    settings = _settings()
    workers = [AdaptiveScheduler(session_factory, settings, clock=clock) for _ in range(4)]
    live_counts = {
        CoverageClass.PROTECTED_ACTIVE: 120,
        CoverageClass.PROTECTED_RESURRECTED: 0,
        CoverageClass.INITIAL: 15,
        CoverageClass.EARLY: 3_000,
        CoverageClass.PROTECTED_WATCH: 0,
        CoverageClass.FADING_TAIL: 4_700,
        CoverageClass.RETIRED_CONTROL: 50,
    }

    async def static_counts(
        session: AsyncSession, *, now: datetime
    ) -> dict[CoverageClass, int]:
        del session, now
        return live_counts

    for worker in workers:
        worker._capacity_counts = static_counts  # type: ignore[method-assign]

    async def persist(worker: AdaptiveScheduler) -> tuple[uuid.UUID, str]:
        async with session_factory() as session, session.begin():
            decision = await worker._capacity_decision(session, now=clock.now())
            return decision.id, decision.plan.mode.value

    decision_ids: set[uuid.UUID] = set()
    for _ in range(800):
        results = await asyncio.gather(*(persist(worker) for worker in workers))
        assert len({decision_id for decision_id, _ in results}) == 1
        assert {mode for _, mode in results} == {"DEGRADED"}
        decision_ids.add(results[0][0])
        clock.advance(30)

    async with session_factory() as session:
        decision_count = await session.scalar(
            select(func.count()).select_from(SchedulerCapacityDecision)
        )
        identity_count = await session.scalar(
            select(func.count(func.distinct(SchedulerCapacityDecision.idempotency_key)))
        )
    assert len(decision_ids) == 800
    assert decision_count == 800
    assert identity_count == 800
    assert all(
        worker._cached_capacity_bucket == clock.now() - timedelta(seconds=30) for worker in workers
    )
