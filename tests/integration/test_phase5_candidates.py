from __future__ import annotations

import asyncio
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.archival import export_epoch_range, verify_archive
from pump_research.candidates.policy import CandidateEvidence, CandidatePolicy, CandidateTier
from pump_research.candidates.repository import CandidateTransitionResult
from pump_research.candidates.service import CandidateOrchestrationService
from pump_research.config import Settings
from pump_research.epochs import close_epoch, create_epoch, start_epoch
from pump_research.persistence.enrichment import BoostCreate, BoostRepository
from pump_research.persistence.models import (
    BoostEvent,
    CandidateCurrentState,
    CandidateEnrichmentTask,
    CandidateEvent,
    CandidateTierEvent,
    FundingRelationshipEvidence,
    HolderBalanceFact,
    HolderSnapshot,
    PollSchedule,
    SecurityFeatureSnapshot,
    SecurityProviderRequest,
    TokenSecurityTask,
    TraderDistributionSnapshot,
    WalletClusterSnapshot,
    WalletRelationshipEdge,
)
from pump_research.persistence.repositories import (
    ApiRequestLogRepository,
    CollectorRunRepository,
    ObservationCreate,
    ObservationRepository,
    PairRepository,
    TokenRepository,
)
from pump_research.research.asof import get_token_state_as_of
from pump_research.research.sources import DuckDBArchiveResearchSource, PostgresResearchSource
from pump_research.scheduling.locks import lock_schedule_token_fk_path
from pump_research.scheduling.policy import CoverageClass, LifecycleState
from pump_research.scheduling.scheduler import AdaptiveScheduler
from pump_research.security_enrichment.analysis import build_holder_metrics
from pump_research.security_enrichment.contracts import (
    AcquisitionMode,
    CreatorEvidencePage,
    EvidenceAvailability,
    EvidenceCompleteness,
    EvidenceEnvelope,
    FundingEvidencePage,
    FundingFact,
    HolderAccountFact,
    HolderEvidencePage,
    LiquidityEvidencePage,
    ProviderPageRequest,
    TradeFact,
    TraderEvidencePage,
    TradeSide,
    WalletEdgeEvidencePage,
    WalletEdgeFact,
    WalletRelationshipType,
)
from pump_research.security_enrichment.policy import SecurityEnrichmentPolicy
from pump_research.security_enrichment.provider import SecurityProviderError
from pump_research.security_enrichment.repository import (
    SecurityEnrichmentRepository,
    SecurityEvidenceIntegrityError,
)
from pump_research.security_enrichment.service import SecurityEnrichmentWorker

NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)


class _FixedClock:
    def __init__(self, current: datetime) -> None:
        self.current = current

    def now(self) -> datetime:
        return self.current


def _settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://unused:unused@localhost/unused",
        environment="test",
    )


async def _subject(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[CandidateOrchestrationService, uuid.UUID, uuid.UUID, uuid.UUID]:
    settings = _settings()
    async with session_factory() as session, session.begin():
        epoch = await create_epoch(
            session, settings, epoch_number=5, purpose="Phase 5 isolated test", now=NOW
        )
        await start_epoch(session, epoch_number=5, now=NOW)
        run = await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="phase5-test",
            configuration_sha256="a" * 64,
            configuration_snapshot={},
            collection_epoch_id=epoch.id,
        )
        token = await TokenRepository().get_or_create(
            session,
            chain="solana",
            address="phase5-token",
            first_discovered_at=NOW - timedelta(minutes=3),
        )
    scheduler = AdaptiveScheduler(session_factory, settings)
    await scheduler.set_lifecycle_state(
        token_id=token.id,
        state=LifecycleState.NEW,
        decided_at=NOW - timedelta(minutes=3),
        admitted_at=NOW - timedelta(minutes=3),
        reason_code="phase5_fixture",
        collector_run_id=run.id,
    )
    return (
        CandidateOrchestrationService(
            session_factory,
            CandidatePolicy.from_settings(settings),
            task_lease=timedelta(seconds=settings.candidate_task_lease_seconds),
            task_max_attempts=settings.candidate_task_max_attempts,
        ),
        epoch.id,
        run.id,
        token.id,
    )


def _evidence(token_id: uuid.UUID, at: datetime = NOW, **changes: object) -> CandidateEvidence:
    values: dict[str, object] = {
        "token_id": str(token_id),
        "evaluated_at": at,
        "watermark": at,
        "lifecycle_state": "NEW",
        "coverage_class": "EARLY",
        "admitted_at": NOW - timedelta(minutes=3),
        "observation_id": "10000000-0000-0000-0000-000000000001",
        "observation_received_at": at,
        "liquidity_usd": Decimal("20000"),
        "volume_m5_usd": Decimal("2000"),
        "buys_m5": 20,
        "sells_m5": 5,
    }
    values.update(changes)
    return CandidateEvidence(**values)  # type: ignore[arg-type]


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


async def _acquire_schedule_gate(
    session_factory: async_sessionmaker[AsyncSession], *, exclusive: bool
) -> None:
    async with session_factory() as session, session.begin():
        await lock_schedule_token_fk_path(session, exclusive=exclusive)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_four_workers_create_one_candidate_and_no_duplicate_tasks(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, epoch_id, run_id, token_id = await _subject(session_factory)
    evidence = _evidence(token_id)
    results = await asyncio.gather(
        *(
            service.evaluate(
                collection_epoch_id=epoch_id,
                collector_run_id=run_id,
                evidence=evidence,
            )
            for _ in range(4)
        )
    )
    assert sum(result.changed for result in results) == 1
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(CandidateEvent)) == 1
        assert await session.scalar(select(func.count()).select_from(CandidateTierEvent)) == 1
        assert await session.scalar(select(func.count()).select_from(CandidateEnrichmentTask)) == 2
        state = await session.get(CandidateCurrentState, (epoch_id, token_id))
        schedule = await session.get(PollSchedule, token_id)
    assert state is not None and state.tier == CandidateTier.TIER_1_INTERESTING.value
    assert schedule is not None
    assert schedule.lifecycle_state == "NEW"
    assert schedule.candidate_coverage_expires_at == NOW + timedelta(minutes=30)
    assert schedule.candidate_coverage_interval_seconds == 15
    histories = await PostgresResearchSource(session_factory).load_histories(
        epoch_number=5, token_addresses=["phase5-token"]
    )
    assert len(histories) == 1
    research_state = get_token_state_as_of(histories[0], NOW)
    assert research_state.candidate is not None
    assert research_state.candidate_tier is not None
    assert research_state.candidate_tier.new_tier == CandidateTier.TIER_1_INTERESTING.value


@pytest.mark.integration
@pytest.mark.asyncio
async def test_retired_candidate_overlay_stays_out_of_ordinary_due_queue(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, epoch_id, run_id, token_id = await _subject(session_factory)
    settings = _settings()
    scheduler = AdaptiveScheduler(session_factory, settings)
    fading_at = NOW - timedelta(minutes=2)
    candidate_at = NOW + timedelta(hours=7)
    await scheduler.set_lifecycle_state(
        token_id=token_id,
        state=LifecycleState.FADING,
        decided_at=fading_at,
        reason_code="production_transition_fixture",
        collector_run_id=run_id,
    )
    async with session_factory() as session, session.begin():
        schedule = await session.scalar(
            select(PollSchedule)
            .where(PollSchedule.token_id == token_id)
            .with_for_update()
        )
        assert schedule is not None
        await scheduler._refresh_one_coverage(
            session,
            schedule=schedule,
            now=candidate_at,
            reason_code="production_transition_fixture",
            collector_run_id=run_id,
        )

    result = await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(
            token_id,
            candidate_at,
            lifecycle_state=LifecycleState.FADING.value,
            coverage_class=CoverageClass.RETIRED_CONTROL.value,
            coverage_resurrection=True,
        ),
    )

    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token_id)
        current = await session.get(CandidateCurrentState, (epoch_id, token_id))
    assert result.changed
    assert current is not None and current.tier == CandidateTier.TIER_1_INTERESTING.value
    assert schedule is not None
    assert schedule.lifecycle_state == LifecycleState.FADING.value
    assert schedule.coverage_class == CoverageClass.RETIRED_CONTROL.value
    assert schedule.next_due_at is None
    assert schedule.candidate_coverage_expires_at == candidate_at + timedelta(minutes=30)
    assert schedule.candidate_coverage_interval_seconds == 15
    assert schedule.candidate_tier_event_id == current.latest_tier_event_id
    assert schedule.priority == 1
    assert schedule.target_interval_seconds == 15
    assert schedule.effective_interval_seconds == 15


@pytest.mark.integration
@pytest.mark.asyncio
async def test_simultaneous_boost_wakeup_is_idempotent_and_does_not_change_lifecycle(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, epoch_id, run_id, token_id = await _subject(session_factory)
    async with session_factory() as session, session.begin():
        pair = await PairRepository().get_or_create(
            session,
            token_id=token_id,
            chain="solana",
            address="phase5-boost-pair",
            dex_identifier="pumpswap",
            first_discovered_at=NOW,
        )
        request = await ApiRequestLogRepository().record(
            session,
            collector_run_id=run_id,
            idempotency_key="phase5-boost-request",
            provider="dexscreener",
            endpoint="/token-boosts/latest/v1",
            requested_at=NOW,
            received_at=NOW,
            outcome="succeeded",
            http_status_code=200,
            request_payload={},
            response_payload={"records": [{"tokenAddress": "phase5-token", "amount": 1}]},
            response_payload_sha256="b" * 64,
            failure_detail=None,
        )
        await ObservationRepository().record_many(
            session,
            api_request=request,
            observations=[
                ObservationCreate(
                    pair_id=pair.id,
                    source_record_locator="records[0]",
                    source_record_sha256="c" * 64,
                    liquidity_usd=Decimal("100"),
                    volume_m5_usd=Decimal("1"),
                    buys_m5=1,
                    sells_m5=1,
                )
            ],
        )
        boost = await BoostRepository().record_if_changed(
            session,
            token_id=token_id,
            pair_id=None,
            collector_run_id=run_id,
            api_request_log_id=request.id,
            provider="dexscreener",
            source_kind="latest_feed",
            feed_rank=1,
            source_observed_at=None,
            received_at=NOW,
            source_record_locator="records[0]",
            source_record_sha256="d" * 64,
            fact=BoostCreate(amount=Decimal("1")),
        )
        assert boost is not None

    async def wake() -> CandidateTransitionResult | None:
        async with session_factory() as session, session.begin():
            return await service.evaluate_boost_observation_in_session(
                session,
                boost_observation_id=boost.id,
                collector_run_id=run_id,
            )

    results = await asyncio.gather(*(wake() for _ in range(4)))
    assert sum(result is not None and result.changed for result in results) == 1
    async with session_factory() as session:
        state = await session.get(CandidateCurrentState, (epoch_id, token_id))
        schedule = await session.get(PollSchedule, token_id)
        event = await session.scalar(
            select(CandidateEvent).where(CandidateEvent.trigger_type == "BOOST_ACTIVITY")
        )
        boost_events = await session.scalar(select(func.count()).select_from(BoostEvent))
    assert state is not None and state.tier == CandidateTier.TIER_1_INTERESTING.value
    assert schedule is not None and schedule.lifecycle_state == "NEW"
    assert event is not None
    assert boost_events == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_task_claims_are_concurrent_restart_safe_and_budgeted(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, epoch_id, run_id, token_id = await _subject(session_factory)
    await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(token_id),
    )
    claimed = await asyncio.gather(
        *(
            service.claim_tasks(
                now=NOW,
                worker_id=f"worker-{index}",
                collector_run_id=run_id,
                limit=2,
            )
            for index in range(4)
        )
    )
    flat = [item for group in claimed for item in group]
    assert len(flat) == 2
    assert len({item.id for item in flat}) == 2

    restarted = CandidateOrchestrationService(
        session_factory,
        CandidatePolicy.from_settings(_settings()),
        task_lease=timedelta(minutes=5),
        task_max_attempts=4,
    )
    reclaimed = await restarted.claim_tasks(
        now=NOW + timedelta(minutes=6),
        worker_id="after-restart",
        collector_run_id=run_id,
        limit=12,
    )
    assert {item.id for item in reclaimed} == {item.id for item in flat}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expired_candidate_demotes_without_changing_lifecycle(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, epoch_id, run_id, token_id = await _subject(session_factory)
    await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(token_id),
    )
    later = NOW + timedelta(minutes=31)
    result = await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(
            token_id,
            later,
            liquidity_usd=Decimal("100"),
            volume_m5_usd=Decimal("0"),
            buys_m5=0,
            sells_m5=0,
        ),
    )
    assert result.current_tier is CandidateTier.TIER_0_UNIVERSAL
    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token_id)
        transitions = int(
            await session.scalar(select(func.count()).select_from(CandidateTierEvent)) or 0
        )
    assert schedule is not None and schedule.lifecycle_state == "NEW"
    assert schedule.candidate_coverage_expires_at is None
    assert transitions == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_candidate_demotion_refresh_marker_is_consumed_and_releases_schedule_gate(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, epoch_id, run_id, token_id = await _subject(session_factory)
    settings = _settings()
    fading_at = NOW
    demoted_at = NOW + timedelta(minutes=31)
    natural_transition_at = fading_at + timedelta(
        seconds=settings.scheduler_fading_tail_total_duration_seconds
    )
    scheduler = AdaptiveScheduler(session_factory, settings, clock=_FixedClock(demoted_at))
    await scheduler.set_lifecycle_state(
        token_id=token_id,
        state=LifecycleState.FADING,
        decided_at=fading_at,
        reason_code="candidate_demotion_marker_regression",
        collector_run_id=run_id,
    )
    await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(
            token_id,
            lifecycle_state=LifecycleState.FADING.value,
            coverage_class=CoverageClass.FADING_TAIL.value,
        ),
    )
    async with session_factory() as session, session.begin():
        schedule = await session.scalar(
            select(PollSchedule).where(PollSchedule.token_id == token_id).with_for_update()
        )
        assert schedule is not None
        await scheduler._refresh_one_coverage(
            session,
            schedule=schedule,
            now=demoted_at,
            reason_code="candidate_demotion_marker_regression",
            collector_run_id=run_id,
        )

    result = await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(
            token_id,
            demoted_at,
            lifecycle_state=LifecycleState.FADING.value,
            coverage_class=CoverageClass.FADING_COOL.value,
            liquidity_usd=Decimal("100"),
            volume_m5_usd=Decimal("0"),
            buys_m5=0,
            sells_m5=0,
        ),
    )
    assert result.current_tier is CandidateTier.TIER_0_UNIVERSAL
    async with session_factory() as session:
        poisoned = await session.get(PollSchedule, token_id)
    assert poisoned is not None
    assert poisoned.coverage_class == CoverageClass.FADING_COOL.value
    assert poisoned.candidate_coverage_expires_at is None
    assert poisoned.candidate_coverage_interval_seconds is None
    assert poisoned.candidate_tier_event_id is None
    assert poisoned.coverage_next_transition_at == demoted_at

    claim = await asyncio.wait_for(
        scheduler.claim_next_batch(collector_run_id=run_id), timeout=2
    )
    assert claim is not None
    async with session_factory() as session:
        refreshed = await session.get(PollSchedule, token_id)
    assert refreshed is not None
    assert refreshed.coverage_class == CoverageClass.FADING_COOL.value
    assert refreshed.coverage_next_transition_at == natural_transition_at
    assert await _schedule_gate_lock_count(session_factory) == 0
    await asyncio.wait_for(_acquire_schedule_gate(session_factory, exclusive=False), timeout=2)
    await asyncio.wait_for(_acquire_schedule_gate(session_factory, exclusive=True), timeout=2)
    assert await _schedule_gate_lock_count(session_factory) == 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_expired_overlay_budget_rejection_marker_is_consumed(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, epoch_id, run_id, first_token_id = await _subject(session_factory)
    settings = _settings().model_copy(update={"candidate_max_active_coverage": 1})
    scheduler = AdaptiveScheduler(session_factory, settings, clock=_FixedClock(NOW))
    async with session_factory() as session, session.begin():
        second = await TokenRepository().get_or_create(
            session,
            chain="solana",
            address="expired-overlay-budget-holder",
            first_discovered_at=NOW,
        )
        second_token_id = second.id
    for token_id in (first_token_id, second_token_id):
        await scheduler.set_lifecycle_state(
            token_id=token_id,
            state=LifecycleState.ACTIVE,
            decided_at=NOW,
            admitted_at=NOW,
            reason_code="expired_overlay_budget_regression",
            collector_run_id=run_id,
        )
    service = CandidateOrchestrationService(
        session_factory,
        CandidatePolicy.from_settings(settings),
        task_lease=timedelta(minutes=5),
        task_max_attempts=4,
    )
    await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(
            first_token_id,
            lifecycle_state=LifecycleState.ACTIVE.value,
            coverage_class=CoverageClass.PROTECTED_ACTIVE.value,
        ),
    )
    later = NOW + timedelta(minutes=31)
    await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(
            second_token_id,
            later,
            observation_id="10000000-0000-0000-0000-000000000002",
            lifecycle_state=LifecycleState.ACTIVE.value,
            coverage_class=CoverageClass.PROTECTED_ACTIVE.value,
        ),
    )
    refreshed = await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(
            first_token_id,
            later,
            observation_id="10000000-0000-0000-0000-000000000003",
            lifecycle_state=LifecycleState.ACTIVE.value,
            coverage_class=CoverageClass.PROTECTED_ACTIVE.value,
            volume_m5_usd=Decimal("2100"),
        ),
    )
    assert refreshed.changed
    assert refreshed.current_tier is CandidateTier.TIER_1_INTERESTING
    async with session_factory() as session:
        schedule = await session.get(PollSchedule, first_token_id)
    assert schedule is not None
    assert schedule.coverage_class == CoverageClass.PROTECTED_ACTIVE.value
    assert schedule.candidate_coverage_expires_at is None
    assert schedule.candidate_coverage_interval_seconds is None
    assert schedule.candidate_tier_event_id is None
    assert schedule.coverage_next_transition_at == later

    scheduler = AdaptiveScheduler(session_factory, settings, clock=_FixedClock(later))
    assert await asyncio.wait_for(scheduler.claim_next_batch(collector_run_id=run_id), timeout=2)
    async with session_factory() as session:
        schedule = await session.get(PollSchedule, first_token_id)
    assert schedule is not None
    assert schedule.coverage_class == CoverageClass.PROTECTED_ACTIVE.value
    assert schedule.coverage_next_transition_at is None


@pytest.mark.integration
@pytest.mark.asyncio
async def test_poison_task_retries_then_fails_without_blocking_queue(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, epoch_id, run_id, token_id = await _subject(session_factory)
    await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(token_id),
    )
    now = NOW
    initial = await service.claim_tasks(
        now=now,
        worker_id="poison-worker",
        collector_run_id=run_id,
        limit=2,
    )
    assert len(initial) == 2
    claim = initial[0]
    poison_id = claim.id
    await service.complete_task(initial[1], completed_at=now, outcome="fixture-success")
    await service.complete_task(initial[1], completed_at=now, outcome="fixture-success")
    for attempt in range(4):
        await service.fail_task(
            claim,
            failed_at=now,
            failure_detail={"attempt": attempt + 1},
            retry_delay=timedelta(minutes=1),
        )
        now += timedelta(minutes=1)
        if attempt < 3:
            claims = await service.claim_tasks(
                now=now,
                worker_id="poison-worker",
                collector_run_id=run_id,
                limit=1,
            )
            assert len(claims) == 1 and claims[0].id == poison_id
            claim = claims[0]
    async with session_factory() as session:
        poison = await session.get(CandidateEnrichmentTask, poison_id)
        succeeded = int(
            await session.scalar(
                select(func.count())
                .select_from(CandidateEnrichmentTask)
                .where(CandidateEnrichmentTask.status == "succeeded")
            )
            or 0
        )
    assert poison is not None and poison.status == "failed"
    assert succeeded == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_candidate_coverage_budget_is_global_under_concurrency(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, epoch_id, run_id, first_token_id = await _subject(session_factory)
    settings = _settings().model_copy(update={"candidate_max_active_coverage": 1})
    scheduler = AdaptiveScheduler(session_factory, settings)
    async with session_factory() as session, session.begin():
        second = await TokenRepository().get_or_create(
            session,
            chain="solana",
            address="phase5-token-second",
            first_discovered_at=NOW - timedelta(minutes=3),
        )
        second_token_id = second.id
    await scheduler.set_lifecycle_state(
        token_id=second_token_id,
        state=LifecycleState.NEW,
        decided_at=NOW - timedelta(minutes=3),
        admitted_at=NOW - timedelta(minutes=3),
        reason_code="phase5_fixture",
        collector_run_id=run_id,
    )
    service = CandidateOrchestrationService(
        session_factory,
        CandidatePolicy.from_settings(settings),
        task_lease=timedelta(minutes=5),
        task_max_attempts=4,
    )
    results = await asyncio.gather(
        service.evaluate(
            collection_epoch_id=epoch_id,
            collector_run_id=run_id,
            evidence=_evidence(first_token_id),
        ),
        service.evaluate(
            collection_epoch_id=epoch_id,
            collector_run_id=run_id,
            evidence=_evidence(
                second_token_id,
                observation_id="10000000-0000-0000-0000-000000000002",
            ),
        ),
    )
    assert all(result.current_tier is CandidateTier.TIER_1_INTERESTING for result in results)
    async with session_factory() as session:
        accelerated = int(
            await session.scalar(
                select(func.count())
                .select_from(PollSchedule)
                .where(PollSchedule.candidate_coverage_expires_at > NOW)
            )
            or 0
        )
    assert accelerated == 1


class _Phase6Provider:
    name = "phase6-fixture"

    @staticmethod
    def _envelope() -> EvidenceEnvelope:
        return EvidenceEnvelope(
            provider="phase6-fixture",
            provider_schema_version="fixture-v1",
            source_observed_at=NOW,
            received_at=NOW + timedelta(seconds=2),
            availability=EvidenceAvailability.AVAILABLE,
            completeness=EvidenceCompleteness.CLOSED_TIME_RANGE,
            acquisition_mode=AcquisitionMode.HISTORICALLY_AVAILABLE,
            raw_payload={"fixture": True},
        )

    async def fetch_holders(self, request: ProviderPageRequest) -> HolderEvidencePage:
        return HolderEvidencePage(
            envelope=self._envelope(),
            mint_supply_raw=Decimal("1000"),
            holder_count=None,
            accounts=(
                HolderAccountFact("account-a", "wallet-a", Decimal("650")),
                HolderAccountFact("account-b", "wallet-b", Decimal("100")),
            ),
        )

    async def fetch_traders(self, request: ProviderPageRequest) -> TraderEvidencePage:
        trades = tuple(
            TradeFact(
                signature=f"trade-{index}",
                source_slot=index,
                source_event_at=NOW,
                received_at=NOW + timedelta(seconds=2),
                wallet=f"wallet-{index % 10}",
                side=TradeSide.BUY if index % 2 else TradeSide.SELL,
                notional_usd=Decimal("10"),
                sequence=index,
            )
            for index in range(100)
        )
        return TraderEvidencePage(
            self._envelope(),
            request.window_start or NOW - timedelta(hours=1),
            request.window_end or NOW,
            trades,
        )

    async def fetch_creator(self, request: ProviderPageRequest) -> CreatorEvidencePage:
        return CreatorEvidencePage(self._envelope(), (), None)

    async def fetch_liquidity(self, request: ProviderPageRequest) -> LiquidityEvidencePage:
        return LiquidityEvidencePage(self._envelope(), ())

    async def fetch_wallet_edges(self, request: ProviderPageRequest) -> WalletEdgeEvidencePage:
        return WalletEdgeEvidencePage(
            self._envelope(),
            (
                WalletEdgeFact(
                    wallet_a="wallet-a",
                    wallet_b="wallet-b",
                    relationship_type=WalletRelationshipType.COMMON_FUNDER,
                    first_observed_at=NOW,
                    evidence_received_at=NOW + timedelta(seconds=2),
                    strength_count=2,
                    source_fact_ids=("funding-a", "funding-b"),
                ),
            ),
        )

    async def fetch_funding(self, request: ProviderPageRequest) -> FundingEvidencePage:
        return FundingEvidencePage(
            self._envelope(),
            (
                FundingFact(
                    wallet="wallet-a",
                    funding_source="funder-z",
                    funding_at=NOW,
                    received_at=NOW + timedelta(seconds=2),
                    amount_lamports=100,
                    hop_depth=1,
                    source_signature="funding-signature",
                    completeness=EvidenceCompleteness.BOUNDED_GRAPH,
                ),
            ),
        )


async def _tier2_subject(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[CandidateOrchestrationService, uuid.UUID, uuid.UUID, uuid.UUID]:
    service, epoch_id, run_id, token_id = await _subject(session_factory)
    await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(token_id),
    )
    tier2_at = NOW + timedelta(seconds=1)
    result = await service.evaluate(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        evidence=_evidence(
            token_id,
            tier2_at,
            liquidity_usd=Decimal("100000"),
            volume_m5_usd=Decimal("20000"),
            buys_m5=60,
            sells_m5=10,
            security_snapshot_id="phase2-security",
            security_received_at=tier2_at,
        ),
    )
    assert result.current_tier is CandidateTier.TIER_2_INVESTIGATE
    return service, epoch_id, run_id, token_id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_four_phase6_workers_claim_once_persist_as_of_and_promote_tier3(
    session_factory: async_sessionmaker[AsyncSession],
    tmp_path: Path,
) -> None:
    service, epoch_id, run_id, token_id = await _tier2_subject(session_factory)
    tier2_at = NOW + timedelta(seconds=1)
    async with session_factory() as session, session.begin():
        pair = await PairRepository().get_or_create(
            session,
            token_id=token_id,
            chain="solana",
            address="phase6-pair",
            dex_identifier="pumpswap",
            first_discovered_at=tier2_at,
        )
        request = await ApiRequestLogRepository().record(
            session,
            collector_run_id=run_id,
            idempotency_key="phase6-observation-request",
            provider="dexscreener",
            endpoint="/tokens/v1/solana",
            requested_at=tier2_at,
            received_at=tier2_at,
            outcome="succeeded",
            http_status_code=200,
            request_payload={},
            response_payload={"pairs": []},
            response_payload_sha256="e" * 64,
            failure_detail=None,
        )
        await ObservationRepository().record_many(
            session,
            api_request=request,
            observations=[
                ObservationCreate(
                    pair_id=pair.id,
                    source_record_locator="pairs[0]",
                    source_record_sha256="f" * 64,
                    liquidity_usd=Decimal("100000"),
                    volume_m5_usd=Decimal("20000"),
                    buys_m5=60,
                    sells_m5=10,
                )
            ],
        )
    worker = SecurityEnrichmentWorker(
        session_factory,
        service,
        _Phase6Provider(),
        SecurityEnrichmentPolicy.from_settings(
            _settings().model_copy(
                update={
                    "security_indexer_requests_per_minute": 8,
                    "security_wallet_graph_requests_per_minute": 2,
                }
            )
        ),
    )
    claims = await asyncio.gather(
        *(
            worker.run_once(
                now=NOW + timedelta(seconds=2),
                worker_id=f"security-worker-{index}",
                collector_run_id=run_id,
                limit=1,
            )
            for index in range(4)
        )
    )
    flat = [claim for group in claims for claim in group]
    assert len(flat) == 4
    assert len({claim.id for claim in flat}) == 4
    async with session_factory() as session:
        state = await session.get(CandidateCurrentState, (epoch_id, token_id))
        provider_count = int(
            await session.scalar(select(func.count()).select_from(SecurityProviderRequest)) or 0
        )
        holder_count = int(
            await session.scalar(select(func.count()).select_from(HolderSnapshot)) or 0
        )
        trader_count = int(
            await session.scalar(select(func.count()).select_from(TraderDistributionSnapshot)) or 0
        )
        feature_count = int(
            await session.scalar(select(func.count()).select_from(SecurityFeatureSnapshot)) or 0
        )
        pending_phase6 = (
            await session.execute(
                select(
                    CandidateEnrichmentTask.analysis_type,
                    CandidateEnrichmentTask.status,
                    CandidateEnrichmentTask.candidate_id,
                ).where(
                    CandidateEnrichmentTask.analysis_type.in_(
                        (
                            "HOLDER_SNAPSHOT",
                            "TRADER_DISTRIBUTION",
                            "CREATOR_HISTORY",
                            "LIQUIDITY_EVENT_ANALYSIS",
                            "WALLET_CLUSTER_ANALYSIS",
                            "FUNDING_GRAPH_ANALYSIS",
                        )
                    )
                )
            )
        ).all()
    assert state is not None and state.tier == CandidateTier.TIER_3_DEEP_REVIEW.value
    assert provider_count == 4
    assert holder_count == 1
    assert trader_count == 1
    assert feature_count >= 1
    assert sorted((kind, status) for kind, status, _ in pending_phase6) == [
        ("CREATOR_HISTORY", "succeeded"),
        ("FUNDING_GRAPH_ANALYSIS", "pending"),
        ("HOLDER_SNAPSHOT", "succeeded"),
        ("LIQUIDITY_EVENT_ANALYSIS", "succeeded"),
        ("TRADER_DISTRIBUTION", "succeeded"),
        ("WALLET_CLUSTER_ANALYSIS", "pending"),
    ]

    deep_claims = await asyncio.gather(
        *(
            worker.run_once(
                now=NOW + timedelta(seconds=2),
                worker_id=f"deep-worker-{index}",
                collector_run_id=run_id,
                limit=1,
            )
            for index in range(4)
        )
    )
    deep_flat = [claim for group in deep_claims for claim in group]
    assert len(deep_flat) == 2
    assert len({claim.id for claim in deep_flat}) == 2
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(WalletRelationshipEdge)) == 1
        assert await session.scalar(select(func.count()).select_from(WalletClusterSnapshot)) == 1
        assert (
            await session.scalar(select(func.count()).select_from(FundingRelationshipEvidence)) == 1
        )
        tier_event_count_before_restart = int(
            await session.scalar(select(func.count()).select_from(CandidateTierEvent)) or 0
        )
        tier3_promotions = int(
            await session.scalar(
                select(func.count())
                .select_from(CandidateTierEvent)
                .where(
                    CandidateTierEvent.previous_tier != CandidateTier.TIER_3_DEEP_REVIEW.value,
                    CandidateTierEvent.new_tier == CandidateTier.TIER_3_DEEP_REVIEW.value,
                )
            )
            or 0
        )
    assert tier3_promotions == 1
    # Compare hot and cold state from the same completed fact set. The fixture gives
    # every concurrent provider response the same receipt time, so loading hot state
    # before the deep tasks would make later rows eligible at the same as-of cutoff.
    histories = await PostgresResearchSource(session_factory).load_histories(
        epoch_number=5, token_addresses=["phase5-token"]
    )
    before = get_token_state_as_of(histories[0], NOW + timedelta(seconds=1))
    after = get_token_state_as_of(histories[0], NOW + timedelta(seconds=2))
    assert before.holder_snapshot is None
    assert after.holder_snapshot is not None
    assert after.security_features is not None
    restarted = CandidateOrchestrationService(
        session_factory,
        CandidatePolicy.from_settings(_settings()),
        task_lease=timedelta(minutes=5),
        task_max_attempts=4,
    )
    await restarted.evaluate_security_token(
        collection_epoch_id=epoch_id,
        collector_run_id=run_id,
        token_id=token_id,
        evaluated_at=NOW + timedelta(seconds=2),
    )
    async with session_factory() as session:
        assert (
            await session.scalar(select(func.count()).select_from(CandidateTierEvent))
            == tier_event_count_before_restart
        )
    async with session_factory() as session, session.begin():
        await CollectorRunRepository().finish(
            session,
            run_id=run_id,
            finished_at=NOW + timedelta(minutes=5),
            status="stopped",
        )
        await close_epoch(
            session,
            epoch_number=5,
            status="completed",
            reason="closed Phase 6 hot/cold equivalence fixture",
            now=NOW + timedelta(minutes=5),
        )
    manifest = await export_epoch_range(
        session_factory,
        epoch_number=5,
        start_at=NOW,
        end_at=NOW + timedelta(minutes=5),
        output=tmp_path / "phase6-archive",
        chunk_rows=10,
        max_file_rows=100,
        minimum_free_bytes=1,
        now=NOW + timedelta(days=1),
    )
    assert (await verify_archive(manifest))["duckdb_readback_passed"] is True
    cold_histories = await DuckDBArchiveResearchSource((manifest,)).load_histories(
        epoch_number=5, token_addresses=["phase5-token"]
    )
    cold_state = get_token_state_as_of(cold_histories[0], NOW + timedelta(seconds=2))
    assert cold_state.holder_snapshot is not None
    assert cold_state.security_features is not None
    assert cold_state.holder_snapshot.id == after.holder_snapshot.id
    assert cold_state.security_features.id == after.security_features.id


class _FailingPhase6Provider(_Phase6Provider):
    async def fetch_holders(self, request: ProviderPageRequest) -> HolderEvidencePage:
        raise SecurityProviderError("fixture timeout")


class _ProductionScaleHolderProvider(_Phase6Provider):
    async def fetch_holders(self, request: ProviderPageRequest) -> HolderEvidencePage:
        return HolderEvidencePage(
            envelope=self._envelope(),
            mint_supply_raw=Decimal("999931476057174"),
            holder_count=None,
            accounts=(
                HolderAccountFact("production-account-a", "wallet-a", Decimal("333333333333333")),
                HolderAccountFact("production-account-b", "wallet-b", Decimal("100000000000000")),
            ),
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_production_holder_numeric_readback_is_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, _, run_id, _ = await _tier2_subject(session_factory)
    claim = (
        await service.claim_tasks(
            now=NOW + timedelta(seconds=1),
            worker_id="production-numeric-worker",
            collector_run_id=run_id,
            limit=1,
            analysis_types=("HOLDER_SNAPSHOT",),
        )
    )[0]
    worker = SecurityEnrichmentWorker(
        session_factory,
        service,
        _ProductionScaleHolderProvider(),
        SecurityEnrichmentPolicy.from_settings(_settings()),
    )

    await worker.process_claim(claim, now=NOW + timedelta(seconds=1))
    await worker.process_claim(claim, now=NOW + timedelta(seconds=1))

    async with session_factory() as session:
        snapshot = await session.scalar(select(HolderSnapshot))
        snapshot_count = await session.scalar(select(func.count()).select_from(HolderSnapshot))
        fact_count = await session.scalar(select(func.count()).select_from(HolderBalanceFact))
        request_count = await session.scalar(
            select(func.count())
            .select_from(SecurityProviderRequest)
            .where(SecurityProviderRequest.method == "HOLDER_SNAPSHOT")
        )
    assert snapshot is not None
    assert snapshot.top_1_pct == Decimal("33.335617621289")
    assert snapshot.top_5_pct == Decimal("43.336302907676")
    assert snapshot.top_10_pct == Decimal("43.336302907676")
    assert snapshot.top_20_pct == Decimal("43.336302907676")
    assert snapshot.largest_holder_pct == Decimal("33.335617621289")
    assert snapshot.largest_non_pool_holder_pct == Decimal("33.335617621289")
    assert snapshot.covered_supply_pct == Decimal("43.336302907676")
    assert snapshot_count == 1
    assert fact_count == 2
    assert request_count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_holder_snapshot_rejects_genuine_top_1_conflict(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, _, run_id, _ = await _tier2_subject(session_factory)
    claim = (
        await service.claim_tasks(
            now=NOW + timedelta(seconds=1),
            worker_id="genuine-conflict-worker",
            collector_run_id=run_id,
            limit=1,
            analysis_types=("HOLDER_SNAPSHOT",),
        )
    )[0]
    provider = _ProductionScaleHolderProvider()
    policy = SecurityEnrichmentPolicy.from_settings(_settings())
    worker = SecurityEnrichmentWorker(session_factory, service, provider, policy)
    await worker.process_claim(claim, now=NOW + timedelta(seconds=1))
    page = await provider.fetch_holders(
        ProviderPageRequest(
            token_address="phase5-token",
            candidate_id=str(claim.candidate_id),
            input_watermark=claim.input_watermark,
            cursor=None,
            limit=20,
        )
    )
    metrics = build_holder_metrics(
        page.accounts,
        mint_supply_raw=page.mint_supply_raw,
        holder_count=page.holder_count,
        completeness=page.envelope.completeness,
    )
    assert metrics.top_1_pct is not None
    repository = SecurityEnrichmentRepository()

    with pytest.raises(
        SecurityEvidenceIntegrityError,
        match="security evidence identity maps to different top_1_pct",
    ):
        async with session_factory() as session, session.begin():
            context = await repository.load_context(session, claim)
            request = await session.scalar(
                select(SecurityProviderRequest).where(
                    SecurityProviderRequest.method == "HOLDER_SNAPSHOT"
                )
            )
            assert request is not None
            await repository.record_holder_snapshot(
                session,
                context=context,
                request=request,
                envelope=page.envelope,
                accounts=page.accounts,
                metrics=replace(metrics, top_1_pct=metrics.top_1_pct + Decimal("1")),
                mint_supply_raw=page.mint_supply_raw,
                page_count=1,
                policy=policy,
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_holder_supply_is_part_of_evidence_identity(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, _, run_id, _ = await _tier2_subject(session_factory)
    claim = (
        await service.claim_tasks(
            now=NOW + timedelta(seconds=1),
            worker_id="holder-supply-identity-worker",
            collector_run_id=run_id,
            limit=1,
            analysis_types=("HOLDER_SNAPSHOT",),
        )
    )[0]
    provider = _ProductionScaleHolderProvider()
    policy = SecurityEnrichmentPolicy.from_settings(_settings())
    worker = SecurityEnrichmentWorker(session_factory, service, provider, policy)
    await worker.process_claim(claim, now=NOW + timedelta(seconds=1))
    page = await provider.fetch_holders(
        ProviderPageRequest(
            token_address="phase5-token",
            candidate_id=str(claim.candidate_id),
            input_watermark=claim.input_watermark,
            cursor=None,
            limit=20,
        )
    )
    assert page.mint_supply_raw is not None
    changed_supply = page.mint_supply_raw + Decimal("1")
    changed_metrics = build_holder_metrics(
        page.accounts,
        mint_supply_raw=changed_supply,
        holder_count=page.holder_count,
        completeness=page.envelope.completeness,
    )
    repository = SecurityEnrichmentRepository()
    async with session_factory() as session, session.begin():
        context = await repository.load_context(session, claim)
        request = await session.scalar(
            select(SecurityProviderRequest).where(
                SecurityProviderRequest.method == "HOLDER_SNAPSHOT"
            )
        )
        assert request is not None
        changed = await repository.record_holder_snapshot(
            session,
            context=context,
            request=request,
            envelope=page.envelope,
            accounts=page.accounts,
            metrics=changed_metrics,
            mint_supply_raw=changed_supply,
            page_count=1,
            policy=policy,
        )

    async with session_factory() as session:
        snapshots = list((await session.scalars(select(HolderSnapshot))).all())
    assert len(snapshots) == 2
    other_keys = {item.semantic_key for item in snapshots if item.id != changed.id}
    assert changed.semantic_key not in other_keys


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_exact_holder_retry_is_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, _, run_id, _ = await _tier2_subject(session_factory)
    claim = (
        await service.claim_tasks(
            now=NOW + timedelta(seconds=1),
            worker_id="concurrent-holder-worker",
            collector_run_id=run_id,
            limit=1,
            analysis_types=("HOLDER_SNAPSHOT",),
        )
    )[0]
    worker = SecurityEnrichmentWorker(
        session_factory,
        service,
        _ProductionScaleHolderProvider(),
        SecurityEnrichmentPolicy.from_settings(_settings()),
    )

    await asyncio.gather(
        worker.process_claim(claim, now=NOW + timedelta(seconds=1)),
        worker.process_claim(claim, now=NOW + timedelta(seconds=1)),
    )

    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(HolderSnapshot)) == 1
        assert await session.scalar(select(func.count()).select_from(HolderBalanceFact)) == 2
        assert (
            await session.scalar(
                select(func.count())
                .select_from(SecurityProviderRequest)
                .where(SecurityProviderRequest.method == "HOLDER_SNAPSHOT")
            )
            == 1
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_phase6_provider_failure_is_durable_and_task_retries(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, _, run_id, _ = await _tier2_subject(session_factory)
    claim = (
        await service.claim_tasks(
            now=NOW + timedelta(seconds=1),
            worker_id="failure-worker",
            collector_run_id=run_id,
            limit=1,
            analysis_types=("HOLDER_SNAPSHOT",),
        )
    )[0]
    worker = SecurityEnrichmentWorker(
        session_factory,
        service,
        _FailingPhase6Provider(),
        SecurityEnrichmentPolicy.from_settings(_settings()),
    )
    await worker.process_claim(claim, now=NOW + timedelta(seconds=1))
    async with session_factory() as session:
        task = await session.get(CandidateEnrichmentTask, claim.id)
        request = await session.scalar(select(SecurityProviderRequest))
    assert task is not None and task.status == "retry"
    assert request is not None and request.outcome == "failed"
    assert request.failure_detail == {"failure_code": "SecurityProviderError"}


@pytest.mark.integration
@pytest.mark.asyncio
async def test_due_universal_security_defers_standard_rpc_holder_work(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, _, run_id, token_id = await _tier2_subject(session_factory)
    async with session_factory() as session, session.begin():
        session.add(
            TokenSecurityTask(
                token_id=token_id,
                phase=0,
                next_due_at=NOW + timedelta(seconds=1),
                last_checked_at=None,
                attempt_count=0,
                lease_id=None,
                lease_expires_at=None,
                updated_at=NOW + timedelta(seconds=1),
            )
        )
    claim = (
        await service.claim_tasks(
            now=NOW + timedelta(seconds=1),
            worker_id="universal-precedence-worker",
            collector_run_id=run_id,
            limit=1,
            analysis_types=("HOLDER_SNAPSHOT",),
        )
    )[0]

    class NeverCalledProvider(_Phase6Provider):
        name = "solana_rpc"

        def __init__(self) -> None:
            self.holder_calls = 0

        async def fetch_holders(self, request: ProviderPageRequest) -> HolderEvidencePage:
            self.holder_calls += 1
            return await super().fetch_holders(request)

    provider = NeverCalledProvider()
    worker = SecurityEnrichmentWorker(
        session_factory,
        service,
        provider,
        SecurityEnrichmentPolicy.from_settings(_settings()),
    )
    await worker.process_claim(claim, now=NOW + timedelta(seconds=1))

    async with session_factory() as session:
        task = await session.get(CandidateEnrichmentTask, claim.id)
        request_count = int(
            await session.scalar(select(func.count()).select_from(SecurityProviderRequest)) or 0
        )
    assert task is not None and task.status == "deferred"
    assert task.attempt_count == 0
    assert provider.holder_calls == 0
    assert request_count == 0


class _PaginatedPhase6Provider(_Phase6Provider):
    async def fetch_traders(self, request: ProviderPageRequest) -> TraderEvidencePage:
        index = 1 if request.cursor is None else 2
        envelope = EvidenceEnvelope(
            provider=self.name,
            provider_schema_version="fixture-v1",
            source_observed_at=NOW,
            received_at=NOW + timedelta(seconds=2),
            availability=EvidenceAvailability.AVAILABLE,
            completeness=EvidenceCompleteness.CLOSED_TIME_RANGE,
            acquisition_mode=AcquisitionMode.HISTORICALLY_AVAILABLE,
            page_cursor=request.cursor,
            next_cursor="page-2" if request.cursor is None else None,
            raw_payload={"page": index},
        )
        return TraderEvidencePage(
            envelope,
            request.window_start or NOW - timedelta(hours=1),
            request.window_end or NOW,
            (
                TradeFact(
                    signature=f"page-trade-{index}",
                    source_slot=index,
                    source_event_at=NOW,
                    received_at=envelope.received_at,
                    wallet=f"wallet-{index}",
                    side=TradeSide.BUY,
                    notional_usd=Decimal("10"),
                    sequence=index,
                ),
            ),
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_phase6_pagination_is_bounded_and_complete(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    service, _, run_id, _ = await _tier2_subject(session_factory)
    claim = (
        await service.claim_tasks(
            now=NOW + timedelta(seconds=1),
            worker_id="pagination-worker",
            collector_run_id=run_id,
            limit=1,
            analysis_types=("TRADER_DISTRIBUTION",),
        )
    )[0]
    worker = SecurityEnrichmentWorker(
        session_factory,
        service,
        _PaginatedPhase6Provider(),
        SecurityEnrichmentPolicy.from_settings(_settings()),
    )
    await worker.process_claim(claim, now=NOW + timedelta(seconds=1))
    async with session_factory() as session:
        snapshot = await session.scalar(select(TraderDistributionSnapshot))
        request_count = int(
            await session.scalar(select(func.count()).select_from(SecurityProviderRequest)) or 0
        )
    assert snapshot is not None
    assert snapshot.page_count == 2
    assert snapshot.total_trades == 2
    assert snapshot.availability == "available"
    assert request_count == 2
