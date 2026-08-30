from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.dex_availability import (
    DexAvailabilityRunResult,
    DexAvailabilityWorkflow,
)
from pump_research.collection.discovery import DiscoveryCoordinator
from pump_research.collection.polling import LifecycleEvaluationError, ScheduledObservationWorkflow
from pump_research.collection.runtime import CollectorAlreadyRunningError, CollectorRuntime
from pump_research.collection.worker import CollectorWorker
from pump_research.config import Settings
from pump_research.discovery.contracts import (
    DiscoveredToken,
    DiscoveryBatch,
    DiscoveryCheckpoint,
    DiscoveryCoverage,
    DiscoveryCoverageStatus,
    TokenDiscoverySource,
)
from pump_research.epochs import create_epoch
from pump_research.lifecycle.classifier import LifecycleClassifier, LifecycleRequestEvaluation
from pump_research.market_data.dexscreener import (
    DexScreenerBatchResult,
    DexScreenerTokenPairsResult,
    DexScreenerTransportError,
)
from pump_research.market_data.dexscreener_models import DexScreenerPair
from pump_research.persistence.models import (
    ApiRequestLog,
    CollectorComponentHealth,
    CollectorRun,
    DexAvailabilityTask,
    DiscoveryEvent,
    LifecycleEvent,
    LifecycleEvidenceEvaluation,
    LifecyclePolicy,
    Observation,
    Pair,
    PollBatchOutcome,
    PollSchedule,
    Token,
)
from pump_research.persistence.repositories import (
    CollectorRunRepository,
    LifecycleEvidenceEvaluationRepository,
    TokenRepository,
)
from pump_research.scheduling.clock import Clock
from pump_research.scheduling.policy import LifecycleState
from pump_research.scheduling.scheduler import AdaptiveScheduler, PollBatchClaim

NOW = datetime(2026, 8, 15, 12, tzinfo=UTC)


@dataclass
class FakeClock(Clock):
    current: datetime = NOW

    def now(self) -> datetime:
        return self.current

    def advance(self, seconds: int) -> None:
        self.current += timedelta(seconds=seconds)


class FakeDiscovery(TokenDiscoverySource):
    source_name = "fake-discovery"

    def __init__(self, event: DiscoveredToken) -> None:
        self.event = event
        self.calls = 0

    async def fetch(self, checkpoint: DiscoveryCheckpoint | None = None) -> DiscoveryBatch:
        self.calls += 1
        return DiscoveryBatch(
            events=(self.event,),
            received_at=self.event.received_at,
            coverage=DiscoveryCoverage(DiscoveryCoverageStatus.COMPLETE, True),
            next_checkpoint=DiscoveryCheckpoint("checkpoint-1"),
        )

    async def aclose(self) -> None:
        return None


class FakeDex:
    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.present = False
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    async def fetch_token_pairs(
        self, *, chain_id: str, token_addresses: list[str]
    ) -> DexScreenerTokenPairsResult:
        addresses = tuple(token_addresses)
        self.calls.append((chain_id, addresses))
        raw: list[dict[str, Any]] = []
        if self.present:
            raw = [
                {
                    "chainId": chain_id,
                    "dexId": "fake-dex",
                    "pairAddress": f"pair-{address}",
                    "baseToken": {"address": address},
                    "quoteToken": {"address": "SOL"},
                    "priceUsd": "0.01",
                    "liquidity": {"usd": "2000"},
                    "volume": {"m5": "200", "h1": "300"},
                    "txns": {"m5": {"buys": 3, "sells": 1}},
                }
                for address in addresses
            ]
        pairs = tuple(DexScreenerPair.model_validate(value) for value in raw)
        batch = DexScreenerBatchResult(
            chain_id=chain_id,
            requested_addresses=addresses,
            pairs=pairs,
            received_at=self.clock.now(),
            raw_response=tuple(raw),
        )
        return DexScreenerTokenPairsResult(chain_id, addresses, (batch,))


class SelectiveDex:
    def __init__(self, clock: FakeClock, returned_addresses: list[str]) -> None:
        self.clock = clock
        self.returned_addresses = returned_addresses

    async def fetch_token_pairs(
        self, *, chain_id: str, token_addresses: list[str]
    ) -> DexScreenerTokenPairsResult:
        raw = [
            {
                "chainId": chain_id,
                "dexId": "fake-dex",
                "pairAddress": f"pair-{address}",
                "baseToken": {"address": address},
                "quoteToken": {"address": "SOL"},
                "priceUsd": "0.01",
                "liquidity": {"usd": "2000"},
                "volume": {"m5": "200", "h1": "300"},
            }
            for address in self.returned_addresses
        ]
        pairs = tuple(DexScreenerPair.model_validate(value) for value in raw)
        batch = DexScreenerBatchResult(
            chain_id=chain_id,
            requested_addresses=tuple(token_addresses),
            pairs=pairs,
            received_at=self.clock.now(),
            raw_response=tuple(raw),
        )
        return DexScreenerTokenPairsResult(
            chain_id,
            tuple(token_addresses),
            (batch,),
        )


class MultiPairDex:
    def __init__(self, clock: FakeClock, pairs: list[dict[str, object]]) -> None:
        self.clock = clock
        self.pairs = pairs

    async def fetch_token_pairs(
        self, *, chain_id: str, token_addresses: list[str]
    ) -> DexScreenerTokenPairsResult:
        assert len(token_addresses) == 1
        token_address = token_addresses[0]
        raw: list[dict[str, Any]] = []
        for specification in self.pairs:
            pair: dict[str, Any] = {
                "chainId": chain_id,
                "dexId": specification.get("dex_id", "fake-dex"),
                "pairAddress": specification["pair_address"],
                "baseToken": {"address": token_address},
                "quoteToken": {"address": "SOL"},
                "priceUsd": "0.01",
                "volume": {
                    "m5": specification.get("volume_m5_usd", "0"),
                    "h1": specification.get("volume_h1_usd", "0"),
                },
            }
            if "liquidity_usd" in specification:
                pair["liquidity"] = {"usd": specification["liquidity_usd"]}
            raw.append(pair)
        addresses = tuple(token_addresses)
        typed = tuple(DexScreenerPair.model_validate(value) for value in raw)
        batch = DexScreenerBatchResult(
            chain_id=chain_id,
            requested_addresses=addresses,
            pairs=typed,
            received_at=self.clock.now(),
            raw_response=tuple(raw),
        )
        return DexScreenerTokenPairsResult(chain_id, addresses, (batch,))


def _settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research",
        dex_availability_retry_seconds=1,
        dex_availability_lease_seconds=10,
    )


async def _declare_epoch(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> None:
    async with session_factory() as session, session.begin():
        await create_epoch(
            session,
            settings,
            epoch_number=1,
            purpose="collector runtime integration test",
            now=NOW,
        )


def _event() -> DiscoveredToken:
    return DiscoveredToken(
        chain="solana",
        address="synthetic-token",
        source_name="fake-discovery",
        source_event_id="synthetic-token",
        event_type="token_created",
        source_event_at=NOW,
        received_at=NOW,
        source_payload={"mint": "synthetic-token"},
        source_payload_sha256="a" * 64,
        idempotency_key="synthetic-discovery",
    )


async def _claim_two_tokens(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    clock: FakeClock,
) -> tuple[AdaptiveScheduler, PollBatchClaim, uuid.UUID]:
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    token_repository = TokenRepository()
    async with session_factory() as session, session.begin():
        run = await CollectorRunRepository().start(
            session,
            started_at=clock.now(),
            collector_version="test",
            configuration_sha256="b" * 64,
            configuration_snapshot={},
        )
        for address in ("token-a", "token-b"):
            token = await token_repository.get_or_create(
                session,
                chain="solana",
                address=address,
                first_discovered_at=clock.now(),
            )
            await scheduler.set_lifecycle_state_in_session(
                session,
                token_id=token.id,
                state=LifecycleState.NEW,
                decided_at=clock.now(),
                reason_code="test_setup",
            )
    clock.advance(settings.scheduler_new_interval_seconds)
    claim = await scheduler.claim_next_batch()
    assert claim is not None
    assert len(claim.members) == 2
    return scheduler, claim, run.id


async def _claim_one_new_token(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    clock: FakeClock,
    *,
    address: str,
) -> tuple[AdaptiveScheduler, PollBatchClaim, uuid.UUID, uuid.UUID]:
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    async with session_factory() as session, session.begin():
        run = await CollectorRunRepository().start(
            session,
            started_at=clock.now(),
            collector_version="test",
            configuration_sha256="b" * 64,
            configuration_snapshot={},
        )
        token = await TokenRepository().get_or_create(
            session,
            chain="solana",
            address=address,
            first_discovered_at=clock.now(),
        )
        await scheduler.set_lifecycle_state_in_session(
            session,
            token_id=token.id,
            state=LifecycleState.NEW,
            decided_at=clock.now(),
            reason_code="test_setup",
        )
    clock.advance(settings.scheduler_new_interval_seconds)
    claim = await scheduler.claim_next_batch()
    assert claim is not None
    return scheduler, claim, run.id, token.id


@pytest.mark.integration
async def test_synthetic_pipeline_persists_then_reconciles_then_observes(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock, settings = FakeClock(), _settings()
    dex = FakeDex(clock)
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    availability = DexAvailabilityWorkflow(session_factory, dex, settings, scheduler=scheduler)
    discovery = FakeDiscovery(_event())
    coordinator = DiscoveryCoordinator(session_factory, discovery, availability)
    lifecycle = LifecycleClassifier(session_factory, settings, clock=clock)
    polling = ScheduledObservationWorkflow(session_factory, dex, scheduler, lifecycle)
    runs = CollectorRunRepository()
    async with session_factory() as session, session.begin():
        run = await runs.start(
            session,
            started_at=clock.now(),
            collector_version="test",
            configuration_sha256="b" * 64,
            configuration_snapshot={},
        )

    await coordinator.run_once()
    await coordinator.run_once()  # duplicate delivery must not create another token/event/task
    absent = await availability.check_due(now=clock.now(), collector_run_id=run.id)
    assert absent.retained_pending_tokens == 1

    clock.advance(1)
    dex.present = True
    admitted = await availability.check_due(now=clock.now(), collector_run_id=run.id)
    assert admitted.promoted_new_tokens == 1
    clock.advance(settings.scheduler_new_interval_seconds)
    claim = await scheduler.claim_next_batch()
    assert claim is not None
    executed = await polling.execute(claim, collector_run_id=run.id)
    assert executed.observations_written == 1

    async with session_factory() as session:
        tokens = await session.scalar(select(func.count()).select_from(Token))
        discoveries = await session.scalar(select(func.count()).select_from(DiscoveryEvent))
        observations = await session.scalar(select(func.count()).select_from(Observation))
        pending = await session.scalar(
            select(func.count())
            .select_from(DexAvailabilityTask)
            .where(DexAvailabilityTask.state == "PENDING_DEX")
        )
        schedule = (await session.execute(select(PollSchedule))).scalar_one()
        outcome = (await session.execute(select(PollBatchOutcome))).scalar_one()
        evidence = (
            await session.execute(select(LifecycleEvidenceEvaluation))
        ).scalar_one()
        evidence_policy = await LifecycleEvidenceEvaluationRepository().resolve_policy_snapshot(
            session,
            evidence,
        )
        normalized_policy = await session.get(LifecyclePolicy, evidence.policy_sha256)
        evidence_is_sql_null = await session.scalar(
            select(LifecycleEvidenceEvaluation.policy_snapshot.is_(None)).where(
                LifecycleEvidenceEvaluation.id == evidence.id,
                LifecycleEvidenceEvaluation.input_watermark == evidence.input_watermark,
            )
        )
    assert tokens == 1
    assert discoveries == 1
    assert observations == 1
    assert pending == 0
    assert outcome.outcome == "succeeded"
    assert evidence.outcome == "selected"
    assert evidence.reason_code == "only_candidate_pair"
    assert evidence.policy_snapshot is None
    assert evidence_is_sql_null is True
    assert normalized_policy is not None
    assert normalized_policy.policy_snapshot == evidence_policy
    assert evidence_policy["schema_version"] == 1
    candidates = evidence.reason_detail["candidates"]
    assert isinstance(candidates, list)
    assert len(candidates) == 1
    assert schedule.next_due_at == clock.now() + timedelta(
        seconds=settings.scheduler_active_interval_seconds
    )


class ExplodingLifecycleClassifier:
    async def evaluate_request_in_session(
        self, *_: object, **__: object
    ) -> LifecycleRequestEvaluation:
        raise RuntimeError("synthetic lifecycle failure")


@pytest.mark.integration
async def test_lifecycle_failure_preserves_raw_facts_and_records_the_incomplete_derivation(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock, settings = FakeClock(), _settings()
    dex = FakeDex(clock)
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    availability = DexAvailabilityWorkflow(session_factory, dex, settings, scheduler=scheduler)
    coordinator = DiscoveryCoordinator(
        session_factory,
        FakeDiscovery(_event()),
        availability,
    )
    async with session_factory() as session, session.begin():
        run = await CollectorRunRepository().start(
            session,
            started_at=clock.now(),
            collector_version="test",
            configuration_sha256="b" * 64,
            configuration_snapshot={},
        )

    await coordinator.run_once()
    dex.present = True
    await availability.check_due(now=clock.now(), collector_run_id=run.id)
    async with session_factory() as session:
        requests_before_poll = await session.scalar(
            select(func.count()).select_from(ApiRequestLog)
        )
    clock.advance(settings.scheduler_new_interval_seconds)
    claim = await scheduler.claim_next_batch()
    assert claim is not None
    polling = ScheduledObservationWorkflow(
        session_factory,
        dex,
        scheduler,
        ExplodingLifecycleClassifier(),
    )

    with pytest.raises(LifecycleEvaluationError):
        await polling.execute(claim, collector_run_id=run.id)

    async with session_factory() as session:
        observations = await session.scalar(select(func.count()).select_from(Observation))
        requests_after_poll = await session.scalar(select(func.count()).select_from(ApiRequestLog))
        outcomes = await session.scalar(select(func.count()).select_from(PollBatchOutcome))
        schedule = (await session.execute(select(PollSchedule))).scalar_one()
        outcome = await session.get(PollBatchOutcome, claim.batch_id)
    assert observations == 1
    assert requests_before_poll is not None
    assert requests_after_poll == requests_before_poll + 1
    assert outcomes == 1
    assert outcome is not None
    assert outcome.outcome == "partial"
    assert outcome.failure_detail is not None
    lifecycle_failure = outcome.failure_detail["lifecycle_evaluation"]
    assert isinstance(lifecycle_failure, dict)
    assert lifecycle_failure["error_type"] == "RuntimeError"
    assert schedule.lease_id is None


@pytest.mark.integration
async def test_partial_batch_records_each_unrepresented_member(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock, settings = FakeClock(), _settings()
    scheduler, claim, run_id = await _claim_two_tokens(session_factory, settings, clock)
    returned_address = claim.members[0].address
    polling = ScheduledObservationWorkflow(
        session_factory,
        SelectiveDex(clock, [returned_address]),
        scheduler,
        LifecycleClassifier(session_factory, settings, clock=clock),
    )

    result = await polling.execute(claim, collector_run_id=run_id)

    async with session_factory() as session:
        outcome = await session.get(PollBatchOutcome, claim.batch_id)
        request = await session.get(ApiRequestLog, outcome.api_request_log_id) if outcome else None
    assert result.outcome.value == "partial"
    assert outcome is not None
    assert outcome.outcome == "partial"
    assert outcome.failure_detail is not None
    assert outcome.failure_detail["empty_addresses"] == [claim.members[1].address]
    assert request is not None
    assert request.outcome == "partial"


@pytest.mark.integration
async def test_observation_locator_indexes_the_raw_batch_response(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock, settings = FakeClock(), _settings()
    scheduler, claim, run_id = await _claim_two_tokens(session_factory, settings, clock)
    response_order = list(reversed(claim.token_addresses))
    polling = ScheduledObservationWorkflow(
        session_factory,
        SelectiveDex(clock, response_order),
        scheduler,
        LifecycleClassifier(session_factory, settings, clock=clock),
    )

    await polling.execute(claim, collector_run_id=run_id)

    async with session_factory() as session:
        rows = (
            await session.execute(
                select(Pair.address, Observation.source_record_locator).join(
                    Observation,
                    Observation.pair_id == Pair.id,
                )
            )
        ).all()
    assert {address: locator for address, locator in rows} == {
        f"pair-{response_order[0]}": "pairs[0]",
        f"pair-{response_order[1]}": "pairs[1]",
    }


@pytest.mark.integration
@pytest.mark.parametrize("reverse_response", [False, True])
async def test_multi_pair_evidence_uses_liquidity_not_response_order(
    session_factory: async_sessionmaker[AsyncSession],
    reverse_response: bool,
) -> None:
    clock, settings = FakeClock(), _settings()
    scheduler, claim, run_id, token_id = await _claim_one_new_token(
        session_factory,
        settings,
        clock,
        address=f"multi-pair-{reverse_response}",
    )
    specifications: list[dict[str, object]] = [
        {
            "pair_address": "pair-low-liquidity",
            "dex_id": "raydium",
            "liquidity_usd": "100",
            "volume_m5_usd": "500",
        },
        {
            "pair_address": "pair-high-liquidity",
            "dex_id": "pumpswap",
            "liquidity_usd": "5000",
            "volume_m5_usd": "50",
        },
    ]
    if reverse_response:
        specifications.reverse()
    workflow = ScheduledObservationWorkflow(
        session_factory,
        MultiPairDex(clock, specifications),
        scheduler,
        LifecycleClassifier(session_factory, settings, clock=clock),
    )

    result = await workflow.execute(claim, collector_run_id=run_id)

    async with session_factory() as session:
        evidence = (
            await session.execute(select(LifecycleEvidenceEvaluation))
        ).scalar_one()
        selected_pair = await session.get(Pair, evidence.selected_pair_id)
        schedule = await session.get(PollSchedule, token_id)
        observation_count = await session.scalar(
            select(func.count()).select_from(Observation)
        )
        event = await session.scalar(
            select(LifecycleEvent).where(LifecycleEvent.reason_code == "new_to_watch")
        )
    assert result.outcome.value == "succeeded"
    assert observation_count == 2
    assert selected_pair is not None
    assert selected_pair.address == "pair-high-liquidity"
    assert schedule is not None
    assert schedule.lifecycle_state == "WATCH"
    assert event is not None
    assert event.lifecycle_evidence_evaluation_id == evidence.id
    candidates = evidence.reason_detail["candidates"]
    assert isinstance(candidates, list)
    assert [candidate["pair_address"] for candidate in candidates] == [
        "pair-high-liquidity",
        "pair-low-liquidity",
    ]


@pytest.mark.integration
async def test_selected_pair_can_change_between_observation_times(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock, settings = FakeClock(), _settings()
    scheduler, first_claim, run_id, token_id = await _claim_one_new_token(
        session_factory,
        settings,
        clock,
        address="changing-primary-pair",
    )
    dex = MultiPairDex(
        clock,
        [
            {
                "pair_address": "pair-a",
                "liquidity_usd": "5000",
                "volume_m5_usd": "101",
            },
            {
                "pair_address": "pair-b",
                "liquidity_usd": "1000",
                "volume_m5_usd": "101",
            },
        ],
    )
    workflow = ScheduledObservationWorkflow(
        session_factory,
        dex,
        scheduler,
        LifecycleClassifier(session_factory, settings, clock=clock),
    )
    await workflow.execute(first_claim, collector_run_id=run_id)

    clock.advance(settings.scheduler_active_interval_seconds)
    second_claim = await scheduler.claim_next_batch()
    assert second_claim is not None
    dex.pairs = [
        {
            "pair_address": "pair-a",
            "liquidity_usd": "1000",
            "volume_m5_usd": "50",
        },
        {
            "pair_address": "pair-b",
            "liquidity_usd": "6000",
            "volume_m5_usd": "50",
        },
    ]
    await workflow.execute(second_claim, collector_run_id=run_id)

    async with session_factory() as session:
        selected_addresses = list(
            (
                await session.execute(
                    select(Pair.address)
                    .join(
                        LifecycleEvidenceEvaluation,
                        LifecycleEvidenceEvaluation.selected_pair_id == Pair.id,
                    )
                    .where(LifecycleEvidenceEvaluation.token_id == token_id)
                    .order_by(LifecycleEvidenceEvaluation.input_watermark)
                )
            ).scalars()
        )
        observation_count = await session.scalar(
            select(func.count()).select_from(Observation)
        )
    assert selected_addresses == ["pair-a", "pair-b"]
    assert observation_count == 4


@pytest.mark.integration
async def test_incomplete_multi_pair_selection_is_explicit_and_preserves_raw_observations(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock, settings = FakeClock(), _settings()
    scheduler, claim, run_id, token_id = await _claim_one_new_token(
        session_factory,
        settings,
        clock,
        address="incomplete-pair-evidence",
    )
    workflow = ScheduledObservationWorkflow(
        session_factory,
        MultiPairDex(
            clock,
            [
                {"pair_address": "pair-complete", "liquidity_usd": "5000"},
                {"pair_address": "pair-missing-liquidity"},
            ],
        ),
        scheduler,
        LifecycleClassifier(session_factory, settings, clock=clock),
    )

    result = await workflow.execute(claim, collector_run_id=run_id)

    async with session_factory() as session:
        evidence = (
            await session.execute(select(LifecycleEvidenceEvaluation))
        ).scalar_one()
        observations = await session.scalar(select(func.count()).select_from(Observation))
        transitions = await session.scalar(select(func.count()).select_from(LifecycleEvent))
        schedule = await session.get(PollSchedule, token_id)
        outcome = await session.get(PollBatchOutcome, claim.batch_id)
    assert result.outcome.value == "partial"
    assert observations == 2
    assert evidence.outcome == "failed"
    assert evidence.selected_pair_id is None
    assert evidence.reason_code == "candidate_missing_required_liquidity_usd"
    assert transitions == 0
    assert schedule is not None
    assert schedule.lifecycle_state == "NEW"
    assert outcome is not None
    assert outcome.failure_detail is not None
    assert "lifecycle_evidence_selection" in outcome.failure_detail


@pytest.mark.integration
async def test_fixed_worker_runs_the_synthetic_pipeline_until_shutdown(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    settings = _settings().model_copy(
        update={
            "collector_discovery_poll_seconds": 0.01,
            "collector_reconciliation_poll_seconds": 0.01,
            "collector_scheduler_poll_seconds": 0.01,
            "collector_heartbeat_seconds": 0.01,
        }
    )
    dex = FakeDex(clock)
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    availability = DexAvailabilityWorkflow(session_factory, dex, settings, scheduler=scheduler)
    lifecycle = LifecycleClassifier(session_factory, settings, clock=clock)
    worker = CollectorWorker(
        session_factory,
        settings,
        discovery=DiscoveryCoordinator(session_factory, FakeDiscovery(_event()), availability),
        availability=availability,
        scheduler=scheduler,
        polling=ScheduledObservationWorkflow(session_factory, dex, scheduler, lifecycle),
        logger=structlog.get_logger("test.fixed-worker"),
        clock=clock,
    )
    async with session_factory() as session, session.begin():
        run = await CollectorRunRepository().start(
            session,
            started_at=clock.now(),
            collector_version="test",
            configuration_sha256="b" * 64,
            configuration_snapshot={},
        )
    shutdown = asyncio.Event()
    task = asyncio.create_task(worker.run(run_id=run.id, shutdown=shutdown))
    try:
        await _wait_until(session_factory, DexAvailabilityTask.attempt_count, 1)
        dex.present = True
        clock.advance(1)
        await _wait_until(session_factory, DexAvailabilityTask.state, "NEW")
        clock.advance(settings.scheduler_new_interval_seconds)
        await _wait_until(session_factory, Observation.id, not_none=True)
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=2)


async def _wait_until(
    session_factory: async_sessionmaker[AsyncSession],
    field: Any,
    expected: object = None,
    *,
    not_none: bool = False,
) -> None:
    for _ in range(200):
        async with session_factory() as session:
            value = await session.scalar(select(field))
        if (not_none and value is not None) or (not not_none and value == expected):
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"timed out waiting for {field}={expected}")


class WaitingWorker:
    def __init__(self) -> None:
        self.stopped = False

    async def run(self, *, run_id: uuid.UUID, shutdown: asyncio.Event) -> None:
        await shutdown.wait()

    async def mark_stopped(self, run_id: uuid.UUID) -> None:
        self.stopped = True


class ExplodingDiscovery:
    async def run_once(
        self, *, collector_run_id: uuid.UUID | None = None
    ) -> None:
        del collector_run_id
        raise RuntimeError("synthetic unexpected component failure")


class IdleAvailability:
    async def check_due(self, **_: object) -> None:
        return None


class QuietDiscovery:
    async def run_once(
        self, *, collector_run_id: uuid.UUID | None = None
    ) -> None:
        del collector_run_id
        return None


class DegradingAvailability:
    def __init__(self) -> None:
        self.calls = 0

    async def check_due(self, **_: object) -> DexAvailabilityRunResult:
        self.calls += 1
        if self.calls == 1:
            return DexAvailabilityRunResult(0, 0, 0, 0, 0)
        return DexAvailabilityRunResult(1, 0, 0, 0, 1)


class IdleScheduler:
    async def claim_next_batch(
        self, *, collector_run_id: uuid.UUID | None = None
    ) -> None:
        del collector_run_id
        return None


class IdlePolling:
    async def execute(self, *_: object, **__: object) -> None:
        return None


class IntegratedAvailability:
    def __init__(self) -> None:
        self.calls = 0

    async def check_due(self, **_: object) -> DexAvailabilityRunResult:
        self.calls += 1
        return DexAvailabilityRunResult(0, 0, 0, 0, 0)


class IntegratedBoosts:
    def __init__(self) -> None:
        self.feeds: set[str] = set()

    async def collect(self, *, feed_kind: str, **_: object) -> None:
        self.feeds.add(feed_kind)


class TransportDegradingBoosts:
    def __init__(self) -> None:
        self.latest_calls = 0

    async def collect(self, *, feed_kind: str, **_: object) -> None:
        if feed_kind != "latest":
            return
        self.latest_calls += 1
        request = httpx.Request("GET", "https://dexscreener.test/token-boosts/latest/v1")
        cause = httpx.ConnectError(
            "[Errno -3] Temporary failure in name resolution",
            request=request,
        )
        raise DexScreenerTransportError(attempt_count=3) from cause


class IntegratedSecurity:
    def __init__(self) -> None:
        self.calls = 0

    async def collect_due(self, **_: object) -> None:
        self.calls += 1


class IntegratedContext:
    def __init__(self) -> None:
        self.calls = 0

    async def record_closed_bucket(self, **_: object) -> None:
        self.calls += 1


class IntegratedSelectiveSecurity:
    def __init__(self) -> None:
        self.workers: set[str] = set()

    async def run_once(self, *, worker_id: str, **_: object) -> None:
        self.workers.add(worker_id)


@pytest.mark.integration
async def test_phase7_all_collector_components_coexist_and_stop_cleanly(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = _settings().model_copy(
        update={
            "collector_discovery_poll_seconds": 0.01,
            "collector_reconciliation_poll_seconds": 0.01,
            "collector_scheduler_poll_seconds": 0.01,
            "collector_heartbeat_seconds": 0.01,
            "security_enrichment_poll_seconds": 0.01,
        }
    )
    await _declare_epoch(session_factory, settings)
    availability = IntegratedAvailability()
    boosts = IntegratedBoosts()
    security = IntegratedSecurity()
    context = IntegratedContext()
    selective = IntegratedSelectiveSecurity()
    scheduler = AdaptiveScheduler(session_factory, settings)
    worker = CollectorWorker(
        session_factory,
        settings,
        discovery=QuietDiscovery(),  # type: ignore[arg-type]
        availability=availability,  # type: ignore[arg-type]
        scheduler=IdleScheduler(),  # type: ignore[arg-type]
        polling=IdlePolling(),  # type: ignore[arg-type]
        logger=structlog.get_logger("test.phase7-integrated"),
        boosts=boosts,  # type: ignore[arg-type]
        security=security,  # type: ignore[arg-type]
        market_context=context,  # type: ignore[arg-type]
        selective_security=selective,  # type: ignore[arg-type]
    )
    runtime = CollectorRuntime(
        session_factory,
        settings,
        logger=structlog.get_logger("test.phase7-integrated-runtime"),
        epoch_number=1,
        worker=worker,
        epoch_initializer=scheduler,
    )
    task = asyncio.create_task(runtime.run_until_stopped())
    for _ in range(200):
        if (
            availability.calls
            and boosts.feeds == {"latest", "top"}
            and security.calls
            and context.calls
            and len(selective.workers) == settings.security_enrichment_workers
        ):
            break
        await asyncio.sleep(0.01)
    else:
        raise AssertionError("not every integrated collector component ran")
    runtime.request_shutdown("phase7_integrated_rehearsal")
    startup = await asyncio.wait_for(task, timeout=5)

    async with session_factory() as session:
        run = await session.get(CollectorRun, startup.run_id)
        component_rows = (
            await session.execute(
                select(
                    CollectorComponentHealth.component_name,
                    CollectorComponentHealth.status,
                ).where(CollectorComponentHealth.collector_run_id == startup.run_id)
            )
        ).all()
    assert run is not None and run.status == "stopped"
    components: dict[str, str] = {
        str(row[0]): str(row[1]) for row in component_rows
    }
    assert {
        "discovery",
        "dex_availability",
        "scheduled_observation",
        "heartbeat",
        "storage_telemetry",
        "boost_latest",
        "boost_top",
        "token_security",
        "market_context",
        "selective_security_enrichment",
    } <= components.keys()
    assert set(components.values()) == {"stopped"}


@pytest.mark.integration
async def test_persisted_component_failure_marks_worker_degraded_without_hiding_last_success(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = _settings().model_copy(
        update={
            "collector_discovery_poll_seconds": 0.01,
            "collector_reconciliation_poll_seconds": 0.01,
            "collector_scheduler_poll_seconds": 0.01,
            "collector_heartbeat_seconds": 0.01,
        }
    )
    logger = structlog.get_logger("test.degraded-worker")
    availability = DegradingAvailability()
    worker = CollectorWorker(
        session_factory,
        settings,
        discovery=QuietDiscovery(),  # type: ignore[arg-type]
        availability=availability,  # type: ignore[arg-type]
        scheduler=IdleScheduler(),  # type: ignore[arg-type]
        polling=IdlePolling(),  # type: ignore[arg-type]
        logger=logger,
    )
    async with session_factory() as session, session.begin():
        run = await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="test",
            configuration_sha256="b" * 64,
            configuration_snapshot={},
        )
    shutdown = asyncio.Event()
    task = asyncio.create_task(worker.run(run_id=run.id, shutdown=shutdown))
    try:
        for _ in range(200):
            async with session_factory() as session:
                current_status = await session.scalar(
                    select(CollectorComponentHealth.status).where(
                        CollectorComponentHealth.component_name == "dex_availability"
                    )
                )
            if current_status == "degraded":
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("availability health never became degraded")
        async with session_factory() as session:
            health = await session.get(CollectorComponentHealth, "dex_availability")
        assert task.done() is False
        assert health is not None
        assert health.last_success_at is not None
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.integration
async def test_exhausted_boost_transport_error_degrades_without_stopping_task_group(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = _settings().model_copy(
        update={
            "collector_discovery_poll_seconds": 60,
            "collector_reconciliation_poll_seconds": 60,
            "collector_scheduler_poll_seconds": 60,
            "collector_heartbeat_seconds": 60,
            "storage_telemetry_interval_seconds": 60,
            "boost_latest_poll_seconds": 60,
            "boost_top_poll_seconds": 60,
        }
    )
    boosts = TransportDegradingBoosts()
    worker = CollectorWorker(
        session_factory,
        settings,
        discovery=QuietDiscovery(),  # type: ignore[arg-type]
        availability=IntegratedAvailability(),  # type: ignore[arg-type]
        scheduler=IdleScheduler(),  # type: ignore[arg-type]
        polling=IdlePolling(),  # type: ignore[arg-type]
        logger=structlog.get_logger("test.boost-transport-degraded-worker"),
        boosts=boosts,  # type: ignore[arg-type]
    )
    async with session_factory() as session, session.begin():
        run = await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="test",
            configuration_sha256="b" * 64,
            configuration_snapshot={},
        )
    shutdown = asyncio.Event()
    task = asyncio.create_task(worker.run(run_id=run.id, shutdown=shutdown))
    try:
        for _ in range(200):
            async with session_factory() as session:
                current_status = await session.scalar(
                    select(CollectorComponentHealth.status).where(
                        CollectorComponentHealth.component_name == "boost_latest"
                    )
                )
            if current_status == "degraded":
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("boost_latest health never became degraded")
        async with session_factory() as session:
            health = await session.get(CollectorComponentHealth, "boost_latest")
        assert boosts.latest_calls == 1
        assert task.done() is False
        assert health is not None
        assert health.detail is not None
        assert health.detail["error_type"] == "DexScreenerTransportError"
    finally:
        shutdown.set()
        await asyncio.wait_for(task, timeout=2)


@pytest.mark.integration
async def test_runtime_graceful_shutdown_and_unexpected_worker_failure_are_durable(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = _settings()
    await _declare_epoch(session_factory, settings)
    logger = structlog.get_logger("test.collector")
    waiting = WaitingWorker()
    runtime = CollectorRuntime(
        session_factory, settings, logger=logger, epoch_number=1, worker=waiting
    )
    task = asyncio.create_task(runtime.run_until_stopped())
    await asyncio.sleep(0)
    runtime.request_shutdown("test_shutdown")
    await task
    assert waiting.stopped is True

    crashing = CollectorRuntime(
        session_factory,
        settings,
        logger=logger,
        epoch_number=1,
        worker=CollectorWorker(
            session_factory,
            settings,
            discovery=ExplodingDiscovery(),  # type: ignore[arg-type]
            availability=IdleAvailability(),  # type: ignore[arg-type]
            scheduler=IdleScheduler(),  # type: ignore[arg-type]
            polling=IdlePolling(),  # type: ignore[arg-type]
            logger=logger,
        ),
    )
    with pytest.raises(ExceptionGroup):
        await crashing.run_until_stopped()

    async with session_factory() as session:
        runs = list(
            (
                await session.execute(select(CollectorRun).order_by(CollectorRun.started_at))
            ).scalars()
        )
        health = await session.get(CollectorComponentHealth, "discovery")
    assert [run.status for run in runs] == ["stopped", "failed"]
    assert health is not None
    assert health.status == "failed"


@pytest.mark.integration
async def test_database_lock_prevents_two_live_collectors_sharing_one_api_budget(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = _settings()
    await _declare_epoch(session_factory, settings)
    logger = structlog.get_logger("test.collector-singleton")
    first = CollectorRuntime(
        session_factory,
        settings,
        logger=logger,
        epoch_number=1,
        worker=WaitingWorker(),
    )
    first_task = asyncio.create_task(first.run_until_stopped())
    try:
        for _ in range(200):
            async with session_factory() as session:
                running = await session.scalar(
                    select(func.count())
                    .select_from(CollectorRun)
                    .where(CollectorRun.status == "running")
                )
            if running == 1:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("first collector never started")

        second = CollectorRuntime(
            session_factory,
            settings,
            logger=logger,
            epoch_number=1,
            worker=WaitingWorker(),
        )
        with pytest.raises(CollectorAlreadyRunningError):
            await asyncio.wait_for(second.run_until_stopped(), timeout=1)
    finally:
        first.request_shutdown("test_complete")
        await asyncio.wait_for(first_task, timeout=2)
