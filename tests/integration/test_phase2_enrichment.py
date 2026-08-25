from __future__ import annotations

import asyncio
import base64
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, cast

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.boosts import BoostCollectionWorkflow
from pump_research.collection.market_context import MarketContextWorkflow
from pump_research.collection.security import SPL_TOKEN_PROGRAM, TokenSecurityWorkflow
from pump_research.config import Settings
from pump_research.market_data.dexscreener import DexScreenerBoostFeedResult
from pump_research.market_data.dexscreener_models import DexScreenerBoostFeedRecord
from pump_research.market_data.solana_rpc import (
    SolanaAccountResult,
    SolanaMultipleAccountsResult,
)
from pump_research.persistence.enrichment import (
    BoostCreate,
    BoostRepository,
    EnrichmentIdentityConflictError,
    MarketContextRepository,
    MetadataCreate,
    SecurityClaim,
    TokenMetadataRepository,
    TokenSecurityTaskRepository,
    latest_as_of,
)
from pump_research.persistence.models import (
    ApiRequestLog,
    BoostEvent,
    BoostObservation,
    MarketContextSnapshot,
    PollBatch,
    PollBatchMember,
    PollSchedule,
    TokenMetadataEvent,
    TokenSecuritySnapshot,
    TokenSecurityTask,
)
from pump_research.persistence.repositories import (
    ApiRequestLogRepository,
    CollectorRunRepository,
    TokenRepository,
)
from pump_research.scheduling.policy import LifecycleState
from pump_research.scheduling.scheduler import AdaptiveScheduler

NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)


def _market_context_values(run_id: uuid.UUID) -> dict[str, object]:
    return {
        "collection_epoch_id": uuid.UUID(int=0),
        "collector_run_id": run_id,
        "bucket_start": NOW - timedelta(minutes=5),
        "bucket_end": NOW,
        "source_observed_at": NOW,
        "received_at": NOW,
        "sol_usd_price": Decimal("123.4567890123456789012"),
        "sol_return_5m": Decimal("0.1234567890124"),
        "sol_realized_volatility_1h": Decimal("0.00000000000049"),
        "admitted_tokens": 87,
        "active_transitions": 11,
        "mature_cohort_tokens": 29,
        "mature_cohort_active_tokens": 7,
        "mature_cohort_active_fraction": Decimal(7) / Decimal(29),
        "pair_sample_count": 87,
        "aggregate_volume_m5_usd": Decimal("123456.1234564"),
        "aggregate_buys_m5": 321,
        "aggregate_sells_m5": 123,
        "policy_sha256": "f" * 64,
        "policy_snapshot": {"component": "market_context", "schema_version": 1},
    }


async def _subject(
    session_factory: async_sessionmaker[AsyncSession],
) -> tuple[uuid.UUID, uuid.UUID, uuid.UUID]:
    async with session_factory() as session, session.begin():
        run = await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="test",
            configuration_sha256="a" * 64,
            configuration_snapshot={},
        )
        token = await TokenRepository().get_or_create(
            session,
            chain="solana",
            address="phase2-token",
            first_discovered_at=NOW,
        )
        request = await ApiRequestLogRepository().record(
            session,
            collector_run_id=run.id,
            idempotency_key="phase2-request",
            provider="dexscreener",
            endpoint="/tokens/v1/{chain}/{tokens}",
            requested_at=NOW,
            received_at=NOW,
            outcome="succeeded",
            http_status_code=200,
            request_payload={},
            response_payload={"pairs": []},
            response_payload_sha256="b" * 64,
            failure_detail=None,
        )
        return run.id, token.id, request.id


@pytest.mark.integration
@pytest.mark.asyncio
async def test_metadata_history_is_change_deduplicated_and_as_of_safe(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id, token_id, request_id = await _subject(session_factory)
    repository = TokenMetadataRepository()
    async with session_factory() as session, session.begin():
        first = await repository.record_if_changed(
            session,
            token_id=token_id,
            pair_id=None,
            collector_run_id=run_id,
            api_request_log_id=request_id,
            discovery_event_id=None,
            provider="dexscreener",
            source_kind="boost_feed",
            source_observed_at=NOW + timedelta(days=1),
            received_at=NOW,
            source_record_locator="records[0]",
            source_record_sha256="c" * 64,
            metadata=MetadataCreate(name="Alpha", website_url=None),
        )
        unchanged = await repository.record_if_changed(
            session,
            token_id=token_id,
            pair_id=None,
            collector_run_id=run_id,
            api_request_log_id=request_id,
            discovery_event_id=None,
            provider="dexscreener",
            source_kind="boost_feed",
            source_observed_at=None,
            received_at=NOW + timedelta(seconds=10),
            source_record_locator="records[0]",
            source_record_sha256="d" * 64,
            metadata=MetadataCreate(name="Alpha", website_url=None),
        )
    assert first is not None
    assert unchanged is None

    async with session_factory() as session, session.begin():
        second_request = await ApiRequestLogRepository().record(
            session,
            collector_run_id=run_id,
            idempotency_key="phase2-request-2",
            provider="dexscreener",
            endpoint="/token-boosts/latest/v1",
            requested_at=NOW + timedelta(minutes=1),
            received_at=NOW + timedelta(minutes=1),
            outcome="succeeded",
            http_status_code=200,
            request_payload={},
            response_payload={"records": []},
            response_payload_sha256="e" * 64,
            failure_detail=None,
        )
        changed = await repository.record_if_changed(
            session,
            token_id=token_id,
            pair_id=None,
            collector_run_id=run_id,
            api_request_log_id=second_request.id,
            discovery_event_id=None,
            provider="dexscreener",
            source_kind="boost_feed",
            source_observed_at=NOW - timedelta(days=1),
            received_at=NOW + timedelta(minutes=1),
            source_record_locator="records[0]",
            source_record_sha256="f" * 64,
            metadata=MetadataCreate(name="Beta", website_url="https://example.test"),
        )
    assert changed is not None

    async with session_factory() as session:
        early = await latest_as_of(
            session,
            TokenMetadataEvent,
            token_id=token_id,
            as_of=NOW + timedelta(seconds=30),
        )
        late = await latest_as_of(
            session,
            TokenMetadataEvent,
            token_id=token_id,
            as_of=NOW + timedelta(minutes=2),
        )
        count = await session.scalar(select(func.count()).select_from(TokenMetadataEvent))
    assert early is not None and early.name == "Alpha"
    assert late is not None and late.name == "Beta"
    assert count == 2


@pytest.mark.integration
@pytest.mark.asyncio
async def test_four_concurrent_metadata_observers_create_one_change(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id, token_id, request_id = await _subject(session_factory)

    async def record() -> uuid.UUID | None:
        async with session_factory() as session, session.begin():
            event = await TokenMetadataRepository().record_if_changed(
                session,
                token_id=token_id,
                pair_id=None,
                collector_run_id=run_id,
                api_request_log_id=request_id,
                discovery_event_id=None,
                provider="dexscreener",
                source_kind="boost_feed",
                source_observed_at=None,
                received_at=NOW,
                source_record_locator="records[0]",
                source_record_sha256="c" * 64,
                metadata=MetadataCreate(name="Concurrent"),
            )
            return event.id if event is not None else None

    await asyncio.gather(*(record() for _ in range(4)))
    async with session_factory() as session:
        count = await session.scalar(select(func.count()).select_from(TokenMetadataEvent))
    assert count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_boost_changes_and_neutral_threshold_crossings_are_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id, token_id, request_id = await _subject(session_factory)
    repository = BoostRepository()
    async with session_factory() as session, session.begin():
        first = await repository.record_if_changed(
            session,
            token_id=token_id,
            pair_id=None,
            collector_run_id=run_id,
            api_request_log_id=request_id,
            provider="dexscreener",
            source_kind="latest_feed",
            feed_rank=1,
            source_observed_at=None,
            received_at=NOW,
            source_record_locator="records[0]",
            source_record_sha256="1" * 64,
            fact=BoostCreate(amount=Decimal("5"), total_amount=None),
        )
        duplicate = await repository.record_if_changed(
            session,
            token_id=token_id,
            pair_id=None,
            collector_run_id=run_id,
            api_request_log_id=request_id,
            provider="dexscreener",
            source_kind="latest_feed",
            feed_rank=1,
            source_observed_at=None,
            received_at=NOW,
            source_record_locator="records[0]",
            source_record_sha256="1" * 64,
            fact=BoostCreate(amount=Decimal("5"), total_amount=None),
        )
    assert first is not None
    assert duplicate is None

    async with session_factory() as session, session.begin():
        request = await ApiRequestLogRepository().record(
            session,
            collector_run_id=run_id,
            idempotency_key="phase2-request-boost-change",
            provider="dexscreener",
            endpoint="/token-boosts/latest/v1",
            requested_at=NOW + timedelta(minutes=1),
            received_at=NOW + timedelta(minutes=1),
            outcome="succeeded",
            http_status_code=200,
            request_payload={},
            response_payload={"records": []},
            response_payload_sha256="2" * 64,
            failure_detail=None,
        )
        await repository.record_if_changed(
            session,
            token_id=token_id,
            pair_id=None,
            collector_run_id=run_id,
            api_request_log_id=request.id,
            provider="dexscreener",
            source_kind="latest_feed",
            feed_rank=1,
            source_observed_at=None,
            received_at=NOW + timedelta(minutes=1),
            source_record_locator="records[0]",
            source_record_sha256="2" * 64,
            fact=BoostCreate(amount=Decimal("150"), total_amount=None),
        )
    async with session_factory() as session:
        observations = await session.scalar(select(func.count()).select_from(BoostObservation))
        events = list((await session.execute(select(BoostEvent))).scalars())
    assert observations == 2
    assert sum(event.event_type == "first_seen" for event in events) == 1
    assert sum(event.event_type == "state_change" for event in events) == 1
    assert {
        event.threshold_value
        for event in events
        if event.event_type == "threshold_crossing"
    } == {Decimal("10"), Decimal("100")}


class _FakeBoostFeed:
    async def fetch_boost_feed(self, *, feed_kind: str) -> DexScreenerBoostFeedResult:
        raw = (
            {
                "chainId": "solana",
                "tokenAddress": "phase2-token",
                "amount": 0,
                "totalAmount": 12,
            },
            {
                "chainId": "solana",
                "tokenAddress": "outside-research-cohort",
                "amount": 999,
                "totalAmount": 1000,
            },
        )
        return DexScreenerBoostFeedResult(
            feed_kind=feed_kind,
            records=tuple(DexScreenerBoostFeedRecord.model_validate(item) for item in raw),
            received_at=NOW,
            raw_response=raw,
        )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_boost_feed_retains_raw_but_does_not_admit_untracked_tokens(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id, token_id, _ = await _subject(session_factory)
    result = await BoostCollectionWorkflow(
        session_factory, _FakeBoostFeed()
    ).collect(feed_kind="latest", collector_run_id=run_id, requested_at=NOW)

    async with session_factory() as session:
        boost = await session.scalar(select(BoostObservation))
        request = await session.scalar(
            select(ApiRequestLog).where(
                ApiRequestLog.endpoint == "/token-boosts/latest/v1"
            )
        )
        token_count = await session.scalar(
            select(func.count()).select_from(TokenMetadataEvent)
        )
    assert result.source_records == 2
    assert result.tracked_records == 1
    assert boost is not None and boost.token_id == token_id
    assert boost.amount == Decimal("0")
    assert request is not None and request.response_payload is not None
    assert len(cast(list[object], request.response_payload["records"])) == 2
    assert token_count == 0


class _FakeSolana:
    async def get_multiple_accounts(
        self, *, addresses: list[str]
    ) -> SolanaMultipleAccountsResult:
        raw = bytearray(82)
        raw[36:44] = (1_000_000).to_bytes(8, "little")
        raw[44] = 6
        raw[45] = 1
        account: dict[str, Any] = {
            "owner": SPL_TOKEN_PROGRAM,
            "data": [base64.b64encode(raw).decode(), "base64"],
            "executable": False,
        }
        response: dict[str, Any] = {
            "jsonrpc": "2.0",
            "id": 1,
            "result": {"context": {"slot": 123}, "value": [account]},
        }
        return SolanaMultipleAccountsResult(
            addresses=tuple(addresses),
            slot=123,
            accounts=(SolanaAccountResult(addresses[0], account),),
            received_at=NOW,
            raw_response=response,
        )


class _GatedSolana(_FakeSolana):
    def __init__(self, *, rpc_started: asyncio.Event, parent_locked: asyncio.Event) -> None:
        self._rpc_started = rpc_started
        self._parent_locked = parent_locked

    async def get_multiple_accounts(
        self, *, addresses: list[str]
    ) -> SolanaMultipleAccountsResult:
        self._rpc_started.set()
        await self._parent_locked.wait()
        return await super().get_multiple_accounts(addresses=addresses)


class _ObservedTokenSecurityWorkflow(TokenSecurityWorkflow):
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        source: _GatedSolana,
        settings: Settings,
        *,
        persistence_lock_attempted: asyncio.Event,
    ) -> None:
        super().__init__(session_factory, source, settings)
        self._persistence_lock_attempted = persistence_lock_attempted

    async def _lock_claimed_tokens(
        self,
        session: AsyncSession,
        *,
        claims: tuple[SecurityClaim, ...],
    ) -> tuple[uuid.UUID, ...]:
        self._persistence_lock_attempted.set()
        return await super()._lock_claimed_tokens(session, claims=claims)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_security_schedule_is_finite_restart_safe_and_does_not_duplicate(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id, token_id, _ = await _subject(session_factory)
    async with session_factory() as session, session.begin():
        await TokenSecurityTaskRepository().create_if_absent(
            session, token_id=token_id, due_at=NOW
        )
    settings = Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research",
        token_security_lease_seconds=60,
    )
    first = TokenSecurityWorkflow(session_factory, _FakeSolana(), settings)
    assert (await first.collect_due(collector_run_id=run_id, now=NOW)).available == 1

    restarted = TokenSecurityWorkflow(session_factory, _FakeSolana(), settings)
    assert (await restarted.collect_due(collector_run_id=run_id, now=NOW)).claimed == 0
    async with session_factory() as session:
        snapshot_count = await session.scalar(
            select(func.count()).select_from(TokenSecuritySnapshot)
        )
        task = await session.get(TokenSecurityTask, token_id)
    assert snapshot_count == 1
    assert task is not None
    assert task.phase == 1
    assert task.next_due_at == NOW + timedelta(hours=1)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_security_persistence_and_parent_first_promotion_complete_without_deadlock(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id, token_id, _ = await _subject(session_factory)
    settings = Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research",
        token_security_lease_seconds=60,
    )
    scheduler = AdaptiveScheduler(session_factory, settings)
    async with session_factory() as session, session.begin():
        await scheduler.set_lifecycle_state_in_session(
            session,
            token_id=token_id,
            state=LifecycleState.NEW,
            decided_at=NOW,
            admitted_at=NOW,
            reason_code="security_deadlock_regression_setup",
        )
        await TokenSecurityTaskRepository().create_if_absent(
            session, token_id=token_id, due_at=NOW
        )

    rpc_started = asyncio.Event()
    parent_locked = asyncio.Event()
    persistence_lock_attempted = asyncio.Event()
    workflow = _ObservedTokenSecurityWorkflow(
        session_factory,
        _GatedSolana(rpc_started=rpc_started, parent_locked=parent_locked),
        settings,
        persistence_lock_attempted=persistence_lock_attempted,
    )

    async def promotion_style_transaction() -> None:
        await rpc_started.wait()
        async with session_factory() as session, session.begin():
            await scheduler.set_lifecycle_state_in_session(
                session,
                token_id=token_id,
                state=LifecycleState.NEW,
                decided_at=NOW + timedelta(seconds=1),
                reason_code="concurrent_security_deadlock_regression",
            )
            parent_locked.set()
            await persistence_lock_attempted.wait()
            task = await session.scalar(
                select(TokenSecurityTask)
                .where(TokenSecurityTask.token_id == token_id)
                .with_for_update()
            )
            assert task is not None

    collection, _ = await asyncio.wait_for(
        asyncio.gather(
            workflow.collect_due(collector_run_id=run_id, now=NOW),
            promotion_style_transaction(),
        ),
        timeout=5,
    )

    assert collection.available == 1
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(TokenSecuritySnapshot)) == 1
        task = await session.get(TokenSecurityTask, token_id)
    assert task is not None and task.phase == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_reverse_security_batches_lock_tokens_in_database_uuid_order(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    _, first_token_id, _ = await _subject(session_factory)
    async with session_factory() as session, session.begin():
        second_token = await TokenRepository().get_or_create(
            session,
            chain="solana",
            address="phase2-second-security-token",
            first_discovered_at=NOW,
        )
        await TokenSecurityTaskRepository().create_if_absent(
            session, token_id=first_token_id, due_at=NOW
        )
        await TokenSecurityTaskRepository().create_if_absent(
            session, token_id=second_token.id, due_at=NOW
        )
    ordered_token_ids = tuple(sorted((first_token_id, second_token.id)))
    claims_by_token_id = {
        token_id: SecurityClaim(token_id, str(token_id), 0, uuid.uuid4())
        for token_id in ordered_token_ids
    }
    forward = tuple(claims_by_token_id[token_id] for token_id in ordered_token_ids)
    reverse = tuple(reversed(forward))
    workflow = TokenSecurityWorkflow(
        session_factory,
        _FakeSolana(),
        Settings(
            database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research"
        ),
    )
    start = asyncio.Barrier(2)

    async def lock_batch(claims: tuple[SecurityClaim, ...]) -> tuple[uuid.UUID, ...]:
        await start.wait()
        async with session_factory() as session, session.begin():
            locked = await workflow._lock_claimed_tokens(session, claims=claims)
            for claim in claims:
                task = await session.scalar(
                    select(TokenSecurityTask)
                    .where(TokenSecurityTask.token_id == claim.token_id)
                    .with_for_update()
                )
                assert task is not None
            return locked

    locked_orders = await asyncio.wait_for(
        asyncio.gather(lock_batch(forward), lock_batch(reverse)),
        timeout=5,
    )

    assert len(locked_orders) == 2
    assert all(locked_order == ordered_token_ids for locked_order in locked_orders)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_security_parent_locks_are_compatible_with_reverse_poll_member_fk_locks(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id, first_created_id, _ = await _subject(session_factory)
    settings = Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research"
    )
    scheduler = AdaptiveScheduler(session_factory, settings)
    async with session_factory() as session, session.begin():
        second_token = await TokenRepository().get_or_create(
            session,
            chain="solana",
            address="phase2-epoch7-deadlock-token",
            first_discovered_at=NOW,
        )
        token_ids = tuple(sorted((first_created_id, second_token.id)))
        for token_id in token_ids:
            await scheduler.set_lifecycle_state_in_session(
                session,
                token_id=token_id,
                state=LifecycleState.NEW,
                decided_at=NOW,
                admitted_at=NOW,
                reason_code="epoch7_deadlock_regression_setup",
            )
        higher_uuid_schedule = await session.get(PollSchedule, token_ids[1])
        assert higher_uuid_schedule is not None
        higher_uuid_schedule.priority = 1

    claims = tuple(
        SecurityClaim(token_id, str(token_id), 0, uuid.uuid4()) for token_id in token_ids
    )
    workflow = TokenSecurityWorkflow(session_factory, _FakeSolana(), settings)
    first_parent_locked = asyncio.Event()
    second_member_flushed = asyncio.Event()
    all_parent_locks_attempted = asyncio.Event()
    first_member_flushed = asyncio.Event()
    batch_id = uuid.uuid4()

    async def security_persistence_transaction() -> None:
        async with session_factory() as session, session.begin():
            await workflow._lock_claimed_tokens(session, claims=claims[:1])
            first_parent_locked.set()
            await second_member_flushed.wait()
            all_parent_locks_attempted.set()
            await workflow._lock_claimed_tokens(session, claims=claims)
            await first_member_flushed.wait()

    async def poll_batch_transaction() -> None:
        await first_parent_locked.wait()
        async with session_factory() as session, session.begin():
            schedules = list(
                (
                    await session.scalars(
                        select(PollSchedule)
                        .where(PollSchedule.token_id.in_(token_ids))
                        .order_by(
                            PollSchedule.priority,
                            PollSchedule.next_due_at,
                            PollSchedule.token_id,
                        )
                        .with_for_update()
                    )
                ).all()
            )
            assert len(schedules) == 2
            assert tuple(schedule.token_id for schedule in schedules) == tuple(
                reversed(token_ids)
            )
            batch = PollBatch(
                id=batch_id,
                collector_run_id=run_id,
                provider="dexscreener",
                chain="solana",
                claimed_at=NOW,
                lease_expires_at=NOW + timedelta(seconds=30),
                reserved_request_capacity=1,
                batch_kind="ordinary",
                control_window_start=None,
                capacity_decision_id=None,
                configuration_sha256="e" * 64,
                configuration_snapshot={},
            )
            session.add(batch)
            await session.flush()

            try:
                for index, schedule in enumerate(schedules):
                    due_at = schedule.next_due_at
                    assert due_at is not None
                    session.add(
                        PollBatchMember(
                            claimed_at=NOW,
                            batch_id=batch.id,
                            token_id=schedule.token_id,
                            due_at=due_at,
                            lifecycle_state=schedule.lifecycle_state,
                            coverage_class=schedule.coverage_class,
                            priority=schedule.priority,
                            claim_lateness_ms=0,
                            capacity_decision_id=schedule.capacity_decision_id,
                            target_interval_seconds=schedule.target_interval_seconds,
                            effective_interval_seconds=schedule.effective_interval_seconds,
                            previous_batch_id=schedule.lease_id,
                        )
                    )
                    schedule.lease_id = batch.id
                    schedule.lease_expires_at = batch.lease_expires_at
                    schedule.last_started_at = NOW
                    schedule.updated_at = NOW
                    await session.flush()
                    if index == 0:
                        second_member_flushed.set()
                        await all_parent_locks_attempted.wait()
            finally:
                first_member_flushed.set()

    await asyncio.wait_for(
        asyncio.gather(security_persistence_transaction(), poll_batch_transaction()),
        timeout=5,
    )

    async with session_factory() as session:
        member_token_ids = set(
            (
                await session.scalars(
                    select(PollBatchMember.token_id).where(PollBatchMember.batch_id == batch_id)
                )
            ).all()
        )
    assert member_token_ids == set(token_ids)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_market_context_postgres_numeric_rounding_is_restart_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id, _, _ = await _subject(session_factory)
    repository = MarketContextRepository()
    values = _market_context_values(run_id)

    async with session_factory() as session, session.begin():
        first = await repository.record(session, **values)

    assert first.sol_usd_price == Decimal("123.456789012345678901")
    assert first.sol_return_5m == Decimal("0.123456789012")
    assert first.sol_realized_volatility_1h == Decimal("0.000000000000")
    assert first.mature_cohort_active_fraction == Decimal("0.241379310345")
    assert first.aggregate_volume_m5_usd == Decimal("123456.123456")

    async with session_factory() as session, session.begin():
        restarted = await repository.record(session, **values)

    async with session_factory() as session:
        count = await session.scalar(select(func.count()).select_from(MarketContextSnapshot))
    assert restarted.id == first.id
    assert count == 1


@pytest.mark.integration
@pytest.mark.asyncio
async def test_market_context_rejects_genuinely_different_numeric_content(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id, _, _ = await _subject(session_factory)
    repository = MarketContextRepository()
    values = _market_context_values(run_id)
    async with session_factory() as session, session.begin():
        await repository.record(session, **values)

    changed = {**values, "sol_return_5m": Decimal("0.123456789014")}
    with pytest.raises(
        EnrichmentIdentityConflictError,
        match=(
            r"^market context bucket identity maps to different content: sol_return_5m$"
        ),
    ):
        async with session_factory() as session, session.begin():
            await repository.record(session, **changed)


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "changed_value"),
    [
        ("admitted_tokens", 88),
        (
            "policy_snapshot",
            {"component": "market_context", "schema_version": 1, "bucket_seconds": 60},
        ),
    ],
)
async def test_market_context_rejects_genuinely_different_integer_or_content_fields(
    session_factory: async_sessionmaker[AsyncSession],
    field: str,
    changed_value: object,
) -> None:
    run_id, _, _ = await _subject(session_factory)
    repository = MarketContextRepository()
    values = _market_context_values(run_id)
    async with session_factory() as session, session.begin():
        await repository.record(session, **values)

    changed = {**values, field: changed_value}
    with pytest.raises(
        EnrichmentIdentityConflictError,
        match=rf"^market context bucket identity maps to different content: {field}$",
    ):
        async with session_factory() as session, session.begin():
            await repository.record(session, **changed)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_market_context_bucket_and_policy_define_distinct_identities(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id, _, _ = await _subject(session_factory)
    repository = MarketContextRepository()
    values = _market_context_values(run_id)
    next_bucket = {
        **values,
        "bucket_start": NOW,
        "bucket_end": NOW + timedelta(minutes=5),
        "source_observed_at": NOW + timedelta(minutes=5),
        "received_at": NOW + timedelta(minutes=5),
    }
    changed_policy = {
        **values,
        "policy_sha256": "e" * 64,
        "policy_snapshot": {
            "component": "market_context",
            "schema_version": 2,
        },
    }

    async with session_factory() as session, session.begin():
        original = await repository.record(session, **values)
        later = await repository.record(session, **next_bucket)
        revised = await repository.record(session, **changed_policy)

    async with session_factory() as session:
        count = await session.scalar(select(func.count()).select_from(MarketContextSnapshot))
    assert len({original.id, later.id, revised.id}) == 3
    assert count == 3


@pytest.mark.integration
@pytest.mark.asyncio
async def test_market_context_closed_bucket_is_restart_idempotent(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    run_id, _, _ = await _subject(session_factory)
    settings = Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research"
    )
    workflow = MarketContextWorkflow(session_factory, settings)

    first = await workflow.record_closed_bucket(collector_run_id=run_id, now=NOW)
    second = await workflow.record_closed_bucket(collector_run_id=run_id, now=NOW)

    async with session_factory() as session:
        count = await session.scalar(select(func.count()).select_from(MarketContextSnapshot))
    assert first.id == second.id
    assert count == 1
    assert first.source_observed_at == NOW
    assert first.sol_usd_price is None
    assert first.mature_cohort_active_fraction is None
