from __future__ import annotations

import asyncio
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.dex_availability import DexAvailabilityWorkflow
from pump_research.config import Settings
from pump_research.discovery.contracts import DiscoveredToken
from pump_research.lifecycle.classifier import LifecycleClassifier
from pump_research.market_data.dexscreener import (
    DexScreenerBatchResult,
    DexScreenerTokenPairsResult,
)
from pump_research.market_data.dexscreener_models import DexScreenerPair
from pump_research.persistence.models import PollSchedule
from pump_research.persistence.repositories import (
    ApiRequestLogRepository,
    ObservationCreate,
    ObservationRepository,
    PairRepository,
    TokenRepository,
)
from pump_research.scheduling.policy import LifecycleState
from pump_research.scheduling.scheduler import AdaptiveScheduler

NOW = datetime(2026, 8, 25, 20, 15, 30, tzinfo=UTC)


@dataclass(slots=True)
class FakeClock:
    current: datetime = NOW

    def now(self) -> datetime:
        return self.current


class PresentDexSource:
    def __init__(self, received_at: datetime) -> None:
        self._received_at = received_at

    async def fetch_token_pairs(
        self,
        *,
        chain_id: str,
        token_addresses: list[str],
    ) -> DexScreenerTokenPairsResult:
        raw = tuple(
            {
                "chainId": chain_id,
                "dexId": "test-dex",
                "pairAddress": f"availability-pair-{address}",
                "baseToken": {"address": address, "name": "Test", "symbol": "TEST"},
                "quoteToken": {"address": "So111", "name": "SOL", "symbol": "SOL"},
            }
            for address in token_addresses
        )
        pairs = tuple(DexScreenerPair.model_validate(value) for value in raw)
        batch = DexScreenerBatchResult(
            chain_id=chain_id,
            requested_addresses=tuple(token_addresses),
            pairs=pairs,
            received_at=self._received_at,
            raw_response=raw,
        )
        return DexScreenerTokenPairsResult(
            chain_id=chain_id,
            requested_addresses=tuple(token_addresses),
            batches=(batch,),
        )


def _settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research",
        dex_availability_retry_seconds=60,
        dex_availability_lease_seconds=10,
    )


def _discovery_event(address: str) -> DiscoveredToken:
    return DiscoveredToken(
        chain="solana",
        address=address,
        source_name="test-discovery",
        source_event_id=address,
        event_type="token_created",
        source_event_at=NOW,
        received_at=NOW,
        source_payload={"mint": address},
        source_payload_sha256="a" * 64,
        idempotency_key=f"epoch10-capacity-{address}",
    )


@pytest.mark.integration
async def test_cross_workflow_capacity_cache_interleaving_keeps_transaction_owners(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Availability and scheduled lifecycle cannot recreate the Epoch 10 A/B cycle."""
    settings = _settings()
    clock = FakeClock()
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    availability_addresses = tuple(f"epoch10-availability-{index}" for index in range(3))
    availability = DexAvailabilityWorkflow(
        session_factory,
        PresentDexSource(NOW + timedelta(seconds=1)),
        settings,
        scheduler=scheduler,
    )
    for address in availability_addresses:
        await availability.admit_discovery(_discovery_event(address))

    token_repository = TokenRepository()
    pair_repository = PairRepository()
    scheduled_token_ids: list[uuid.UUID] = []
    scheduled_pairs = []
    async with session_factory() as session, session.begin():
        for index in range(2):
            token = await token_repository.get_or_create(
                session,
                chain="solana",
                address=f"epoch10-scheduled-{index}",
                first_discovered_at=NOW,
            )
            pair = await pair_repository.get_or_create(
                session,
                token_id=token.id,
                chain="solana",
                address=f"epoch10-scheduled-pair-{index}",
                dex_identifier="test-dex",
                first_discovered_at=NOW,
            )
            await scheduler.set_lifecycle_state_in_session(
                session,
                token_id=token.id,
                state=LifecycleState.NEW,
                decided_at=NOW,
                admitted_at=NOW,
                reason_code="epoch10_concurrency_setup",
            )
            scheduled_token_ids.append(token.id)
            scheduled_pairs.append(pair)

    observed_at = NOW + timedelta(seconds=1)
    async with session_factory() as session, session.begin():
        request = await ApiRequestLogRepository().record(
            session,
            idempotency_key="epoch10-scheduled-request",
            provider="test",
            endpoint="/test",
            requested_at=observed_at,
            received_at=observed_at,
            outcome="succeeded",
            http_status_code=200,
            request_payload={},
            response_payload={},
        )
        inserted = await ObservationRepository().record_many(
            session,
            api_request=request,
            observations=[
                ObservationCreate(pair_id=pair.id, volume_m5_usd=Decimal("100"))
                for pair in scheduled_pairs
            ],
        )
        assert inserted == 2

    classifier = LifecycleClassifier(
        session_factory,
        settings,
        clock=FakeClock(observed_at),
        scheduler=scheduler,
    )
    scheduler._cached_capacity_bucket = None
    scheduler._cached_capacity_decision = None
    planning_barrier = asyncio.Barrier(2)
    original_counts = scheduler._capacity_counts
    original_persist = scheduler._persist_capacity_decision
    planned_transactions: set[object] = set()
    attempted_by_transaction: defaultdict[object, list[uuid.UUID]] = defaultdict(list)

    async def synchronize_first_plan(
        session: AsyncSession,
        *,
        now: datetime,
    ) -> Any:
        transaction = session.get_transaction()
        assert transaction is not None
        counts = await original_counts(session, now=now)
        if transaction not in planned_transactions:
            planned_transactions.add(transaction)
            await planning_barrier.wait()
        return counts

    async def persist_and_replace_process_cache(
        session: AsyncSession,
        decision: Any,
    ) -> None:
        transaction = session.get_transaction()
        assert transaction is not None
        attempted_by_transaction[transaction].append(decision.id)
        await original_persist(session, decision)
        scheduler._cached_capacity_bucket = None
        scheduler._cached_capacity_decision = None

    scheduler._capacity_counts = synchronize_first_plan  # type: ignore[method-assign]
    scheduler._persist_capacity_decision = persist_and_replace_process_cache  # type: ignore[method-assign]

    async def evaluate_scheduled_request() -> Any:
        async with session_factory() as session, session.begin():
            return await classifier.evaluate_request_in_session(
                session,
                api_request_log_id=request.id,
            )

    availability_result, lifecycle_result = await asyncio.wait_for(
        asyncio.gather(
            availability.check_due(now=NOW),
            evaluate_scheduled_request(),
        ),
        timeout=5,
    )

    assert availability_result.promoted_new_tokens == len(availability_addresses)
    assert len(lifecycle_result.transitions) == len(scheduled_token_ids)
    assert len(attempted_by_transaction) == 2
    assert all(len(set(ids)) == 1 for ids in attempted_by_transaction.values())
    assert len({ids[0] for ids in attempted_by_transaction.values()}) == 2
    async with session_factory() as session:
        scheduled_capacity_ids = set(
            await session.scalars(
                select(PollSchedule.capacity_decision_id).where(
                    PollSchedule.token_id.in_(scheduled_token_ids)
                )
            )
        )
    assert len(scheduled_capacity_ids) == 1
