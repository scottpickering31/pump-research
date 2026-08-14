from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.dex_availability import DexAvailabilityWorkflow
from pump_research.config import Settings
from pump_research.discovery.contracts import DiscoveredToken
from pump_research.market_data.dexscreener import (
    DexScreenerBatchResult,
    DexScreenerTokenPairsResult,
)
from pump_research.market_data.dexscreener_models import DexScreenerPair
from pump_research.persistence.models import (
    ApiRequestLog,
    DexAvailabilityTask,
    DiscoveryEvent,
    LifecycleEvent,
    Token,
)
from pump_research.persistence.repositories import DexAvailabilityTaskRepository

NOW = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)


class FakeDexSource:
    """Mockable DEX boundary returning only explicitly configured matching pairs."""

    def __init__(self, *, present_addresses: set[str], received_at: datetime) -> None:
        self._present_addresses = present_addresses
        self._received_at = received_at
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    async def fetch_token_pairs(
        self,
        *,
        chain_id: str,
        token_addresses: list[str],
    ) -> DexScreenerTokenPairsResult:
        addresses = tuple(token_addresses)
        self.calls.append((chain_id, addresses))
        raw_pairs: list[dict[str, Any]] = [
            {
                "chainId": chain_id,
                "dexId": "test-dex",
                "pairAddress": f"pair-{address}",
                "baseToken": {"address": address, "name": "Test", "symbol": "TEST"},
                "quoteToken": {"address": "So111", "name": "SOL", "symbol": "SOL"},
            }
            for address in addresses
            if address in self._present_addresses
        ]
        pairs = tuple(DexScreenerPair.model_validate(pair) for pair in raw_pairs)
        batch = DexScreenerBatchResult(
            chain_id=chain_id,
            requested_addresses=addresses,
            pairs=pairs,
            received_at=self._received_at,
            raw_response=tuple(raw_pairs),
        )
        return DexScreenerTokenPairsResult(
            chain_id=chain_id,
            requested_addresses=addresses,
            batches=(batch,),
        )


def _settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research",
        dex_availability_retry_seconds=60,
        dex_availability_lease_seconds=10,
    )


def _discovery_event(address: str, received_at: datetime = NOW) -> DiscoveredToken:
    return DiscoveredToken(
        chain="solana",
        address=address,
        source_name="test-discovery",
        source_event_id=address,
        event_type="token_created",
        source_event_at=received_at,
        received_at=received_at,
        source_payload={"mint": address},
        source_payload_sha256="a" * 64,
        idempotency_key=f"discovery-{address}",
    )


@pytest.mark.integration
async def test_token_that_never_appears_is_retained_and_retried(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    never_listed = "never-listed-token"
    dex = FakeDexSource(present_addresses=set(), received_at=NOW + timedelta(seconds=1))
    workflow = DexAvailabilityWorkflow(session_factory, dex, _settings())

    admission = await workflow.admit_discovery(_discovery_event(never_listed))
    assert admission.state == "PENDING_DEX"
    assert admission.pending_task_created is True

    first_pass = await workflow.check_due(now=NOW)
    assert first_pass.checked_tokens == 1
    assert first_pass.retained_pending_tokens == 1
    assert first_pass.promoted_new_tokens == 0
    assert dex.calls == [("solana", (never_listed,))]

    second_pass = await workflow.check_due(now=NOW + timedelta(seconds=61))
    assert second_pass.checked_tokens == 1
    assert second_pass.retained_pending_tokens == 1

    async with session_factory() as session:
        task = await session.get(DexAvailabilityTask, admission.token_id)
        token_count = await session.scalar(select(func.count()).select_from(Token))
        discovery_count = await session.scalar(select(func.count()).select_from(DiscoveryEvent))
        request_outcomes = list(
            (
                await session.execute(
                    select(ApiRequestLog.outcome).order_by(ApiRequestLog.requested_at)
                )
            ).scalars()
        )
        lifecycle_states = list(
            (
                await session.execute(
                    select(LifecycleEvent.new_state).order_by(LifecycleEvent.decided_at)
                )
            ).scalars()
        )

    assert task is not None
    assert task.state == "PENDING_DEX"
    assert task.attempt_count == 2
    assert task.lease_id is None
    assert token_count == 1
    assert discovery_count == 1
    assert request_outcomes == ["empty", "empty"]
    assert lifecycle_states == ["PENDING_DEX", "PENDING_DEX", "PENDING_DEX"]


@pytest.mark.integration
async def test_due_pending_tokens_are_checked_in_one_dex_batch(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    addresses = ("batched-pending-one", "batched-pending-two")
    dex = FakeDexSource(present_addresses=set(), received_at=NOW + timedelta(seconds=1))
    workflow = DexAvailabilityWorkflow(session_factory, dex, _settings())
    for address in addresses:
        await workflow.admit_discovery(_discovery_event(address))

    result = await workflow.check_due(now=NOW)

    assert result.claimed_tokens == 2
    assert result.checked_tokens == 2
    assert result.retained_pending_tokens == 2
    assert dex.calls == [("solana", addresses)]


@pytest.mark.integration
async def test_restart_recovers_a_leased_pending_token_and_promotes_when_present(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    address = "appears-after-restart"
    first_process = DexAvailabilityWorkflow(
        session_factory,
        FakeDexSource(present_addresses=set(), received_at=NOW),
        _settings(),
    )
    admission = await first_process.admit_discovery(_discovery_event(address))

    task_repository = DexAvailabilityTaskRepository()
    async with session_factory() as session, session.begin():
        claims = await task_repository.claim_due(
            session,
            now=NOW,
            limit=30,
            lease_duration=timedelta(seconds=10),
        )
    assert [claim.address for claim in claims] == [address]

    dex_after_restart = FakeDexSource(
        present_addresses={address},
        received_at=NOW + timedelta(seconds=11),
    )
    restarted_process = DexAvailabilityWorkflow(session_factory, dex_after_restart, _settings())
    recovered = await restarted_process.check_due(now=NOW + timedelta(seconds=11))

    assert recovered.claimed_tokens == 1
    assert recovered.promoted_new_tokens == 1
    assert dex_after_restart.calls == [("solana", (address,))]

    async with session_factory() as session:
        task = await session.get(DexAvailabilityTask, admission.token_id)
        states = list(
            (
                await session.execute(
                    select(LifecycleEvent.new_state).order_by(LifecycleEvent.decided_at)
                )
            ).scalars()
        )

    assert task is not None
    assert task.state == "NEW"
    assert task.attempt_count == 1
    assert task.lease_id is None
    assert states == ["PENDING_DEX", "NEW"]
