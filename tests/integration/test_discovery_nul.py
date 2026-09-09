from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.dex_availability import DexAvailabilityWorkflow, DiscoveryAdmission
from pump_research.collection.discovery import DiscoveryCoordinator
from pump_research.config import Settings
from pump_research.discovery.contracts import DiscoveredToken, DiscoveryBatch
from pump_research.discovery.pumpportal import PumpPortalDiscoverySource, PumpPortalWebSocket
from pump_research.market_data.dexscreener import DexScreenerTokenPairsResult
from pump_research.persistence.models import (
    DexAvailabilityTask,
    DiscoveryEvent,
    DiscoveryRejectedMessage,
    Token,
    TokenMetadataEvent,
)
from pump_research.persistence.repositories import DiscoveryEventRepository, TokenRepository

NOW = datetime(2026, 9, 7, 8, 45, 18, tzinfo=UTC)
RAW_MESSAGE = r'{"mint":"nul-test-mint","name":"WORLD BTC \u0000","symbol":"BTC"}'


class _Socket:
    def __init__(self, messages: list[str | bytes]) -> None:
        self.messages = messages
        self.closed = asyncio.Event()

    async def send(self, message: str) -> None:
        assert json.loads(message) == {"method": "subscribeNewToken"}

    async def recv(self) -> str | bytes:
        if self.messages:
            return self.messages.pop(0)
        await self.closed.wait()
        raise ConnectionError("closed")

    async def close(self) -> None:
        self.closed.set()


class _NoDexCalls:
    async def fetch_token_pairs(
        self,
        *,
        chain_id: str,
        token_addresses: list[str],
    ) -> DexScreenerTokenPairsResult:
        raise AssertionError("This test must never make an external request")


def _pipeline(
    session_factory: async_sessionmaker[AsyncSession],
    messages: list[str | bytes],
    *,
    batch_size: int = 100,
) -> tuple[PumpPortalDiscoverySource, DexAvailabilityWorkflow]:
    socket = _Socket(messages)

    @asynccontextmanager
    async def connect(
        url: str,
        max_queue: int,
        open_timeout: float,
    ) -> AsyncIterator[PumpPortalWebSocket]:
        yield socket

    settings = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://unused:unused@localhost/unused_test",
        pumpportal_api_key="fake-only",
        pumpportal_batch_size=batch_size,
        pumpportal_fetch_wait_seconds=0.1,
    )
    source = PumpPortalDiscoverySource(settings, connection_factory=connect, now=lambda: NOW)
    return source, DexAvailabilityWorkflow(session_factory, _NoDexCalls(), settings)


@pytest.mark.integration
async def test_original_payload_fails_in_postgresql_discovery_events_jsonb(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Keep the real pre-fix INSERT failure reproducible, without a mocked DB error."""
    payload = json.loads(RAW_MESSAGE)
    assert payload["name"] == "WORLD BTC \x00"
    async with session_factory() as session, session.begin():
        token = await TokenRepository().get_or_create(
            session, chain="solana", address=payload["mint"], first_discovered_at=None
        )
        with pytest.raises(DBAPIError, match="unsupported Unicode escape sequence") as failure:
            async with session.begin_nested():
                await DiscoveryEventRepository().record(
                    session,
                    token_id=token.id,
                    idempotency_key="original-nul-failure",
                    provider="pumpportal",
                    provider_event_id=payload["mint"],
                    event_type="token_created",
                    source_event_at=None,
                    received_at=NOW,
                    source_payload=payload,
                    source_payload_sha256="a" * 64,
                )
        assert getattr(failure.value.orig, "sqlstate", None) == "22P05"
        assert "cannot be converted to text" in str(failure.value)


@pytest.mark.integration
async def test_nul_messages_commit_locally_and_following_discovery_survives_taskgroup(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    nested = json.dumps({"mint": "nested-mint", "extra": [{"nul\x00key": ["\x00"]}]}).encode()
    good = {"mint": "following-mint", "name": "Good", "symbol": "OK", "uri": "https://test"}
    source, admission = _pipeline(
        session_factory,
        [RAW_MESSAGE, nested, json.dumps(good)],
        batch_size=2,
    )
    coordinator = DiscoveryCoordinator(session_factory, source, admission)
    finished = asyncio.Event()
    sibling_finished = False

    async def collect_messages() -> None:
        rejected = await coordinator.run_once()
        assert len(rejected.rejected_messages) == 2
        accepted = await coordinator.run_once()
        assert len(accepted.events) == 1
        finished.set()

    async def sibling() -> None:
        nonlocal sibling_finished
        await finished.wait()
        sibling_finished = True

    try:
        async with asyncio.timeout(5), asyncio.TaskGroup() as group:
            group.create_task(collect_messages())
            group.create_task(sibling())
        assert sibling_finished
        assert source.metrics.disconnects == 0
    finally:
        await source.aclose()

    async with session_factory() as session:
        rejected_rows = (await session.scalars(select(DiscoveryRejectedMessage))).all()
        assert {row.raw_message for row in rejected_rows} == {RAW_MESSAGE.encode(), nested}
        for row in rejected_rows:
            assert row.raw_message_sha256 == hashlib.sha256(row.raw_message).hexdigest()
            assert row.reason_code == "postgresql_nul_string"
            assert row.endpoint == "websocket:subscribeNewToken"
            assert row.schema_version == 1
            assert row.received_at == NOW
            assert row.persisted_at >= NOW
        assert {row.message_encoding for row in rejected_rows} == {"binary", "utf8-surrogatepass"}
        discovery = (await session.scalars(select(DiscoveryEvent))).one()
        assert discovery.source_payload == good
        assert (
            discovery.source_payload_sha256
            == hashlib.sha256(
                json.dumps(good, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
            ).hexdigest()
        )
        assert (await session.scalars(select(Token.address))).all() == ["following-mint"]
        assert await session.scalar(select(func.count()).select_from(DexAvailabilityTask)) == 1
        metadata = (await session.scalars(select(TokenMetadataEvent))).one()
        assert (metadata.name, metadata.symbol, metadata.metadata_uri) == (
            "Good",
            "OK",
            "https://test",
        )


@pytest.mark.integration
async def test_quarantine_commit_before_ack_is_idempotent_on_redelivery(
    session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, admission = _pipeline(session_factory, [RAW_MESSAGE])
    coordinator = DiscoveryCoordinator(session_factory, source, admission)
    acknowledge = source.acknowledge

    async def interrupted_ack(batch: DiscoveryBatch) -> None:
        raise RuntimeError("simulated interruption after commit")

    monkeypatch.setattr(source, "acknowledge", interrupted_ack)
    try:
        with pytest.raises(RuntimeError, match="interruption after commit"):
            await coordinator.run_once()
        async with session_factory() as session:
            first = (await session.scalars(select(DiscoveryRejectedMessage))).one()
        monkeypatch.setattr(source, "acknowledge", acknowledge)
        await coordinator.run_once()
        async with session_factory() as session:
            replay = (await session.scalars(select(DiscoveryRejectedMessage))).one()
            assert replay.id == first.id
            assert replay.persisted_at == first.persisted_at
    finally:
        await source.aclose()


@pytest.mark.integration
async def test_unrelated_database_error_propagates_and_whole_batch_can_retry(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    source, admission = _pipeline(session_factory, [RAW_MESSAGE, '{"mint":"good-after-bad"}'])

    class FailingAdmission:
        fail = True

        async def admit_discovery_in_session(
            self,
            session: AsyncSession,
            event: DiscoveredToken,
            collector_run_id: uuid.UUID | None = None,
        ) -> DiscoveryAdmission:
            result = await admission.admit_discovery_in_session(session, event, collector_run_id)
            if self.fail:
                await session.execute(text("SELECT CAST('not-an-integer' AS integer)"))
            return result

    sink = FailingAdmission()
    coordinator = DiscoveryCoordinator(session_factory, source, sink)
    try:
        with pytest.raises(DBAPIError, match="invalid input syntax for type integer"):
            await coordinator.run_once()
        async with session_factory() as session:
            count = await session.scalar(select(func.count()).select_from(DiscoveryRejectedMessage))
            assert count == 0
            assert await session.scalar(select(func.count()).select_from(Token)) == 0
        pending = await source.fetch()
        assert len(pending.rejected_messages) == len(pending.events) == 1
        sink.fail = False
        assert await coordinator.run_once() is pending
        async with session_factory() as session:
            count = await session.scalar(select(func.count()).select_from(DiscoveryRejectedMessage))
            assert count == 1
            assert await session.scalar(select(func.count()).select_from(DiscoveryEvent)) == 1
    finally:
        await source.aclose()
