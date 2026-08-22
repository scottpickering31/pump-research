from __future__ import annotations

import asyncio
import json
from collections.abc import Sequence
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from types import TracebackType
from urllib.parse import parse_qs, urlsplit

import pytest
from pydantic import ValidationError

from pump_research.config import Settings
from pump_research.discovery.contracts import (
    DiscoveryConnectivityEventType,
    DiscoveryCoverageStatus,
    DiscoveryResponseParseError,
    DiscoverySourceError,
    TokenDiscoverySource,
)
from pump_research.discovery.pumpportal import (
    PUMPPORTAL_SOURCE_NAME,
    PumpPortalConfigurationError,
    PumpPortalDiscoverySource,
    PumpPortalWebSocket,
)

NOW = datetime(2026, 8, 15, 12, 0, tzinfo=UTC)


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": (
            "postgresql+asyncpg://researcher:password@localhost:5433/pump_research"
        ),
        "pumpportal_api_key": "test-api-key",
        "pumpportal_websocket_url": "wss://pumpportal.test/api/data",
        "pumpportal_fetch_wait_seconds": 0.05,
        "pumpportal_reconnect_initial_seconds": 0.01,
        "pumpportal_reconnect_max_seconds": 0.1,
    }
    values.update(overrides)
    return Settings.model_validate(values)


def _new_token(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "signature": "5signature111111111111111111111111111111111111111111111111111111111",
        "mint": "ExampleMint11111111111111111111111111111111111",
        "traderPublicKey": "Creator111111111111111111111111111111111111",
        "txType": "create",
        "initialBuy": 12345,
        "bondingCurveKey": "Curve1111111111111111111111111111111111111",
        "vTokensInBondingCurve": 1,
        "vSolInBondingCurve": 2,
        "marketCapSol": 3,
        "name": "Example",
        "symbol": "EX",
        "uri": "https://example.invalid/metadata.json",
        "pool": "pump",
        "timestamp": "2026-08-15T11:59:59Z",
    }
    payload.update(overrides)
    return payload


class FakeSocket(PumpPortalWebSocket):
    def __init__(self, messages: Sequence[str | bytes | BaseException]) -> None:
        self._messages = list(messages)
        self._closed = asyncio.Event()
        self.sent: list[str] = []
        self.close_calls = 0

    async def send(self, message: str) -> None:
        self.sent.append(message)

    async def recv(self) -> str | bytes:
        if self._messages:
            message = self._messages.pop(0)
            if isinstance(message, BaseException):
                raise message
            return message
        await self._closed.wait()
        raise ConnectionError("socket closed")

    async def close(self) -> None:
        self.close_calls += 1
        self._closed.set()


class _SocketContext(AbstractAsyncContextManager[PumpPortalWebSocket]):
    def __init__(self, socket: FakeSocket) -> None:
        self.socket = socket

    async def __aenter__(self) -> PumpPortalWebSocket:
        return self.socket

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        del exc_type, exc_value, traceback


class FakeConnectionFactory:
    def __init__(self, sockets: Sequence[FakeSocket]) -> None:
        self._sockets = list(sockets)
        self.calls: list[tuple[str, int, float]] = []

    def __call__(
        self, url: str, max_queue: int, open_timeout: float
    ) -> AbstractAsyncContextManager[PumpPortalWebSocket]:
        self.calls.append((url, max_queue, open_timeout))
        if not self._sockets:
            raise ConnectionError("no more fake connections")
        return _SocketContext(self._sockets.pop(0))


async def _no_wait(_: float) -> None:
    await asyncio.sleep(0)


@pytest.mark.asyncio
async def test_valid_new_token_event_maps_to_provider_neutral_contract() -> None:
    payload = _new_token()
    socket = FakeSocket(
        [
            json.dumps({"message": "Successfully subscribed to new token events."}),
            json.dumps(payload),
        ]
    )
    factory = FakeConnectionFactory([socket])
    source = PumpPortalDiscoverySource(
        _settings(), connection_factory=factory, now=lambda: NOW
    )
    try:
        batch = await source.fetch()
    finally:
        await source.aclose()

    abstract_source: TokenDiscoverySource = source
    assert abstract_source.source_name == PUMPPORTAL_SOURCE_NAME
    assert json.loads(socket.sent[0]) == {"method": "subscribeNewToken"}
    assert len(factory.calls) == 1
    query = parse_qs(urlsplit(factory.calls[0][0]).query)
    assert query == {"api-key": ["test-api-key"]}
    assert batch.coverage.status is DiscoveryCoverageStatus.BEST_EFFORT
    assert batch.coverage.supports_replay is False
    assert batch.next_checkpoint is None
    assert len(batch.events) == 1
    event = batch.events[0]
    assert event.chain == "solana"
    assert event.address == payload["mint"]
    assert event.source_name == "pumpportal"
    assert event.source_event_id == payload["signature"]
    assert event.source_event_at == datetime(2026, 8, 15, 11, 59, 59, tzinfo=UTC)
    assert event.received_at == NOW
    assert event.source_payload == payload
    assert len(event.source_payload_sha256) == 64


@pytest.mark.asyncio
async def test_malformed_event_is_reported_not_silently_dropped() -> None:
    socket = FakeSocket([json.dumps({"name": "missing mint"})])
    source = PumpPortalDiscoverySource(
        _settings(), connection_factory=FakeConnectionFactory([socket]), now=lambda: NOW
    )
    try:
        with pytest.raises(DiscoveryResponseParseError, match="malformed"):
            await source.fetch()
        assert source.metrics.parse_failures == 1
    finally:
        await source.aclose()


@pytest.mark.asyncio
async def test_duplicate_messages_have_the_same_durable_idempotency_key() -> None:
    message = json.dumps(_new_token())
    socket = FakeSocket([message, message])
    source = PumpPortalDiscoverySource(
        _settings(), connection_factory=FakeConnectionFactory([socket]), now=lambda: NOW
    )
    try:
        batch = await source.fetch()
    finally:
        await source.aclose()

    assert len(batch.events) == 2
    assert batch.events[0].idempotency_key == batch.events[1].idempotency_key


@pytest.mark.asyncio
async def test_disconnect_reconnects_and_resubscribes_with_bounded_backoff() -> None:
    first = FakeSocket([ConnectionError("simulated disconnect")])
    second = FakeSocket([json.dumps(_new_token(signature="second-signature"))])
    factory = FakeConnectionFactory([first, second])
    delays: list[float] = []

    async def record_delay(delay: float) -> None:
        delays.append(delay)
        await asyncio.sleep(0)

    source = PumpPortalDiscoverySource(
        _settings(),
        connection_factory=factory,
        now=lambda: NOW,
        sleep=record_delay,
        random_value=lambda: 0.5,
    )
    try:
        batches = [await source.fetch()]
        await source.acknowledge(batches[-1])
        while not any(batch.events for batch in batches):
            batches.append(await source.fetch())
            await source.acknowledge(batches[-1])
    finally:
        await source.aclose()

    assert len(factory.calls) == 2
    assert first.sent == second.sent == [_SUBSCRIBE_MESSAGE]
    assert delays == [pytest.approx(0.011)]
    connectivity = [event for batch in batches for event in batch.connectivity_events]
    assert [event.event_type for event in connectivity] == [
        DiscoveryConnectivityEventType.DISCONNECTED,
        DiscoveryConnectivityEventType.RECONNECTED,
    ]
    assert connectivity[0].gap_id == connectivity[1].gap_id
    assert source.metrics.reconnections == 1


@pytest.mark.asyncio
async def test_unacknowledged_live_batch_is_redelivered_until_durable_commit() -> None:
    first_message = json.dumps(_new_token(signature="first-signature"))
    second_message = json.dumps(_new_token(signature="second-signature"))
    socket = FakeSocket([first_message, second_message])
    source = PumpPortalDiscoverySource(
        _settings(pumpportal_batch_size=1),
        connection_factory=FakeConnectionFactory([socket]),
        now=lambda: NOW,
    )
    try:
        first = await source.fetch()
        redelivery = await source.fetch()
        assert redelivery is first

        await source.acknowledge(first)
        second = await source.fetch()
    finally:
        await source.aclose()

    assert first.events[0].source_event_id == "first-signature"
    assert second.events[0].source_event_id == "second-signature"


@pytest.mark.asyncio
async def test_shutdown_closes_socket_and_reader_task() -> None:
    socket = FakeSocket([])
    source = PumpPortalDiscoverySource(
        _settings(), connection_factory=FakeConnectionFactory([socket]), now=lambda: NOW
    )
    fetch_task = asyncio.create_task(source.fetch())
    for _ in range(10):
        if socket.sent:
            break
        await asyncio.sleep(0)
    assert socket.sent == [_SUBSCRIBE_MESSAGE]
    await source.aclose()
    await asyncio.gather(fetch_task, return_exceptions=True)

    assert socket.close_calls == 1
    with pytest.raises(DiscoverySourceError, match="closed"):
        await source.fetch()


def test_missing_api_key_fails_before_a_connection_is_created() -> None:
    settings = Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research",
        pumpportal_api_key=None,
    )
    with pytest.raises(PumpPortalConfigurationError, match="required"):
        PumpPortalDiscoverySource(settings)


def test_blank_api_key_and_embedded_credentials_are_rejected() -> None:
    with pytest.raises(ValidationError, match="must not be blank"):
        _settings(pumpportal_api_key="  ")
    with pytest.raises(ValidationError, match="separately"):
        _settings(pumpportal_websocket_url="wss://pumpportal.test/api/data?api-key=secret")


_SUBSCRIBE_MESSAGE = json.dumps({"method": "subscribeNewToken"}, separators=(",", ":"))
