from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest

from pump_research.config import Settings
from pump_research.discovery.contracts import (
    DiscoveryCheckpoint,
    DiscoveryCoverageStatus,
    DiscoveryResponseParseError,
    DiscoverySourceError,
    TokenDiscoverySource,
)
from pump_research.discovery.pumpfun import (
    PUMP_FUN_SOURCE_NAME,
    PumpFunDiscoverySource,
    PumpFunHttpError,
)

TEST_BASE_URL = "https://pumpfun.test"


def _settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research",
        pump_fun_base_url=TEST_BASE_URL,
        pump_fun_api_token="test-discovery-token",
    )


def _latest_coin() -> dict[str, object]:
    return {
        "mint": "ExampleMint11111111111111111111111111111111111",
        "created_timestamp": 1_700_000_000_000,
        "name": "Example",
        "symbol": "EXAMPLE",
    }


def _accepts_any_discovery_source(source: TokenDiscoverySource) -> str:
    """Represents code outside discovery that only knows the abstract type."""
    return source.source_name


@pytest.mark.asyncio
async def test_pumpfun_source_emits_provider_neutral_discovery_event() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=_latest_coin(), headers={"ETag": "latest-v1"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        source = PumpFunDiscoverySource(_settings(), http_client=http)
        result = await source.fetch()

    assert _accepts_any_discovery_source(source) == PUMP_FUN_SOURCE_NAME
    assert len(requests) == 1
    assert requests[0].url.path == "/coins/latest"
    assert requests[0].headers["Accept"] == "application/json"
    assert requests[0].headers["Origin"] == "https://pump.fun"
    assert requests[0].headers["Authorization"] == "Bearer test-discovery-token"
    assert result.coverage.status == DiscoveryCoverageStatus.BEST_EFFORT
    assert result.coverage.supports_replay is False
    assert result.next_checkpoint == DiscoveryCheckpoint("latest-v1")
    assert len(result.events) == 1
    event = result.events[0]
    assert event.chain == "solana"
    assert event.address == _latest_coin()["mint"]
    assert event.source_event_at == datetime.fromtimestamp(1_700_000_000, tz=UTC)
    assert event.source_payload == _latest_coin()
    assert len(event.source_payload_sha256) == 64
    assert len(event.idempotency_key) == 64


@pytest.mark.asyncio
async def test_pumpfun_not_modified_response_retains_opaque_checkpoint() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(304)

    checkpoint = DiscoveryCheckpoint("latest-v1")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        source = PumpFunDiscoverySource(_settings(), http_client=http)
        result = await source.fetch(checkpoint)

    assert requests[0].headers["If-None-Match"] == "latest-v1"
    assert result.events == ()
    assert result.not_modified is True
    assert result.next_checkpoint == checkpoint
    assert source.metrics.requests_not_modified == 1


@pytest.mark.asyncio
async def test_pumpfun_malformed_response_is_never_silently_dropped() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"name": "missing mint"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        source = PumpFunDiscoverySource(_settings(), http_client=http)
        with pytest.raises(DiscoveryResponseParseError, match="malformed"):
            await source.fetch()

    assert source.metrics.parse_failures == 1


@pytest.mark.asyncio
async def test_pumpfun_http_error_is_explicit() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(401, text="unauthorized")

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        source = PumpFunDiscoverySource(_settings(), http_client=http)
        with pytest.raises(PumpFunHttpError) as error:
            await source.fetch()

    assert error.value.status_code == 401
    assert error.value.body_preview == "unauthorized"


@pytest.mark.asyncio
async def test_pumpfun_disconnect_is_explicit_and_does_not_advance_checkpoint() -> None:
    requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        raise httpx.ConnectError("simulated discovery disconnect", request=request)

    checkpoint = DiscoveryCheckpoint("durable-before-disconnect")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        source = PumpFunDiscoverySource(_settings(), http_client=http)
        with pytest.raises(DiscoverySourceError, match="request failed"):
            await source.fetch(checkpoint)

    assert requests == 1
    assert source.metrics.requests_failed == 1
