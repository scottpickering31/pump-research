from __future__ import annotations

from decimal import Decimal

import httpx
import pytest

from pump_research.config import Settings
from pump_research.market_data.dexscreener import (
    DexScreenerClient,
    DexScreenerMetrics,
    DexScreenerResponseParseError,
)
from pump_research.market_data.rate_limiter import AsyncRateLimiter

TEST_BASE_URL = "https://dexscreener.test"


def _settings(*, max_attempts: int = 3) -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5433/pump_research",
        dex_screener_base_url=TEST_BASE_URL,
        dex_screener_requests_per_minute=300,
        dex_screener_max_attempts=max_attempts,
        dex_screener_retry_backoff_seconds=0,
    )


def _fast_limiter() -> AsyncRateLimiter:
    return AsyncRateLimiter(1_000_000)


def _sample_pair() -> dict[str, object]:
    return {
        "chainId": "solana",
        "dexId": "raydium",
        "pairAddress": "pair-address",
        "baseToken": {"address": "token-0", "name": "Token", "symbol": "TOK"},
        "quoteToken": {"address": "So111", "name": "Wrapped SOL", "symbol": "SOL"},
        "priceNative": "0.00001",
        "priceUsd": "0.00123",
        "txns": {"m5": {"buys": 2, "sells": 1}},
        "volume": {"m5": 12.5},
        "priceChange": {"m5": -1.25},
        "liquidity": {"usd": 1234.5},
        "fdv": 2000,
        "marketCap": 1900,
        "pairCreatedAt": 1_700_000_000_000,
    }


@pytest.mark.asyncio
async def test_thirty_addresses_issue_one_eligible_batched_request() -> None:
    addresses = [f"token-{index:02d}" for index in range(30)]
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[_sample_pair()])

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        metrics = DexScreenerMetrics()
        client = DexScreenerClient(
            _settings(),
            http_client=http,
            rate_limiter=_fast_limiter(),
            metrics=metrics,
        )

        result = await client.fetch_token_pairs(chain_id="solana", token_addresses=addresses)

    assert len(requests) == 1
    assert requests[0].url.path == f"/tokens/v1/solana/{','.join(addresses)}"
    assert result.requested_addresses == tuple(addresses)
    assert len(result.batches) == 1
    assert result.pairs[0].price_usd == Decimal("0.00123")
    assert metrics.batches_requested == 1
    assert metrics.addresses_requested == 30
    assert metrics.http_requests_started == 1
    assert metrics.http_requests_succeeded == 1


@pytest.mark.asyncio
async def test_thirty_one_addresses_issue_two_batched_requests() -> None:
    addresses = [f"token-{index:02d}" for index in range(31)]
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json=[])

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        client = DexScreenerClient(
            _settings(),
            http_client=http,
            rate_limiter=_fast_limiter(),
        )
        result = await client.fetch_token_pairs(chain_id="solana", token_addresses=addresses)

    assert len(requests) == 2
    assert len(result.batches[0].requested_addresses) == 30
    assert result.batches[1].requested_addresses == ("token-30",)


@pytest.mark.asyncio
async def test_throttled_request_retries_through_the_client() -> None:
    attempts = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, text="rate limited")
        return httpx.Response(200, json=[])

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        metrics = DexScreenerMetrics()
        client = DexScreenerClient(
            _settings(max_attempts=2),
            http_client=http,
            rate_limiter=_fast_limiter(),
            metrics=metrics,
        )
        result = await client.fetch_token_pairs(chain_id="solana", token_addresses=["token-0"])

    assert result.pairs == ()
    assert attempts == 2
    assert metrics.retries == 1
    assert metrics.http_requests_throttled == 1
    assert metrics.http_requests_succeeded == 1


@pytest.mark.asyncio
async def test_invalid_response_is_explicit_parse_error() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"pairs": []})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        metrics = DexScreenerMetrics()
        client = DexScreenerClient(
            _settings(),
            http_client=http,
            rate_limiter=_fast_limiter(),
            metrics=metrics,
        )
        with pytest.raises(DexScreenerResponseParseError, match="JSON array"):
            await client.fetch_token_pairs(chain_id="solana", token_addresses=["token-0"])

    assert metrics.parse_failures == 1
