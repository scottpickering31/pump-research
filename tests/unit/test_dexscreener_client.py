from __future__ import annotations

from decimal import Decimal

import httpx
import pytest

from pump_research.config import Settings
from pump_research.market_data.dexscreener import (
    DexScreenerClient,
    DexScreenerError,
    DexScreenerMetrics,
    DexScreenerResponseParseError,
    DexScreenerTransportError,
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
        "txns": {
            "m5": {"buys": 2, "sells": 1},
            "h6": {"buys": 23, "sells": 11},
            "h24": {"buys": 50, "sells": 40},
        },
        "volume": {"m5": 12.5},
        "priceChange": {"m5": -1.25},
        "liquidity": {"usd": 1234.5, "base": "1200000", "quote": "4.5"},
        "fdv": 2000,
        "marketCap": 1900,
        "pairCreatedAt": 1_700_000_000_000,
        "info": {
            "imageUrl": "https://example.test/token.png",
            "websites": [{"label": "site", "url": "https://example.test"}],
            "socials": [{"platform": "twitter", "handle": "example"}],
        },
        "boosts": {"active": 0},
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
    assert result.pairs[0].txns["h6"].buys == 23
    assert result.pairs[0].txns["h24"].sells == 40
    assert result.pairs[0].liquidity is not None
    assert result.pairs[0].liquidity.base == Decimal("1200000")
    assert result.pairs[0].pair_created_at == 1_700_000_000_000
    assert result.pairs[0].boosts is not None
    assert result.pairs[0].boosts.active == 0
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
    assert result.batches[0].attempt_count == 2
    assert attempts == 2
    assert metrics.retries == 1
    assert metrics.http_requests_throttled == 1
    assert metrics.http_requests_succeeded == 1


@pytest.mark.asyncio
async def test_token_pair_transport_failure_retries_then_surfaces_domain_error() -> None:
    attempts = 0
    failures: list[httpx.ReadTimeout] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        failure = httpx.ReadTimeout("simulated timeout", request=request)
        failures.append(failure)
        raise failure

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
        with pytest.raises(
            DexScreenerTransportError,
            match=r"transport failed after 2 attempt\(s\)",
        ) as captured:
            await client.fetch_token_pairs(
                chain_id="solana",
                token_addresses=["timeout-token"],
            )

    assert attempts == 2
    assert isinstance(captured.value, DexScreenerError)
    assert not isinstance(captured.value, httpx.TransportError)
    assert captured.value.attempt_count == 2
    assert captured.value.dexscreener_attempt_count == 2
    assert captured.value.__cause__ is failures[-1]
    assert metrics.retries == 1
    assert metrics.http_requests_failed == 2


@pytest.mark.asyncio
async def test_boost_connect_error_retries_then_surfaces_domain_error() -> None:
    attempts = 0
    failures: list[httpx.ConnectError] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        failure = httpx.ConnectError(
            "[Errno -3] Temporary failure in name resolution",
            request=request,
        )
        failures.append(failure)
        raise failure

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        metrics = DexScreenerMetrics()
        client = DexScreenerClient(
            _settings(max_attempts=3),
            http_client=http,
            rate_limiter=_fast_limiter(),
            boost_rate_limiter=_fast_limiter(),
            metrics=metrics,
        )
        with pytest.raises(DexScreenerTransportError) as captured:
            await client.fetch_boost_feed(feed_kind="latest")

    assert attempts == 3
    assert isinstance(captured.value, DexScreenerError)
    assert not isinstance(captured.value, httpx.TransportError)
    assert captured.value.attempt_count == 3
    assert captured.value.dexscreener_attempt_count == 3
    assert captured.value.__cause__ is failures[-1]
    assert metrics.retries == 2
    assert metrics.http_requests_failed == 3


@pytest.mark.asyncio
async def test_server_error_retries_through_rate_limiter() -> None:
    attempts = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(500, text="temporary server failure")
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
        result = await client.fetch_token_pairs(
            chain_id="solana",
            token_addresses=["server-error-token"],
        )

    assert result.pairs == ()
    assert result.batches[0].attempt_count == 2
    assert attempts == 2
    assert metrics.retries == 1
    assert metrics.http_requests_failed == 1
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


@pytest.mark.asyncio
async def test_invalid_json_is_explicit_parse_error_without_retry() -> None:
    requests = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(
            200,
            content=b"{not valid json",
            headers={"Content-Type": "application/json"},
        )

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
        with pytest.raises(DexScreenerResponseParseError, match="parse"):
            await client.fetch_token_pairs(
                chain_id="solana",
                token_addresses=["bad-json-token"],
            )

    assert requests == 1
    assert metrics.parse_failures == 1
    assert metrics.retries == 0


@pytest.mark.asyncio
async def test_global_boost_feed_preserves_zero_and_nullable_numeric_facts() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/token-boosts/latest/v1"
        return httpx.Response(
            200,
            json=[
                {
                    "chainId": "solana",
                    "tokenAddress": "token-0",
                    "amount": 0,
                    "totalAmount": None,
                }
            ],
        )

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        client = DexScreenerClient(
            _settings(),
            http_client=http,
            rate_limiter=_fast_limiter(),
            boost_rate_limiter=_fast_limiter(),
        )
        result = await client.fetch_boost_feed(feed_kind="latest")

    assert result.records[0].amount == Decimal("0")
    assert result.records[0].total_amount is None
    assert "amount" in result.records[0].model_fields_set
    assert "total_amount" in result.records[0].model_fields_set


@pytest.mark.asyncio
async def test_global_boost_feed_rejects_non_list_shape() -> None:
    async def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"tokenAddress": "token-0"})

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url=TEST_BASE_URL
    ) as http:
        client = DexScreenerClient(
            _settings(),
            http_client=http,
            rate_limiter=_fast_limiter(),
            boost_rate_limiter=_fast_limiter(),
        )
        with pytest.raises(DexScreenerResponseParseError, match="JSON array"):
            await client.fetch_boost_feed(feed_kind="top")
