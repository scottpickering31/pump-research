"""Async DEX Screener token-pairs client using the official public API contract."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from time import perf_counter
from typing import Any

import httpx
import structlog
from pydantic import TypeAdapter, ValidationError
from tenacity import AsyncRetrying, RetryCallState, retry_if_exception, stop_after_attempt
from tenacity.wait import wait_exponential

from pump_research.config import Settings
from pump_research.market_data.dexscreener_models import (
    DexScreenerBoostFeedRecord,
    DexScreenerPair,
)
from pump_research.market_data.rate_limiter import AsyncRateLimiter, get_process_rate_limiter

DEX_SCREENER_PROVIDER = "dexscreener"
TOKEN_BATCH_LIMIT = 30
_PAIR_LIST_ADAPTER = TypeAdapter(list[DexScreenerPair])
_BOOST_LIST_ADAPTER = TypeAdapter(list[DexScreenerBoostFeedRecord])


class DexScreenerError(RuntimeError):
    """Base error for a DEX Screener request that cannot yield usable facts."""


class DexScreenerHttpError(DexScreenerError):
    """An HTTP response whose status is not successful."""

    def __init__(
        self,
        status_code: int,
        body_preview: str,
        retry_after_seconds: float | None,
    ) -> None:
        self.status_code = status_code
        self.body_preview = body_preview
        self.retry_after_seconds = retry_after_seconds
        super().__init__(f"DEX Screener returned HTTP {status_code}")


class DexScreenerResponseParseError(DexScreenerError):
    """A successful HTTP response that does not match the documented list shape."""


@dataclass(slots=True)
class DexScreenerMetrics:
    """In-process counters for API health and collection diagnostics."""

    batches_requested: int = 0
    addresses_requested: int = 0
    http_requests_started: int = 0
    http_requests_succeeded: int = 0
    http_requests_failed: int = 0
    http_requests_throttled: int = 0
    retries: int = 0
    parse_failures: int = 0
    pairs_returned: int = 0
    rate_limiter_wait_seconds: float = 0.0
    request_latency_seconds: float = 0.0


@dataclass(frozen=True, slots=True)
class DexScreenerBatchResult:
    """Typed and raw result of one eligible token-address batch request."""

    chain_id: str
    requested_addresses: tuple[str, ...]
    pairs: tuple[DexScreenerPair, ...]
    received_at: datetime
    raw_response: tuple[dict[str, Any], ...]
    attempt_count: int = 1


@dataclass(frozen=True, slots=True)
class DexScreenerTokenPairsResult:
    """Aggregate result for one or more API-eligible address batches."""

    chain_id: str
    requested_addresses: tuple[str, ...]
    batches: tuple[DexScreenerBatchResult, ...]

    @property
    def pairs(self) -> tuple[DexScreenerPair, ...]:
        """Flatten pair facts without changing their request provenance."""
        return tuple(pair for batch in self.batches for pair in batch.pairs)


@dataclass(frozen=True, slots=True)
class DexScreenerBoostFeedResult:
    """Typed and raw evidence from one bounded global boost feed request."""

    feed_kind: str
    records: tuple[DexScreenerBoostFeedRecord, ...]
    received_at: datetime
    raw_response: tuple[dict[str, Any], ...]
    attempt_count: int = 1


def _chunked(values: tuple[str, ...], size: int) -> Iterable[tuple[str, ...]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


def _deduplicated_addresses(addresses: Iterable[str]) -> tuple[str, ...]:
    unique: dict[str, None] = {}
    for address in addresses:
        normalized = address.strip()
        if not normalized:
            msg = "DEX Screener token addresses must be non-empty"
            raise ValueError(msg)
        unique.setdefault(normalized, None)
    if not unique:
        msg = "At least one token address is required"
        raise ValueError(msg)
    return tuple(unique)


def _retry_after_seconds(
    value: str | None,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> float | None:
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        return max(0.0, (retry_at - now()).total_seconds())


def _is_retryable(error: BaseException) -> bool:
    return isinstance(error, httpx.TransportError) or (
        isinstance(error, DexScreenerHttpError)
        and (error.status_code == 429 or 500 <= error.status_code < 600)
    )


class _WaitForRetryAfter:
    """Honor provider retry guidance before falling back to bounded exponential delay."""

    def __init__(self, fallback: wait_exponential) -> None:
        self._fallback = fallback

    def __call__(self, retry_state: RetryCallState) -> float:
        exception = retry_state.outcome.exception() if retry_state.outcome is not None else None
        if (
            isinstance(exception, DexScreenerHttpError)
            and exception.retry_after_seconds is not None
        ):
            return exception.retry_after_seconds
        return self._fallback(retry_state)


class DexScreenerClient:
    """Mockable asynchronous client for the official `/tokens/v1` batch endpoint."""

    def __init__(
        self,
        settings: Settings,
        *,
        http_client: httpx.AsyncClient | None = None,
        rate_limiter: AsyncRateLimiter | None = None,
        boost_rate_limiter: AsyncRateLimiter | None = None,
        metrics: DexScreenerMetrics | None = None,
        logger: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or httpx.AsyncClient(
            base_url=settings.dex_screener_base_url.rstrip("/"),
            timeout=httpx.Timeout(settings.dex_screener_timeout_seconds),
        )
        self._owns_http_client = http_client is None
        self._rate_limiter = rate_limiter or get_process_rate_limiter(
            settings.dex_screener_requests_per_minute
        )
        self._boost_rate_limiter = boost_rate_limiter or AsyncRateLimiter(
            settings.dex_screener_boost_route_requests_per_minute
        )
        self.metrics = metrics or DexScreenerMetrics()
        self._logger = logger or structlog.get_logger("pump_research.market_data.dexscreener")

    async def __aenter__(self) -> DexScreenerClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close only an HTTP client constructed by this instance."""
        if self._owns_http_client:
            await self._http_client.aclose()

    async def fetch_token_pairs(
        self,
        *,
        chain_id: str,
        token_addresses: Iterable[str],
    ) -> DexScreenerTokenPairsResult:
        """Fetch all pairs for token addresses, using no more than 30 addresses per request."""
        if not chain_id.strip():
            msg = "chain_id must be non-empty"
            raise ValueError(msg)
        addresses = _deduplicated_addresses(token_addresses)
        batches = tuple(_chunked(addresses, TOKEN_BATCH_LIMIT))
        results: list[DexScreenerBatchResult] = []
        for batch in batches:
            results.append(await self._fetch_batch(chain_id=chain_id, token_addresses=batch))
        return DexScreenerTokenPairsResult(
            chain_id=chain_id,
            requested_addresses=addresses,
            batches=tuple(results),
        )

    async def fetch_boost_feed(self, *, feed_kind: str) -> DexScreenerBoostFeedResult:
        """Fetch the bounded global `latest` or `top` boost feed."""
        if feed_kind not in {"latest", "top"}:
            raise ValueError("feed_kind must be 'latest' or 'top'")
        retry_wait = _WaitForRetryAfter(
            wait_exponential(
                multiplier=self._settings.dex_screener_retry_backoff_seconds,
                min=0,
                max=30,
            )
        )
        attempt_count = 0
        try:
            async for attempt in AsyncRetrying(
                retry=retry_if_exception(_is_retryable),
                stop=stop_after_attempt(self._settings.dex_screener_max_attempts),
                wait=retry_wait,
                reraise=True,
                before_sleep=self._record_retry,
            ):
                attempt_count = attempt.retry_state.attempt_number
                with attempt:
                    result = await self._send_boost_request(feed_kind=feed_kind)
                    return replace(result, attempt_count=attempt_count)
        except BaseException as error:
            error.__dict__["dexscreener_attempt_count"] = attempt_count
            raise
        raise AssertionError("Tenacity completed without a DEX Screener boost result")

    async def _fetch_batch(
        self,
        *,
        chain_id: str,
        token_addresses: tuple[str, ...],
    ) -> DexScreenerBatchResult:
        self.metrics.batches_requested += 1
        self.metrics.addresses_requested += len(token_addresses)
        retry_wait = _WaitForRetryAfter(
            wait_exponential(
                multiplier=self._settings.dex_screener_retry_backoff_seconds,
                min=0,
                max=30,
            )
        )

        attempt_count = 0
        try:
            async for attempt in AsyncRetrying(
                retry=retry_if_exception(_is_retryable),
                stop=stop_after_attempt(self._settings.dex_screener_max_attempts),
                wait=retry_wait,
                reraise=True,
                before_sleep=self._record_retry,
            ):
                attempt_count = attempt.retry_state.attempt_number
                with attempt:
                    result = await self._send_batch_request(
                        chain_id=chain_id,
                        token_addresses=token_addresses,
                    )
                    return replace(result, attempt_count=attempt_count)
        except BaseException as error:
            error.__dict__["dexscreener_attempt_count"] = attempt_count
            raise

        msg = "Tenacity completed without producing a DEX Screener request result"
        raise AssertionError(msg)

    def _record_retry(self, _: RetryCallState) -> None:
        self.metrics.retries += 1

    async def _send_batch_request(
        self,
        *,
        chain_id: str,
        token_addresses: tuple[str, ...],
    ) -> DexScreenerBatchResult:
        wait_seconds = await self._rate_limiter.acquire()
        self.metrics.rate_limiter_wait_seconds += wait_seconds
        self.metrics.http_requests_started += 1
        started = perf_counter()
        path = f"/tokens/v1/{chain_id}/{','.join(token_addresses)}"
        try:
            response = await self._http_client.get(path, headers={"Accept": "application/json"})
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            status_code = error.response.status_code
            if status_code == 429:
                self.metrics.http_requests_throttled += 1
            self.metrics.http_requests_failed += 1
            retry_after_seconds = _retry_after_seconds(error.response.headers.get("Retry-After"))
            self._logger.warning(
                "dexscreener_http_error",
                chain_id=chain_id,
                address_count=len(token_addresses),
                status_code=status_code,
                retry_after_seconds=retry_after_seconds,
            )
            raise DexScreenerHttpError(
                status_code=status_code,
                body_preview=error.response.text[:1_000],
                retry_after_seconds=retry_after_seconds,
            ) from error
        except httpx.TransportError as error:
            self.metrics.http_requests_failed += 1
            self._logger.warning(
                "dexscreener_transport_error",
                chain_id=chain_id,
                address_count=len(token_addresses),
                error_type=type(error).__name__,
            )
            raise
        finally:
            self.metrics.request_latency_seconds += perf_counter() - started

        received_at = datetime.now(UTC)
        try:
            payload = response.json()
            if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
                msg = "DEX Screener /tokens/v1 response must be a JSON array of objects"
                raise DexScreenerResponseParseError(msg)
            raw_response = tuple(dict(item) for item in payload)
            pairs = tuple(_PAIR_LIST_ADAPTER.validate_python(payload))
        except DexScreenerResponseParseError:
            self.metrics.parse_failures += 1
            self.metrics.http_requests_failed += 1
            self._logger.warning(
                "dexscreener_response_shape_error",
                chain_id=chain_id,
                address_count=len(token_addresses),
            )
            raise
        except (ValueError, ValidationError) as error:
            self.metrics.parse_failures += 1
            self.metrics.http_requests_failed += 1
            self._logger.warning(
                "dexscreener_response_parse_error",
                chain_id=chain_id,
                address_count=len(token_addresses),
                error_type=type(error).__name__,
            )
            msg = "Could not parse DEX Screener token-pairs response"
            raise DexScreenerResponseParseError(msg) from error

        self.metrics.http_requests_succeeded += 1
        self.metrics.pairs_returned += len(pairs)
        self._logger.info(
            "dexscreener_batch_succeeded",
            chain_id=chain_id,
            address_count=len(token_addresses),
            pair_count=len(pairs),
        )
        return DexScreenerBatchResult(
            chain_id=chain_id,
            requested_addresses=token_addresses,
            pairs=pairs,
            received_at=received_at,
            raw_response=raw_response,
        )

    async def _send_boost_request(self, *, feed_kind: str) -> DexScreenerBoostFeedResult:
        """Send one request subject to both application-wide and route-specific limits."""
        route_wait = await self._boost_rate_limiter.acquire()
        global_wait = await self._rate_limiter.acquire()
        self.metrics.rate_limiter_wait_seconds += route_wait + global_wait
        self.metrics.http_requests_started += 1
        started = perf_counter()
        path = f"/token-boosts/{feed_kind}/v1"
        try:
            response = await self._http_client.get(path, headers={"Accept": "application/json"})
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            status_code = error.response.status_code
            if status_code == 429:
                self.metrics.http_requests_throttled += 1
            self.metrics.http_requests_failed += 1
            raise DexScreenerHttpError(
                status_code=status_code,
                body_preview=error.response.text[:1_000],
                retry_after_seconds=_retry_after_seconds(error.response.headers.get("Retry-After")),
            ) from error
        except httpx.TransportError:
            self.metrics.http_requests_failed += 1
            raise
        finally:
            self.metrics.request_latency_seconds += perf_counter() - started

        received_at = datetime.now(UTC)
        try:
            payload = response.json()
            if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
                raise DexScreenerResponseParseError(
                    "DEX Screener boost response must be a JSON array of objects"
                )
            raw_response = tuple(dict(item) for item in payload)
            records = tuple(_BOOST_LIST_ADAPTER.validate_python(payload))
        except DexScreenerResponseParseError:
            self.metrics.parse_failures += 1
            self.metrics.http_requests_failed += 1
            raise
        except (ValueError, ValidationError) as error:
            self.metrics.parse_failures += 1
            self.metrics.http_requests_failed += 1
            raise DexScreenerResponseParseError(
                "Could not parse DEX Screener boost response"
            ) from error

        self.metrics.http_requests_succeeded += 1
        return DexScreenerBoostFeedResult(
            feed_kind=feed_kind,
            records=records,
            received_at=received_at,
            raw_response=raw_response,
        )
