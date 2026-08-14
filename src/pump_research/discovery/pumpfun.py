"""Initial Pump.fun adapter for the provider-neutral discovery contract.

This adapter deliberately exposes the documented latest-coin endpoint as a
best-effort source. It does not claim replayable or complete coverage.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from time import perf_counter
from typing import cast

import httpx
import structlog
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from pump_research.config import Settings
from pump_research.discovery.contracts import (
    DiscoveredToken,
    DiscoveryBatch,
    DiscoveryCheckpoint,
    DiscoveryCoverage,
    DiscoveryCoverageStatus,
    DiscoveryResponseParseError,
    DiscoverySourceError,
    TokenDiscoverySource,
)

PUMP_FUN_SOURCE_NAME = "pumpfun"
_LATEST_COIN_PATH = "/coins/latest"
_LATEST_COIN_COVERAGE_NOTE = (
    "The latest-coin endpoint is not a replayable or exhaustive discovery feed; "
    "a token created between polls can be missed."
)


class PumpFunHttpError(DiscoverySourceError):
    """A Pump.fun HTTP response that cannot yield usable discovery evidence."""

    def __init__(self, status_code: int, body_preview: str) -> None:
        self.status_code = status_code
        self.body_preview = body_preview
        super().__init__(f"Pump.fun discovery endpoint returned HTTP {status_code}")


class PumpFunDiscoveryMetrics:
    """In-process request counters; durable attempt recording is a later phase."""

    def __init__(self) -> None:
        self.requests_started = 0
        self.requests_succeeded = 0
        self.requests_not_modified = 0
        self.requests_failed = 0
        self.parse_failures = 0
        self.request_latency_seconds = 0.0


class _PumpFunLatestCoin(BaseModel):
    """Fields this adapter uses; unknown source fields remain raw evidence."""

    model_config = ConfigDict(extra="allow")

    mint: str = Field(min_length=1)
    created_timestamp: int | float | str | None = None


class PumpFunDiscoverySource(TokenDiscoverySource):
    """Fetch the latest Pump.fun token through the generic discovery boundary."""

    def __init__(
        self,
        settings: Settings,
        *,
        http_client: httpx.AsyncClient | None = None,
        metrics: PumpFunDiscoveryMetrics | None = None,
        logger: structlog.stdlib.BoundLogger | None = None,
    ) -> None:
        self._settings = settings
        self._http_client = http_client or httpx.AsyncClient(
            base_url=settings.pump_fun_base_url.rstrip("/"),
            timeout=httpx.Timeout(settings.pump_fun_timeout_seconds),
        )
        self._owns_http_client = http_client is None
        self.metrics = metrics or PumpFunDiscoveryMetrics()
        self._logger = logger or structlog.get_logger("pump_research.discovery.pumpfun")

    @property
    def source_name(self) -> str:
        return PUMP_FUN_SOURCE_NAME

    async def aclose(self) -> None:
        """Close only an HTTP client constructed by this adapter."""
        if self._owns_http_client:
            await self._http_client.aclose()

    async def fetch(self, checkpoint: DiscoveryCheckpoint | None = None) -> DiscoveryBatch:
        """Fetch the current latest coin and report its limited coverage."""
        headers = {"Accept": "application/json", "Origin": "https://pump.fun"}
        token = self._settings.pump_fun_api_token
        if token is not None:
            headers["Authorization"] = f"Bearer {token.get_secret_value()}"
        if checkpoint is not None:
            headers["If-None-Match"] = checkpoint.value

        received_at = datetime.now(UTC)
        self.metrics.requests_started += 1
        started = perf_counter()
        try:
            response = await self._http_client.get(_LATEST_COIN_PATH, headers=headers)
        except httpx.HTTPError as error:
            self.metrics.requests_failed += 1
            self._logger.warning("pumpfun_discovery_transport_error", error=str(error))
            raise DiscoverySourceError("Pump.fun discovery request failed") from error
        finally:
            self.metrics.request_latency_seconds += perf_counter() - started

        next_checkpoint = _response_checkpoint(response) or checkpoint
        coverage = DiscoveryCoverage(
            status=DiscoveryCoverageStatus.BEST_EFFORT,
            supports_replay=False,
            note=_LATEST_COIN_COVERAGE_NOTE,
        )
        if response.status_code == httpx.codes.NOT_MODIFIED:
            self.metrics.requests_not_modified += 1
            self._logger.info("pumpfun_discovery_not_modified")
            return DiscoveryBatch(
                events=(),
                received_at=received_at,
                coverage=coverage,
                next_checkpoint=next_checkpoint,
                not_modified=True,
            )
        if not response.is_success:
            self.metrics.requests_failed += 1
            body_preview = response.text[:1_000]
            self._logger.warning(
                "pumpfun_discovery_http_error",
                status_code=response.status_code,
                body_preview=body_preview,
            )
            raise PumpFunHttpError(response.status_code, body_preview)

        event = self._parse_event(response=response, received_at=received_at)
        self.metrics.requests_succeeded += 1
        self._logger.info(
            "pumpfun_discovery_received",
            chain=event.chain,
            address=event.address,
            source_event_at=event.source_event_at.isoformat() if event.source_event_at else None,
        )
        return DiscoveryBatch(
            events=(event,),
            received_at=received_at,
            coverage=coverage,
            next_checkpoint=next_checkpoint,
        )

    def _parse_event(self, *, response: httpx.Response, received_at: datetime) -> DiscoveredToken:
        try:
            payload = response.json()
        except json.JSONDecodeError as error:
            self.metrics.parse_failures += 1
            raise DiscoveryResponseParseError("Pump.fun returned invalid JSON") from error
        if not isinstance(payload, dict):
            self.metrics.parse_failures += 1
            msg = "Pump.fun latest-coin response must be a JSON object"
            raise DiscoveryResponseParseError(msg)
        try:
            coin = _PumpFunLatestCoin.model_validate(payload)
            source_event_at = _parse_source_timestamp(coin.created_timestamp)
        except (ValidationError, ValueError) as error:
            self.metrics.parse_failures += 1
            msg = "Pump.fun latest-coin response is malformed"
            raise DiscoveryResponseParseError(msg) from error

        source_payload = cast(dict[str, object], payload)
        payload_digest = _payload_sha256(source_payload)
        return DiscoveredToken(
            chain="solana",
            address=coin.mint,
            source_name=self.source_name,
            source_event_id=coin.mint,
            event_type="token_created",
            source_event_at=source_event_at,
            received_at=received_at,
            source_payload=source_payload,
            source_payload_sha256=payload_digest,
            idempotency_key=_event_idempotency_key(coin.mint),
        )


def _response_checkpoint(response: httpx.Response) -> DiscoveryCheckpoint | None:
    etag = response.headers.get("ETag")
    return DiscoveryCheckpoint(etag) if etag else None


def _parse_source_timestamp(value: int | float | str | None) -> datetime | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError as error:
        msg = "Pump.fun created_timestamp must be a Unix timestamp"
        raise ValueError(msg) from error
    if seconds != seconds or seconds in (float("inf"), float("-inf")):
        msg = "Pump.fun created_timestamp must be finite"
        raise ValueError(msg)
    if abs(seconds) >= 100_000_000_000:
        seconds /= 1_000
    try:
        return datetime.fromtimestamp(seconds, tz=UTC)
    except (OverflowError, OSError, ValueError) as error:
        msg = "Pump.fun created_timestamp is outside the supported range"
        raise ValueError(msg) from error


def _payload_sha256(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _event_idempotency_key(mint: str) -> str:
    encoded = f"{PUMP_FUN_SOURCE_NAME}:token_created:{mint}".encode()
    return hashlib.sha256(encoded).hexdigest()
