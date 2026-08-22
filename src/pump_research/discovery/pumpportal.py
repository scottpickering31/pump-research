"""PumpPortal real-time new-token discovery adapter.

Only this module knows PumpPortal's WebSocket authentication and payload shape.
Consumers receive the provider-neutral discovery contract and opaque raw evidence.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import random
import uuid
from collections import deque
from collections.abc import Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol, cast
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import structlog
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from websockets.asyncio.client import connect

from pump_research.config import Settings
from pump_research.discovery.contracts import (
    DiscoveredToken,
    DiscoveryBatch,
    DiscoveryCheckpoint,
    DiscoveryConnectivityEvent,
    DiscoveryConnectivityEventType,
    DiscoveryCoverage,
    DiscoveryCoverageStatus,
    DiscoveryResponseParseError,
    DiscoverySourceError,
    TokenDiscoverySource,
)

PUMPPORTAL_SOURCE_NAME = "pumpportal"
_SUBSCRIBE_NEW_TOKEN = json.dumps({"method": "subscribeNewToken"}, separators=(",", ":"))
_COVERAGE_NOTE = (
    "PumpPortal is a live WebSocket source with no historical replay. Coverage is "
    "best-effort; disconnect-to-reconnect intervals and collector downtime may contain "
    "unrecoverable missed token-creation events."
)


class PumpPortalConfigurationError(DiscoverySourceError):
    """Required PumpPortal live-discovery configuration is absent or unsafe."""


class PumpPortalWebSocket(Protocol):
    """Minimal socket surface used by the adapter and test fakes."""

    async def send(self, message: str) -> None: ...

    async def recv(self) -> str | bytes: ...

    async def close(self) -> None: ...


type PumpPortalConnectionFactory = Callable[
    [str, int, float], AbstractAsyncContextManager[PumpPortalWebSocket]
]


class PumpPortalDiscoveryMetrics:
    """Bounded in-process counters supplementing durable connectivity evidence."""

    def __init__(self) -> None:
        self.connections_established = 0
        self.subscriptions_sent = 0
        self.messages_received = 0
        self.events_parsed = 0
        self.parse_failures = 0
        self.disconnects = 0
        self.reconnections = 0


class _PumpPortalNewToken(BaseModel):
    """The minimal normalized fields needed from a creation message."""

    model_config = ConfigDict(extra="allow")

    mint: str = Field(min_length=1)
    signature: str | None = Field(default=None, min_length=1)


@dataclass(frozen=True, slots=True)
class _TokenEnvelope:
    event: DiscoveredToken


@dataclass(frozen=True, slots=True)
class _ConnectivityEnvelope:
    event: DiscoveryConnectivityEvent


@dataclass(frozen=True, slots=True)
class _FailureEnvelope:
    error: DiscoveryResponseParseError


type _Envelope = _TokenEnvelope | _ConnectivityEnvelope | _FailureEnvelope


class PumpPortalDiscoverySource(TokenDiscoverySource):
    """Receive new-token events over one reconnecting PumpPortal WebSocket."""

    def __init__(
        self,
        settings: Settings,
        *,
        connection_factory: PumpPortalConnectionFactory | None = None,
        metrics: PumpPortalDiscoveryMetrics | None = None,
        logger: structlog.stdlib.BoundLogger | None = None,
        now: Callable[[], datetime] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        random_value: Callable[[], float] | None = None,
    ) -> None:
        api_key = settings.pumpportal_api_key
        if api_key is None or not api_key.get_secret_value().strip():
            msg = (
                "PUMP_RESEARCH_PUMPPORTAL_API_KEY is required for live discovery; "
                "obtain it from PumpPortal and do not leave it blank"
            )
            raise PumpPortalConfigurationError(msg)

        self._settings = settings
        self._connection_url = _authenticated_url(
            settings.pumpportal_websocket_url,
            api_key.get_secret_value(),
        )
        self._connection_factory = connection_factory or _default_connection_factory
        self.metrics = metrics or PumpPortalDiscoveryMetrics()
        self._logger = logger or structlog.get_logger("pump_research.discovery.pumpportal")
        self._now = now or (lambda: datetime.now(UTC))
        self._sleep = sleep or asyncio.sleep
        self._random_value: Callable[[], float] = (
            random_value if random_value is not None else random.random
        )
        self._queue: asyncio.Queue[_Envelope] = asyncio.Queue(
            maxsize=settings.pumpportal_queue_capacity
        )
        self._deferred: deque[_Envelope] = deque()
        self._reader_task: asyncio.Task[None] | None = None
        self._active_socket: PumpPortalWebSocket | None = None
        self._unacknowledged_batch: DiscoveryBatch | None = None
        self._closing = False

    @property
    def source_name(self) -> str:
        return PUMPPORTAL_SOURCE_NAME

    async def fetch(self, checkpoint: DiscoveryCheckpoint | None = None) -> DiscoveryBatch:
        """Wait briefly for stream evidence, then return one bounded batch.

        PumpPortal has no cursor/replay operation, so a durable checkpoint cannot
        be sent upstream. A supplied checkpoint is intentionally left untouched.
        """
        del checkpoint
        if self._closing:
            raise DiscoverySourceError("PumpPortal discovery source is closed")
        if self._unacknowledged_batch is not None:
            return self._unacknowledged_batch
        self._ensure_reader()

        first = await self._next_envelope()
        if first is None:
            self._raise_if_reader_failed()
            return self._batch(events=(), connectivity_events=())

        events: list[DiscoveredToken] = []
        connectivity_events: list[DiscoveryConnectivityEvent] = []
        envelopes = [first]
        for _ in range(self._settings.pumpportal_batch_size - 1):
            envelope = self._next_envelope_nowait()
            if envelope is None:
                break
            envelopes.append(envelope)

        for index, envelope in enumerate(envelopes):
            if isinstance(envelope, _FailureEnvelope):
                if events or connectivity_events:
                    self._deferred.extendleft(reversed(envelopes[index:]))
                    break
                self._deferred.extendleft(reversed(envelopes[index + 1 :]))
                raise envelope.error
            if isinstance(envelope, _TokenEnvelope):
                events.append(envelope.event)
            else:
                connectivity_events.append(envelope.event)

        batch = self._batch(
            events=tuple(events),
            connectivity_events=tuple(connectivity_events),
        )
        if batch.events or batch.connectivity_events:
            self._unacknowledged_batch = batch
        return batch

    async def acknowledge(self, batch: DiscoveryBatch) -> None:
        """Release a live batch only after its PostgreSQL transaction commits."""
        pending = self._unacknowledged_batch
        if pending is None:
            return
        if batch is not pending:
            raise ValueError("Cannot acknowledge a different PumpPortal discovery batch")
        self._unacknowledged_batch = None

    async def aclose(self) -> None:
        """Stop the sole reader task and close its current socket."""
        if self._closing:
            return
        self._closing = True
        socket = self._active_socket
        if socket is not None:
            try:
                await socket.close()
            except Exception as error:
                self._logger.warning(
                    "pumpportal_websocket_close_failed",
                    error_type=type(error).__name__,
                )
        task = self._reader_task
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def _ensure_reader(self) -> None:
        task = self._reader_task
        if task is None:
            self._reader_task = asyncio.create_task(
                self._reader_loop(), name="pumpportal-discovery-reader"
            )
            return
        self._raise_if_reader_failed()

    def _raise_if_reader_failed(self) -> None:
        task = self._reader_task
        if task is None or not task.done() or task.cancelled():
            return
        error = task.exception()
        if error is not None:
            raise DiscoverySourceError("PumpPortal discovery reader stopped") from error
        raise DiscoverySourceError("PumpPortal discovery reader stopped unexpectedly")

    async def _next_envelope(self) -> _Envelope | None:
        if self._deferred:
            return self._deferred.popleft()
        try:
            return await asyncio.wait_for(
                self._queue.get(), timeout=self._settings.pumpportal_fetch_wait_seconds
            )
        except TimeoutError:
            return None

    def _next_envelope_nowait(self) -> _Envelope | None:
        if self._deferred:
            return self._deferred.popleft()
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    def _batch(
        self,
        *,
        events: tuple[DiscoveredToken, ...],
        connectivity_events: tuple[DiscoveryConnectivityEvent, ...],
    ) -> DiscoveryBatch:
        return DiscoveryBatch(
            events=events,
            connectivity_events=connectivity_events,
            received_at=self._now(),
            coverage=DiscoveryCoverage(
                status=DiscoveryCoverageStatus.BEST_EFFORT,
                supports_replay=False,
                note=_COVERAGE_NOTE,
            ),
            next_checkpoint=None,
        )

    async def _reader_loop(self) -> None:
        attempt = 0
        open_gap_id: str | None = None
        while not self._closing:
            try:
                async with self._connection_factory(
                    self._connection_url,
                    self._settings.pumpportal_queue_capacity,
                    self._settings.pumpportal_connect_timeout_seconds,
                ) as websocket:
                    self._active_socket = websocket
                    await websocket.send(_SUBSCRIBE_NEW_TOKEN)
                    self.metrics.connections_established += 1
                    self.metrics.subscriptions_sent += 1
                    if open_gap_id is not None:
                        await self._put_connectivity_event(
                            open_gap_id,
                            DiscoveryConnectivityEventType.RECONNECTED,
                            reason="subscription_reestablished",
                            detail={"failed_connection_attempts": attempt},
                        )
                        self.metrics.reconnections += 1
                        open_gap_id = None
                    attempt = 0
                    self._logger.info("pumpportal_new_token_subscription_established")

                    while not self._closing:
                        raw_message = await websocket.recv()
                        received_at = self._now()
                        self.metrics.messages_received += 1
                        try:
                            event = _parse_new_token_message(raw_message, received_at=received_at)
                        except DiscoveryResponseParseError as error:
                            self.metrics.parse_failures += 1
                            await self._queue.put(_FailureEnvelope(error))
                            self._logger.warning(
                                "pumpportal_discovery_message_rejected",
                                error_type=type(error).__name__,
                            )
                            continue
                        if event is not None:
                            self.metrics.events_parsed += 1
                            await self._queue.put(_TokenEnvelope(event))
            except asyncio.CancelledError:
                raise
            except Exception as error:
                if self._is_closing():
                    break
                self._active_socket = None
                self.metrics.disconnects += 1
                if open_gap_id is None:
                    open_gap_id = str(uuid.uuid4())
                    await self._put_connectivity_event(
                        open_gap_id,
                        DiscoveryConnectivityEventType.DISCONNECTED,
                        reason=type(error).__name__,
                        detail={"message": str(error)[:1_000]},
                    )
                delay = self._retry_delay(attempt)
                attempt += 1
                self._logger.warning(
                    "pumpportal_websocket_disconnected",
                    error_type=type(error).__name__,
                    reconnect_delay_seconds=delay,
                )
                await self._sleep(delay)
            finally:
                self._active_socket = None

    async def _put_connectivity_event(
        self,
        gap_id: str,
        event_type: DiscoveryConnectivityEventType,
        *,
        reason: str,
        detail: dict[str, object],
    ) -> None:
        observed_at = self._now()
        key_material = f"{PUMPPORTAL_SOURCE_NAME}:{gap_id}:{event_type.value}".encode()
        event = DiscoveryConnectivityEvent(
            source_name=PUMPPORTAL_SOURCE_NAME,
            gap_id=gap_id,
            event_type=event_type,
            observed_at=observed_at,
            reason=reason,
            detail=detail,
            idempotency_key=hashlib.sha256(key_material).hexdigest(),
        )
        await self._queue.put(_ConnectivityEnvelope(event))

    def _retry_delay(self, attempt: int) -> float:
        initial = self._settings.pumpportal_reconnect_initial_seconds
        maximum = self._settings.pumpportal_reconnect_max_seconds
        base = min(maximum, initial * (2 ** min(attempt, 30)))
        jitter = base * self._settings.pumpportal_reconnect_jitter_ratio * self._random_value()
        return float(min(maximum, base + jitter))

    def _is_closing(self) -> bool:
        """Keep shutdown state observable across the independently running task."""
        return self._closing


def _default_connection_factory(
    url: str,
    max_queue: int,
    open_timeout: float,
) -> AbstractAsyncContextManager[PumpPortalWebSocket]:
    return cast(
        "AbstractAsyncContextManager[PumpPortalWebSocket]",
        connect(url, max_queue=max_queue, open_timeout=open_timeout),
    )


def _authenticated_url(base_url: str, api_key: str) -> str:
    """Add PumpPortal's documented query credential without exposing it in config."""
    if not api_key.strip():
        raise PumpPortalConfigurationError("PumpPortal API key must not be blank")
    parsed = urlsplit(base_url)
    query = [(key, value) for key, value in parse_qsl(parsed.query) if key != "api-key"]
    query.append(("api-key", api_key))
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), ""))


def _parse_new_token_message(
    raw_message: str | bytes,
    *,
    received_at: datetime,
) -> DiscoveredToken | None:
    """Parse one creation message; return None only for a known subscription ack."""
    try:
        message = raw_message.decode("utf-8") if isinstance(raw_message, bytes) else raw_message
        payload = json.loads(message)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise DiscoveryResponseParseError("PumpPortal returned invalid JSON") from error
    if not isinstance(payload, dict):
        raise DiscoveryResponseParseError("PumpPortal message must be a JSON object")

    acknowledgement = payload.get("message")
    if (
        "mint" not in payload
        and isinstance(acknowledgement, str)
        and "successfully subscribed" in acknowledgement.lower()
    ):
        return None

    try:
        parsed = _PumpPortalNewToken.model_validate(payload)
        source_event_at = _source_timestamp(payload)
    except (ValidationError, ValueError) as error:
        raise DiscoveryResponseParseError("PumpPortal new-token event is malformed") from error

    source_payload = cast(dict[str, object], payload)
    payload_digest = _payload_sha256(source_payload)
    provider_event_id = parsed.signature or parsed.mint
    key_material = f"{PUMPPORTAL_SOURCE_NAME}:token_created:{provider_event_id}".encode()
    return DiscoveredToken(
        chain="solana",
        address=parsed.mint,
        source_name=PUMPPORTAL_SOURCE_NAME,
        source_event_id=provider_event_id,
        event_type="token_created",
        source_event_at=source_event_at,
        received_at=received_at,
        source_payload=source_payload,
        source_payload_sha256=payload_digest,
        idempotency_key=hashlib.sha256(key_material).hexdigest(),
    )


def _source_timestamp(payload: dict[str, object]) -> datetime | None:
    for field_name in ("timestamp", "created_timestamp", "createdTimestamp"):
        if field_name in payload:
            return _parse_timestamp(payload[field_name], field_name=field_name)
    return None


def _parse_timestamp(value: object, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a timestamp")
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            try:
                numeric = float(value)
            except ValueError as error:
                raise ValueError(f"{field_name} must be a timestamp") from error
        else:
            if parsed.tzinfo is None or parsed.utcoffset() is None:
                raise ValueError(f"{field_name} must include a timezone")
            return parsed.astimezone(UTC)
    elif isinstance(value, int | float):
        numeric = float(value)
    else:
        raise ValueError(f"{field_name} must be a timestamp")

    if numeric != numeric or numeric in (float("inf"), float("-inf")):
        raise ValueError(f"{field_name} must be finite")
    if abs(numeric) >= 100_000_000_000:
        numeric /= 1_000
    try:
        return datetime.fromtimestamp(numeric, tz=UTC)
    except (OverflowError, OSError, ValueError) as error:
        raise ValueError(f"{field_name} is outside the supported range") from error


def _payload_sha256(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).hexdigest()
