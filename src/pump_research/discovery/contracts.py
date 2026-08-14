"""Provider-neutral token-discovery contracts.

Callers consume canonical token identities and source evidence through these
types. They must not inspect a provider adapter's request or response schema.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum


class DiscoveryCoverageStatus(StrEnum):
    """How completely a discovery result represents its source's feed."""

    COMPLETE = "complete"
    BEST_EFFORT = "best_effort"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class DiscoveryCheckpoint:
    """An opaque source-owned cursor or conditional-request validator.

    Orchestration may persist and return this value to the same source, but it
    must not interpret or construct it. A future checkpoint store must scope
    values by ``TokenDiscoverySource.source_name``.
    """

    value: str

    def __post_init__(self) -> None:
        if not self.value:
            msg = "A discovery checkpoint must be non-empty"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class DiscoveryCoverage:
    """Evidence about the portion of source history represented by a batch."""

    status: DiscoveryCoverageStatus
    supports_replay: bool
    note: str | None = None


@dataclass(frozen=True, slots=True)
class DiscoveredToken:
    """One immutable, provider-neutral token discovery occurrence.

    ``source_payload`` is opaque evidence to consumers outside ``discovery``;
    it can be persisted but must not be used as an application domain model.
    """

    chain: str
    address: str
    source_name: str
    source_event_id: str | None
    event_type: str
    source_event_at: datetime | None
    received_at: datetime
    source_payload: Mapping[str, object]
    source_payload_sha256: str
    idempotency_key: str

    def __post_init__(self) -> None:
        for field_name in ("chain", "address", "source_name", "event_type", "idempotency_key"):
            if not getattr(self, field_name).strip():
                msg = f"{field_name} must be non-empty"
                raise ValueError(msg)
        _require_utc("received_at", self.received_at)
        if self.source_event_at is not None:
            _require_utc("source_event_at", self.source_event_at)
        if len(self.source_payload_sha256) != 64:
            msg = "source_payload_sha256 must be a SHA-256 hexadecimal digest"
            raise ValueError(msg)


@dataclass(frozen=True, slots=True)
class DiscoveryBatch:
    """The result of one source poll, including its data-quality semantics."""

    events: tuple[DiscoveredToken, ...]
    received_at: datetime
    coverage: DiscoveryCoverage
    next_checkpoint: DiscoveryCheckpoint | None
    not_modified: bool = False

    def __post_init__(self) -> None:
        _require_utc("received_at", self.received_at)
        if self.not_modified and self.events:
            msg = "A not-modified discovery result cannot contain events"
            raise ValueError(msg)


class DiscoverySourceError(RuntimeError):
    """A discovery source failed without yielding durable usable evidence."""


class DiscoveryResponseParseError(DiscoverySourceError):
    """A successful source response could not be interpreted safely."""


class TokenDiscoverySource(ABC):
    """Replaceable asynchronous source of canonical token discovery events."""

    @property
    @abstractmethod
    def source_name(self) -> str:
        """Return the stable persistence namespace for this source."""

    @abstractmethod
    async def fetch(self, checkpoint: DiscoveryCheckpoint | None = None) -> DiscoveryBatch:
        """Fetch one source-defined batch without exposing provider schema."""

    @abstractmethod
    async def aclose(self) -> None:
        """Release resources owned by the source."""

    async def __aenter__(self) -> TokenDiscoverySource:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.aclose()


def _require_utc(field_name: str, value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    if value.utcoffset() != UTC.utcoffset(value):
        msg = f"{field_name} must use UTC"
        raise ValueError(msg)
