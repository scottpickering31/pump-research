"""SQLAlchemy persistence models for immutable research facts and operations."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import Uuid


class Base(DeclarativeBase):
    """Base class for all database tables."""


class Token(Base):
    """Provider-neutral, non-deletable identity for a tracked token."""

    __tablename__ = "tokens"
    __table_args__ = (
        UniqueConstraint("chain", "address", name="uq_tokens_chain_address"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    chain: Mapped[str] = mapped_column(String(32), nullable=False)
    address: Mapped[str] = mapped_column(String(128), nullable=False)
    first_discovered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Pair(Base):
    """Canonical DEX pair identity associated with one tracked token."""

    __tablename__ = "pairs"
    __table_args__ = (
        UniqueConstraint("chain", "address", name="uq_pairs_chain_address"),
        Index("ix_pairs_token_id", "token_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    chain: Mapped[str] = mapped_column(String(32), nullable=False)
    address: Mapped[str] = mapped_column(String(128), nullable=False)
    dex_identifier: Mapped[str | None] = mapped_column(String(128))
    first_discovered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CollectorRun(Base):
    """A mutable operational record for one collector process invocation."""

    __tablename__ = "collector_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'succeeded', 'failed', 'cancelled')",
            name="ck_collector_runs_status",
        ),
        Index("ix_collector_runs_started_at", "started_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    collector_version: Mapped[str] = mapped_column(String(128), nullable=False)
    configuration_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    failure_detail: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ApiRequestLog(Base):
    """Immutable evidence of one completed or failed external API request."""

    __tablename__ = "api_request_log"
    __table_args__ = (
        CheckConstraint(
            "outcome IN ('succeeded', 'empty', 'partial', 'failed', 'throttled', 'malformed')",
            name="ck_api_request_log_outcome",
        ),
        UniqueConstraint("idempotency_key", name="uq_api_request_log_idempotency_key"),
        Index("ix_api_request_log_provider_requested_at", "provider", "requested_at"),
        Index("ix_api_request_log_collector_run_id", "collector_run_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    endpoint: Mapped[str] = mapped_column(String(256), nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)
    http_status_code: Mapped[int | None] = mapped_column(Integer)
    request_payload: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    response_payload: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    response_payload_sha256: Mapped[str | None] = mapped_column(String(64))
    failure_detail: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class DiscoveryEvent(Base):
    """Immutable source event announcing or otherwise describing a token."""

    __tablename__ = "discovery_events"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_discovery_events_idempotency_key"),
        Index("ix_discovery_events_token_received_at", "token_id", "received_at"),
        Index("ix_discovery_events_provider_source_event_at", "provider", "source_event_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    provider_event_id: Mapped[str | None] = mapped_column(String(256))
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    source_payload_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Observation(Base):
    """Immutable normalized market facts backed by an immutable API response."""

    __tablename__ = "observations"
    __table_args__ = (
        PrimaryKeyConstraint("received_at", "id", name="pk_observations"),
        UniqueConstraint(
            "received_at",
            "api_request_log_id",
            "pair_id",
            name="uq_observations_request_pair",
        ),
        Index("ix_observations_pair_received_at", "pair_id", "received_at"),
        {"postgresql_partition_by": "RANGE (received_at)"},
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), default=uuid.uuid4, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    pair_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pairs.id", ondelete="RESTRICT"), nullable=False
    )
    api_request_log_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("api_request_log.id", ondelete="RESTRICT"), nullable=False
    )
    source_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_record_locator: Mapped[str | None] = mapped_column(String(256))
    source_record_sha256: Mapped[str | None] = mapped_column(String(64))
    price_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    price_native: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    liquidity_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    market_cap_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    fully_diluted_valuation_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    volume_m5_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    volume_h1_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    volume_h6_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    volume_h24_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    price_change_m5_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 10))
    price_change_h1_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 10))
    price_change_h6_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 10))
    price_change_h24_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 10))
    buys_m5: Mapped[int | None] = mapped_column(BigInteger)
    sells_m5: Mapped[int | None] = mapped_column(BigInteger)
    buys_h1: Mapped[int | None] = mapped_column(BigInteger)
    sells_h1: Mapped[int | None] = mapped_column(BigInteger)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class LifecycleEvent(Base):
    """Immutable derived lifecycle transition, never a source observation."""

    __tablename__ = "lifecycle_events"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_lifecycle_events_idempotency_key"),
        Index("ix_lifecycle_events_token_decided_at", "token_id", "decided_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    previous_state: Mapped[str | None] = mapped_column(String(64))
    new_state: Mapped[str] = mapped_column(String(64), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    input_watermark: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(128), nullable=False)
    reason_detail: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    configuration_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
