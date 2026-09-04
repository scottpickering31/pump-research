"""SQLAlchemy persistence models for immutable research facts and operations."""

from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import Uuid


class Base(DeclarativeBase):
    """Base class for all database tables."""


class Token(Base):
    """Provider-neutral, non-deletable identity for a tracked token."""

    __tablename__ = "tokens"
    __table_args__ = (UniqueConstraint("chain", "address", name="uq_tokens_chain_address"),)

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


class CollectionEpoch(Base):
    """Immutable declaration of one isolated collection/research epoch."""

    __tablename__ = "collection_epochs"
    __table_args__ = (
        UniqueConstraint("epoch_number", name="uq_collection_epochs_number"),
        CheckConstraint(
            "(data_valid AND invalid_reason IS NULL) OR "
            "(NOT data_valid AND invalid_reason IS NOT NULL)",
            name="ck_collection_epochs_validity_reason",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    epoch_number: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    purpose: Mapped[str] = mapped_column(String(2048), nullable=False)
    data_valid: Mapped[bool] = mapped_column(Boolean, nullable=False)
    invalid_reason: Mapped[str | None] = mapped_column(String(2048))
    configuration_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    code_revision: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CollectionEpochEvent(Base):
    """Immutable status transition giving an epoch its start and end instants."""

    __tablename__ = "collection_epoch_events"
    __table_args__ = (
        CheckConstraint(
            "status IN ('planned', 'running', 'completed', 'aborted', 'invalid')",
            name="ck_collection_epoch_events_status",
        ),
        UniqueConstraint("idempotency_key", name="uq_collection_epoch_events_idempotency"),
        Index("ix_collection_epoch_events_epoch_occurred", "collection_epoch_id", "occurred_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("collection_epochs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reason: Mapped[str] = mapped_column(String(2048), nullable=False)
    detail: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CollectionEpochCurrent(Base):
    """Rebuildable mutable projection of the latest immutable epoch event."""

    __tablename__ = "collection_epoch_current"
    __table_args__ = (
        CheckConstraint(
            "status IN ('planned', 'running', 'completed', 'aborted', 'invalid')",
            name="ck_collection_epoch_current_status",
        ),
        CheckConstraint(
            "(data_valid AND invalid_reason IS NULL) OR "
            "(NOT data_valid AND invalid_reason IS NOT NULL)",
            name="ck_collection_epoch_current_validity_reason",
        ),
        Index(
            "uq_collection_epoch_current_one_running",
            "status",
            unique=True,
            postgresql_where=text("status = 'running'"),
        ),
    )

    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("collection_epochs.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    data_valid: Mapped[bool] = mapped_column(Boolean, nullable=False)
    invalid_reason: Mapped[str | None] = mapped_column(String(2048))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    latest_event_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("collection_epoch_events.id", ondelete="RESTRICT"),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CollectorRun(Base):
    """A mutable operational record for one collector process invocation."""

    __tablename__ = "collector_runs"
    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'stopped', 'succeeded', 'failed', 'cancelled')",
            name="ck_collector_runs_status",
        ),
        CheckConstraint(
            "collection_started_at IS NULL OR collection_started_at >= started_at",
            name="ck_collector_runs_collection_started_after_invocation",
        ),
        Index("ix_collector_runs_started_at", "started_at"),
        Index("ix_collector_runs_collection_epoch_id", "collection_epoch_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("collection_epochs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    collection_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    collector_version: Mapped[str] = mapped_column(String(128), nullable=False)
    configuration_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    failure_detail: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CollectorRunEvent(Base):
    """Immutable evidence of a collector run's terminal transition."""

    __tablename__ = "collector_run_events"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('graceful_stop', 'failed', 'stale_reconciled')",
            name="ck_collector_run_events_type",
        ),
        UniqueConstraint("idempotency_key", name="uq_collector_run_events_idempotency"),
        Index("ix_collector_run_events_run_occurred", "collector_run_id", "occurred_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collector_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reason: Mapped[str] = mapped_column(String(2048), nullable=False)
    detail: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CollectorComponentHealth(Base):
    """Current durable health projection for each fixed collector pipeline component."""

    __tablename__ = "collector_component_health"
    __table_args__ = (
        CheckConstraint(
            "status IN ('healthy', 'degraded', 'failed', 'stopped')",
            name="ck_component_health_status",
        ),
        Index("ix_collector_component_health_run_id", "collector_run_id"),
    )

    component_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    collector_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    last_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    detail: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
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
        Index("ix_discovery_events_collector_run_id", "collector_run_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
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


class DiscoveryCheckpointState(Base):
    """Mutable provider-neutral projection of the last durable discovery cursor."""

    __tablename__ = "discovery_checkpoint_states"
    __table_args__ = (
        CheckConstraint(
            "coverage_status IN ('complete', 'best_effort', 'unknown')",
            name="ck_discovery_checkpoint_states_coverage",
        ),
    )

    source_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    checkpoint_value: Mapped[str] = mapped_column(String(2048), nullable=False)
    last_batch_received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    coverage_status: Mapped[str] = mapped_column(String(16), nullable=False)
    supports_replay: Mapped[bool] = mapped_column(Boolean, nullable=False)
    coverage_note: Mapped[str | None] = mapped_column(String(2048))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class DiscoveryConnectivityEvent(Base):
    """Immutable disconnect/reconnect evidence for a live discovery source."""

    __tablename__ = "discovery_connectivity_events"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('disconnected', 'reconnected')",
            name="ck_discovery_connectivity_events_type",
        ),
        UniqueConstraint(
            "idempotency_key", name="uq_discovery_connectivity_events_idempotency_key"
        ),
        Index(
            "ix_discovery_connectivity_events_source_observed_at",
            "source_name",
            "observed_at",
        ),
        Index("ix_discovery_connectivity_events_gap_id", "gap_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    source_name: Mapped[str] = mapped_column(String(64), nullable=False)
    gap_id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(String(16), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reason: Mapped[str] = mapped_column(String(128), nullable=False)
    detail: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class DeduplicationConflict(Base):
    """Append-only evidence that a durable idempotency key was delivered again.

    Only collisions are written.  Accepted records remain in their own fact
    tables, which keeps duplicate-rate instrumentation inexpensive at scale.
    """

    __tablename__ = "deduplication_conflicts"
    __table_args__ = (
        Index("ix_deduplication_conflicts_record_type_occurred_at", "record_type", "occurred_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    record_type: Mapped[str] = mapped_column(String(64), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class DexAvailabilityTask(Base):
    """Mutable operational projection for DEX-availability admission work.

    The append-only lifecycle history remains authoritative for state history;
    this table only makes due-work lookup and crash recovery efficient.
    """

    __tablename__ = "dex_availability_tasks"
    __table_args__ = (
        CheckConstraint(
            "state IN ('PENDING_DEX', 'NEW')",
            name="ck_dex_availability_tasks_state",
        ),
        CheckConstraint("attempt_count >= 0", name="ck_dex_availability_tasks_attempt_count"),
        Index(
            "ix_dex_availability_tasks_due_pending",
            "next_check_at",
            postgresql_where=text("state = 'PENDING_DEX'"),
        ),
    )

    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tokens.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    next_check_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
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
    buys_h6: Mapped[int | None] = mapped_column(BigInteger)
    sells_h6: Mapped[int | None] = mapped_column(BigInteger)
    buys_h24: Mapped[int | None] = mapped_column(BigInteger)
    sells_h24: Mapped[int | None] = mapped_column(BigInteger)
    liquidity_base: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    liquidity_quote: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PairFactEvent(Base):
    """Immutable source-attributed pair creation/composition assertion."""

    __tablename__ = "pair_fact_events"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_pair_fact_events_idempotency"),
        Index("ix_pair_fact_events_pair_received", "pair_id", "received_at"),
        Index("ix_pair_fact_events_run_received", "collector_run_id", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    pair_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pairs.id", ondelete="RESTRICT"), nullable=False
    )
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
    api_request_log_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("api_request_log.id", ondelete="RESTRICT"), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    pair_created_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    dex_identifier: Mapped[str | None] = mapped_column(String(128))
    labels: Mapped[list[object] | None] = mapped_column(JSONB)
    base_token_address: Mapped[str | None] = mapped_column(String(128))
    base_token_name: Mapped[str | None] = mapped_column(String(512))
    base_token_symbol: Mapped[str | None] = mapped_column(String(128))
    quote_token_address: Mapped[str | None] = mapped_column(String(128))
    quote_token_name: Mapped[str | None] = mapped_column(String(512))
    quote_token_symbol: Mapped[str | None] = mapped_column(String(128))
    source_record_locator: Mapped[str] = mapped_column(String(256), nullable=False)
    source_record_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class BoostObservation(Base):
    """Immutable numeric promotion facts from pair responses or bounded feeds."""

    __tablename__ = "boost_observations"
    __table_args__ = (
        CheckConstraint(
            "source_kind IN ('pair_response', 'latest_feed', 'top_feed')",
            name="ck_boost_observations_source_kind",
        ),
        CheckConstraint(
            "active_boost_count IS NOT NULL OR amount IS NOT NULL OR total_amount IS NOT NULL",
            name="ck_boost_observations_has_fact",
        ),
        CheckConstraint(
            "active_boost_count IS NULL OR active_boost_count >= 0",
            name="ck_boost_observations_active_nonnegative",
        ),
        UniqueConstraint("idempotency_key", name="uq_boost_observations_idempotency"),
        Index("ix_boost_observations_token_received", "token_id", "received_at"),
        Index("ix_boost_observations_run_received", "collector_run_id", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    pair_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pairs.id", ondelete="RESTRICT")
    )
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
    api_request_log_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("api_request_log.id", ondelete="RESTRICT"), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    source_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    feed_rank: Mapped[int | None] = mapped_column(Integer)
    source_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    active_boost_count: Mapped[int | None] = mapped_column(Integer)
    amount: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    total_amount: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    source_record_locator: Mapped[str] = mapped_column(String(256), nullable=False)
    source_record_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class BoostEvent(Base):
    """Immutable first-seen, state-change, and neutral numeric crossing event."""

    __tablename__ = "boost_events"
    __table_args__ = (
        CheckConstraint(
            "event_type IN ('first_seen', 'state_change', 'threshold_crossing')",
            name="ck_boost_events_type",
        ),
        CheckConstraint(
            "metric IN ('active_boost_count', 'amount', 'total_amount')",
            name="ck_boost_events_metric",
        ),
        CheckConstraint("direction IN ('none', 'up', 'down')", name="ck_boost_events_direction"),
        CheckConstraint(
            "(event_type = 'threshold_crossing' AND threshold_value IS NOT NULL "
            "AND direction IN ('up', 'down')) OR "
            "(event_type <> 'threshold_crossing' AND threshold_value IS NULL)",
            name="ck_boost_events_threshold_shape",
        ),
        UniqueConstraint("idempotency_key", name="uq_boost_events_idempotency"),
        Index("ix_boost_events_token_decided", "token_id", "decided_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    boost_observation_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("boost_observations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    metric: Mapped[str] = mapped_column(String(32), nullable=False)
    direction: Mapped[str] = mapped_column(String(8), nullable=False)
    previous_value: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    new_value: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    threshold_value: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    policy_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class TokenMetadataEvent(Base):
    """Immutable source-specific token display/URI/link state change."""

    __tablename__ = "token_metadata_events"
    __table_args__ = (
        CheckConstraint(
            "source_kind IN ('discovery', 'pair_response', 'boost_feed')",
            name="ck_token_metadata_events_source_kind",
        ),
        CheckConstraint(
            "api_request_log_id IS NOT NULL OR discovery_event_id IS NOT NULL",
            name="ck_token_metadata_events_provenance",
        ),
        UniqueConstraint("idempotency_key", name="uq_token_metadata_events_idempotency"),
        Index("ix_token_metadata_events_token_received", "token_id", "received_at"),
        Index("ix_token_metadata_events_run_received", "collector_run_id", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    pair_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pairs.id", ondelete="RESTRICT")
    )
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
    api_request_log_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("api_request_log.id", ondelete="RESTRICT")
    )
    discovery_event_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("discovery_events.id", ondelete="RESTRICT")
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    source_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    source_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    name: Mapped[str | None] = mapped_column(String(512))
    symbol: Mapped[str | None] = mapped_column(String(128))
    metadata_uri: Mapped[str | None] = mapped_column(String(4096))
    image_url: Mapped[str | None] = mapped_column(String(4096))
    header_url: Mapped[str | None] = mapped_column(String(4096))
    website_url: Mapped[str | None] = mapped_column(String(4096))
    twitter: Mapped[str | None] = mapped_column(String(2048))
    telegram: Mapped[str | None] = mapped_column(String(2048))
    other_links: Mapped[list[object] | None] = mapped_column(JSONB)
    source_record_locator: Mapped[str] = mapped_column(String(256), nullable=False)
    source_record_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    content_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class TokenSecurityTask(Base):
    """Finite operational schedule for inexpensive mint-account snapshots."""

    __tablename__ = "token_security_tasks"
    __table_args__ = (
        CheckConstraint("phase >= 0 AND phase <= 4", name="ck_token_security_tasks_phase"),
        CheckConstraint("attempt_count >= 0", name="ck_token_security_tasks_attempt_count"),
        CheckConstraint(
            "(next_due_at IS NULL AND phase = 4) OR next_due_at IS NOT NULL",
            name="ck_token_security_tasks_completion",
        ),
        CheckConstraint(
            "(lease_id IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_token_security_tasks_lease",
        ),
        Index(
            "ix_token_security_tasks_due",
            "next_due_at",
            postgresql_where=text("next_due_at IS NOT NULL"),
        ),
    )

    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), primary_key=True
    )
    phase: Mapped[int] = mapped_column(Integer, nullable=False)
    next_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_checked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class TokenSecuritySnapshot(Base):
    """Immutable decoded mint account state with raw-account provenance."""

    __tablename__ = "token_security_snapshots"
    __table_args__ = (
        CheckConstraint(
            "status IN ('available', 'unavailable', 'malformed')",
            name="ck_token_security_snapshots_status",
        ),
        CheckConstraint(
            "token_program IN ('spl_token', 'token_2022', 'unknown')",
            name="ck_token_security_snapshots_program",
        ),
        CheckConstraint(
            "(status = 'available' AND account_owner IS NOT NULL "
            "AND raw_account_sha256 IS NOT NULL) OR status <> 'available'",
            name="ck_token_security_snapshots_available_shape",
        ),
        UniqueConstraint("idempotency_key", name="uq_token_security_snapshots_idempotency"),
        Index("ix_token_security_snapshots_token_received", "token_id", "received_at"),
        Index("ix_token_security_snapshots_run_received", "collector_run_id", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
    api_request_log_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("api_request_log.id", ondelete="RESTRICT"), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    source_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    rpc_slot: Mapped[int | None] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    account_owner: Mapped[str | None] = mapped_column(String(128))
    token_program: Mapped[str] = mapped_column(String(16), nullable=False)
    mint_authority: Mapped[str | None] = mapped_column(String(128))
    freeze_authority: Mapped[str | None] = mapped_column(String(128))
    raw_supply: Mapped[Decimal | None] = mapped_column(Numeric(38, 0))
    decimals: Mapped[int | None] = mapped_column(Integer)
    is_initialized: Mapped[bool | None] = mapped_column(Boolean)
    extension_types: Mapped[list[object] | None] = mapped_column(JSONB)
    raw_account_sha256: Mapped[str | None] = mapped_column(String(64))
    decode_error: Mapped[str | None] = mapped_column(String(2048))
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class MarketContextSnapshot(Base):
    """Shared as-of-safe context derived once per fixed UTC bucket."""

    __tablename__ = "market_context_snapshots"
    __table_args__ = (
        CheckConstraint("bucket_end > bucket_start", name="ck_market_context_bucket"),
        UniqueConstraint(
            "collection_epoch_id",
            "bucket_start",
            "policy_sha256",
            name="uq_market_context_epoch_bucket_policy",
        ),
        Index("ix_market_context_epoch_received", "collection_epoch_id", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collection_epochs.id", ondelete="RESTRICT"), nullable=False
    )
    collector_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT"), nullable=False
    )
    bucket_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    bucket_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    sol_usd_price: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    sol_return_5m: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    sol_realized_volatility_1h: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    admitted_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    active_transitions: Mapped[int] = mapped_column(Integer, nullable=False)
    mature_cohort_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    mature_cohort_active_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    mature_cohort_active_fraction: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    pair_sample_count: Mapped[int] = mapped_column(Integer, nullable=False)
    aggregate_volume_m5_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 6))
    aggregate_buys_m5: Mapped[int | None] = mapped_column(BigInteger)
    aggregate_sells_m5: Mapped[int | None] = mapped_column(BigInteger)
    policy_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


_CANDIDATE_TIERS_SQL = (
    "'TIER_0_UNIVERSAL', 'TIER_1_INTERESTING', 'TIER_2_INVESTIGATE', "
    "'TIER_3_DEEP_REVIEW', 'TIER_4_PRETRADE'"
)


class CandidatePolicyRecord(Base):
    """Immutable orchestration policy contents addressed by digest."""

    __tablename__ = "candidate_policies"

    policy_sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    policy_name: Mapped[str] = mapped_column(String(128), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(32), nullable=False)
    policy_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CandidateEvent(Base):
    """Immutable fact that contemporaneous evidence justified enrichment."""

    __tablename__ = "candidate_events"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_candidate_events_idempotency"),
        Index("ix_candidate_events_token_at", "token_id", "candidate_at"),
        Index("ix_candidate_events_epoch_at", "collection_epoch_id", "candidate_at"),
        Index("ix_candidate_events_run", "collector_run_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("collection_epochs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
    candidate_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    trigger_type: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger_version: Mapped[str] = mapped_column(String(32), nullable=False)
    feature_set_name: Mapped[str | None] = mapped_column(String(128))
    feature_set_version: Mapped[str | None] = mapped_column(String(32))
    input_watermark: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lifecycle_state: Mapped[str] = mapped_column(String(32), nullable=False)
    coverage_class: Mapped[str] = mapped_column(String(32), nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    evidence_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    source_fact_ids: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    policy_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("candidate_policies.policy_sha256", ondelete="RESTRICT"),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CandidateTierEvent(Base):
    """Immutable promotion/demotion evidence; current tier is a projection."""

    __tablename__ = "candidate_tier_events"
    __table_args__ = (
        CheckConstraint(
            f"previous_tier IN ({_CANDIDATE_TIERS_SQL})",
            name="ck_candidate_tier_events_previous",
        ),
        CheckConstraint(
            f"new_tier IN ({_CANDIDATE_TIERS_SQL})",
            name="ck_candidate_tier_events_new",
        ),
        UniqueConstraint("idempotency_key", name="uq_candidate_tier_events_idempotency"),
        Index("ix_candidate_tier_events_token_at", "token_id", "decided_at"),
        Index("ix_candidate_tier_events_epoch_at", "collection_epoch_id", "decided_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    candidate_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT")
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("collection_epochs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
    previous_tier: Mapped[str] = mapped_column(String(32), nullable=False)
    new_tier: Mapped[str] = mapped_column(String(32), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    input_watermark: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(64), nullable=False)
    reason_detail: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    transition_version: Mapped[str] = mapped_column(String(32), nullable=False)
    policy_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("candidate_policies.policy_sha256", ondelete="RESTRICT"),
        nullable=False,
    )
    evidence_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CandidateCurrentState(Base):
    """Rebuildable compact projection of the latest immutable tier event."""

    __tablename__ = "candidate_current_state"
    __table_args__ = (
        PrimaryKeyConstraint("collection_epoch_id", "token_id", name="pk_candidate_current_state"),
        CheckConstraint(
            f"tier IN ({_CANDIDATE_TIERS_SQL})", name="ck_candidate_current_state_tier"
        ),
        Index("ix_candidate_current_state_tier_due", "tier", "next_evaluation_at"),
        Index("ix_candidate_current_state_coverage", "coverage_expires_at"),
    )

    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("collection_epochs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    tier: Mapped[str] = mapped_column(String(32), nullable=False)
    latest_candidate_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT")
    )
    latest_tier_event_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("candidate_tier_events.id", ondelete="RESTRICT"),
        nullable=False,
    )
    tier_since: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    coverage_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_evaluation_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    input_watermark: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    evidence_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("candidate_policies.policy_sha256", ondelete="RESTRICT"),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class CandidateEnrichmentTask(Base):
    """Durable leased work for selective candidate enrichment."""

    __tablename__ = "candidate_enrichment_tasks"
    __table_args__ = (
        CheckConstraint(f"tier IN ({_CANDIDATE_TIERS_SQL})", name="ck_candidate_tasks_tier"),
        CheckConstraint(
            "status IN ('pending', 'claimed', 'succeeded', 'retry', 'failed', 'deferred')",
            name="ck_candidate_tasks_status",
        ),
        CheckConstraint("attempt_count >= 0", name="ck_candidate_tasks_attempts"),
        CheckConstraint("max_attempts > 0", name="ck_candidate_tasks_max_attempts"),
        CheckConstraint(
            "(lease_id IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_candidate_tasks_lease",
        ),
        UniqueConstraint("semantic_key", name="uq_candidate_tasks_semantic_key"),
        Index("ix_candidate_tasks_claim", "status", "not_before", "created_at"),
        Index("ix_candidate_tasks_token", "token_id", "created_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT"), nullable=False
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("collection_epochs.id", ondelete="RESTRICT"),
        nullable=False,
    )
    tier: Mapped[str] = mapped_column(String(32), nullable=False)
    analysis_type: Mapped[str] = mapped_column(String(64), nullable=False)
    input_watermark: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    input_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    not_before: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    lease_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    claimed_by: Mapped[str | None] = mapped_column(String(128))
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(String(64))
    failure_detail: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    evidence_generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    evidence_received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    fresh_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result_identity: Mapped[str | None] = mapped_column(String(256))
    result_sha256: Mapped[str | None] = mapped_column(String(64))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


_SECURITY_AVAILABILITY_SQL = "'available', 'partial', 'unavailable', 'failed'"
_SECURITY_COMPLETENESS_SQL = (
    "'full_distribution', 'top_20_token_accounts', 'closed_time_range', "
    "'partial_pagination', 'bounded_graph', 'unknown'"
)
_SECURITY_ACQUISITION_SQL = "'historically_available', 'retrospectively_reconstructed'"


class SecurityEnrichmentPolicyRecord(Base):
    """Immutable Phase 6 policy contents addressed by digest."""

    __tablename__ = "security_enrichment_policies"

    policy_sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    policy_name: Mapped[str] = mapped_column(String(128), nullable=False)
    policy_version: Mapped[str] = mapped_column(String(32), nullable=False)
    policy_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class SecurityProviderBudgetReservation(Base):
    """Append-only per-minute provider capacity reservation; crashes consume budget safely."""

    __tablename__ = "security_provider_budget_reservations"
    __table_args__ = (
        UniqueConstraint("semantic_key", name="uq_security_provider_budget_semantic"),
        Index(
            "ix_security_provider_budget_provider_reserved",
            "provider",
            "reserved_at",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    candidate_task_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("candidate_enrichment_tasks.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    budget_class: Mapped[str] = mapped_column(String(64), nullable=False)
    reserved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class SecurityProviderRequest(Base):
    """Immutable bounded provider attempt, including partial and failed evidence."""

    __tablename__ = "security_provider_requests"
    __table_args__ = (
        CheckConstraint(
            f"outcome IN ({_SECURITY_AVAILABILITY_SQL})",
            name="ck_security_provider_requests_outcome",
        ),
        CheckConstraint(
            f"completeness IN ({_SECURITY_COMPLETENESS_SQL})",
            name="ck_security_provider_requests_completeness",
        ),
        CheckConstraint(
            f"acquisition_mode IN ({_SECURITY_ACQUISITION_SQL})",
            name="ck_security_provider_requests_acquisition",
        ),
        UniqueConstraint("semantic_key", name="uq_security_provider_requests_semantic"),
        Index("ix_security_provider_requests_provider_at", "provider", "requested_at"),
        Index("ix_security_provider_requests_task", "candidate_task_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    candidate_task_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("candidate_enrichment_tasks.id", ondelete="RESTRICT"),
        nullable=False,
    )
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT"), nullable=False
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collection_epochs.id", ondelete="RESTRICT"), nullable=False
    )
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    method: Mapped[str] = mapped_column(String(64), nullable=False)
    provider_schema_version: Mapped[str] = mapped_column(String(32), nullable=False)
    requested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)
    completeness: Mapped[str] = mapped_column(String(32), nullable=False)
    acquisition_mode: Mapped[str] = mapped_column(String(40), nullable=False)
    source_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    source_slot: Mapped[int | None] = mapped_column(BigInteger)
    page_cursor: Mapped[str | None] = mapped_column(String(256))
    next_cursor: Mapped[str | None] = mapped_column(String(256))
    http_status_code: Mapped[int | None] = mapped_column(Integer)
    request_payload: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    response_payload: Mapped[object | None] = mapped_column(JSONB)
    response_payload_sha256: Mapped[str | None] = mapped_column(String(64))
    failure_detail: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class HolderSnapshot(Base):
    """Immutable aggregate holder evidence with explicit source completeness."""

    __tablename__ = "holder_snapshots"
    __table_args__ = (
        CheckConstraint(
            f"availability IN ({_SECURITY_AVAILABILITY_SQL})",
            name="ck_holder_snapshots_availability",
        ),
        CheckConstraint(
            f"completeness IN ({_SECURITY_COMPLETENESS_SQL})",
            name="ck_holder_snapshots_completeness",
        ),
        CheckConstraint(
            f"acquisition_mode IN ({_SECURITY_ACQUISITION_SQL})",
            name="ck_holder_snapshots_acquisition",
        ),
        UniqueConstraint("semantic_key", name="uq_holder_snapshots_semantic"),
        Index("ix_holder_snapshots_token_received", "token_id", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT"), nullable=False
    )
    candidate_task_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("candidate_enrichment_tasks.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider_request_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("security_provider_requests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collection_epochs.id", ondelete="RESTRICT"), nullable=False
    )
    source_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    availability: Mapped[str] = mapped_column(String(16), nullable=False)
    completeness: Mapped[str] = mapped_column(String(32), nullable=False)
    acquisition_mode: Mapped[str] = mapped_column(String(40), nullable=False)
    source_slot: Mapped[int | None] = mapped_column(BigInteger)
    mint_supply_raw: Mapped[Decimal | None] = mapped_column(Numeric(78, 0))
    holder_count: Mapped[int | None] = mapped_column(BigInteger)
    top_1_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    top_5_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    top_10_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    top_20_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    largest_holder_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    largest_non_pool_holder_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    creator_holder_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    hhi: Mapped[Decimal | None] = mapped_column(Numeric(24, 18))
    covered_supply_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    holder_growth: Mapped[int | None] = mapped_column(BigInteger)
    page_count: Mapped[int] = mapped_column(Integer, nullable=False)
    truncated: Mapped[bool] = mapped_column(Boolean, nullable=False)
    input_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("security_enrichment_policies.policy_sha256", ondelete="RESTRICT"),
        nullable=False,
    )
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class HolderBalanceFact(Base):
    """One retained holder account; exclusions require an explicit reason."""

    __tablename__ = "holder_balance_facts"
    __table_args__ = (
        UniqueConstraint("holder_snapshot_id", "token_account", name="uq_holder_balance_account"),
        Index("ix_holder_balance_wallet", "owner_wallet"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    holder_snapshot_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("holder_snapshots.id", ondelete="RESTRICT"), nullable=False
    )
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    token_account: Mapped[str] = mapped_column(String(128), nullable=False)
    owner_wallet: Mapped[str | None] = mapped_column(String(128))
    raw_balance: Mapped[Decimal] = mapped_column(Numeric(78, 0), nullable=False)
    balance_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    is_known_pool: Mapped[bool] = mapped_column(Boolean, nullable=False)
    is_creator: Mapped[bool] = mapped_column(Boolean, nullable=False)
    exclusion_reason: Mapped[str | None] = mapped_column(String(256))
    source_fact_identity: Mapped[str] = mapped_column(String(256), nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class TraderDistributionSnapshot(Base):
    """Immutable bounded-window trader aggregates; raw signatures stay in provenance."""

    __tablename__ = "trader_distribution_snapshots"
    __table_args__ = (
        CheckConstraint("window_end > window_start", name="ck_trader_distribution_window"),
        CheckConstraint(
            f"availability IN ({_SECURITY_AVAILABILITY_SQL})",
            name="ck_trader_distribution_availability",
        ),
        CheckConstraint(
            f"completeness IN ({_SECURITY_COMPLETENESS_SQL})",
            name="ck_trader_distribution_completeness",
        ),
        CheckConstraint(
            f"acquisition_mode IN ({_SECURITY_ACQUISITION_SQL})",
            name="ck_trader_distribution_acquisition",
        ),
        UniqueConstraint("semantic_key", name="uq_trader_distribution_semantic"),
        Index("ix_trader_distribution_token_received", "token_id", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT"), nullable=False
    )
    candidate_task_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("candidate_enrichment_tasks.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider_request_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("security_provider_requests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collection_epochs.id", ondelete="RESTRICT"), nullable=False
    )
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_observed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    availability: Mapped[str] = mapped_column(String(16), nullable=False)
    completeness: Mapped[str] = mapped_column(String(32), nullable=False)
    acquisition_mode: Mapped[str] = mapped_column(String(40), nullable=False)
    unique_buyers: Mapped[int | None] = mapped_column(BigInteger)
    unique_sellers: Mapped[int | None] = mapped_column(BigInteger)
    unique_traders: Mapped[int | None] = mapped_column(BigInteger)
    total_trades: Mapped[int] = mapped_column(BigInteger, nullable=False)
    buy_trades: Mapped[int] = mapped_column(BigInteger, nullable=False)
    sell_trades: Mapped[int] = mapped_column(BigInteger, nullable=False)
    volume_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    median_trade_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    p90_trade_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    p95_trade_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    largest_trade_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    top_1_trader_volume_share: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    top_5_trader_volume_share: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    top_10_trader_volume_share: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    repeat_trader_ratio: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    new_wallet_ratio: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    buy_sell_wallet_overlap: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    page_count: Mapped[int] = mapped_column(Integer, nullable=False)
    source_fact_ids: Mapped[list[object]] = mapped_column(JSONB, nullable=False)
    input_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("security_enrichment_policies.policy_sha256", ondelete="RESTRICT"),
        nullable=False,
    )
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CreatorRelationshipEvent(Base):
    """Immutable typed token-to-creator/deployer relationship."""

    __tablename__ = "creator_relationship_events"
    __table_args__ = (
        CheckConstraint(
            f"acquisition_mode IN ({_SECURITY_ACQUISITION_SQL})",
            name="ck_creator_relationship_acquisition",
        ),
        UniqueConstraint("semantic_key", name="uq_creator_relationship_semantic"),
        Index("ix_creator_relationship_token_received", "token_id", "received_at"),
        Index("ix_creator_relationship_wallet_received", "creator_wallet", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT"), nullable=False
    )
    provider_request_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("security_provider_requests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    creator_wallet: Mapped[str] = mapped_column(String(128), nullable=False)
    relationship_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    acquisition_mode: Mapped[str] = mapped_column(String(40), nullable=False)
    source_fact_identity: Mapped[str] = mapped_column(String(256), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CreatorHistorySnapshot(Base):
    """As-of-safe factual history calculated only from launches received by T."""

    __tablename__ = "creator_history_snapshots"
    __table_args__ = (
        CheckConstraint(
            f"availability IN ({_SECURITY_AVAILABILITY_SQL})",
            name="ck_creator_history_availability",
        ),
        CheckConstraint(
            f"acquisition_mode IN ({_SECURITY_ACQUISITION_SQL})",
            name="ck_creator_history_acquisition",
        ),
        UniqueConstraint("semantic_key", name="uq_creator_history_semantic"),
        Index("ix_creator_history_wallet_received", "creator_wallet", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT"), nullable=False
    )
    candidate_task_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("candidate_enrichment_tasks.id", ondelete="RESTRICT"),
        nullable=False,
    )
    provider_request_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("security_provider_requests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collection_epochs.id", ondelete="RESTRICT"), nullable=False
    )
    creator_wallet: Mapped[str] = mapped_column(String(128), nullable=False)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    availability: Mapped[str] = mapped_column(String(16), nullable=False)
    acquisition_mode: Mapped[str] = mapped_column(String(40), nullable=False)
    prior_token_count: Mapped[int | None] = mapped_column(Integer)
    prior_tracked_launches: Mapped[int | None] = mapped_column(Integer)
    prior_collapse_count: Mapped[int | None] = mapped_column(Integer)
    prior_large_winner_count: Mapped[int | None] = mapped_column(Integer)
    median_survival_seconds: Mapped[Decimal | None] = mapped_column(Numeric(24, 6))
    mean_survival_seconds: Mapped[Decimal | None] = mapped_column(Numeric(24, 6))
    launches_last_30d: Mapped[int | None] = mapped_column(Integer)
    creator_hold_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    source_token_ids: Mapped[list[object]] = mapped_column(JSONB, nullable=False)
    input_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("security_enrichment_policies.policy_sha256", ondelete="RESTRICT"),
        nullable=False,
    )
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class LiquidityEventEvidence(Base):
    """Immutable decoded or observation-derived structural liquidity event."""

    __tablename__ = "liquidity_event_evidence"
    __table_args__ = (
        CheckConstraint(
            f"availability IN ({_SECURITY_AVAILABILITY_SQL})",
            name="ck_liquidity_event_availability",
        ),
        CheckConstraint(
            f"acquisition_mode IN ({_SECURITY_ACQUISITION_SQL})",
            name="ck_liquidity_event_acquisition",
        ),
        UniqueConstraint("semantic_key", name="uq_liquidity_event_semantic"),
        Index("ix_liquidity_event_token_received", "token_id", "received_at"),
        Index("ix_liquidity_event_pair_source", "pair_id", "source_event_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    pair_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pairs.id", ondelete="RESTRICT")
    )
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT"), nullable=False
    )
    provider_request_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("security_provider_requests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    source_event_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    availability: Mapped[str] = mapped_column(String(16), nullable=False)
    acquisition_mode: Mapped[str] = mapped_column(String(40), nullable=False)
    source_slot: Mapped[int | None] = mapped_column(BigInteger)
    signature: Mapped[str | None] = mapped_column(String(128))
    base_delta: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    quote_delta: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    liquidity_usd_before: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    liquidity_usd_after: Mapped[Decimal | None] = mapped_column(Numeric(38, 18))
    removal_pct: Mapped[Decimal | None] = mapped_column(Numeric(20, 12))
    lp_wallet: Mapped[str | None] = mapped_column(String(128))
    source_fact_ids: Mapped[list[object]] = mapped_column(JSONB, nullable=False)
    decoder_version: Mapped[str] = mapped_column(String(32), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class WalletRelationshipEdge(Base):
    """Immutable factual edge; it never asserts two wallets share an identity."""

    __tablename__ = "wallet_relationship_edges"
    __table_args__ = (
        CheckConstraint("wallet_a <> wallet_b", name="ck_wallet_edges_distinct"),
        CheckConstraint(
            f"acquisition_mode IN ({_SECURITY_ACQUISITION_SQL})",
            name="ck_wallet_edges_acquisition",
        ),
        UniqueConstraint("semantic_key", name="uq_wallet_edges_semantic"),
        Index("ix_wallet_edges_token_received", "token_id", "evidence_received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT"), nullable=False
    )
    provider_request_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("security_provider_requests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    wallet_a: Mapped[str] = mapped_column(String(128), nullable=False)
    wallet_b: Mapped[str] = mapped_column(String(128), nullable=False)
    relationship_type: Mapped[str] = mapped_column(String(64), nullable=False)
    first_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    evidence_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    strength_count: Mapped[int] = mapped_column(Integer, nullable=False)
    acquisition_mode: Mapped[str] = mapped_column(String(40), nullable=False)
    source_fact_ids: Mapped[list[object]] = mapped_column(JSONB, nullable=False)
    method_version: Mapped[str] = mapped_column(String(32), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class FundingRelationshipEvidence(Base):
    """Bounded one-to-two-hop funding provenance."""

    __tablename__ = "funding_relationship_evidence"
    __table_args__ = (
        CheckConstraint("hop_depth BETWEEN 1 AND 2", name="ck_funding_relationship_depth"),
        CheckConstraint(
            f"completeness IN ({_SECURITY_COMPLETENESS_SQL})",
            name="ck_funding_relationship_completeness",
        ),
        CheckConstraint(
            f"acquisition_mode IN ({_SECURITY_ACQUISITION_SQL})",
            name="ck_funding_relationship_acquisition",
        ),
        UniqueConstraint("semantic_key", name="uq_funding_relationship_semantic"),
        Index("ix_funding_relationship_token_received", "token_id", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT"), nullable=False
    )
    provider_request_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("security_provider_requests.id", ondelete="RESTRICT"),
        nullable=False,
    )
    wallet: Mapped[str] = mapped_column(String(128), nullable=False)
    funding_source: Mapped[str] = mapped_column(String(128), nullable=False)
    funding_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    amount_lamports: Mapped[int | None] = mapped_column(BigInteger)
    hop_depth: Mapped[int] = mapped_column(Integer, nullable=False)
    source_signature: Mapped[str] = mapped_column(String(128), nullable=False)
    completeness: Mapped[str] = mapped_column(String(32), nullable=False)
    acquisition_mode: Mapped[str] = mapped_column(String(40), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class WalletClusterSnapshot(Base):
    """Explainable connected component derived from a fixed edge set."""

    __tablename__ = "wallet_cluster_snapshots"
    __table_args__ = (
        CheckConstraint(
            f"acquisition_mode IN ({_SECURITY_ACQUISITION_SQL})",
            name="ck_wallet_clusters_acquisition",
        ),
        UniqueConstraint("semantic_key", name="uq_wallet_clusters_semantic"),
        Index("ix_wallet_clusters_token_generated", "token_id", "generated_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    cluster_id: Mapped[str] = mapped_column(String(64), nullable=False)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT"), nullable=False
    )
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    acquisition_mode: Mapped[str] = mapped_column(String(40), nullable=False)
    algorithm_version: Mapped[str] = mapped_column(String(64), nullable=False)
    input_edge_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    members: Mapped[list[object]] = mapped_column(JSONB, nullable=False)
    explanation: Mapped[list[object]] = mapped_column(JSONB, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class SecurityFeatureSnapshot(Base):
    """Immutable versioned security-v1 features available at received_at."""

    __tablename__ = "security_feature_snapshots"
    __table_args__ = (
        CheckConstraint(
            f"acquisition_mode IN ({_SECURITY_ACQUISITION_SQL})",
            name="ck_security_features_acquisition",
        ),
        UniqueConstraint("semantic_key", name="uq_security_features_semantic"),
        Index("ix_security_features_token_received", "token_id", "received_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    semantic_key: Mapped[str] = mapped_column(String(64), nullable=False)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    candidate_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_events.id", ondelete="RESTRICT"), nullable=False
    )
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collection_epochs.id", ondelete="RESTRICT"), nullable=False
    )
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    acquisition_mode: Mapped[str] = mapped_column(String(40), nullable=False)
    feature_set_name: Mapped[str] = mapped_column(String(64), nullable=False)
    feature_set_version: Mapped[str] = mapped_column(String(32), nullable=False)
    values: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    input_fact_ids: Mapped[list[object]] = mapped_column(JSONB, nullable=False)
    input_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    schema_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("security_enrichment_policies.policy_sha256", ondelete="RESTRICT"),
        nullable=False,
    )
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class LifecycleEvidenceEvaluation(Base):
    """Immutable derived record selecting lifecycle evidence from pair facts."""

    __tablename__ = "lifecycle_evidence_evaluations"
    __table_args__ = (
        PrimaryKeyConstraint(
            "input_watermark",
            "id",
            name="pk_lifecycle_evidence_evaluations",
        ),
        UniqueConstraint(
            "input_watermark",
            "token_id",
            "api_request_log_id",
            "policy_sha256",
            name="uq_lifecycle_evidence_token_request_policy",
        ),
        ForeignKeyConstraint(
            ["selected_observation_received_at", "selected_observation_id"],
            ["observations.received_at", "observations.id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "outcome IN ('selected', 'failed')",
            name="ck_lifecycle_evidence_evaluations_outcome",
        ),
        CheckConstraint(
            "(outcome = 'selected' AND selected_pair_id IS NOT NULL "
            "AND selected_observation_id IS NOT NULL "
            "AND selected_observation_received_at IS NOT NULL) OR "
            "(outcome = 'failed' AND selected_pair_id IS NULL "
            "AND selected_observation_id IS NULL "
            "AND selected_observation_received_at IS NULL)",
            name="ck_lifecycle_evidence_evaluations_selection",
        ),
        Index(
            "ix_lifecycle_evidence_token_watermark",
            "token_id",
            "input_watermark",
        ),
        {"postgresql_partition_by": "RANGE (input_watermark)"},
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), default=uuid.uuid4, nullable=False)
    input_watermark: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    api_request_log_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("api_request_log.id", ondelete="RESTRICT"), nullable=False
    )
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)
    selected_pair_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("pairs.id", ondelete="RESTRICT")
    )
    selected_observation_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True))
    selected_observation_received_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    reason_code: Mapped[str] = mapped_column(String(128), nullable=False)
    reason_detail: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    policy_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey(
            "lifecycle_policies.policy_sha256",
            name="fk_lifecycle_evidence_evaluations_policy",
            ondelete="RESTRICT",
        ),
        nullable=False,
    )
    # Historical rows retain their inline copy. New writes resolve the immutable
    # normalized policy by policy_sha256 and intentionally leave this NULL.
    policy_snapshot: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class LifecyclePolicy(Base):
    """One immutable lifecycle-evidence policy document, addressed by digest."""

    __tablename__ = "lifecycle_policies"

    policy_sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    policy_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class LifecycleEvent(Base):
    """Immutable derived lifecycle transition, never a source observation."""

    __tablename__ = "lifecycle_events"
    __table_args__ = (
        ForeignKeyConstraint(
            ["lifecycle_evidence_input_watermark", "lifecycle_evidence_evaluation_id"],
            [
                "lifecycle_evidence_evaluations.input_watermark",
                "lifecycle_evidence_evaluations.id",
            ],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "(lifecycle_evidence_input_watermark IS NULL) = "
            "(lifecycle_evidence_evaluation_id IS NULL)",
            name="ck_lifecycle_events_evidence_reference",
        ),
        UniqueConstraint("idempotency_key", name="uq_lifecycle_events_idempotency_key"),
        Index("ix_lifecycle_events_token_decided_at", "token_id", "decided_at"),
        Index("ix_lifecycle_events_collector_run_id", "collector_run_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
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
    lifecycle_evidence_evaluation_id: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True))
    lifecycle_evidence_input_watermark: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    configuration_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


_SCHEDULABLE_STATES_SQL = "'NEW', 'ACTIVE', 'WATCH', 'FADING', 'DORMANT', 'RESURRECTED'"
_COVERAGE_CLASSES_SQL = (
    "'PROTECTED_ACTIVE', 'PROTECTED_RESURRECTED', 'PROTECTED_WATCH', "
    "'INITIAL', 'EARLY', 'MATURE', 'FADING_TAIL', 'FADING_COOL', "
    "'COOLED', 'LONG_TAIL_DAY', 'LONG_TAIL_WEEK', 'RETIRED_CONTROL'"
)


class CoveragePolicy(Base):
    """One immutable coverage-class policy addressed by content digest."""

    __tablename__ = "coverage_policies"

    policy_sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    policy_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class SchedulerPolicy(Base):
    """One immutable capacity-aware scheduler policy addressed by its digest."""

    __tablename__ = "scheduler_policies"

    policy_sha256: Mapped[str] = mapped_column(String(64), primary_key=True)
    policy_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class SchedulerCapacityDecision(Base):
    """Immutable population and effective-cadence decision for one time bucket."""

    __tablename__ = "scheduler_capacity_decisions"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_scheduler_capacity_decisions_idempotency_key"),
        CheckConstraint(
            "capacity_mode IN ('NORMAL', 'DEGRADED', 'CRITICAL')",
            name="ck_scheduler_capacity_decisions_mode",
        ),
        Index("ix_scheduler_capacity_decisions_decided_at", "decided_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    idempotency_key: Mapped[str] = mapped_column(String(64), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    capacity_mode: Mapped[str] = mapped_column(String(16), nullable=False)
    policy_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("scheduler_policies.policy_sha256", ondelete="RESTRICT"),
        nullable=False,
    )
    decision_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CoverageDecision(Base):
    """Immutable evidence of a coverage-class transition or reconstruction."""

    __tablename__ = "coverage_decisions"
    __table_args__ = (
        CheckConstraint(
            f"previous_coverage_class IS NULL OR previous_coverage_class IN "
            f"({_COVERAGE_CLASSES_SQL})",
            name="ck_coverage_decisions_previous_class",
        ),
        CheckConstraint(
            f"new_coverage_class IN ({_COVERAGE_CLASSES_SQL})",
            name="ck_coverage_decisions_new_class",
        ),
        CheckConstraint(
            f"lifecycle_state IN ({_SCHEDULABLE_STATES_SQL})",
            name="ck_coverage_decisions_lifecycle_state",
        ),
        UniqueConstraint("idempotency_key", name="uq_coverage_decisions_idempotency_key"),
        Index("ix_coverage_decisions_token_decided", "token_id", "decided_at"),
        Index("ix_coverage_decisions_epoch_decided", "collection_epoch_id", "decided_at"),
        Index("ix_coverage_decisions_run_decided", "collector_run_id", "decided_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collection_epoch_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collection_epochs.id", ondelete="RESTRICT")
    )
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    previous_coverage_class: Mapped[str | None] = mapped_column(String(32))
    new_coverage_class: Mapped[str] = mapped_column(String(32), nullable=False)
    lifecycle_state: Mapped[str] = mapped_column(String(32), nullable=False)
    admitted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    coverage_effective_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(128), nullable=False)
    policy_sha256: Mapped[str] = mapped_column(
        String(64),
        ForeignKey("coverage_policies.policy_sha256", ondelete="RESTRICT"),
        nullable=False,
    )
    capacity_decision_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("scheduler_capacity_decisions.id", ondelete="RESTRICT"),
    )
    target_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    effective_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    next_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    detail: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PollBatch(Base):
    """Immutable evidence that one bounded poll batch was claimed."""

    __tablename__ = "poll_batches"
    __table_args__ = (
        CheckConstraint("reserved_request_capacity > 0", name="ck_poll_batches_reservation"),
        CheckConstraint(
            "batch_kind IN ('ordinary', 'retired_control')",
            name="ck_poll_batches_kind",
        ),
        CheckConstraint(
            "(batch_kind = 'ordinary' AND control_window_start IS NULL) OR "
            "(batch_kind = 'retired_control' AND control_window_start IS NOT NULL)",
            name="ck_poll_batches_control_window",
        ),
        UniqueConstraint("control_window_start", name="uq_poll_batches_control_window"),
        Index("ix_poll_batches_provider_claimed_at", "provider", "claimed_at"),
        Index("ix_poll_batches_collector_run_id", "collector_run_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collector_run_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT")
    )
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    chain: Mapped[str] = mapped_column(String(32), nullable=False)
    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reserved_request_capacity: Mapped[int] = mapped_column(Integer, nullable=False)
    batch_kind: Mapped[str] = mapped_column(String(32), nullable=False, default="ordinary")
    control_window_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    capacity_decision_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("scheduler_capacity_decisions.id", ondelete="RESTRICT"),
    )
    configuration_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PollSchedule(Base):
    """Mutable current projection for recurring token observation work."""

    __tablename__ = "poll_schedules"
    __table_args__ = (
        CheckConstraint(
            f"lifecycle_state IN ({_SCHEDULABLE_STATES_SQL})",
            name="ck_poll_schedules_lifecycle_state",
        ),
        CheckConstraint(
            f"coverage_class IS NULL OR coverage_class IN ({_COVERAGE_CLASSES_SQL})",
            name="ck_poll_schedules_coverage_class",
        ),
        CheckConstraint(
            "(coverage_class IS NULL AND admitted_at IS NULL "
            "AND coverage_decided_at IS NULL AND coverage_policy_sha256 IS NULL) OR "
            "(coverage_class IS NOT NULL AND admitted_at IS NOT NULL "
            "AND coverage_decided_at IS NOT NULL AND coverage_policy_sha256 IS NOT NULL)",
            name="ck_poll_schedules_coverage_projection",
        ),
        CheckConstraint(
            "coverage_class IS DISTINCT FROM 'RETIRED_CONTROL' OR next_due_at IS NULL",
            name="ck_poll_schedules_retired_not_due",
        ),
        CheckConstraint("priority >= 0", name="ck_poll_schedules_priority"),
        CheckConstraint("attempt_count >= 0", name="ck_poll_schedules_attempt_count"),
        CheckConstraint("control_scan_count >= 0", name="ck_poll_schedules_control_count"),
        CheckConstraint(
            "target_interval_seconds IS NULL OR target_interval_seconds > 0",
            name="ck_poll_schedules_target_interval",
        ),
        CheckConstraint(
            "effective_interval_seconds IS NULL OR effective_interval_seconds > 0",
            name="ck_poll_schedules_effective_interval",
        ),
        CheckConstraint(
            "(lease_id IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_poll_schedules_lease_pair",
        ),
        CheckConstraint(
            "(candidate_coverage_expires_at IS NULL AND "
            "candidate_coverage_interval_seconds IS NULL AND candidate_tier_event_id IS NULL) "
            "OR (candidate_coverage_expires_at IS NOT NULL AND "
            "candidate_coverage_interval_seconds > 0 AND candidate_tier_event_id IS NOT NULL)",
            name="ck_poll_schedules_candidate_coverage",
        ),
        Index("ix_poll_schedules_due_priority", "next_due_at", "priority"),
        Index("ix_poll_schedules_coverage_transition", "coverage_next_transition_at"),
        Index(
            "ix_poll_schedules_control_rotation",
            "last_control_scan_at",
            "admitted_at",
            "token_id",
            postgresql_where=text("coverage_class = 'RETIRED_CONTROL'"),
        ),
    )

    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("tokens.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    lifecycle_state: Mapped[str] = mapped_column(String(32), nullable=False)
    state_decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    admitted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    coverage_class: Mapped[str | None] = mapped_column(String(32))
    coverage_decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    coverage_next_transition_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    coverage_policy_sha256: Mapped[str | None] = mapped_column(
        String(64), ForeignKey("coverage_policies.policy_sha256", ondelete="RESTRICT")
    )
    candidate_coverage_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    candidate_coverage_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    candidate_tier_event_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("candidate_tier_events.id", ondelete="RESTRICT")
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    next_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_control_scan_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    control_scan_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("poll_batches.id", ondelete="RESTRICT"),
    )
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    capacity_decision_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("scheduler_capacity_decisions.id", ondelete="RESTRICT"),
    )
    target_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    effective_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    configuration_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PollScheduleDecision(Base):
    """Immutable evidence of initial scheduling or a lifecycle-policy change."""

    __tablename__ = "poll_schedule_decisions"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_poll_schedule_decisions_idempotency_key"),
        Index("ix_poll_schedule_decisions_token_decided_at", "token_id", "decided_at"),
        Index("ix_poll_schedule_decisions_collection_epoch_id", "collection_epoch_id"),
        CheckConstraint(
            "target_interval_seconds IS NULL OR target_interval_seconds > 0",
            name="ck_poll_schedule_decisions_target_interval",
        ),
        CheckConstraint(
            "effective_interval_seconds IS NULL OR effective_interval_seconds > 0",
            name="ck_poll_schedule_decisions_effective_interval",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collection_epoch_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collection_epochs.id", ondelete="RESTRICT")
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    previous_state: Mapped[str | None] = mapped_column(String(32))
    new_state: Mapped[str] = mapped_column(String(32), nullable=False)
    previous_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    new_due_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    decided_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reason_code: Mapped[str] = mapped_column(String(128), nullable=False)
    capacity_decision_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("scheduler_capacity_decisions.id", ondelete="RESTRICT"),
    )
    target_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    effective_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    configuration_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PollBatchMember(Base):
    """Immutable per-token membership and due-time evidence for a claimed batch."""

    __tablename__ = "poll_batch_members"
    __table_args__ = (
        PrimaryKeyConstraint("claimed_at", "batch_id", "token_id", name="pk_poll_batch_members"),
        CheckConstraint("priority >= 0", name="ck_poll_batch_members_priority"),
        CheckConstraint("claim_lateness_ms >= 0", name="ck_poll_batch_members_lateness"),
        CheckConstraint(
            "target_interval_seconds IS NULL OR target_interval_seconds > 0",
            name="ck_poll_batch_members_target_interval",
        ),
        CheckConstraint(
            "effective_interval_seconds IS NULL OR effective_interval_seconds > 0",
            name="ck_poll_batch_members_effective_interval",
        ),
        Index("ix_poll_batch_members_token_claimed_at", "token_id", "claimed_at"),
        {"postgresql_partition_by": "RANGE (claimed_at)"},
    )

    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    batch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("poll_batches.id", ondelete="RESTRICT"), nullable=False
    )
    token_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("tokens.id", ondelete="RESTRICT"), nullable=False
    )
    due_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lifecycle_state: Mapped[str] = mapped_column(String(32), nullable=False)
    coverage_class: Mapped[str | None] = mapped_column(String(32))
    priority: Mapped[int] = mapped_column(Integer, nullable=False)
    claim_lateness_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    capacity_decision_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("scheduler_capacity_decisions.id", ondelete="RESTRICT"),
    )
    target_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    effective_interval_seconds: Mapped[int | None] = mapped_column(Integer)
    previous_batch_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("poll_batches.id", ondelete="RESTRICT")
    )
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class PollBatchOutcome(Base):
    """Immutable completion and observation-lateness summary for a poll batch."""

    __tablename__ = "poll_batch_outcomes"
    __table_args__ = (
        CheckConstraint(
            "outcome IN ('succeeded', 'empty', 'partial', 'failed', 'throttled', "
            "'malformed', 'cancelled')",
            name="ck_poll_batch_outcomes_outcome",
        ),
        CheckConstraint("member_count > 0", name="ck_poll_batch_outcomes_member_count"),
        CheckConstraint(
            "observation_lateness_min_ms >= 0 AND observation_lateness_max_ms >= 0 "
            "AND observation_lateness_mean_ms >= 0",
            name="ck_poll_batch_outcomes_lateness",
        ),
        Index("ix_poll_batch_outcomes_completed_at", "completed_at"),
    )

    batch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("poll_batches.id", ondelete="RESTRICT"),
        primary_key=True,
    )
    api_request_log_id: Mapped[uuid.UUID | None] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("api_request_log.id", ondelete="RESTRICT")
    )
    outcome: Mapped[str] = mapped_column(String(16), nullable=False)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    member_count: Mapped[int] = mapped_column(Integer, nullable=False)
    observation_lateness_min_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    observation_lateness_max_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    observation_lateness_mean_ms: Mapped[Decimal] = mapped_column(Numeric(20, 3), nullable=False)
    failure_detail: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    configuration_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    configuration_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    persisted_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class StorageSample(Base):
    """Compact immutable database-size sample for one collector epoch."""

    __tablename__ = "storage_samples"
    __table_args__ = (
        UniqueConstraint("collector_run_id", "sampled_at", name="uq_storage_samples_run_time"),
        Index("ix_storage_samples_epoch_sampled", "collection_epoch_id", "sampled_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collection_epochs.id", ondelete="RESTRICT"), nullable=False
    )
    collector_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collector_runs.id", ondelete="RESTRICT"), nullable=False
    )
    sampled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    database_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class StorageRelationSample(Base):
    """Immutable storage and row-count estimate for one tracked relation."""

    __tablename__ = "storage_relation_samples"
    __table_args__ = (
        UniqueConstraint("storage_sample_id", "relation_name", name="uq_storage_relation_sample"),
        Index("ix_storage_relation_samples_relation", "relation_name", "storage_sample_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    storage_sample_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("storage_samples.id", ondelete="RESTRICT"), nullable=False
    )
    relation_name: Mapped[str] = mapped_column(String(128), nullable=False)
    relation_family: Mapped[str] = mapped_column(String(128), nullable=False)
    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    table_data_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    index_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    toast_and_aux_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    estimated_row_count: Mapped[int] = mapped_column(BigInteger, nullable=False)
    rows_per_minute_since_previous: Mapped[Decimal | None] = mapped_column(Numeric(20, 3))
    bytes_per_row_delta: Mapped[Decimal | None] = mapped_column(Numeric(20, 3))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class BackupVerification(Base):
    """Immutable evidence that a backup artifact was independently readable."""

    __tablename__ = "backup_verifications"
    __table_args__ = (
        UniqueConstraint(
            "collection_epoch_id",
            "artifact_sha256",
            "artifact_path",
            name="uq_backup_verifications_artifact",
        ),
        Index("ix_backup_verifications_epoch_verified", "collection_epoch_id", "verified_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collection_epochs.id", ondelete="RESTRICT"), nullable=False
    )
    artifact_path: Mapped[str] = mapped_column(String(4096), nullable=False)
    artifact_kind: Mapped[str] = mapped_column(String(64), nullable=False)
    artifact_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    artifact_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    verification_method: Mapped[str] = mapped_column(String(256), nullable=False)
    independent_copy: Mapped[bool] = mapped_column(Boolean, nullable=False)
    verified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    detail: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ArchiveScope(Base):
    """Mutable projection for one deterministic, immutable archive source scope."""

    __tablename__ = "archive_scopes"
    __table_args__ = (
        UniqueConstraint("archive_identity_sha256", name="uq_archive_scopes_identity"),
        CheckConstraint(
            "state IN ('pending', 'exporting', 'exported', 'verified', "
            "'independently_copied', 'retention_eligible', 'failed')",
            name="ck_archive_scopes_state",
        ),
        CheckConstraint("start_at < end_at", name="ck_archive_scopes_range"),
        CheckConstraint(
            "(claim_token IS NULL AND claim_expires_at IS NULL) OR "
            "(claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)",
            name="ck_archive_scopes_claim",
        ),
        Index("ix_archive_scopes_epoch_range", "collection_epoch_id", "start_at", "end_at"),
        Index("ix_archive_scopes_state_updated", "state", "updated_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True)
    archive_identity_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    collection_epoch_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("collection_epochs.id", ondelete="RESTRICT"), nullable=False
    )
    start_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    end_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    archive_schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    source_db_schema_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    source_scope_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(String(32), nullable=False)
    manifest_path: Mapped[str | None] = mapped_column(String(4096))
    manifest_sha256: Mapped[str | None] = mapped_column(String(64))
    aggregate_file_sha256: Mapped[str | None] = mapped_column(String(64))
    source_row_count: Mapped[int | None] = mapped_column(BigInteger)
    parquet_bytes: Mapped[int | None] = mapped_column(BigInteger)
    verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    verification_detail: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    analytical_reads_verified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    claim_token: Mapped[uuid.UUID | None] = mapped_column(Uuid(as_uuid=True))
    claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    failure_detail: Mapped[dict[str, object] | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class ArchiveScopeEvent(Base):
    """Immutable state-transition or failure evidence for an archive scope."""

    __tablename__ = "archive_scope_events"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_archive_scope_events_idempotency"),
        CheckConstraint(
            "event_type IN ('claimed', 'exported', 'verified', 'copy_verified', "
            "'retention_evaluated', 'failed')",
            name="ck_archive_scope_events_type",
        ),
        Index("ix_archive_scope_events_scope_time", "archive_scope_id", "occurred_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    archive_scope_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("archive_scopes.id", ondelete="RESTRICT"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    detail: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ArchiveCopyVerification(Base):
    """Immutable proof that one complete archive copy was independently read."""

    __tablename__ = "archive_copy_verifications"
    __table_args__ = (
        UniqueConstraint(
            "archive_scope_id",
            "copy_role",
            "location",
            "aggregate_file_sha256",
            name="uq_archive_copy_verifications_identity",
        ),
        CheckConstraint("copy_role IN ('primary', 'secondary')", name="ck_archive_copy_role"),
        CheckConstraint(
            "provider_kind IN ('filesystem', 's3_compatible', 'fake')",
            name="ck_archive_copy_provider",
        ),
        CheckConstraint(
            "copy_role <> 'secondary' OR independence_asserted",
            name="ck_archive_secondary_independence",
        ),
        Index("ix_archive_copy_verifications_scope", "archive_scope_id", "verified_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    archive_scope_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("archive_scopes.id", ondelete="RESTRICT"), nullable=False
    )
    copy_role: Mapped[str] = mapped_column(String(16), nullable=False)
    provider_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    location: Mapped[str] = mapped_column(String(4096), nullable=False)
    manifest_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    aggregate_file_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    total_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    object_count: Mapped[int] = mapped_column(Integer, nullable=False)
    independence_asserted: Mapped[bool] = mapped_column(Boolean, nullable=False)
    independence_detail: Mapped[str | None] = mapped_column(String(2048))
    verification_method: Mapped[str] = mapped_column(String(512), nullable=False)
    verified_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    detail: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ArchiveRetentionEvaluation(Base):
    """Immutable, non-destructive retention-eligibility calculation."""

    __tablename__ = "archive_retention_evaluations"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_archive_retention_idempotency"),
        Index("ix_archive_retention_scope_time", "archive_scope_id", "evaluated_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    archive_scope_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True), ForeignKey("archive_scopes.id", ondelete="RESTRICT"), nullable=False
    )
    evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    eligible: Mapped[bool] = mapped_column(Boolean, nullable=False)
    minimum_hot_retention_days: Mapped[int] = mapped_column(Integer, nullable=False)
    policy_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    policy_snapshot: Mapped[dict[str, object]] = mapped_column(JSONB, nullable=False)
    reasons: Mapped[list[object]] = mapped_column(JSONB, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
