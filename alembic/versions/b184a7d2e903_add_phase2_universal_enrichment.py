"""add Collector V2 Phase 2 universal enrichment facts

Revision ID: b184a7d2e903
Revises: 7c31a8e4d5f2
Create Date: 2026-08-21 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b184a7d2e903"
down_revision: str | Sequence[str] | None = "7c31a8e4d5f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add future-only normalized fields and append-only enrichment families."""
    for name, type_ in (
        ("buys_h6", sa.BigInteger()),
        ("sells_h6", sa.BigInteger()),
        ("buys_h24", sa.BigInteger()),
        ("sells_h24", sa.BigInteger()),
        ("liquidity_base", sa.Numeric(38, 18)),
        ("liquidity_quote", sa.Numeric(38, 18)),
    ):
        op.add_column("observations", sa.Column(name, type_, nullable=True))

    op.create_table(
        "pair_fact_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("pair_id", sa.Uuid(), nullable=False),
        sa.Column("collector_run_id", sa.Uuid(), nullable=True),
        sa.Column("api_request_log_id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("pair_created_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dex_identifier", sa.String(128), nullable=True),
        sa.Column("labels", postgresql.JSONB(), nullable=True),
        sa.Column("base_token_address", sa.String(128), nullable=True),
        sa.Column("base_token_name", sa.String(512), nullable=True),
        sa.Column("base_token_symbol", sa.String(128), nullable=True),
        sa.Column("quote_token_address", sa.String(128), nullable=True),
        sa.Column("quote_token_name", sa.String(512), nullable=True),
        sa.Column("quote_token_symbol", sa.String(128), nullable=True),
        sa.Column("source_record_locator", sa.String(256), nullable=False),
        sa.Column("source_record_sha256", sa.String(64), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("persisted_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.ForeignKeyConstraint(["pair_id"], ["pairs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["api_request_log_id"], ["api_request_log.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_pair_fact_events_idempotency"),
    )
    op.create_index("ix_pair_fact_events_pair_received", "pair_fact_events", ["pair_id", "received_at"])
    op.create_index("ix_pair_fact_events_run_received", "pair_fact_events", ["collector_run_id", "received_at"])

    op.create_table(
        "boost_observations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("pair_id", sa.Uuid(), nullable=True),
        sa.Column("collector_run_id", sa.Uuid(), nullable=True),
        sa.Column("api_request_log_id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("source_kind", sa.String(32), nullable=False),
        sa.Column("feed_rank", sa.Integer(), nullable=True),
        sa.Column("source_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("active_boost_count", sa.Integer(), nullable=True),
        sa.Column("amount", sa.Numeric(38, 18), nullable=True),
        sa.Column("total_amount", sa.Numeric(38, 18), nullable=True),
        sa.Column("source_record_locator", sa.String(256), nullable=False),
        sa.Column("source_record_sha256", sa.String(64), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("persisted_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("source_kind IN ('pair_response', 'latest_feed', 'top_feed')", name="ck_boost_observations_source_kind"),
        sa.CheckConstraint("active_boost_count IS NOT NULL OR amount IS NOT NULL OR total_amount IS NOT NULL", name="ck_boost_observations_has_fact"),
        sa.CheckConstraint("active_boost_count IS NULL OR active_boost_count >= 0", name="ck_boost_observations_active_nonnegative"),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["pair_id"], ["pairs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["api_request_log_id"], ["api_request_log.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_boost_observations_idempotency"),
    )
    op.create_index("ix_boost_observations_token_received", "boost_observations", ["token_id", "received_at"])
    op.create_index("ix_boost_observations_run_received", "boost_observations", ["collector_run_id", "received_at"])

    op.create_table(
        "boost_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("boost_observation_id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("event_type", sa.String(32), nullable=False),
        sa.Column("metric", sa.String(32), nullable=False),
        sa.Column("direction", sa.String(8), nullable=False),
        sa.Column("previous_value", sa.Numeric(38, 18), nullable=True),
        sa.Column("new_value", sa.Numeric(38, 18), nullable=True),
        sa.Column("threshold_value", sa.Numeric(38, 18), nullable=True),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("policy_sha256", sa.String(64), nullable=False),
        sa.Column("policy_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("persisted_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("event_type IN ('first_seen', 'state_change', 'threshold_crossing')", name="ck_boost_events_type"),
        sa.CheckConstraint("metric IN ('active_boost_count', 'amount', 'total_amount')", name="ck_boost_events_metric"),
        sa.CheckConstraint("direction IN ('none', 'up', 'down')", name="ck_boost_events_direction"),
        sa.CheckConstraint("(event_type = 'threshold_crossing' AND threshold_value IS NOT NULL AND direction IN ('up', 'down')) OR (event_type <> 'threshold_crossing' AND threshold_value IS NULL)", name="ck_boost_events_threshold_shape"),
        sa.ForeignKeyConstraint(["boost_observation_id"], ["boost_observations.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_boost_events_idempotency"),
    )
    op.create_index("ix_boost_events_token_decided", "boost_events", ["token_id", "decided_at"])

    op.create_table(
        "token_metadata_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("pair_id", sa.Uuid(), nullable=True),
        sa.Column("collector_run_id", sa.Uuid(), nullable=True),
        sa.Column("api_request_log_id", sa.Uuid(), nullable=True),
        sa.Column("discovery_event_id", sa.Uuid(), nullable=True),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("source_kind", sa.String(32), nullable=False),
        sa.Column("source_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("name", sa.String(512), nullable=True),
        sa.Column("symbol", sa.String(128), nullable=True),
        sa.Column("metadata_uri", sa.String(4096), nullable=True),
        sa.Column("image_url", sa.String(4096), nullable=True),
        sa.Column("header_url", sa.String(4096), nullable=True),
        sa.Column("website_url", sa.String(4096), nullable=True),
        sa.Column("twitter", sa.String(2048), nullable=True),
        sa.Column("telegram", sa.String(2048), nullable=True),
        sa.Column("other_links", postgresql.JSONB(), nullable=True),
        sa.Column("source_record_locator", sa.String(256), nullable=False),
        sa.Column("source_record_sha256", sa.String(64), nullable=False),
        sa.Column("content_sha256", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("persisted_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("source_kind IN ('discovery', 'pair_response', 'boost_feed')", name="ck_token_metadata_events_source_kind"),
        sa.CheckConstraint("api_request_log_id IS NOT NULL OR discovery_event_id IS NOT NULL", name="ck_token_metadata_events_provenance"),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["pair_id"], ["pairs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["api_request_log_id"], ["api_request_log.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["discovery_event_id"], ["discovery_events.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_token_metadata_events_idempotency"),
    )
    op.create_index("ix_token_metadata_events_token_received", "token_metadata_events", ["token_id", "received_at"])
    op.create_index("ix_token_metadata_events_run_received", "token_metadata_events", ["collector_run_id", "received_at"])

    op.create_table(
        "token_security_tasks",
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("phase", sa.Integer(), nullable=False),
        sa.Column("next_due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), server_default="0", nullable=False),
        sa.Column("lease_id", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("persisted_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("phase >= 0 AND phase <= 4", name="ck_token_security_tasks_phase"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_token_security_tasks_attempt_count"),
        sa.CheckConstraint("(next_due_at IS NULL AND phase = 4) OR next_due_at IS NOT NULL", name="ck_token_security_tasks_completion"),
        sa.CheckConstraint("(lease_id IS NULL AND lease_expires_at IS NULL) OR (lease_id IS NOT NULL AND lease_expires_at IS NOT NULL)", name="ck_token_security_tasks_lease"),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("token_id"),
    )
    op.create_index("ix_token_security_tasks_due", "token_security_tasks", ["next_due_at"], postgresql_where=sa.text("next_due_at IS NOT NULL"))

    op.create_table(
        "token_security_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("collector_run_id", sa.Uuid(), nullable=True),
        sa.Column("api_request_log_id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("source_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rpc_slot", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("account_owner", sa.String(128), nullable=True),
        sa.Column("token_program", sa.String(16), nullable=False),
        sa.Column("mint_authority", sa.String(128), nullable=True),
        sa.Column("freeze_authority", sa.String(128), nullable=True),
        sa.Column("raw_supply", sa.Numeric(38, 0), nullable=True),
        sa.Column("decimals", sa.Integer(), nullable=True),
        sa.Column("is_initialized", sa.Boolean(), nullable=True),
        sa.Column("extension_types", postgresql.JSONB(), nullable=True),
        sa.Column("raw_account_sha256", sa.String(64), nullable=True),
        sa.Column("decode_error", sa.String(2048), nullable=True),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("persisted_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("status IN ('available', 'unavailable', 'malformed')", name="ck_token_security_snapshots_status"),
        sa.CheckConstraint("token_program IN ('spl_token', 'token_2022', 'unknown')", name="ck_token_security_snapshots_program"),
        sa.CheckConstraint("(status = 'available' AND account_owner IS NOT NULL AND raw_account_sha256 IS NOT NULL) OR status <> 'available'", name="ck_token_security_snapshots_available_shape"),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["api_request_log_id"], ["api_request_log.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_token_security_snapshots_idempotency"),
    )
    op.create_index("ix_token_security_snapshots_token_received", "token_security_snapshots", ["token_id", "received_at"])
    op.create_index("ix_token_security_snapshots_run_received", "token_security_snapshots", ["collector_run_id", "received_at"])

    op.create_table(
        "market_context_snapshots",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=False),
        sa.Column("collector_run_id", sa.Uuid(), nullable=False),
        sa.Column("bucket_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("bucket_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("source_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("sol_usd_price", sa.Numeric(38, 18), nullable=True),
        sa.Column("sol_return_5m", sa.Numeric(20, 12), nullable=True),
        sa.Column("sol_realized_volatility_1h", sa.Numeric(20, 12), nullable=True),
        sa.Column("admitted_tokens", sa.Integer(), nullable=False),
        sa.Column("active_transitions", sa.Integer(), nullable=False),
        sa.Column("mature_cohort_tokens", sa.Integer(), nullable=False),
        sa.Column("mature_cohort_active_tokens", sa.Integer(), nullable=False),
        sa.Column("mature_cohort_active_fraction", sa.Numeric(20, 12), nullable=True),
        sa.Column("pair_sample_count", sa.Integer(), nullable=False),
        sa.Column("aggregate_volume_m5_usd", sa.Numeric(38, 6), nullable=True),
        sa.Column("aggregate_buys_m5", sa.BigInteger(), nullable=True),
        sa.Column("aggregate_sells_m5", sa.BigInteger(), nullable=True),
        sa.Column("policy_sha256", sa.String(64), nullable=False),
        sa.Column("policy_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("persisted_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("bucket_end > bucket_start", name="ck_market_context_bucket"),
        sa.ForeignKeyConstraint(["collection_epoch_id"], ["collection_epochs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("collection_epoch_id", "bucket_start", "policy_sha256", name="uq_market_context_epoch_bucket_policy"),
    )
    op.create_index("ix_market_context_epoch_received", "market_context_snapshots", ["collection_epoch_id", "received_at"])

    for table_name in (
        "pair_fact_events",
        "boost_observations",
        "boost_events",
        "token_metadata_events",
        "token_security_snapshots",
        "market_context_snapshots",
    ):
        op.execute(
            f"CREATE TRIGGER {table_name}_immutable BEFORE UPDATE OR DELETE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
        )


def downgrade() -> None:
    """Remove Phase 2 structures without modifying historical pre-Phase-2 rows."""
    for table_name in (
        "market_context_snapshots",
        "token_security_snapshots",
        "token_metadata_events",
        "boost_events",
        "boost_observations",
        "pair_fact_events",
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {table_name}_immutable ON {table_name}")

    op.drop_index("ix_market_context_epoch_received", table_name="market_context_snapshots")
    op.drop_table("market_context_snapshots")
    op.drop_index("ix_token_security_snapshots_run_received", table_name="token_security_snapshots")
    op.drop_index("ix_token_security_snapshots_token_received", table_name="token_security_snapshots")
    op.drop_table("token_security_snapshots")
    op.drop_index("ix_token_security_tasks_due", table_name="token_security_tasks")
    op.drop_table("token_security_tasks")
    op.drop_index("ix_token_metadata_events_run_received", table_name="token_metadata_events")
    op.drop_index("ix_token_metadata_events_token_received", table_name="token_metadata_events")
    op.drop_table("token_metadata_events")
    op.drop_index("ix_boost_events_token_decided", table_name="boost_events")
    op.drop_table("boost_events")
    op.drop_index("ix_boost_observations_run_received", table_name="boost_observations")
    op.drop_index("ix_boost_observations_token_received", table_name="boost_observations")
    op.drop_table("boost_observations")
    op.drop_index("ix_pair_fact_events_run_received", table_name="pair_fact_events")
    op.drop_index("ix_pair_fact_events_pair_received", table_name="pair_fact_events")
    op.drop_table("pair_fact_events")

    for name in (
        "liquidity_quote",
        "liquidity_base",
        "sells_h24",
        "buys_h24",
        "sells_h6",
        "buys_h6",
    ):
        op.drop_column("observations", name)
