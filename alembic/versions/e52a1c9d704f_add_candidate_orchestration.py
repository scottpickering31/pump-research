"""add deterministic candidate orchestration

Revision ID: e52a1c9d704f
Revises: c61e29d841af
Create Date: 2026-08-21 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "e52a1c9d704f"
down_revision: str | None = "c61e29d841af"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

TIERS = (
    "'TIER_0_UNIVERSAL', 'TIER_1_INTERESTING', 'TIER_2_INVESTIGATE', "
    "'TIER_3_DEEP_REVIEW', 'TIER_4_PRETRADE'"
)


def upgrade() -> None:
    """Create append-only evidence and compact recoverable projections."""
    op.create_table(
        "candidate_policies",
        sa.Column("policy_sha256", sa.String(64), primary_key=True),
        sa.Column("policy_name", sa.String(128), nullable=False),
        sa.Column("policy_version", sa.String(32), nullable=False),
        sa.Column("policy_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("persisted_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
    )
    op.create_table(
        "candidate_events",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("idempotency_key", sa.String(64), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=False),
        sa.Column("collector_run_id", sa.Uuid(), nullable=True),
        sa.Column("candidate_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("trigger_type", sa.String(64), nullable=False),
        sa.Column("trigger_version", sa.String(32), nullable=False),
        sa.Column("feature_set_name", sa.String(128), nullable=True),
        sa.Column("feature_set_version", sa.String(32), nullable=True),
        sa.Column("input_watermark", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lifecycle_state", sa.String(32), nullable=False),
        sa.Column("coverage_class", sa.String(32), nullable=False),
        sa.Column("evidence_sha256", sa.String(64), nullable=False),
        sa.Column("evidence_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("source_fact_ids", postgresql.JSONB(), nullable=False),
        sa.Column("policy_sha256", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["collection_epoch_id"], ["collection_epochs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["policy_sha256"], ["candidate_policies.policy_sha256"], ondelete="RESTRICT"),
        sa.UniqueConstraint("idempotency_key", name="uq_candidate_events_idempotency"),
    )
    op.create_index("ix_candidate_events_token_at", "candidate_events", ["token_id", "candidate_at"])
    op.create_index("ix_candidate_events_epoch_at", "candidate_events", ["collection_epoch_id", "candidate_at"])
    op.create_index("ix_candidate_events_run", "candidate_events", ["collector_run_id"])
    op.create_table(
        "candidate_tier_events",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("idempotency_key", sa.String(64), nullable=False),
        sa.Column("candidate_id", sa.Uuid(), nullable=True),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=False),
        sa.Column("collector_run_id", sa.Uuid(), nullable=True),
        sa.Column("previous_tier", sa.String(32), nullable=False),
        sa.Column("new_tier", sa.String(32), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("input_watermark", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason_code", sa.String(64), nullable=False),
        sa.Column("reason_detail", postgresql.JSONB(), nullable=False),
        sa.Column("transition_version", sa.String(32), nullable=False),
        sa.Column("policy_sha256", sa.String(64), nullable=False),
        sa.Column("evidence_sha256", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(f"previous_tier IN ({TIERS})", name="ck_candidate_tier_events_previous"),
        sa.CheckConstraint(f"new_tier IN ({TIERS})", name="ck_candidate_tier_events_new"),
        sa.ForeignKeyConstraint(["candidate_id"], ["candidate_events.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["collection_epoch_id"], ["collection_epochs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["policy_sha256"], ["candidate_policies.policy_sha256"], ondelete="RESTRICT"),
        sa.UniqueConstraint("idempotency_key", name="uq_candidate_tier_events_idempotency"),
    )
    op.create_index("ix_candidate_tier_events_token_at", "candidate_tier_events", ["token_id", "decided_at"])
    op.create_index("ix_candidate_tier_events_epoch_at", "candidate_tier_events", ["collection_epoch_id", "decided_at"])
    op.create_table(
        "candidate_current_state",
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("tier", sa.String(32), nullable=False),
        sa.Column("latest_candidate_id", sa.Uuid(), nullable=True),
        sa.Column("latest_tier_event_id", sa.Uuid(), nullable=False),
        sa.Column("tier_since", sa.DateTime(timezone=True), nullable=False),
        sa.Column("coverage_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_evaluation_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("input_watermark", sa.DateTime(timezone=True), nullable=False),
        sa.Column("evidence_sha256", sa.String(64), nullable=False),
        sa.Column("policy_sha256", sa.String(64), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(f"tier IN ({TIERS})", name="ck_candidate_current_state_tier"),
        sa.ForeignKeyConstraint(["collection_epoch_id"], ["collection_epochs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["latest_candidate_id"], ["candidate_events.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["latest_tier_event_id"], ["candidate_tier_events.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["policy_sha256"], ["candidate_policies.policy_sha256"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("collection_epoch_id", "token_id", name="pk_candidate_current_state"),
    )
    op.create_index("ix_candidate_current_state_tier_due", "candidate_current_state", ["tier", "next_evaluation_at"])
    op.create_index("ix_candidate_current_state_coverage", "candidate_current_state", ["coverage_expires_at"])
    op.create_table(
        "candidate_enrichment_tasks",
        sa.Column("id", sa.Uuid(), primary_key=True),
        sa.Column("semantic_key", sa.String(64), nullable=False),
        sa.Column("candidate_id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=False),
        sa.Column("tier", sa.String(32), nullable=False),
        sa.Column("analysis_type", sa.String(64), nullable=False),
        sa.Column("input_watermark", sa.DateTime(timezone=True), nullable=False),
        sa.Column("input_sha256", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("not_before", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("lease_id", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claimed_by", sa.String(128), nullable=True),
        sa.Column("collector_run_id", sa.Uuid(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.String(64), nullable=True),
        sa.Column("failure_detail", postgresql.JSONB(), nullable=True),
        sa.Column("evidence_generated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("evidence_received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("fresh_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result_identity", sa.String(256), nullable=True),
        sa.Column("result_sha256", sa.String(64), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(f"tier IN ({TIERS})", name="ck_candidate_tasks_tier"),
        sa.CheckConstraint("status IN ('pending', 'claimed', 'succeeded', 'retry', 'failed', 'deferred')", name="ck_candidate_tasks_status"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_candidate_tasks_attempts"),
        sa.CheckConstraint("max_attempts > 0", name="ck_candidate_tasks_max_attempts"),
        sa.CheckConstraint("(lease_id IS NULL AND lease_expires_at IS NULL) OR (lease_id IS NOT NULL AND lease_expires_at IS NOT NULL)", name="ck_candidate_tasks_lease"),
        sa.ForeignKeyConstraint(["candidate_id"], ["candidate_events.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["collection_epoch_id"], ["collection_epochs.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("semantic_key", name="uq_candidate_tasks_semantic_key"),
    )
    op.create_index("ix_candidate_tasks_claim", "candidate_enrichment_tasks", ["status", "not_before", "created_at"])
    op.create_index("ix_candidate_tasks_token", "candidate_enrichment_tasks", ["token_id", "created_at"])
    for table in ("candidate_events", "candidate_tier_events"):
        op.execute(
            f"CREATE TRIGGER {table}_immutable BEFORE UPDATE OR DELETE ON {table} "
            "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
        )
    op.add_column(
        "poll_schedules",
        sa.Column("candidate_coverage_expires_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "poll_schedules",
        sa.Column("candidate_coverage_interval_seconds", sa.Integer(), nullable=True),
    )
    op.add_column(
        "poll_schedules", sa.Column("candidate_tier_event_id", sa.Uuid(), nullable=True)
    )
    op.create_foreign_key(
        "fk_poll_schedules_candidate_tier_event",
        "poll_schedules",
        "candidate_tier_events",
        ["candidate_tier_event_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_poll_schedules_candidate_coverage",
        "poll_schedules",
        "(candidate_coverage_expires_at IS NULL AND "
        "candidate_coverage_interval_seconds IS NULL AND candidate_tier_event_id IS NULL) OR "
        "(candidate_coverage_expires_at IS NOT NULL AND "
        "candidate_coverage_interval_seconds > 0 AND candidate_tier_event_id IS NOT NULL)",
    )


def downgrade() -> None:
    """Remove only Phase 5 schema; historical collection facts are untouched."""
    op.drop_constraint("ck_poll_schedules_candidate_coverage", "poll_schedules", type_="check")
    op.drop_constraint(
        "fk_poll_schedules_candidate_tier_event", "poll_schedules", type_="foreignkey"
    )
    op.drop_column("poll_schedules", "candidate_tier_event_id")
    op.drop_column("poll_schedules", "candidate_coverage_interval_seconds")
    op.drop_column("poll_schedules", "candidate_coverage_expires_at")
    op.drop_index("ix_candidate_tasks_token", table_name="candidate_enrichment_tasks")
    op.drop_index("ix_candidate_tasks_claim", table_name="candidate_enrichment_tasks")
    op.drop_table("candidate_enrichment_tasks")
    op.drop_index("ix_candidate_current_state_coverage", table_name="candidate_current_state")
    op.drop_index("ix_candidate_current_state_tier_due", table_name="candidate_current_state")
    op.drop_table("candidate_current_state")
    op.execute("DROP TRIGGER IF EXISTS candidate_tier_events_immutable ON candidate_tier_events")
    op.drop_index("ix_candidate_tier_events_epoch_at", table_name="candidate_tier_events")
    op.drop_index("ix_candidate_tier_events_token_at", table_name="candidate_tier_events")
    op.drop_table("candidate_tier_events")
    op.execute("DROP TRIGGER IF EXISTS candidate_events_immutable ON candidate_events")
    op.drop_index("ix_candidate_events_run", table_name="candidate_events")
    op.drop_index("ix_candidate_events_epoch_at", table_name="candidate_events")
    op.drop_index("ix_candidate_events_token_at", table_name="candidate_events")
    op.drop_table("candidate_events")
    op.drop_table("candidate_policies")
