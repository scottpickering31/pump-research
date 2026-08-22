"""add production archive catalog and retention eligibility evidence

Revision ID: c61e29d841af
Revises: b184a7d2e903
Create Date: 2026-08-21 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c61e29d841af"
down_revision: str | None = "b184a7d2e903"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create catalog projections plus immutable verification/evaluation evidence."""
    op.create_table(
        "archive_scopes",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("archive_identity_sha256", sa.String(length=64), nullable=False),
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=False),
        sa.Column("start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("archive_schema_version", sa.Integer(), nullable=False),
        sa.Column("source_db_schema_revision", sa.String(length=64), nullable=False),
        sa.Column("source_scope_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("manifest_path", sa.String(length=4096), nullable=True),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=True),
        sa.Column("aggregate_file_sha256", sa.String(length=64), nullable=True),
        sa.Column("source_row_count", sa.BigInteger(), nullable=True),
        sa.Column("parquet_bytes", sa.BigInteger(), nullable=True),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("verification_detail", postgresql.JSONB(), nullable=True),
        sa.Column("analytical_reads_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("claim_token", sa.Uuid(), nullable=True),
        sa.Column("claim_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_detail", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("start_at < end_at", name="ck_archive_scopes_range"),
        sa.CheckConstraint(
            "(claim_token IS NULL AND claim_expires_at IS NULL) OR "
            "(claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)",
            name="ck_archive_scopes_claim",
        ),
        sa.CheckConstraint(
            "state IN ('pending', 'exporting', 'exported', 'verified', "
            "'independently_copied', 'retention_eligible', 'failed')",
            name="ck_archive_scopes_state",
        ),
        sa.ForeignKeyConstraint(
            ["collection_epoch_id"], ["collection_epochs.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("archive_identity_sha256", name="uq_archive_scopes_identity"),
    )
    op.create_index(
        "ix_archive_scopes_epoch_range",
        "archive_scopes",
        ["collection_epoch_id", "start_at", "end_at"],
    )
    op.create_index(
        "ix_archive_scopes_state_updated", "archive_scopes", ["state", "updated_at"]
    )
    op.create_table(
        "archive_scope_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("archive_scope_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("detail", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint(
            "event_type IN ('claimed', 'exported', 'verified', 'copy_verified', "
            "'retention_evaluated', 'failed')",
            name="ck_archive_scope_events_type",
        ),
        sa.ForeignKeyConstraint(["archive_scope_id"], ["archive_scopes.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_archive_scope_events_idempotency"),
    )
    op.create_index(
        "ix_archive_scope_events_scope_time",
        "archive_scope_events",
        ["archive_scope_id", "occurred_at"],
    )
    op.create_table(
        "archive_copy_verifications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("archive_scope_id", sa.Uuid(), nullable=False),
        sa.Column("copy_role", sa.String(length=16), nullable=False),
        sa.Column("provider_kind", sa.String(length=32), nullable=False),
        sa.Column("location", sa.String(length=4096), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("aggregate_file_sha256", sa.String(length=64), nullable=False),
        sa.Column("total_bytes", sa.BigInteger(), nullable=False),
        sa.Column("object_count", sa.Integer(), nullable=False),
        sa.Column("independence_asserted", sa.Boolean(), nullable=False),
        sa.Column("independence_detail", sa.String(length=2048), nullable=True),
        sa.Column("verification_method", sa.String(length=512), nullable=False),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("detail", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.CheckConstraint("copy_role IN ('primary', 'secondary')", name="ck_archive_copy_role"),
        sa.CheckConstraint(
            "provider_kind IN ('filesystem', 's3_compatible', 'fake')",
            name="ck_archive_copy_provider",
        ),
        sa.CheckConstraint(
            "copy_role <> 'secondary' OR independence_asserted",
            name="ck_archive_secondary_independence",
        ),
        sa.ForeignKeyConstraint(["archive_scope_id"], ["archive_scopes.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "archive_scope_id",
            "copy_role",
            "location",
            "aggregate_file_sha256",
            name="uq_archive_copy_verifications_identity",
        ),
    )
    op.create_index(
        "ix_archive_copy_verifications_scope",
        "archive_copy_verifications",
        ["archive_scope_id", "verified_at"],
    )
    op.create_table(
        "archive_retention_evaluations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("archive_scope_id", sa.Uuid(), nullable=False),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("eligible", sa.Boolean(), nullable=False),
        sa.Column("minimum_hot_retention_days", sa.Integer(), nullable=False),
        sa.Column("policy_sha256", sa.String(length=64), nullable=False),
        sa.Column("policy_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("reasons", postgresql.JSONB(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["archive_scope_id"], ["archive_scopes.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_archive_retention_idempotency"),
    )
    op.create_index(
        "ix_archive_retention_scope_time",
        "archive_retention_evaluations",
        ["archive_scope_id", "evaluated_at"],
    )
    for table_name in (
        "archive_scope_events",
        "archive_copy_verifications",
        "archive_retention_evaluations",
    ):
        op.execute(
            f"CREATE TRIGGER {table_name}_immutable BEFORE UPDATE OR DELETE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
        )


def downgrade() -> None:
    """Remove only the Phase 3 archive catalog."""
    for table_name in (
        "archive_retention_evaluations",
        "archive_copy_verifications",
        "archive_scope_events",
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {table_name}_immutable ON {table_name}")
    op.drop_index("ix_archive_retention_scope_time", table_name="archive_retention_evaluations")
    op.drop_table("archive_retention_evaluations")
    op.drop_index("ix_archive_copy_verifications_scope", table_name="archive_copy_verifications")
    op.drop_table("archive_copy_verifications")
    op.drop_index("ix_archive_scope_events_scope_time", table_name="archive_scope_events")
    op.drop_table("archive_scope_events")
    op.drop_index("ix_archive_scopes_state_updated", table_name="archive_scopes")
    op.drop_index("ix_archive_scopes_epoch_range", table_name="archive_scopes")
    op.drop_table("archive_scopes")
