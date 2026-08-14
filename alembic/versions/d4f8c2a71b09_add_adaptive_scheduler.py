"""add durable adaptive scheduler

Revision ID: d4f8c2a71b09
Revises: 9a4b2d8f1c30
Create Date: 2026-08-14 21:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "d4f8c2a71b09"
down_revision: Union[str, Sequence[str], None] = "9a4b2d8f1c30"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create scheduler projections and immutable scheduling evidence."""
    op.create_table(
        "poll_batches",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("provider", sa.String(length=64), nullable=False),
        sa.Column("chain", sa.String(length=32), nullable=False),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reserved_request_capacity", sa.Integer(), nullable=False),
        sa.Column("configuration_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "configuration_snapshot",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "reserved_request_capacity > 0",
            name="ck_poll_batches_reservation",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_poll_batches_provider_claimed_at",
        "poll_batches",
        ["provider", "claimed_at"],
        unique=False,
    )
    op.create_table(
        "poll_schedules",
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("lifecycle_state", sa.String(length=32), nullable=False),
        sa.Column("state_decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("next_due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("lease_id", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("configuration_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "configuration_snapshot",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("attempt_count >= 0", name="ck_poll_schedules_attempt_count"),
        sa.CheckConstraint(
            "(lease_id IS NULL AND lease_expires_at IS NULL) OR "
            "(lease_id IS NOT NULL AND lease_expires_at IS NOT NULL)",
            name="ck_poll_schedules_lease_pair",
        ),
        sa.CheckConstraint(
            "lifecycle_state IN "
            "('NEW', 'ACTIVE', 'WATCH', 'FADING', 'DORMANT', 'RESURRECTED')",
            name="ck_poll_schedules_lifecycle_state",
        ),
        sa.CheckConstraint("priority >= 0", name="ck_poll_schedules_priority"),
        sa.ForeignKeyConstraint(["lease_id"], ["poll_batches.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("token_id"),
    )
    op.create_index(
        "ix_poll_schedules_due_priority",
        "poll_schedules",
        ["next_due_at", "priority"],
        unique=False,
    )
    op.create_table(
        "poll_schedule_decisions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("previous_state", sa.String(length=32), nullable=True),
        sa.Column("new_state", sa.String(length=32), nullable=False),
        sa.Column("previous_due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("new_due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason_code", sa.String(length=128), nullable=False),
        sa.Column("configuration_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "configuration_snapshot",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_poll_schedule_decisions_idempotency_key",
        ),
    )
    op.create_index(
        "ix_poll_schedule_decisions_token_decided_at",
        "poll_schedule_decisions",
        ["token_id", "decided_at"],
        unique=False,
    )
    op.create_table(
        "poll_batch_members",
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("due_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lifecycle_state", sa.String(length=32), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("claim_lateness_ms", sa.BigInteger(), nullable=False),
        sa.Column("previous_batch_id", sa.Uuid(), nullable=True),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("claim_lateness_ms >= 0", name="ck_poll_batch_members_lateness"),
        sa.CheckConstraint("priority >= 0", name="ck_poll_batch_members_priority"),
        sa.ForeignKeyConstraint(["batch_id"], ["poll_batches.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(
            ["previous_batch_id"],
            ["poll_batches.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint(
            "claimed_at",
            "batch_id",
            "token_id",
            name="pk_poll_batch_members",
        ),
        postgresql_partition_by="RANGE (claimed_at)",
    )
    op.create_index(
        "ix_poll_batch_members_token_due_at",
        "poll_batch_members",
        ["token_id", "due_at"],
        unique=False,
    )
    op.execute(
        """
        DO $$
        DECLARE
            partition_start date;
        BEGIN
            FOR partition_start IN
                SELECT generate_series(DATE '2026-01-01', DATE '2027-12-01', INTERVAL '1 month')::date
            LOOP
                EXECUTE format(
                    'CREATE TABLE poll_batch_members_%s PARTITION OF poll_batch_members '
                    'FOR VALUES FROM (%L) TO (%L)',
                    to_char(partition_start, 'YYYY_MM'),
                    partition_start,
                    (partition_start + INTERVAL '1 month')::date
                );
            END LOOP;
        END $$;
        """
    )
    op.create_table(
        "poll_batch_outcomes",
        sa.Column("batch_id", sa.Uuid(), nullable=False),
        sa.Column("api_request_log_id", sa.Uuid(), nullable=True),
        sa.Column("outcome", sa.String(length=16), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("member_count", sa.Integer(), nullable=False),
        sa.Column("observation_lateness_min_ms", sa.BigInteger(), nullable=False),
        sa.Column("observation_lateness_max_ms", sa.BigInteger(), nullable=False),
        sa.Column("observation_lateness_mean_ms", sa.Numeric(precision=20, scale=3), nullable=False),
        sa.Column("failure_detail", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("configuration_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "configuration_snapshot",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("member_count > 0", name="ck_poll_batch_outcomes_member_count"),
        sa.CheckConstraint(
            "observation_lateness_min_ms >= 0 AND observation_lateness_max_ms >= 0 "
            "AND observation_lateness_mean_ms >= 0",
            name="ck_poll_batch_outcomes_lateness",
        ),
        sa.CheckConstraint(
            "outcome IN ('succeeded', 'empty', 'partial', 'failed', 'throttled', "
            "'malformed', 'cancelled')",
            name="ck_poll_batch_outcomes_outcome",
        ),
        sa.ForeignKeyConstraint(
            ["api_request_log_id"],
            ["api_request_log.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["batch_id"], ["poll_batches.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("batch_id"),
    )
    op.create_index(
        "ix_poll_batch_outcomes_completed_at",
        "poll_batch_outcomes",
        ["completed_at"],
        unique=False,
    )

    for table_name in (
        "poll_batches",
        "poll_schedule_decisions",
        "poll_batch_members",
        "poll_batch_outcomes",
    ):
        op.execute(
            f"CREATE TRIGGER {table_name}_immutable "
            f"BEFORE UPDATE OR DELETE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
        )


def downgrade() -> None:
    """Remove scheduler tables in foreign-key order."""
    for table_name in (
        "poll_batch_outcomes",
        "poll_batch_members",
        "poll_schedule_decisions",
        "poll_batches",
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {table_name}_immutable ON {table_name}")
    op.drop_index("ix_poll_batch_outcomes_completed_at", table_name="poll_batch_outcomes")
    op.drop_table("poll_batch_outcomes")
    op.drop_index("ix_poll_batch_members_token_due_at", table_name="poll_batch_members")
    op.drop_table("poll_batch_members")
    op.drop_index(
        "ix_poll_schedule_decisions_token_decided_at",
        table_name="poll_schedule_decisions",
    )
    op.drop_table("poll_schedule_decisions")
    op.drop_index("ix_poll_schedules_due_priority", table_name="poll_schedules")
    op.drop_table("poll_schedules")
    op.drop_index("ix_poll_batches_provider_claimed_at", table_name="poll_batches")
    op.drop_table("poll_batches")
