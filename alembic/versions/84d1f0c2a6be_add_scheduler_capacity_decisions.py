"""add scheduler capacity decisions

Revision ID: 84d1f0c2a6be
Revises: 6a71c2d90e4b
Create Date: 2026-08-15 18:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "84d1f0c2a6be"
down_revision: str | Sequence[str] | None = "6a71c2d90e4b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add normalized policies and immutable dynamic capacity decisions."""
    op.create_table(
        "scheduler_policies",
        sa.Column("policy_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "policy_snapshot",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("policy_sha256"),
    )
    op.create_table(
        "scheduler_capacity_decisions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=64), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("capacity_mode", sa.String(length=16), nullable=False),
        sa.Column("policy_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "decision_snapshot",
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
            "capacity_mode IN ('NORMAL', 'DEGRADED', 'CRITICAL')",
            name="ck_scheduler_capacity_decisions_mode",
        ),
        sa.ForeignKeyConstraint(
            ["policy_sha256"],
            ["scheduler_policies.policy_sha256"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_key",
            name="uq_scheduler_capacity_decisions_idempotency_key",
        ),
    )
    op.create_index(
        "ix_scheduler_capacity_decisions_decided_at",
        "scheduler_capacity_decisions",
        ["decided_at"],
        unique=False,
    )

    for table_name in ("poll_batches", "poll_schedules", "poll_schedule_decisions"):
        op.add_column(table_name, sa.Column("capacity_decision_id", sa.Uuid(), nullable=True))
        op.create_foreign_key(
            f"fk_{table_name}_capacity_decision",
            table_name,
            "scheduler_capacity_decisions",
            ["capacity_decision_id"],
            ["id"],
            ondelete="RESTRICT",
        )
    for table_name in ("poll_schedules", "poll_schedule_decisions"):
        op.add_column(
            table_name,
            sa.Column("target_interval_seconds", sa.Integer(), nullable=True),
        )
        op.add_column(
            table_name,
            sa.Column("effective_interval_seconds", sa.Integer(), nullable=True),
        )
        op.create_check_constraint(
            f"ck_{table_name}_target_interval",
            table_name,
            "target_interval_seconds IS NULL OR target_interval_seconds > 0",
        )
        op.create_check_constraint(
            f"ck_{table_name}_effective_interval",
            table_name,
            "effective_interval_seconds IS NULL OR effective_interval_seconds > 0",
        )

    op.add_column(
        "poll_batch_members",
        sa.Column("capacity_decision_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "poll_batch_members",
        sa.Column("target_interval_seconds", sa.Integer(), nullable=True),
    )
    op.add_column(
        "poll_batch_members",
        sa.Column("effective_interval_seconds", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_poll_batch_members_capacity_decision",
        "poll_batch_members",
        "scheduler_capacity_decisions",
        ["capacity_decision_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_poll_batch_members_target_interval",
        "poll_batch_members",
        "target_interval_seconds IS NULL OR target_interval_seconds > 0",
    )
    op.create_check_constraint(
        "ck_poll_batch_members_effective_interval",
        "poll_batch_members",
        "effective_interval_seconds IS NULL OR effective_interval_seconds > 0",
    )

    for table_name in ("scheduler_policies", "scheduler_capacity_decisions"):
        op.execute(
            f"CREATE TRIGGER {table_name}_immutable "
            f"BEFORE UPDATE OR DELETE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
        )


def downgrade() -> None:
    """Remove capacity references and normalized decision evidence."""
    for constraint_name in (
        "ck_poll_batch_members_effective_interval",
        "ck_poll_batch_members_target_interval",
    ):
        op.drop_constraint(constraint_name, "poll_batch_members", type_="check")
    op.drop_constraint(
        "fk_poll_batch_members_capacity_decision",
        "poll_batch_members",
        type_="foreignkey",
    )
    for column_name in (
        "effective_interval_seconds",
        "target_interval_seconds",
        "capacity_decision_id",
    ):
        op.drop_column("poll_batch_members", column_name)

    for table_name in ("poll_schedule_decisions", "poll_schedules"):
        op.drop_constraint(
            f"ck_{table_name}_effective_interval", table_name, type_="check"
        )
        op.drop_constraint(f"ck_{table_name}_target_interval", table_name, type_="check")
        op.drop_column(table_name, "effective_interval_seconds")
        op.drop_column(table_name, "target_interval_seconds")
    for table_name in ("poll_schedule_decisions", "poll_schedules", "poll_batches"):
        op.drop_constraint(
            f"fk_{table_name}_capacity_decision", table_name, type_="foreignkey"
        )
        op.drop_column(table_name, "capacity_decision_id")

    for table_name in ("scheduler_capacity_decisions", "scheduler_policies"):
        op.execute(f"DROP TRIGGER IF EXISTS {table_name}_immutable ON {table_name}")
    op.drop_index(
        "ix_scheduler_capacity_decisions_decided_at",
        table_name="scheduler_capacity_decisions",
    )
    op.drop_table("scheduler_capacity_decisions")
    op.drop_table("scheduler_policies")
