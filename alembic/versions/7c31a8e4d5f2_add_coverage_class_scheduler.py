"""add coverage-class scheduler and bounded control scans

Revision ID: 7c31a8e4d5f2
Revises: f2c8d4a6197e
Create Date: 2026-08-17 20:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "7c31a8e4d5f2"
down_revision: str | Sequence[str] | None = "f2c8d4a6197e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_COVERAGE_CLASSES = (
    "'PROTECTED_ACTIVE', 'PROTECTED_RESURRECTED', 'PROTECTED_WATCH', "
    "'INITIAL', 'EARLY', 'MATURE', 'FADING_TAIL', 'FADING_COOL', "
    "'COOLED', 'LONG_TAIL_DAY', 'LONG_TAIL_WEEK', 'RETIRED_CONTROL'"
)
_LIFECYCLE_STATES = "'NEW', 'ACTIVE', 'WATCH', 'FADING', 'DORMANT', 'RESURRECTED'"


def upgrade() -> None:
    """Add an explicit, initially-unmapped coverage projection and audit ledger."""
    op.create_table(
        "coverage_policies",
        sa.Column("policy_sha256", sa.String(length=64), nullable=False),
        sa.Column("policy_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("policy_sha256"),
    )
    op.create_table(
        "coverage_decisions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=True),
        sa.Column("collector_run_id", sa.Uuid(), nullable=True),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("previous_coverage_class", sa.String(length=32), nullable=True),
        sa.Column("new_coverage_class", sa.String(length=32), nullable=False),
        sa.Column("lifecycle_state", sa.String(length=32), nullable=False),
        sa.Column("admitted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("coverage_effective_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason_code", sa.String(length=128), nullable=False),
        sa.Column("policy_sha256", sa.String(length=64), nullable=False),
        sa.Column("capacity_decision_id", sa.Uuid(), nullable=True),
        sa.Column("target_interval_seconds", sa.Integer(), nullable=True),
        sa.Column("effective_interval_seconds", sa.Integer(), nullable=True),
        sa.Column("next_due_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("detail", postgresql.JSONB(), nullable=False),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "previous_coverage_class IS NULL OR previous_coverage_class IN "
            f"({_COVERAGE_CLASSES})",
            name="ck_coverage_decisions_previous_class",
        ),
        sa.CheckConstraint(
            f"new_coverage_class IN ({_COVERAGE_CLASSES})",
            name="ck_coverage_decisions_new_class",
        ),
        sa.CheckConstraint(
            f"lifecycle_state IN ({_LIFECYCLE_STATES})",
            name="ck_coverage_decisions_lifecycle_state",
        ),
        sa.ForeignKeyConstraint(
            ["collection_epoch_id"], ["collection_epochs.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["token_id"], ["tokens.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["policy_sha256"], ["coverage_policies.policy_sha256"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["capacity_decision_id"],
            ["scheduler_capacity_decisions.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_key", name="uq_coverage_decisions_idempotency_key"
        ),
    )
    op.create_index(
        "ix_coverage_decisions_token_decided",
        "coverage_decisions",
        ["token_id", "decided_at"],
    )
    op.create_index(
        "ix_coverage_decisions_epoch_decided",
        "coverage_decisions",
        ["collection_epoch_id", "decided_at"],
    )
    op.create_index(
        "ix_coverage_decisions_run_decided",
        "coverage_decisions",
        ["collector_run_id", "decided_at"],
    )

    op.add_column(
        "poll_batches",
        sa.Column(
            "batch_kind",
            sa.String(length=32),
            server_default="ordinary",
            nullable=False,
        ),
    )
    op.add_column(
        "poll_batches",
        sa.Column("control_window_start", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_poll_batches_kind",
        "poll_batches",
        "batch_kind IN ('ordinary', 'retired_control')",
    )
    op.create_check_constraint(
        "ck_poll_batches_control_window",
        "poll_batches",
        "(batch_kind = 'ordinary' AND control_window_start IS NULL) OR "
        "(batch_kind = 'retired_control' AND control_window_start IS NOT NULL)",
    )
    op.create_unique_constraint(
        "uq_poll_batches_control_window", "poll_batches", ["control_window_start"]
    )

    for name, type_ in (
        ("admitted_at", sa.DateTime(timezone=True)),
        ("coverage_class", sa.String(length=32)),
        ("coverage_decided_at", sa.DateTime(timezone=True)),
        ("coverage_next_transition_at", sa.DateTime(timezone=True)),
        ("coverage_policy_sha256", sa.String(length=64)),
        ("last_control_scan_at", sa.DateTime(timezone=True)),
    ):
        op.add_column("poll_schedules", sa.Column(name, type_, nullable=True))
    op.add_column(
        "poll_schedules",
        sa.Column(
            "control_scan_count", sa.Integer(), server_default="0", nullable=False
        ),
    )
    op.create_foreign_key(
        "fk_poll_schedules_coverage_policy",
        "poll_schedules",
        "coverage_policies",
        ["coverage_policy_sha256"],
        ["policy_sha256"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_poll_schedules_coverage_class",
        "poll_schedules",
        f"coverage_class IS NULL OR coverage_class IN ({_COVERAGE_CLASSES})",
    )
    op.create_check_constraint(
        "ck_poll_schedules_coverage_projection",
        "poll_schedules",
        "(coverage_class IS NULL AND admitted_at IS NULL "
        "AND coverage_decided_at IS NULL AND coverage_policy_sha256 IS NULL) OR "
        "(coverage_class IS NOT NULL AND admitted_at IS NOT NULL "
        "AND coverage_decided_at IS NOT NULL AND coverage_policy_sha256 IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_poll_schedules_retired_not_due",
        "poll_schedules",
        "coverage_class IS DISTINCT FROM 'RETIRED_CONTROL' OR next_due_at IS NULL",
    )
    op.create_check_constraint(
        "ck_poll_schedules_control_count",
        "poll_schedules",
        "control_scan_count >= 0",
    )
    op.create_index(
        "ix_poll_schedules_coverage_transition",
        "poll_schedules",
        ["coverage_next_transition_at"],
    )
    op.create_index(
        "ix_poll_schedules_control_rotation",
        "poll_schedules",
        ["last_control_scan_at", "admitted_at", "token_id"],
        postgresql_where=sa.text("coverage_class = 'RETIRED_CONTROL'"),
    )
    op.alter_column("poll_schedules", "next_due_at", nullable=True)
    op.alter_column("poll_schedule_decisions", "new_due_at", nullable=True)
    op.add_column(
        "poll_batch_members",
        sa.Column("coverage_class", sa.String(length=32), nullable=True),
    )

    for table_name in ("coverage_policies", "coverage_decisions"):
        op.execute(
            f"CREATE TRIGGER {table_name}_immutable "
            f"BEFORE UPDATE OR DELETE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
        )


def downgrade() -> None:
    """Remove V2 coverage structures after restoring legacy due-time nullability."""
    for table_name in ("coverage_decisions", "coverage_policies"):
        op.execute(f"DROP TRIGGER IF EXISTS {table_name}_immutable ON {table_name}")

    op.drop_column("poll_batch_members", "coverage_class")
    op.execute(
        "UPDATE poll_schedule_decisions "
        "SET new_due_at = decided_at WHERE new_due_at IS NULL"
    )
    op.alter_column("poll_schedule_decisions", "new_due_at", nullable=False)
    op.execute(
        "UPDATE poll_schedules SET next_due_at = updated_at WHERE next_due_at IS NULL"
    )
    op.alter_column("poll_schedules", "next_due_at", nullable=False)
    op.drop_index("ix_poll_schedules_control_rotation", table_name="poll_schedules")
    op.drop_index("ix_poll_schedules_coverage_transition", table_name="poll_schedules")
    for constraint_name in (
        "ck_poll_schedules_control_count",
        "ck_poll_schedules_retired_not_due",
        "ck_poll_schedules_coverage_projection",
        "ck_poll_schedules_coverage_class",
    ):
        op.drop_constraint(constraint_name, "poll_schedules", type_="check")
    op.drop_constraint(
        "fk_poll_schedules_coverage_policy", "poll_schedules", type_="foreignkey"
    )
    for column_name in (
        "control_scan_count",
        "last_control_scan_at",
        "coverage_policy_sha256",
        "coverage_next_transition_at",
        "coverage_decided_at",
        "coverage_class",
        "admitted_at",
    ):
        op.drop_column("poll_schedules", column_name)

    op.drop_constraint("uq_poll_batches_control_window", "poll_batches", type_="unique")
    op.drop_constraint("ck_poll_batches_control_window", "poll_batches", type_="check")
    op.drop_constraint("ck_poll_batches_kind", "poll_batches", type_="check")
    op.drop_column("poll_batches", "control_window_start")
    op.drop_column("poll_batches", "batch_kind")

    op.drop_index("ix_coverage_decisions_epoch_decided", table_name="coverage_decisions")
    op.drop_index("ix_coverage_decisions_run_decided", table_name="coverage_decisions")
    op.drop_index("ix_coverage_decisions_token_decided", table_name="coverage_decisions")
    op.drop_table("coverage_decisions")
    op.drop_table("coverage_policies")
