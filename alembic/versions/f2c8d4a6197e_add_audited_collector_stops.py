"""add audited collector stop and stale-run reconciliation

Revision ID: f2c8d4a6197e
Revises: e4b7a9c1d203
Create Date: 2026-08-17 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f2c8d4a6197e"
down_revision: str | Sequence[str] | None = "e4b7a9c1d203"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add an explicit stopped status and immutable terminal-run evidence."""
    op.drop_constraint("ck_collector_runs_status", "collector_runs", type_="check")
    op.create_check_constraint(
        "ck_collector_runs_status",
        "collector_runs",
        "status IN ('running', 'stopped', 'succeeded', 'failed', 'cancelled')",
    )
    op.create_table(
        "collector_run_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("collector_run_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.String(length=2048), nullable=False),
        sa.Column("detail", postgresql.JSONB(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.CheckConstraint(
            "event_type IN ('graceful_stop', 'failed', 'stale_reconciled')",
            name="ck_collector_run_events_type",
        ),
        sa.ForeignKeyConstraint(
            ["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("idempotency_key", name="uq_collector_run_events_idempotency"),
    )
    op.create_index(
        "ix_collector_run_events_run_occurred",
        "collector_run_events",
        ["collector_run_id", "occurred_at"],
    )
    op.execute(
        "CREATE TRIGGER collector_run_events_immutable "
        "BEFORE UPDATE OR DELETE ON collector_run_events "
        "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
    )


def downgrade() -> None:
    """Remove audited stops after mapping stopped rows to the legacy status."""
    op.execute("DROP TRIGGER IF EXISTS collector_run_events_immutable ON collector_run_events")
    op.drop_index("ix_collector_run_events_run_occurred", table_name="collector_run_events")
    op.drop_table("collector_run_events")
    op.execute("UPDATE collector_runs SET status = 'cancelled' WHERE status = 'stopped'")
    op.drop_constraint("ck_collector_runs_status", "collector_runs", type_="check")
    op.create_check_constraint(
        "ck_collector_runs_status",
        "collector_runs",
        "status IN ('running', 'succeeded', 'failed', 'cancelled')",
    )
