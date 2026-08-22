"""Add collector heartbeat and current component-health projections.

Revision ID: b7de3f64a921
Revises: a31c9f8e27b4
Create Date: 2026-08-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b7de3f64a921"
down_revision: str | Sequence[str] | None = "a31c9f8e27b4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("collector_runs", sa.Column("last_heartbeat_at", sa.DateTime(timezone=True)))
    op.create_table(
        "collector_component_health",
        sa.Column("component_name", sa.String(length=64), nullable=False),
        sa.Column("collector_run_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_success_at", sa.DateTime(timezone=True)),
        sa.Column("detail", postgresql.JSONB()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('healthy', 'degraded', 'failed', 'stopped')",
            name="ck_component_health_status",
        ),
        sa.ForeignKeyConstraint(["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("component_name"),
    )
    op.create_index(
        "ix_collector_component_health_run_id", "collector_component_health", ["collector_run_id"]
    )


def downgrade() -> None:
    op.drop_index("ix_collector_component_health_run_id", table_name="collector_component_health")
    op.drop_table("collector_component_health")
    op.drop_column("collector_runs", "last_heartbeat_at")
