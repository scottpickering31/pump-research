"""Add durable provider-neutral discovery checkpoint state.

Revision ID: e6b51d99c214
Revises: d4f8c2a71b09
Create Date: 2026-08-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e6b51d99c214"
down_revision: str | Sequence[str] | None = "d4f8c2a71b09"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "discovery_checkpoint_states",
        sa.Column("source_name", sa.String(length=64), nullable=False),
        sa.Column("checkpoint_value", sa.String(length=2048), nullable=False),
        sa.Column("last_batch_received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("coverage_status", sa.String(length=16), nullable=False),
        sa.Column("supports_replay", sa.Boolean(), nullable=False),
        sa.Column("coverage_note", sa.String(length=2048), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "coverage_status IN ('complete', 'best_effort', 'unknown')",
            name="ck_discovery_checkpoint_states_coverage",
        ),
        sa.PrimaryKeyConstraint("source_name"),
    )


def downgrade() -> None:
    op.drop_table("discovery_checkpoint_states")
