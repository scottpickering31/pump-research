"""Add append-only duplicate-delivery evidence for operational reporting.

Revision ID: a31c9f8e27b4
Revises: e6b51d99c214
Create Date: 2026-08-14
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a31c9f8e27b4"
down_revision: str | Sequence[str] | None = "e6b51d99c214"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "deduplication_conflicts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("record_type", sa.String(length=64), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_deduplication_conflicts_record_type_occurred_at",
        "deduplication_conflicts",
        ["record_type", "occurred_at"],
    )
    op.create_index(
        "ix_poll_batch_members_token_claimed_at",
        "poll_batch_members",
        ["token_id", "claimed_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_poll_batch_members_token_claimed_at",
        table_name="poll_batch_members",
    )
    op.drop_index(
        "ix_deduplication_conflicts_record_type_occurred_at",
        table_name="deduplication_conflicts",
    )
    op.drop_table("deduplication_conflicts")
