"""add effective epoch validity projection

Revision ID: e4b7a9c1d203
Revises: d92e14a7c5f0
Create Date: 2026-08-16 08:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e4b7a9c1d203"
down_revision: str | Sequence[str] | None = "d92e14a7c5f0"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Track audited effective validity without mutating epoch declarations."""
    op.add_column("collection_epoch_current", sa.Column("data_valid", sa.Boolean(), nullable=True))
    op.add_column(
        "collection_epoch_current",
        sa.Column("invalid_reason", sa.String(length=2048), nullable=True),
    )
    op.execute(
        """
        UPDATE collection_epoch_current current
        SET data_valid = epoch.data_valid,
            invalid_reason = epoch.invalid_reason
        FROM collection_epochs epoch
        WHERE epoch.id = current.collection_epoch_id
        """
    )
    op.alter_column("collection_epoch_current", "data_valid", nullable=False)
    op.create_check_constraint(
        "ck_collection_epoch_current_validity_reason",
        "collection_epoch_current",
        "(data_valid AND invalid_reason IS NULL) OR "
        "(NOT data_valid AND invalid_reason IS NOT NULL)",
    )
    op.add_column(
        "poll_schedule_decisions",
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=True),
    )
    op.create_foreign_key(
        "fk_poll_schedule_decisions_collection_epoch",
        "poll_schedule_decisions",
        "collection_epochs",
        ["collection_epoch_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_poll_schedule_decisions_collection_epoch_id",
        "poll_schedule_decisions",
        ["collection_epoch_id"],
    )


def downgrade() -> None:
    """Remove the rebuildable effective-validity projection."""
    op.drop_index(
        "ix_poll_schedule_decisions_collection_epoch_id",
        table_name="poll_schedule_decisions",
    )
    op.drop_constraint(
        "fk_poll_schedule_decisions_collection_epoch",
        "poll_schedule_decisions",
        type_="foreignkey",
    )
    op.drop_column("poll_schedule_decisions", "collection_epoch_id")
    op.drop_constraint(
        "ck_collection_epoch_current_validity_reason",
        "collection_epoch_current",
        type_="check",
    )
    op.drop_column("collection_epoch_current", "invalid_reason")
    op.drop_column("collection_epoch_current", "data_valid")
