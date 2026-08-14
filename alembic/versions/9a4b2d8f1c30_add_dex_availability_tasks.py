"""add durable DEX availability tasks

Revision ID: 9a4b2d8f1c30
Revises: 03a8779c3f27
Create Date: 2026-08-14 19:15:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "9a4b2d8f1c30"
down_revision: Union[str, Sequence[str], None] = "03a8779c3f27"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create the small mutable projection used to recover pending DEX work."""
    op.create_table(
        "dex_availability_tasks",
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("next_check_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_checked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("lease_id", sa.Uuid(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("attempt_count >= 0", name="ck_dex_availability_tasks_attempt_count"),
        sa.CheckConstraint(
            "state IN ('PENDING_DEX', 'NEW')",
            name="ck_dex_availability_tasks_state",
        ),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("token_id"),
    )
    op.create_index(
        "ix_dex_availability_tasks_due_pending",
        "dex_availability_tasks",
        ["next_check_at"],
        unique=False,
        postgresql_where=sa.text("state = 'PENDING_DEX'"),
    )


def downgrade() -> None:
    """Remove the mutable projection; append-only evidence remains untouched."""
    op.drop_index("ix_dex_availability_tasks_due_pending", table_name="dex_availability_tasks")
    op.drop_table("dex_availability_tasks")
