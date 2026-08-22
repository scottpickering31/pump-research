"""add discovery connectivity events

Revision ID: f19c8a42d6e1
Revises: c3e91a7d4b62
"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "f19c8a42d6e1"
down_revision: str | Sequence[str] | None = "c3e91a7d4b62"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "discovery_connectivity_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("source_name", sa.String(length=64), nullable=False),
        sa.Column("gap_id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=16), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.String(length=128), nullable=False),
        sa.Column("detail", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "event_type IN ('disconnected', 'reconnected')",
            name="ck_discovery_connectivity_events_type",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_key", name="uq_discovery_connectivity_events_idempotency_key"
        ),
    )
    op.create_index(
        "ix_discovery_connectivity_events_gap_id",
        "discovery_connectivity_events",
        ["gap_id"],
        unique=False,
    )
    op.create_index(
        "ix_discovery_connectivity_events_source_observed_at",
        "discovery_connectivity_events",
        ["source_name", "observed_at"],
        unique=False,
    )
    op.execute(
        "CREATE TRIGGER discovery_connectivity_events_immutable "
        "BEFORE UPDATE OR DELETE ON discovery_connectivity_events "
        "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS discovery_connectivity_events_immutable "
        "ON discovery_connectivity_events"
    )
    op.drop_index(
        "ix_discovery_connectivity_events_source_observed_at",
        table_name="discovery_connectivity_events",
    )
    op.drop_index(
        "ix_discovery_connectivity_events_gap_id",
        table_name="discovery_connectivity_events",
    )
    op.drop_table("discovery_connectivity_events")
