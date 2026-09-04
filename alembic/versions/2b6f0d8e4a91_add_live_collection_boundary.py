"""add durable live collection boundary

Revision ID: 2b6f0d8e4a91
Revises: f63b7d9a20ce
Create Date: 2026-09-04 12:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "2b6f0d8e4a91"
down_revision: str | Sequence[str] | None = "f63b7d9a20ce"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add an unknown-by-default, one-way boundary to mutable run records."""
    op.add_column(
        "collector_runs",
        sa.Column("collection_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_collector_runs_collection_started_after_invocation",
        "collector_runs",
        "collection_started_at IS NULL OR collection_started_at >= started_at",
    )
    op.execute("""
        CREATE FUNCTION enforce_collector_run_collection_start_one_way() RETURNS trigger AS $$
        BEGIN
            IF OLD.collection_started_at IS NOT NULL
               AND NEW.collection_started_at IS DISTINCT FROM OLD.collection_started_at THEN
                RAISE EXCEPTION 'collector_runs.collection_started_at is one-way and immutable'
                    USING ERRCODE = '55000';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
    """)
    op.execute(
        "CREATE TRIGGER collector_runs_collection_start_one_way "
        "BEFORE UPDATE OF collection_started_at ON collector_runs "
        "FOR EACH ROW EXECUTE FUNCTION enforce_collector_run_collection_start_one_way()"
    )


def downgrade() -> None:
    """Remove the live collection boundary and its one-way guard."""
    op.execute(
        "DROP TRIGGER IF EXISTS collector_runs_collection_start_one_way ON collector_runs"
    )
    op.execute("DROP FUNCTION IF EXISTS enforce_collector_run_collection_start_one_way()")
    op.drop_constraint(
        "ck_collector_runs_collection_started_after_invocation",
        "collector_runs",
        type_="check",
    )
    op.drop_column("collector_runs", "collection_started_at")
