"""Add lossless quarantine for PostgreSQL-incompatible discovery messages.

Revision ID: 7d9b3e5f0a12
Revises: 2b6f0d8e4a91
"""

from alembic import op
import sqlalchemy as sa

revision = "7d9b3e5f0a12"
down_revision = "2b6f0d8e4a91"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "discovery_rejected_messages",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("collector_run_id", sa.Uuid(), nullable=True),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("endpoint", sa.String(256), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason_code", sa.String(64), nullable=False),
        sa.Column("raw_message", sa.LargeBinary(), nullable=False),
        sa.Column("message_encoding", sa.String(32), nullable=False),
        sa.Column("raw_message_sha256", sa.String(64), nullable=False),
        sa.Column("persisted_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("idempotency_key", name="uq_discovery_rejected_messages_idempotency"),
        sa.CheckConstraint(
            "message_encoding IN ('binary', 'utf8-surrogatepass')",
            name="ck_discovery_rejected_messages_encoding",
        ),
    )
    op.create_index(
        "ix_discovery_rejected_messages_provider_received",
        "discovery_rejected_messages",
        ["provider", "received_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_discovery_rejected_messages_provider_received", "discovery_rejected_messages")
    op.drop_table("discovery_rejected_messages")
