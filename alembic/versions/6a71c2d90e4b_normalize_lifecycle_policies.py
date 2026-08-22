"""Normalize lifecycle policies and remove an unused poll-member index.

Revision ID: 6a71c2d90e4b
Revises: f19c8a42d6e1
Create Date: 2026-08-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "6a71c2d90e4b"
down_revision: str | Sequence[str] | None = "f19c8a42d6e1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Normalize policy documents without rewriting historical evaluations."""
    op.create_table(
        "lifecycle_policies",
        sa.Column("policy_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "policy_snapshot",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("policy_sha256"),
    )
    op.execute(
        "LOCK TABLE lifecycle_evidence_evaluations IN SHARE ROW EXCLUSIVE MODE"
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT policy_sha256
                FROM lifecycle_evidence_evaluations
                GROUP BY policy_sha256
                HAVING count(DISTINCT policy_snapshot) <> 1
            ) THEN
                RAISE EXCEPTION
                    'one lifecycle policy_sha256 maps to multiple policy snapshots';
            END IF;
        END $$
        """
    )
    op.execute(
        """
        INSERT INTO lifecycle_policies (policy_sha256, policy_snapshot)
        SELECT DISTINCT policy_sha256, policy_snapshot
        FROM lifecycle_evidence_evaluations
        """
    )
    op.execute(
        """
        ALTER TABLE lifecycle_evidence_evaluations
        ADD CONSTRAINT fk_lifecycle_evidence_evaluations_policy
        FOREIGN KEY (policy_sha256)
        REFERENCES lifecycle_policies (policy_sha256)
        ON DELETE RESTRICT
        """
    )
    op.alter_column(
        "lifecycle_evidence_evaluations",
        "policy_snapshot",
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        nullable=True,
    )
    op.execute(
        "CREATE TRIGGER lifecycle_policies_immutable "
        "BEFORE UPDATE OR DELETE ON lifecycle_policies "
        "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
    )
    op.drop_index(
        "ix_poll_batch_members_token_due_at",
        table_name="poll_batch_members",
    )


def downgrade() -> None:
    """Restore inline snapshots and the removed index without losing policy facts."""
    op.execute(
        "LOCK TABLE lifecycle_evidence_evaluations IN SHARE ROW EXCLUSIVE MODE"
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM lifecycle_evidence_evaluations AS evaluation
                JOIN lifecycle_policies AS policy
                  ON policy.policy_sha256 = evaluation.policy_sha256
                WHERE evaluation.policy_snapshot IS NOT NULL
                  AND evaluation.policy_snapshot IS DISTINCT FROM policy.policy_snapshot
            ) THEN
                RAISE EXCEPTION
                    'inline and normalized lifecycle policy snapshots disagree';
            END IF;
        END $$
        """
    )
    op.execute(
        "DROP TRIGGER IF EXISTS lifecycle_evidence_evaluations_immutable "
        "ON lifecycle_evidence_evaluations"
    )
    op.execute(
        """
        UPDATE lifecycle_evidence_evaluations AS evaluation
        SET policy_snapshot = policy.policy_snapshot
        FROM lifecycle_policies AS policy
        WHERE evaluation.policy_sha256 = policy.policy_sha256
          AND evaluation.policy_snapshot IS NULL
        """
    )
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM lifecycle_evidence_evaluations
                WHERE policy_snapshot IS NULL
            ) THEN
                RAISE EXCEPTION
                    'cannot restore non-null inline lifecycle policy snapshots';
            END IF;
        END $$
        """
    )
    op.alter_column(
        "lifecycle_evidence_evaluations",
        "policy_snapshot",
        existing_type=postgresql.JSONB(astext_type=sa.Text()),
        nullable=False,
    )
    op.execute(
        "CREATE TRIGGER lifecycle_evidence_evaluations_immutable "
        "BEFORE UPDATE OR DELETE ON lifecycle_evidence_evaluations "
        "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
    )
    op.drop_constraint(
        "fk_lifecycle_evidence_evaluations_policy",
        "lifecycle_evidence_evaluations",
        type_="foreignkey",
    )
    op.create_index(
        "ix_poll_batch_members_token_due_at",
        "poll_batch_members",
        ["token_id", "due_at"],
        unique=False,
    )
    op.execute(
        "DROP TRIGGER IF EXISTS lifecycle_policies_immutable ON lifecycle_policies"
    )
    op.drop_table("lifecycle_policies")
