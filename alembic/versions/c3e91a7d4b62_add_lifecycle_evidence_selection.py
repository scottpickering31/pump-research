"""Add immutable deterministic lifecycle evidence selections.

Revision ID: c3e91a7d4b62
Revises: b7de3f64a921
Create Date: 2026-08-15
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c3e91a7d4b62"
down_revision: str | Sequence[str] | None = "b7de3f64a921"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "lifecycle_evidence_evaluations",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("input_watermark", sa.DateTime(timezone=True), nullable=False),
        sa.Column("token_id", sa.Uuid(), nullable=False),
        sa.Column("api_request_log_id", sa.Uuid(), nullable=False),
        sa.Column("outcome", sa.String(length=16), nullable=False),
        sa.Column("selected_pair_id", sa.Uuid(), nullable=True),
        sa.Column("selected_observation_id", sa.Uuid(), nullable=True),
        sa.Column(
            "selected_observation_received_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("reason_code", sa.String(length=128), nullable=False),
        sa.Column(
            "reason_detail",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("policy_sha256", sa.String(length=64), nullable=False),
        sa.Column(
            "policy_snapshot",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "persisted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "outcome IN ('selected', 'failed')",
            name="ck_lifecycle_evidence_evaluations_outcome",
        ),
        sa.CheckConstraint(
            "(outcome = 'selected' AND selected_pair_id IS NOT NULL "
            "AND selected_observation_id IS NOT NULL "
            "AND selected_observation_received_at IS NOT NULL) OR "
            "(outcome = 'failed' AND selected_pair_id IS NULL "
            "AND selected_observation_id IS NULL "
            "AND selected_observation_received_at IS NULL)",
            name="ck_lifecycle_evidence_evaluations_selection",
        ),
        sa.ForeignKeyConstraint(
            ["api_request_log_id"],
            ["api_request_log.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["selected_observation_received_at", "selected_observation_id"],
            ["observations.received_at", "observations.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["selected_pair_id"],
            ["pairs.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["token_id"], ["tokens.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint(
            "input_watermark",
            "id",
            name="pk_lifecycle_evidence_evaluations",
        ),
        sa.UniqueConstraint(
            "input_watermark",
            "token_id",
            "api_request_log_id",
            "policy_sha256",
            name="uq_lifecycle_evidence_token_request_policy",
        ),
        postgresql_partition_by="RANGE (input_watermark)",
    )
    op.create_index(
        "ix_lifecycle_evidence_token_watermark",
        "lifecycle_evidence_evaluations",
        ["token_id", "input_watermark"],
    )
    op.execute(
        """
        DO $$
        DECLARE
            partition_start date;
        BEGIN
            FOR partition_start IN
                SELECT generate_series(
                    DATE '2026-01-01', DATE '2027-12-01', INTERVAL '1 month'
                )::date
            LOOP
                EXECUTE format(
                    'CREATE TABLE lifecycle_evidence_evaluations_%s '
                    'PARTITION OF lifecycle_evidence_evaluations '
                    'FOR VALUES FROM (%L) TO (%L)',
                    to_char(partition_start, 'YYYY_MM'),
                    partition_start,
                    (partition_start + INTERVAL '1 month')::date
                );
            END LOOP;
        END $$;
        """
    )
    op.execute(
        "CREATE TRIGGER lifecycle_evidence_evaluations_immutable "
        "BEFORE UPDATE OR DELETE ON lifecycle_evidence_evaluations "
        "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
    )
    op.add_column(
        "lifecycle_events",
        sa.Column("lifecycle_evidence_evaluation_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "lifecycle_events",
        sa.Column(
            "lifecycle_evidence_input_watermark",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_check_constraint(
        "ck_lifecycle_events_evidence_reference",
        "lifecycle_events",
        "(lifecycle_evidence_input_watermark IS NULL) = "
        "(lifecycle_evidence_evaluation_id IS NULL)",
    )
    op.create_foreign_key(
        "fk_lifecycle_events_evidence_evaluation",
        "lifecycle_events",
        "lifecycle_evidence_evaluations",
        ["lifecycle_evidence_input_watermark", "lifecycle_evidence_evaluation_id"],
        ["input_watermark", "id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_lifecycle_events_evidence_evaluation",
        "lifecycle_events",
        type_="foreignkey",
    )
    op.drop_constraint(
        "ck_lifecycle_events_evidence_reference",
        "lifecycle_events",
        type_="check",
    )
    op.drop_column("lifecycle_events", "lifecycle_evidence_input_watermark")
    op.drop_column("lifecycle_events", "lifecycle_evidence_evaluation_id")
    op.execute(
        "DROP TRIGGER IF EXISTS lifecycle_evidence_evaluations_immutable "
        "ON lifecycle_evidence_evaluations"
    )
    op.drop_index(
        "ix_lifecycle_evidence_token_watermark",
        table_name="lifecycle_evidence_evaluations",
    )
    op.drop_table("lifecycle_evidence_evaluations")
