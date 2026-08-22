"""add collection epochs, storage telemetry, and backup verification

Revision ID: d92e14a7c5f0
Revises: 84d1f0c2a6be
Create Date: 2026-08-15 20:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d92e14a7c5f0"
down_revision: str | Sequence[str] | None = "84d1f0c2a6be"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_EPOCH0_ID = "00000000-0000-0000-0000-000000000000"
_EPOCH0_EVENT_ID = "00000000-0000-0000-0000-000000000001"
_EPOCH0_CONFIGURATION_SHA256 = (
    "1e58165118b9b0c49744ae0a6a63ef3aa71704f870696223c2edf18b81696d96"
)


def upgrade() -> None:
    """Create auditable epoch provenance and compact operational measurements."""
    op.create_table(
        "collection_epochs",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("epoch_number", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("purpose", sa.String(length=2048), nullable=False),
        sa.Column("data_valid", sa.Boolean(), nullable=False),
        sa.Column("invalid_reason", sa.String(length=2048), nullable=True),
        sa.Column("configuration_sha256", sa.String(length=64), nullable=False),
        sa.Column("configuration_snapshot", postgresql.JSONB(), nullable=False),
        sa.Column("code_revision", sa.String(length=128), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.CheckConstraint(
            "(data_valid AND invalid_reason IS NULL) OR "
            "(NOT data_valid AND invalid_reason IS NOT NULL)",
            name="ck_collection_epochs_validity_reason",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("epoch_number", name="uq_collection_epochs_number"),
    )
    op.create_table(
        "collection_epoch_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("reason", sa.String(length=2048), nullable=False),
        sa.Column("detail", postgresql.JSONB(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=128), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.CheckConstraint(
            "status IN ('planned', 'running', 'completed', 'aborted', 'invalid')",
            name="ck_collection_epoch_events_status",
        ),
        sa.ForeignKeyConstraint(
            ["collection_epoch_id"], ["collection_epochs.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "idempotency_key", name="uq_collection_epoch_events_idempotency"
        ),
    )
    op.create_index(
        "ix_collection_epoch_events_epoch_occurred",
        "collection_epoch_events",
        ["collection_epoch_id", "occurred_at"],
    )
    op.create_table(
        "collection_epoch_current",
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("latest_event_id", sa.Uuid(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('planned', 'running', 'completed', 'aborted', 'invalid')",
            name="ck_collection_epoch_current_status",
        ),
        sa.ForeignKeyConstraint(
            ["collection_epoch_id"], ["collection_epochs.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["latest_event_id"], ["collection_epoch_events.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("collection_epoch_id"),
    )
    op.create_index(
        "uq_collection_epoch_current_one_running",
        "collection_epoch_current",
        ["status"],
        unique=True,
        postgresql_where=sa.text("status = 'running'"),
    )

    epoch0_snapshot = (
        '{"component":"collection_epoch","epoch_number":0,'
        '"purpose":"engineering burn-in","research_validity":"NONE","schema_version":1}'
    )
    op.execute(
        sa.text(
            "INSERT INTO collection_epochs "
            "(id, epoch_number, name, purpose, data_valid, invalid_reason, "
            "configuration_sha256, configuration_snapshot, code_revision, created_at) "
            "VALUES (CAST(:epoch_id AS uuid), 0, 'Epoch 0', 'engineering burn-in only', false, "
            "'data destroyed by integration fixture using TRUNCATE CASCADE; unrecoverable; "
            "research validity NONE', :digest, CAST(:snapshot AS jsonb), NULL, "
            "TIMESTAMPTZ '2026-08-15 00:00:00+00')"
        ).bindparams(
            epoch_id=_EPOCH0_ID,
            digest=_EPOCH0_CONFIGURATION_SHA256,
            snapshot=epoch0_snapshot,
        )
    )
    op.execute(
        sa.text(
            "INSERT INTO collection_epoch_events "
            "(id, collection_epoch_id, status, occurred_at, reason, detail, idempotency_key) "
            "VALUES (CAST(:event_id AS uuid), CAST(:epoch_id AS uuid), 'invalid', "
            "TIMESTAMPTZ '2026-08-15 00:00:00+00', 'Epoch 0 permanently lost', "
            "'{\"research_validity\":\"NONE\",\"recovery\":\"forbidden\"}'::jsonb, "
            "'epoch:0:invalid:lost-2026-08-15')"
        ).bindparams(event_id=_EPOCH0_EVENT_ID, epoch_id=_EPOCH0_ID)
    )
    op.execute(
        sa.text(
            "INSERT INTO collection_epoch_current "
            "(collection_epoch_id, status, started_at, ended_at, latest_event_id, updated_at) "
            "VALUES (CAST(:epoch_id AS uuid), 'invalid', NULL, "
            "TIMESTAMPTZ '2026-08-15 00:00:00+00', CAST(:event_id AS uuid), "
            "TIMESTAMPTZ '2026-08-15 00:00:00+00')"
        ).bindparams(epoch_id=_EPOCH0_ID, event_id=_EPOCH0_EVENT_ID)
    )

    op.add_column("collector_runs", sa.Column("collection_epoch_id", sa.Uuid(), nullable=True))
    op.execute(
        sa.text("UPDATE collector_runs SET collection_epoch_id = CAST(:epoch_id AS uuid)").bindparams(
            epoch_id=_EPOCH0_ID
        )
    )
    op.alter_column("collector_runs", "collection_epoch_id", nullable=False)
    op.create_foreign_key(
        "fk_collector_runs_collection_epoch",
        "collector_runs",
        "collection_epochs",
        ["collection_epoch_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_collector_runs_collection_epoch_id",
        "collector_runs",
        ["collection_epoch_id"],
    )

    for table_name in ("discovery_events", "lifecycle_events", "poll_batches"):
        op.add_column(table_name, sa.Column("collector_run_id", sa.Uuid(), nullable=True))
        op.create_foreign_key(
            f"fk_{table_name}_collector_run",
            table_name,
            "collector_runs",
            ["collector_run_id"],
            ["id"],
            ondelete="RESTRICT",
        )
        op.create_index(
            f"ix_{table_name}_collector_run_id", table_name, ["collector_run_id"]
        )
    op.execute(
        """
        UPDATE poll_batches pb
        SET collector_run_id = ar.collector_run_id
        FROM poll_batch_outcomes pbo
        JOIN api_request_log ar ON ar.id = pbo.api_request_log_id
        WHERE pbo.batch_id = pb.id AND ar.collector_run_id IS NOT NULL
        """
    )

    op.create_table(
        "storage_samples",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=False),
        sa.Column("collector_run_id", sa.Uuid(), nullable=False),
        sa.Column("sampled_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("database_bytes", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["collection_epoch_id"], ["collection_epochs.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(["collector_run_id"], ["collector_runs.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "collector_run_id", "sampled_at", name="uq_storage_samples_run_time"
        ),
    )
    op.create_index(
        "ix_storage_samples_epoch_sampled",
        "storage_samples",
        ["collection_epoch_id", "sampled_at"],
    )
    op.create_table(
        "storage_relation_samples",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("storage_sample_id", sa.Uuid(), nullable=False),
        sa.Column("relation_name", sa.String(length=128), nullable=False),
        sa.Column("relation_family", sa.String(length=128), nullable=False),
        sa.Column("total_bytes", sa.BigInteger(), nullable=False),
        sa.Column("table_data_bytes", sa.BigInteger(), nullable=False),
        sa.Column("index_bytes", sa.BigInteger(), nullable=False),
        sa.Column("toast_and_aux_bytes", sa.BigInteger(), nullable=False),
        sa.Column("estimated_row_count", sa.BigInteger(), nullable=False),
        sa.Column("rows_per_minute_since_previous", sa.Numeric(20, 3), nullable=True),
        sa.Column("bytes_per_row_delta", sa.Numeric(20, 3), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(["storage_sample_id"], ["storage_samples.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "storage_sample_id", "relation_name", name="uq_storage_relation_sample"
        ),
    )
    op.create_index(
        "ix_storage_relation_samples_relation",
        "storage_relation_samples",
        ["relation_name", "storage_sample_id"],
    )
    op.create_table(
        "backup_verifications",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("collection_epoch_id", sa.Uuid(), nullable=False),
        sa.Column("artifact_path", sa.String(length=4096), nullable=False),
        sa.Column("artifact_kind", sa.String(length=64), nullable=False),
        sa.Column("artifact_bytes", sa.BigInteger(), nullable=False),
        sa.Column("artifact_sha256", sa.String(length=64), nullable=False),
        sa.Column("verification_method", sa.String(length=256), nullable=False),
        sa.Column("independent_copy", sa.Boolean(), nullable=False),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("detail", postgresql.JSONB(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False
        ),
        sa.ForeignKeyConstraint(
            ["collection_epoch_id"], ["collection_epochs.id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "collection_epoch_id",
            "artifact_sha256",
            "artifact_path",
            name="uq_backup_verifications_artifact",
        ),
    )
    op.create_index(
        "ix_backup_verifications_epoch_verified",
        "backup_verifications",
        ["collection_epoch_id", "verified_at"],
    )

    for table_name in (
        "collection_epochs",
        "collection_epoch_events",
        "storage_samples",
        "storage_relation_samples",
        "backup_verifications",
    ):
        op.execute(
            f"CREATE TRIGGER {table_name}_immutable "
            f"BEFORE UPDATE OR DELETE ON {table_name} "
            "FOR EACH ROW EXECUTE FUNCTION prevent_immutable_table_mutation()"
        )


def downgrade() -> None:
    """Remove Epoch 1 readiness schema while retaining pre-migration facts."""
    for table_name in (
        "backup_verifications",
        "storage_relation_samples",
        "storage_samples",
        "collection_epoch_events",
        "collection_epochs",
    ):
        op.execute(f"DROP TRIGGER IF EXISTS {table_name}_immutable ON {table_name}")

    op.drop_index("ix_backup_verifications_epoch_verified", table_name="backup_verifications")
    op.drop_table("backup_verifications")
    op.drop_index(
        "ix_storage_relation_samples_relation", table_name="storage_relation_samples"
    )
    op.drop_table("storage_relation_samples")
    op.drop_index("ix_storage_samples_epoch_sampled", table_name="storage_samples")
    op.drop_table("storage_samples")
    for table_name in ("poll_batches", "lifecycle_events", "discovery_events"):
        op.drop_index(f"ix_{table_name}_collector_run_id", table_name=table_name)
        op.drop_constraint(
            f"fk_{table_name}_collector_run", table_name, type_="foreignkey"
        )
        op.drop_column(table_name, "collector_run_id")
    op.drop_index("ix_collector_runs_collection_epoch_id", table_name="collector_runs")
    op.drop_constraint(
        "fk_collector_runs_collection_epoch", "collector_runs", type_="foreignkey"
    )
    op.drop_column("collector_runs", "collection_epoch_id")
    op.drop_index(
        "uq_collection_epoch_current_one_running", table_name="collection_epoch_current"
    )
    op.drop_table("collection_epoch_current")
    op.drop_index(
        "ix_collection_epoch_events_epoch_occurred", table_name="collection_epoch_events"
    )
    op.drop_table("collection_epoch_events")
    op.drop_table("collection_epochs")
