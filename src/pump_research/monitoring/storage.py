"""Low-frequency PostgreSQL storage telemetry for Epoch 1 measurement."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.persistence.models import (
    CollectorRun,
    StorageRelationSample,
    StorageSample,
)

_RELATIONS = (
    "api_request_log",
    "lifecycle_events",
    "poll_batches",
    "poll_batch_outcomes",
    "poll_schedule_decisions",
    "discovery_events",
    "pair_fact_events",
    "boost_observations",
    "boost_events",
    "token_metadata_events",
    "token_security_snapshots",
    "token_security_tasks",
    "market_context_snapshots",
    "pairs",
    "tokens",
)
_PARTITION_FAMILIES = (
    "observations",
    "lifecycle_evidence_evaluations",
    "poll_batch_members",
)


async def record_storage_sample(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    collector_run_id: uuid.UUID,
    sampled_at: datetime | None = None,
) -> StorageSample:
    """Persist exact byte sizes and inexpensive PostgreSQL row-count estimates."""
    now = (sampled_at or datetime.now(UTC)).astimezone(UTC)
    async with session_factory() as session, session.begin():
        run = await session.get(CollectorRun, collector_run_id)
        if run is None:
            raise RuntimeError(f"collector run does not exist: {collector_run_id}")
        database_bytes = int(
            await session.scalar(text("SELECT pg_database_size(current_database())")) or 0
        )
        sample = StorageSample(
            collection_epoch_id=run.collection_epoch_id,
            collector_run_id=run.id,
            sampled_at=now,
            database_bytes=database_bytes,
        )
        session.add(sample)
        await session.flush()
        current_suffix = now.strftime("%Y_%m")
        tracked = list(_RELATIONS) + [
            f"{family}_{current_suffix}" for family in _PARTITION_FAMILIES
        ]
        relation_rows = (
            await session.execute(
                text("""
                SELECT c.relname,
                       pg_total_relation_size(c.oid)::bigint AS total_bytes,
                       pg_relation_size(c.oid)::bigint AS data_bytes,
                       pg_indexes_size(c.oid)::bigint AS index_bytes,
                       GREATEST(
                         pg_table_size(c.oid) - pg_relation_size(c.oid), 0
                       )::bigint AS toast_and_aux_bytes,
                       GREATEST(c.reltuples, 0)::bigint AS estimated_rows
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = current_schema()
                  AND c.relkind IN ('r', 'p')
                  AND c.relname = ANY(CAST(:relations AS text[]))
                ORDER BY c.relname
                """),
                {"relations": tracked},
            )
        ).all()
        previous_sample = await session.scalar(
            select(StorageSample)
            .where(
                StorageSample.collection_epoch_id == run.collection_epoch_id,
                StorageSample.sampled_at < now,
            )
            .order_by(StorageSample.sampled_at.desc())
            .limit(1)
        )
        previous_by_relation: dict[str, StorageRelationSample] = {}
        elapsed_minutes: Decimal | None = None
        if previous_sample is not None:
            previous_by_relation = {
                row.relation_name: row
                for row in (
                    await session.execute(
                        select(StorageRelationSample).where(
                            StorageRelationSample.storage_sample_id == previous_sample.id
                        )
                    )
                ).scalars()
            }
            elapsed_minutes = Decimal(str((now - previous_sample.sampled_at).total_seconds() / 60))
        for relation_name, total, data, indexes, toast_aux, row_count in relation_rows:
            family = next(
                (
                    candidate
                    for candidate in _PARTITION_FAMILIES
                    if relation_name.startswith(f"{candidate}_")
                ),
                relation_name,
            )
            previous = previous_by_relation.get(relation_name)
            rows_per_minute: Decimal | None = None
            bytes_per_row_delta: Decimal | None = None
            if previous is not None and elapsed_minutes and elapsed_minutes > 0:
                row_delta = int(row_count) - previous.estimated_row_count
                byte_delta = int(total) - previous.total_bytes
                rows_per_minute = Decimal(row_delta) / elapsed_minutes
                if row_delta > 0:
                    bytes_per_row_delta = Decimal(byte_delta) / Decimal(row_delta)
            session.add(
                StorageRelationSample(
                    storage_sample_id=sample.id,
                    relation_name=str(relation_name),
                    relation_family=family,
                    total_bytes=int(total),
                    table_data_bytes=int(data),
                    index_bytes=int(indexes),
                    toast_and_aux_bytes=int(toast_aux),
                    estimated_row_count=int(row_count),
                    rows_per_minute_since_previous=rows_per_minute,
                    bytes_per_row_delta=bytes_per_row_delta,
                )
            )
        await session.flush()
        return sample
