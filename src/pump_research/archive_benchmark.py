"""Read-only core-observation benchmark usable before the Phase 3 live migration."""

from __future__ import annotations

import hashlib
import json
import os
import resource
import shutil
import time
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from sqlalchemy import BigInteger, DateTime, Numeric, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.types import Uuid

from pump_research.archival import InsufficientArchiveDiskError
from pump_research.persistence.models import Observation

_COLUMN_NAMES = (
    "id",
    "received_at",
    "pair_id",
    "api_request_log_id",
    "source_observed_at",
    "source_record_locator",
    "source_record_sha256",
    "price_usd",
    "price_native",
    "liquidity_usd",
    "market_cap_usd",
    "fully_diluted_valuation_usd",
    "volume_m5_usd",
    "volume_h1_usd",
    "volume_h6_usd",
    "volume_h24_usd",
    "price_change_m5_pct",
    "price_change_h1_pct",
    "price_change_h6_pct",
    "price_change_h24_pct",
    "buys_m5",
    "sells_m5",
    "buys_h1",
    "sells_h1",
    "persisted_at",
)


async def benchmark_observation_window(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    epoch_number: int,
    start_at: datetime,
    end_at: datetime,
    output: Path,
    chunk_rows: int = 25_000,
    minimum_free_bytes: int = 2 * 1024**3,
) -> Path:
    """Export a read-only representative window without writing archive catalog state."""
    start = _utc(start_at)
    end = _utc(end_at)
    if start >= end:
        raise ValueError("benchmark range must have start_at < end_at")
    output = output.expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    async with session_factory() as session:
        source = (
            await session.execute(
                text(
                    """SELECT count(*)::bigint,
                              coalesce(sum(pg_column_size(o)), 0)::bigint
                       FROM observations o
                       JOIN api_request_log ar ON ar.id=o.api_request_log_id
                       JOIN collector_runs cr ON cr.id=ar.collector_run_id
                       JOIN collection_epochs e ON e.id=cr.collection_epoch_id
                       WHERE e.epoch_number=:epoch
                         AND o.received_at >= :start_at AND o.received_at < :end_at"""
                ),
                {"epoch": epoch_number, "start_at": start, "end_at": end},
            )
        ).one()
        source_revision = cast(
            str, await session.scalar(text("SELECT version_num FROM alembic_version"))
        )
        relation_bytes = int(
            cast(
                int,
                await session.scalar(
                    text(
                        """SELECT pg_total_relation_size('observations'::regclass)
                                  + coalesce(sum(pg_total_relation_size(inhrelid)), 0)
                           FROM pg_inherits
                           WHERE inhparent='observations'::regclass"""
                    )
                ),
            )
        )
    source_rows, source_logical_bytes = int(source[0]), int(source[1])
    free = shutil.disk_usage(output).free
    required = minimum_free_bytes + max(256 * 1024**2, int(source_logical_bytes * 1.25))
    if free < required:
        raise InsufficientArchiveDiskError(
            f"benchmark requires {required} free bytes; only {free} are available"
        )
    target = output / (
        f"epoch={epoch_number}_from={start.strftime('%Y%m%dT%H%M%SZ')}_"
        f"to={end.strftime('%Y%m%dT%H%M%SZ')}.parquet"
    )
    partial = target.with_name(f".{target.name}.partial-{uuid.uuid4().hex}")
    schema = _schema()
    writer = pq.ParquetWriter(
        partial,
        schema,
        compression="zstd",
        compression_level=6,
        use_dictionary=True,
        write_statistics=True,
    )
    exported = 0
    export_started = time.perf_counter()
    try:
        async with session_factory() as session:
            stream_result = await session.stream(
                text(
                    f"""SELECT {", ".join(f"o.{name}" for name in _COLUMN_NAMES)}
                        FROM observations o
                        JOIN api_request_log ar ON ar.id=o.api_request_log_id
                        JOIN collector_runs cr ON cr.id=ar.collector_run_id
                        JOIN collection_epochs e ON e.id=cr.collection_epoch_id
                        WHERE e.epoch_number=:epoch
                          AND o.received_at >= :start_at AND o.received_at < :end_at
                        ORDER BY o.received_at, o.id"""
                ),
                {"epoch": epoch_number, "start_at": start, "end_at": end},
            )
            async for partition in stream_result.mappings().partitions(chunk_rows):
                rows = [_normalize(dict(row)) for row in partition]
                writer.write_table(
                    pa.Table.from_pylist(rows, schema=schema),
                    row_group_size=chunk_rows,
                )
                exported += len(rows)
    finally:
        writer.close()
    os.replace(partial, target)
    export_seconds = time.perf_counter() - export_started
    if exported != source_rows:
        raise ValueError(f"benchmark source/export row mismatch: {source_rows} != {exported}")
    verification_started = time.perf_counter()
    parquet = pq.ParquetFile(target)
    verified_rows = sum(batch.num_rows for batch in parquet.iter_batches(batch_size=chunk_rows))
    connection = duckdb.connect(database=":memory:")
    try:
        count_row = connection.execute(
            "SELECT count(*) FROM read_parquet(?)", [str(target)]
        ).fetchone()
        if count_row is None or not isinstance(count_row[0], int):
            raise ValueError("DuckDB benchmark count did not return an integer")
        duckdb_rows = count_row[0]
        query_started = time.perf_counter()
        connection.execute(
            "SELECT count(DISTINCT pair_id), min(price_usd), max(market_cap_usd) "
            "FROM read_parquet(?)",
            [str(target)],
        ).fetchone()
        duckdb_query_ms = (time.perf_counter() - query_started) * 1000
    finally:
        connection.close()
    verification_seconds = time.perf_counter() - verification_started
    if verified_rows != source_rows or duckdb_rows != source_rows:
        raise ValueError("benchmark Parquet/DuckDB readback row mismatch")
    parquet_bytes = target.stat().st_size
    summary: dict[str, object] = {
        "benchmark_schema_version": 1,
        "canonical_archive": False,
        "purpose": "read-only Epoch 2 compression/throughput benchmark",
        "source_db_schema_revision": source_revision,
        "epoch": epoch_number,
        "start_at": start.isoformat(),
        "end_at": end.isoformat(),
        "source_rows": source_rows,
        "source_logical_heap_bytes": source_logical_bytes,
        "source_observation_relation_physical_bytes": relation_bytes,
        "parquet_bytes": parquet_bytes,
        "logical_to_parquet_compression_ratio": round(source_logical_bytes / parquet_bytes, 6),
        "rows_per_second_export": round(source_rows / export_seconds, 3),
        "rows_per_second_verification": round(source_rows / verification_seconds, 3),
        "export_seconds": round(export_seconds, 6),
        "verification_seconds": round(verification_seconds, 6),
        "duckdb_query_ms": round(duckdb_query_ms, 3),
        "max_rss_bytes_after": _max_rss_bytes(),
        "parquet_file": str(target),
        "parquet_sha256": _sha256_file(target),
        "disk_free_before": free,
        "disk_safety_required": required,
        "source_modified": False,
    }
    manifest = target.with_suffix(".benchmark.json")
    manifest.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def _schema() -> pa.Schema:
    fields: list[pa.Field] = []
    for name in _COLUMN_NAMES:
        column = Observation.__table__.columns[name]
        if isinstance(column.type, Uuid):
            arrow_type: pa.DataType = pa.string()
        elif isinstance(column.type, DateTime):
            arrow_type = pa.timestamp("us", tz="UTC")
        elif isinstance(column.type, Numeric):
            arrow_type = pa.decimal128(column.type.precision or 38, column.type.scale or 0)
        elif isinstance(column.type, BigInteger):
            arrow_type = pa.int64()
        else:
            arrow_type = pa.string()
        fields.append(pa.field(name, arrow_type, nullable=column.nullable))
    return pa.schema(fields)


def _normalize(row: dict[str, object]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name in _COLUMN_NAMES:
        value = row[name]
        if isinstance(value, uuid.UUID):
            result[name] = str(value)
        elif isinstance(value, datetime):
            result[name] = _utc(value)
        elif isinstance(value, Decimal):
            result[name] = value
        else:
            result[name] = value
    return result


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("benchmark timestamps must be timezone-aware")
    return value.astimezone(UTC)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _max_rss_bytes() -> int:
    value = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
    # macOS reports bytes; Linux reports KiB.
    return value if __import__("sys").platform == "darwin" else value * 1024
