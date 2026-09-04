"""Database-backed, as-of 24-hour collection and data-quality reporting."""

from __future__ import annotations

import csv
import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.boundaries import (
    live_collection_intervals,
    live_seconds,
    load_run_boundaries,
    require_known_collection_boundary,
)
from pump_research.epochs import get_epoch_status
from pump_research.persistence.models import (
    ApiRequestLog,
    DeduplicationConflict,
    DiscoveryEvent,
    LifecycleEvent,
    Observation,
    PollBatch,
    PollBatchMember,
    PollBatchOutcome,
    PollScheduleDecision,
)

_HOURS = 24
_NULL_RATE_COLUMNS = (
    "price_usd",
    "liquidity_usd",
    "market_cap_usd",
    "volume_m5_usd",
    "volume_h1_usd",
    "volume_h6_usd",
    "volume_h24_usd",
)


class TwentyFourHourReport(dict[str, Any]):
    """JSON-serializable report whose time bounds are explicit UTC instants."""


def _hour_key(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _normalise_end_at(end_at: datetime | None) -> datetime:
    value = end_at or datetime.now(UTC)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("end_at must be timezone-aware")
    normalized = value.astimezone(UTC)
    if end_at is not None and (
        normalized.minute != 0 or normalized.second != 0 or normalized.microsecond != 0
    ):
        raise ValueError("end_at must be an exact UTC hour boundary")
    return normalized.replace(minute=0, second=0, microsecond=0)


async def generate_report(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    end_at: datetime | None = None,
    epoch_number: int | None = None,
    include_invalid: bool = False,
) -> TwentyFourHourReport:
    """Build a bounded report using only facts available no later than ``end_at``.

    The PostgreSQL database-size reading is necessarily a report-generation
    snapshot rather than a historical value. Mutable schedule projections are
    deliberately not used for historical metrics.
    """
    epoch_metadata: dict[str, object] | None = None
    epoch_id: uuid.UUID | None = None
    if epoch_number is None:
        report_end = _normalise_end_at(end_at)
        report_start = report_end - timedelta(hours=_HOURS)
    else:
        async with session_factory() as epoch_session:
            epoch = await get_epoch_status(epoch_session, epoch_number)
            epoch_run_boundaries = await load_run_boundaries(
                epoch_session, collection_epoch_id=epoch.id
            )
        if epoch.started_at is None:
            raise ValueError(f"epoch {epoch_number} has not started")
        if not epoch.data_valid and not include_invalid:
            raise ValueError(
                f"epoch {epoch_number} is invalid and excluded from research reports by default; "
                "set include_invalid=True for explicit engineering analysis"
            )
        require_known_collection_boundary(
            epoch_run_boundaries, context=f"epoch {epoch_number} research report"
        )
        report_start = min(
            boundary.collection_started_at
            for boundary in epoch_run_boundaries
            if boundary.collection_started_at is not None
        )
        requested_end = (
            _aware_utc(end_at, "end_at")
            if end_at is not None
            else epoch.ended_at or datetime.now(UTC)
        )
        report_end = min(requested_end, report_start + timedelta(hours=_HOURS))
        if report_end <= report_start:
            raise ValueError("epoch report end must be after its start")
        epoch_id = epoch.id
        epoch_metadata = epoch.as_dict()
    bucket_start = report_start.replace(minute=0, second=0, microsecond=0)
    hours: list[datetime] = []
    cursor = bucket_start
    while cursor < report_end:
        hours.append(cursor)
        cursor += timedelta(hours=1)
    rows: dict[datetime, dict[str, Any]] = {hour: _empty_hour(hour) for hour in hours}

    async with session_factory() as session:
        await _add_counts(
            session,
            rows,
            DiscoveryEvent.received_at,
            DiscoveryEvent,
            "tokens_discovered",
            report_start,
            report_end,
        )
        await _add_counts(
            session,
            rows,
            Observation.received_at,
            Observation,
            "observations",
            report_start,
            report_end,
        )
        await _add_counts(
            session,
            rows,
            ApiRequestLog.requested_at,
            ApiRequestLog,
            "requests",
            report_start,
            report_end,
        )
        await _add_counts(
            session,
            rows,
            LifecycleEvent.decided_at,
            LifecycleEvent,
            "state_transitions",
            report_start,
            report_end,
        )
        await _add_counts(
            session, rows, PollBatch.claimed_at, PollBatch, "poll_batches", report_start, report_end
        )
        await _add_counts(
            session,
            rows,
            PollBatchMember.claimed_at,
            PollBatchMember,
            "poll_members",
            report_start,
            report_end,
        )
        await _add_counts(
            session,
            rows,
            PollBatchOutcome.completed_at,
            PollBatchOutcome,
            "poll_outcomes",
            report_start,
            report_end,
        )
        await _add_counts(
            session,
            rows,
            PollScheduleDecision.decided_at,
            PollScheduleDecision,
            "schedule_decisions",
            report_start,
            report_end,
        )
        await _add_counts(
            session,
            rows,
            DeduplicationConflict.occurred_at,
            DeduplicationConflict,
            "duplicate_deliveries",
            report_start,
            report_end,
        )
        await _add_filtered_counts(
            session,
            rows,
            ApiRequestLog.requested_at,
            ApiRequestLog,
            "http_429s",
            report_start,
            report_end,
            ApiRequestLog.http_status_code == 429,
        )
        await _add_filtered_counts(
            session,
            rows,
            LifecycleEvent.decided_at,
            LifecycleEvent,
            "tokens_reaching_dex",
            report_start,
            report_end,
            LifecycleEvent.previous_state == "PENDING_DEX",
            LifecycleEvent.new_state == "NEW",
        )
        await _add_filtered_counts(
            session,
            rows,
            LifecycleEvent.decided_at,
            LifecycleEvent,
            "resurrections",
            report_start,
            report_end,
            LifecycleEvent.new_state == "RESURRECTED",
        )
        await _add_request_latency(session, rows, report_start, report_end)
        await _add_null_rates(session, rows, report_start, report_end)
        await _add_batch_metrics(session, rows, report_start, report_end)
        await _add_polling_cadence(session, rows, report_start, report_end)
        await _add_pending_counts(session, rows, report_start, report_end)
        largest_gaps = await _largest_poll_gaps(session, report_start, report_end)
        database_size_bytes = await session.scalar(
            text("SELECT pg_database_size(current_database())")
        )
        validation = await _validation_summary(
            session,
            epoch_id=epoch_id,
            start=report_start,
            end=report_end,
        )

    for row in rows.values():
        accepted = row["tokens_discovered"] + row["observations"] + row["requests"]
        row["duplicate_rate"] = _rate(
            row["duplicate_deliveries"], accepted + row["duplicate_deliveries"]
        )
        row["rows_written"] = (
            accepted
            + row["state_transitions"]
            + row["poll_batches"]
            + row["poll_members"]
            + row["poll_outcomes"]
            + row["schedule_decisions"]
            + row["duplicate_deliveries"]
        )

    return TwentyFourHourReport(
        {
            "schema_version": 1,
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "window": {"start": _hour_key(report_start), "end_exclusive": _hour_key(report_end)},
            "collection_epoch": epoch_metadata,
            "database_size_bytes_at_generation": int(database_size_bytes or 0),
            "definitions": {
                "tokens_reaching_dex": "PENDING_DEX to NEW lifecycle transitions",
                "pending_tokens": (
                    "latest PENDING_DEX/NEW lifecycle state is PENDING_DEX at the hour end"
                ),
                "batch_occupancy_pct": (
                    "poll-batch members divided by its captured batch-size configuration"
                ),
                "actual_polling_cadence_seconds": (
                    "mean interval between successive durable poll claims for a token"
                ),
                "expected_polling_cadence_seconds": (
                    "mean configured state interval captured with each claimed batch"
                ),
                "duplicate_rate": (
                    "recorded duplicates / accepted discovery, request, and observation "
                    "facts plus duplicates"
                ),
                "null_rates": (
                    "source-normalized observation fields that were null; null is not an error"
                ),
            },
            "hourly": [rows[hour] for hour in hours],
            "largest_poll_gaps": largest_gaps,
            "validation": validation,
        }
    )


def write_report_files(report: TwentyFourHourReport, output_directory: Path) -> tuple[Path, Path]:
    """Write stable JSON and Markdown report files after the report is fully built."""
    output_directory.mkdir(parents=True, exist_ok=True)
    json_path = output_directory / "24h_report.json"
    markdown_path = output_directory / "24h_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path.write_text(_render_markdown(report), encoding="utf-8")
    csv_path = output_directory / "24h_hourly.csv"
    hourly = report["hourly"]
    if hourly:
        fields = [field for field in hourly[0] if field != "null_rates"]
        with csv_path.open("w", encoding="utf-8", newline="") as file_handle:
            writer = csv.DictWriter(file_handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows({field: row[field] for field in fields} for row in hourly)
    return markdown_path, json_path


def _aware_utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _empty_hour(hour: datetime) -> dict[str, Any]:
    return {
        "hour_start": _hour_key(hour),
        "tokens_discovered": 0,
        "tokens_reaching_dex": 0,
        "pending_tokens": 0,
        "observations": 0,
        "requests": 0,
        "http_429s": 0,
        "state_transitions": 0,
        "resurrections": 0,
        "poll_batches": 0,
        "poll_members": 0,
        "poll_outcomes": 0,
        "schedule_decisions": 0,
        "duplicate_deliveries": 0,
        "rows_written": 0,
        "batch_occupancy_pct": None,
        "api_latency_mean_ms": None,
        "api_latency_p50_ms": None,
        "api_latency_p95_ms": None,
        "actual_polling_cadence_seconds": None,
        "expected_polling_cadence_seconds": None,
        "duplicate_rate": None,
        "null_rates": {field: None for field in _NULL_RATE_COLUMNS},
    }


async def _add_counts(
    session: AsyncSession,
    rows: dict[datetime, dict[str, Any]],
    column: Any,
    model: Any,
    metric: str,
    start: datetime,
    end: datetime,
) -> None:
    hour = func.date_trunc("hour", column).label("hour")
    result = await session.execute(
        select(hour, func.count())
        .select_from(model)
        .where(column >= start, column < end)
        .group_by(hour)
    )
    for key, value in result:
        rows[key][metric] = int(value)


async def _add_filtered_counts(
    session: AsyncSession,
    rows: dict[datetime, dict[str, Any]],
    column: Any,
    model: Any,
    metric: str,
    start: datetime,
    end: datetime,
    *conditions: Any,
) -> None:
    hour = func.date_trunc("hour", column).label("hour")
    result = await session.execute(
        select(hour, func.count())
        .select_from(model)
        .where(column >= start, column < end, *conditions)
        .group_by(hour)
    )
    for key, value in result:
        rows[key][metric] = int(value)


async def _add_request_latency(
    session: AsyncSession, rows: dict[datetime, dict[str, Any]], start: datetime, end: datetime
) -> None:
    hour = func.date_trunc("hour", ApiRequestLog.requested_at).label("hour")
    latency_value = (
        func.extract("epoch", ApiRequestLog.received_at - ApiRequestLog.requested_at) * 1000
    )
    latency = func.avg(latency_value).label("latency")
    p50 = func.percentile_cont(0.5).within_group(latency_value).label("p50")
    p95 = func.percentile_cont(0.95).within_group(latency_value).label("p95")
    result = await session.execute(
        select(hour, latency)
        .add_columns(p50, p95)
        .where(
            ApiRequestLog.requested_at >= start,
            ApiRequestLog.requested_at < end,
            ApiRequestLog.received_at.is_not(None),
        )
        .group_by(hour)
    )
    for key, value, p50_value, p95_value in result:
        rows[key]["api_latency_mean_ms"] = round(float(value), 3)
        rows[key]["api_latency_p50_ms"] = round(float(p50_value), 3)
        rows[key]["api_latency_p95_ms"] = round(float(p95_value), 3)


async def _add_null_rates(
    session: AsyncSession, rows: dict[datetime, dict[str, Any]], start: datetime, end: datetime
) -> None:
    hour = func.date_trunc("hour", Observation.received_at).label("hour")
    null_counts = [
        func.sum(case((getattr(Observation, field).is_(None), 1), else_=0)).label(field)
        for field in _NULL_RATE_COLUMNS
    ]
    result = await session.execute(
        select(hour, func.count().label("total"), *null_counts)
        .where(Observation.received_at >= start, Observation.received_at < end)
        .group_by(hour)
    )
    for result_row in result.mappings():
        total = int(result_row["total"])
        rows[result_row["hour"]]["null_rates"] = {
            field: _rate(int(result_row[field]), total) for field in _NULL_RATE_COLUMNS
        }


async def _add_batch_metrics(
    session: AsyncSession, rows: dict[datetime, dict[str, Any]], start: datetime, end: datetime
) -> None:
    result = await session.execute(
        text("""
        WITH batches AS (
          SELECT pb.id AS batch_id, pb.claimed_at, count(pbm.token_id) AS members,
                 NULLIF((pb.configuration_snapshot ->> 'batch_size')::numeric, 0) AS capacity
          FROM poll_batches pb
          JOIN poll_batch_members pbm ON pbm.batch_id = pb.id AND pbm.claimed_at = pb.claimed_at
          WHERE pb.claimed_at >= :start AND pb.claimed_at < :end
          GROUP BY pb.id, pb.claimed_at, pb.configuration_snapshot
        )
        SELECT date_trunc('hour', b.claimed_at) AS hour,
               avg(100.0 * members / capacity) AS occupancy,
               avg(
                 ((pb.configuration_snapshot -> 'interval_seconds')
                   ->> pbm.lifecycle_state)::numeric
               ) AS expected_cadence
        FROM batches b
        JOIN poll_batches pb ON pb.id = b.batch_id
        JOIN poll_batch_members pbm ON pbm.batch_id = b.batch_id AND pbm.claimed_at = b.claimed_at
        GROUP BY date_trunc('hour', b.claimed_at)
    """),
        {"start": start, "end": end},
    )
    for hour, occupancy, expected in result:
        rows[hour]["batch_occupancy_pct"] = (
            round(float(occupancy), 3) if occupancy is not None else None
        )
        rows[hour]["expected_polling_cadence_seconds"] = (
            round(float(expected), 3) if expected is not None else None
        )


async def _add_polling_cadence(
    session: AsyncSession, rows: dict[datetime, dict[str, Any]], start: datetime, end: datetime
) -> None:
    result = await session.execute(
        text("""
        WITH current_window AS (
          SELECT token_id, claimed_at FROM poll_batch_members
          WHERE claimed_at >= :start AND claimed_at < :end
        ), previous AS (
          SELECT DISTINCT ON (token_id) token_id, claimed_at FROM poll_batch_members
          WHERE claimed_at < :start AND token_id IN (SELECT DISTINCT token_id FROM current_window)
          ORDER BY token_id, claimed_at DESC
        ), ordered AS (
          SELECT token_id, claimed_at,
                 lag(claimed_at) OVER (PARTITION BY token_id ORDER BY claimed_at) AS previous_at
          FROM (SELECT * FROM current_window UNION ALL SELECT * FROM previous) candidates
        )
        SELECT date_trunc('hour', claimed_at) AS hour,
               avg(extract(epoch FROM claimed_at - previous_at)) AS cadence
        FROM ordered
        WHERE claimed_at >= :start AND previous_at IS NOT NULL
        GROUP BY date_trunc('hour', claimed_at)
    """),
        {"start": start, "end": end},
    )
    for hour, cadence in result:
        rows[hour]["actual_polling_cadence_seconds"] = round(float(cadence), 3)


async def _add_pending_counts(
    session: AsyncSession, rows: dict[datetime, dict[str, Any]], start: datetime, end: datetime
) -> None:
    events = await session.execute(
        select(LifecycleEvent.token_id, LifecycleEvent.new_state, LifecycleEvent.decided_at)
        .where(
            LifecycleEvent.decided_at < end, LifecycleEvent.new_state.in_(("PENDING_DEX", "NEW"))
        )
        .order_by(LifecycleEvent.decided_at, LifecycleEvent.id)
    )
    state_by_token: dict[Any, str] = {}
    event_rows = list(events)
    index = 0
    for hour in rows:
        boundary = hour + timedelta(hours=1)
        while index < len(event_rows) and event_rows[index].decided_at < boundary:
            event = event_rows[index]
            state_by_token[event.token_id] = event.new_state
            index += 1
        rows[hour]["pending_tokens"] = sum(
            state == "PENDING_DEX" for state in state_by_token.values()
        )


async def _largest_poll_gaps(
    session: AsyncSession, start: datetime, end: datetime
) -> list[dict[str, Any]]:
    result = await session.execute(
        text("""
        WITH current_window AS (
          SELECT token_id, claimed_at FROM poll_batch_members
          WHERE claimed_at >= :start AND claimed_at < :end
        ), previous AS (
          SELECT DISTINCT ON (token_id) token_id, claimed_at FROM poll_batch_members
          WHERE claimed_at < :start AND token_id IN (SELECT DISTINCT token_id FROM current_window)
          ORDER BY token_id, claimed_at DESC
        ), ordered AS (
          SELECT token_id, claimed_at,
                 lag(claimed_at) OVER (PARTITION BY token_id ORDER BY claimed_at) AS previous_at
          FROM (SELECT * FROM current_window UNION ALL SELECT * FROM previous) candidates
        )
        SELECT t.chain, t.address, previous_at, claimed_at,
               extract(epoch FROM claimed_at - previous_at) AS gap_seconds
        FROM ordered JOIN tokens t ON t.id = ordered.token_id
        WHERE claimed_at >= :start AND previous_at IS NOT NULL
        ORDER BY gap_seconds DESC, t.address
        LIMIT 10
    """),
        {"start": start, "end": end},
    )
    return [
        {
            "chain": chain,
            "address": address,
            "previous_claimed_at": _hour_key(previous_at),
            "claimed_at": _hour_key(claimed_at),
            "gap_seconds": round(float(gap), 3),
        }
        for chain, address, previous_at, claimed_at, gap in result
    ]


async def _validation_summary(
    session: AsyncSession,
    *,
    epoch_id: uuid.UUID | None,
    start: datetime,
    end: datetime,
) -> dict[str, object]:
    """Compute the cross-cutting Epoch 1 validation aggregates."""
    parameters = {"epoch_id": epoch_id, "start": start, "end": end}
    run_filter = (
        "(CAST(:epoch_id AS uuid) IS NULL OR cr.collection_epoch_id = CAST(:epoch_id AS uuid))"
    )
    collection = (
        (
            await session.execute(
                text(f"""
            SELECT count(DISTINCT cr.id) AS restart_count,
                   count(DISTINCT de.token_id) AS tokens_discovered,
                   count(DISTINCT CASE WHEN le.previous_state = 'PENDING_DEX'
                                        AND le.new_state = 'NEW' THEN le.token_id END)
                     AS tokens_reaching_dex,
                   count(DISTINCT o.pair_id) AS pairs_observed
            FROM collector_runs cr
            LEFT JOIN discovery_events de ON de.collector_run_id = cr.id
              AND de.received_at >= :start AND de.received_at < :end
            LEFT JOIN lifecycle_events le ON le.collector_run_id = cr.id
              AND le.decided_at >= :start AND le.decided_at < :end
            LEFT JOIN api_request_log ar ON ar.collector_run_id = cr.id
              AND ar.requested_at >= :start AND ar.requested_at < :end
            LEFT JOIN observations o ON o.api_request_log_id = ar.id
            WHERE {run_filter}
              AND cr.collection_started_at IS NOT NULL
              AND cr.collection_started_at < :end
              AND COALESCE(cr.finished_at, :end) >= :start
            """),
                parameters,
            )
        )
        .mappings()
        .one()
    )
    pending_dex = int(
        await session.scalar(
            text("SELECT count(*) FROM dex_availability_tasks WHERE state = 'PENDING_DEX'")
        )
        or 0
    )
    connectivity_gap_seconds = float(
        await session.scalar(
            text("""
            WITH gaps AS (
              SELECT gap_id,
                     min(observed_at) FILTER (WHERE event_type = 'disconnected') AS down_at,
                     max(observed_at) FILTER (WHERE event_type = 'reconnected') AS up_at
              FROM discovery_connectivity_events
              WHERE observed_at >= :start AND observed_at < :end
              GROUP BY gap_id
            )
            SELECT COALESCE(sum(extract(epoch FROM
              LEAST(COALESCE(up_at, :end), :end) - GREATEST(down_at, :start)
            )), 0) FROM gaps WHERE down_at IS NOT NULL
            """),
            parameters,
        )
        or 0
    )
    capacity = (
        (
            await session.execute(
                text("""
            SELECT capacity_mode, count(*) AS samples,
                   avg((decision_snapshot ->> 'requested_token_observations_per_minute')::numeric)
                     AS requested_observations_per_minute,
                   avg((decision_snapshot ->> 'available_token_observations_per_minute')::numeric)
                     AS available_observations_per_minute,
                   avg((decision_snapshot ->> 'effective_token_observations_per_minute')::numeric)
                     AS adapted_observations_per_minute,
                   avg((decision_snapshot ->> 'effective_requests_per_minute')::numeric)
                     AS adapted_requests_per_minute,
                   avg((decision_snapshot ->> 'degraded_schedule_pct')::numeric)
                     AS degraded_schedule_pct
            FROM scheduler_capacity_decisions
            WHERE decided_at >= :start AND decided_at < :end
            GROUP BY capacity_mode ORDER BY capacity_mode
            """),
                parameters,
            )
        )
        .mappings()
        .all()
    )
    lateness = (
        (
            await session.execute(
                text("""
            SELECT pm.lifecycle_state, count(*) AS observations,
                   percentile_cont(0.50) WITHIN GROUP (ORDER BY pm.claim_lateness_ms) AS p50_ms,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY pm.claim_lateness_ms) AS p95_ms,
                   percentile_cont(0.99) WITHIN GROUP (ORDER BY pm.claim_lateness_ms) AS p99_ms,
                   max(pm.claim_lateness_ms) AS max_ms,
                   avg(pm.target_interval_seconds) AS target_interval_seconds,
                   avg(pm.effective_interval_seconds) AS effective_interval_seconds,
                   100.0 * count(*) FILTER (
                     WHERE pm.effective_interval_seconds > pm.target_interval_seconds
                   ) / NULLIF(count(*), 0) AS degraded_pct
            FROM poll_batch_members pm
            JOIN poll_batches pb ON pb.id = pm.batch_id
            JOIN collector_runs cr ON cr.id = pb.collector_run_id
            WHERE pm.claimed_at >= :start AND pm.claimed_at < :end
              AND (CAST(:epoch_id AS uuid) IS NULL
                   OR cr.collection_epoch_id = CAST(:epoch_id AS uuid))
            GROUP BY pm.lifecycle_state ORDER BY pm.lifecycle_state
            """),
                parameters,
            )
        )
        .mappings()
        .all()
    )
    scheduler_operations = (
        (
            await session.execute(
                text("""
                WITH batches AS (
                  SELECT pb.id, count(pm.token_id)::numeric AS members,
                         NULLIF((pb.configuration_snapshot ->> 'batch_size')::numeric, 0)
                           AS capacity
                  FROM poll_batches pb
                  JOIN collector_runs cr ON cr.id = pb.collector_run_id
                  JOIN poll_batch_members pm ON pm.batch_id = pb.id
                  WHERE pb.claimed_at >= :start AND pb.claimed_at < :end
                    AND (CAST(:epoch_id AS uuid) IS NULL
                         OR cr.collection_epoch_id = CAST(:epoch_id AS uuid))
                  GROUP BY pb.id
                )
                SELECT count(*) AS poll_batches,
                       avg(100.0 * members / capacity) AS batch_occupancy_pct
                FROM batches
                """),
                parameters,
            )
        )
        .mappings()
        .one()
    )
    schedules_without_claim = int(
        await session.scalar(
            text("""
            SELECT count(*) FROM poll_schedules ps
            WHERE NOT EXISTS (
              SELECT 1 FROM poll_batch_members pm
              WHERE pm.token_id = ps.token_id
                AND pm.claimed_at >= :start AND pm.claimed_at < :end
            )
            """),
            parameters,
        )
        or 0
    )
    api = (
        (
            await session.execute(
                text("""
            SELECT count(*) AS total_calls,
                   count(*) FILTER (WHERE ar.http_status_code = 200) AS http_200,
                   count(*) FILTER (WHERE ar.http_status_code = 429) AS http_429,
                   count(*) FILTER (WHERE ar.http_status_code BETWEEN 400 AND 499
                                    AND ar.http_status_code <> 429) AS http_other_4xx,
                   count(*) FILTER (WHERE ar.http_status_code >= 500) AS http_5xx,
                   sum(GREATEST(
                     COALESCE((ar.request_payload ->> 'provider_attempt_count')::integer, 1) - 1,
                     0
                   )) AS retry_attempts,
                   max((cr.configuration_snapshot -> 'settings'
                        ->> 'dex_screener_requests_per_minute')::integer)
                     AS configured_request_ceiling_per_minute,
                   percentile_cont(0.50) WITHIN GROUP (
                     ORDER BY extract(epoch FROM ar.received_at - ar.requested_at) * 1000
                   ) FILTER (WHERE ar.received_at IS NOT NULL) AS latency_p50_ms,
                   percentile_cont(0.95) WITHIN GROUP (
                     ORDER BY extract(epoch FROM ar.received_at - ar.requested_at) * 1000
                   ) FILTER (WHERE ar.received_at IS NOT NULL) AS latency_p95_ms,
                   percentile_cont(0.99) WITHIN GROUP (
                     ORDER BY extract(epoch FROM ar.received_at - ar.requested_at) * 1000
                   ) FILTER (WHERE ar.received_at IS NOT NULL) AS latency_p99_ms
            FROM api_request_log ar
            JOIN collector_runs cr ON cr.id = ar.collector_run_id
            WHERE ar.requested_at >= :start AND ar.requested_at < :end
              AND (CAST(:epoch_id AS uuid) IS NULL
                   OR cr.collection_epoch_id = CAST(:epoch_id AS uuid))
            """),
                parameters,
            )
        )
        .mappings()
        .one()
    )
    peak_requests_per_minute = int(
        await session.scalar(
            text("""
            SELECT COALESCE(max(requests), 0) FROM (
              SELECT date_trunc('minute', ar.requested_at), count(*) AS requests
              FROM api_request_log ar
              JOIN collector_runs cr ON cr.id = ar.collector_run_id
              WHERE ar.requested_at >= :start AND ar.requested_at < :end
                AND (CAST(:epoch_id AS uuid) IS NULL
                     OR cr.collection_epoch_id = CAST(:epoch_id AS uuid))
              GROUP BY date_trunc('minute', ar.requested_at)
            ) minute_counts
            """),
            parameters,
        )
        or 0
    )
    outcomes = (
        await session.execute(
            text("""
            SELECT ar.outcome, count(*) AS count
            FROM api_request_log ar JOIN collector_runs cr ON cr.id = ar.collector_run_id
            WHERE ar.requested_at >= :start AND ar.requested_at < :end
              AND (CAST(:epoch_id AS uuid) IS NULL
                   OR cr.collection_epoch_id = CAST(:epoch_id AS uuid))
            GROUP BY ar.outcome ORDER BY ar.outcome
            """),
            parameters,
        )
    ).all()
    lifecycle = (
        await session.execute(
            text("""
            SELECT previous_state, new_state, count(*) AS count
            FROM lifecycle_events le JOIN collector_runs cr ON cr.id = le.collector_run_id
            WHERE le.decided_at >= :start AND le.decided_at < :end
              AND (CAST(:epoch_id AS uuid) IS NULL
                   OR cr.collection_epoch_id = CAST(:epoch_id AS uuid))
            GROUP BY previous_state, new_state ORDER BY previous_state, new_state
            """),
            parameters,
        )
    ).all()
    lifecycle_residence = (
        await session.execute(
            text("""
            WITH ordered AS (
              SELECT le.new_state, le.decided_at,
                     lead(le.decided_at) OVER (
                       PARTITION BY le.token_id ORDER BY le.decided_at, le.id
                     ) AS next_at
              FROM lifecycle_events le
              JOIN collector_runs cr ON cr.id = le.collector_run_id
              WHERE le.decided_at >= :start AND le.decided_at < :end
                AND (CAST(:epoch_id AS uuid) IS NULL
                     OR cr.collection_epoch_id = CAST(:epoch_id AS uuid))
            )
            SELECT new_state,
                   percentile_cont(0.50) WITHIN GROUP (
                     ORDER BY extract(epoch FROM next_at - decided_at)
                   ) AS median_seconds
            FROM ordered WHERE next_at IS NOT NULL
            GROUP BY new_state ORDER BY new_state
            """),
            parameters,
        )
    ).all()
    dataset = (
        (
            await session.execute(
                text("""
            WITH epoch_observations AS (
              SELECT o.*, p.token_id
              FROM observations o JOIN pairs p ON p.id = o.pair_id
              JOIN api_request_log ar ON ar.id = o.api_request_log_id
              JOIN collector_runs cr ON cr.id = ar.collector_run_id
              WHERE o.received_at >= :start AND o.received_at < :end
                AND (CAST(:epoch_id AS uuid) IS NULL
                     OR cr.collection_epoch_id = CAST(:epoch_id AS uuid))
            ), per_token AS (
              SELECT token_id, count(*) AS observations, count(DISTINCT pair_id) AS pairs
              FROM epoch_observations GROUP BY token_id
            )
            SELECT (SELECT count(*) FROM epoch_observations) AS observation_rows,
                   count(*) AS unique_observed_tokens,
                   percentile_cont(0.50) WITHIN GROUP (ORDER BY observations)
                     AS observations_per_token_p50,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY observations)
                     AS observations_per_token_p95,
                   count(*) FILTER (WHERE pairs > 1) AS multi_pair_tokens
            FROM per_token
            """),
                parameters,
            )
        )
        .mappings()
        .one()
    )
    storage = (
        (
            await session.execute(
                text("""
            SELECT min(database_bytes) AS first_database_bytes,
                   max(database_bytes) AS last_database_bytes,
                   max(database_bytes) - min(database_bytes) AS growth_bytes,
                   min(sampled_at) AS first_sample_at,
                   max(sampled_at) AS last_sample_at
            FROM storage_samples
            WHERE sampled_at >= :start AND sampled_at < :end
              AND (CAST(:epoch_id AS uuid) IS NULL
                   OR collection_epoch_id = CAST(:epoch_id AS uuid))
            """),
                parameters,
            )
        )
        .mappings()
        .one()
    )
    storage_contributors = (
        await session.execute(
            text("""
            WITH bounds AS (
              SELECT (array_agg(id ORDER BY sampled_at))[1] AS first_id,
                     (array_agg(id ORDER BY sampled_at DESC))[1] AS last_id
              FROM storage_samples
              WHERE sampled_at >= :start AND sampled_at < :end
                AND (CAST(:epoch_id AS uuid) IS NULL
                     OR collection_epoch_id = CAST(:epoch_id AS uuid))
            )
            SELECT latest.relation_family,
                   sum(latest.total_bytes - earlier.total_bytes)::bigint AS growth_bytes
            FROM bounds
            JOIN storage_relation_samples latest ON latest.storage_sample_id = bounds.last_id
            JOIN storage_relation_samples earlier
              ON earlier.storage_sample_id = bounds.first_id
             AND earlier.relation_name = latest.relation_name
            GROUP BY latest.relation_family ORDER BY growth_bytes DESC
            """),
            parameters,
        )
    ).all()
    backup = (
        (
            await session.execute(
                text("""
                SELECT count(*) AS verified_artifacts,
                       count(*) FILTER (WHERE independent_copy) AS independent_copies,
                       max(verified_at) AS latest_verified_at
                FROM backup_verifications
                WHERE CAST(:epoch_id AS uuid) IS NOT NULL
                  AND collection_epoch_id = CAST(:epoch_id AS uuid)
                """),
                parameters,
            )
        )
        .mappings()
        .one()
    )
    all_boundaries = await load_run_boundaries(session, collection_epoch_id=epoch_id)
    scoped_boundaries = tuple(
        boundary
        for boundary in all_boundaries
        if boundary.started_at < end
        and (boundary.finished_at or end) >= start
    )
    unknown_boundary_run_ids = tuple(
        boundary.run_id
        for boundary in scoped_boundaries
        if boundary.collection_started_at is None
    )
    intervals = live_collection_intervals(scoped_boundaries, start=start, end=end)
    collection_boundary_known = bool(scoped_boundaries) and not unknown_boundary_run_ids
    live_duration_seconds = live_seconds(intervals) if collection_boundary_known else None
    scope_duration_seconds = max(1.0, (end - start).total_seconds())
    total_calls = int(api["total_calls"] or 0)
    growth = int(storage["growth_bytes"] or 0)
    gib = 1024**3
    return {
        "collection": {
            **_json_mapping(collection),
            "start": start.isoformat(),
            "end_exclusive": end.isoformat(),
            "uptime_window_seconds": live_duration_seconds,
            "scope_window_seconds": scope_duration_seconds,
            "collection_start_boundary_status": (
                "known" if collection_boundary_known else "unknown"
            ),
            "unknown_collection_start_run_ids": [
                str(run_id) for run_id in unknown_boundary_run_ids
            ],
            "live_run_intervals": [
                {
                    "collector_run_id": str(interval.run_id),
                    "start": interval.start.isoformat(),
                    "end_exclusive": interval.end.isoformat(),
                    "duration_seconds": interval.duration_seconds,
                }
                for interval in intervals
            ],
            "non_live_gap_seconds": (
                None
                if live_duration_seconds is None
                else max(0.0, scope_duration_seconds - live_duration_seconds)
            ),
            "pending_dex_at_report_generation": pending_dex,
            "discovery_connectivity_gap_seconds": connectivity_gap_seconds,
            "discovery_connectivity_uptime_pct": round(
                100.0
                * max(0.0, live_duration_seconds - connectivity_gap_seconds)
                / max(1.0, live_duration_seconds),
                6,
            )
            if live_duration_seconds is not None
            else None,
        },
        "scheduler": {
            "capacity_mode_over_time": [_json_mapping(row) for row in capacity],
            "cadence_and_lateness_by_state": [_json_mapping(row) for row in lateness],
            "operations": _json_mapping(scheduler_operations),
            "currently_scheduled_tokens_without_claim_in_window": schedules_without_claim,
            "starvation_check_definition": (
                "current schedules with no durable claim in this epoch window"
            ),
        },
        "api": {
            **_json_mapping(api),
            "outcomes": {str(name): int(count) for name, count in outcomes},
            "average_requests_per_minute": (
                round(total_calls / (live_duration_seconds / 60), 6)
                if live_duration_seconds
                else None
            ),
            "peak_requests_per_minute": peak_requests_per_minute,
            "peak_request_headroom": (
                None
                if api["configured_request_ceiling_per_minute"] is None
                else int(api["configured_request_ceiling_per_minute"]) - peak_requests_per_minute
            ),
        },
        "lifecycle": {
            "transitions": [
                {"from": previous, "to": new, "count": int(count)}
                for previous, new, count in lifecycle
            ],
            "median_residence_seconds_by_state": {
                str(state): float(seconds) for state, seconds in lifecycle_residence
            },
        },
        "dataset": _json_mapping(dataset),
        "storage": {
            **_json_mapping(storage),
            "actual_gib_per_day_extrapolation": (
                round(growth * 86_400 / live_duration_seconds / gib, 6)
                if live_duration_seconds
                else None
            ),
            "hot_postgres_projection_gib": {
                str(days): (
                    round(growth * 86_400 / live_duration_seconds * days / gib, 6)
                    if live_duration_seconds
                    else None
                )
                for days in (30, 90, 365)
            },
            "projection_label": "extrapolation from epoch storage samples",
            "growth_by_relation_family": [
                {"relation_family": family, "growth_bytes": int(bytes_ or 0)}
                for family, bytes_ in storage_contributors
            ],
        },
        "backup": _json_mapping(backup),
    }


def _json_mapping(mapping: Any) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in mapping.items():
        if isinstance(value, Decimal):
            result[str(key)] = float(value)
        elif isinstance(value, datetime):
            result[str(key)] = value.isoformat()
        else:
            result[str(key)] = value
    return result


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def _render_markdown(report: TwentyFourHourReport) -> str:
    lines = [
        "# Pump Research — 24-hour collection report",
        "",
        (
            f"Window: `{report['window']['start']}` to "
            f"`{report['window']['end_exclusive']}` (end exclusive)"
        ),
        f"Generated: `{report['generated_at']}`",
        f"Database size at generation: `{report['database_size_bytes_at_generation']}` bytes",
        "",
        "## Hourly metrics",
        "",
        (
            "| Hour (UTC) | Discovered | Reached DEX | Pending | Observations | Requests | "
            "429s | Occupancy | Actual cadence (s) | Expected cadence (s) | Rows/hour |"
        ),
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for hour in report["hourly"]:
        lines.append(
            "| {hour_start} | {tokens_discovered} | {tokens_reaching_dex} | {pending_tokens} | "
            "{observations} | {requests} | {http_429s} | {batch_occupancy_pct} | "
            "{actual_polling_cadence_seconds} | {expected_polling_cadence_seconds} | "
            "{rows_written} |".format(**hour)
        )
    lines.extend(
        [
            "",
            "## Hourly data quality",
            "",
            "| Hour (UTC) | API mean/p50/p95 (ms) | Transitions | Resurrections | "
            "Duplicate rate | Null rates |",
            "| --- | --- | ---: | ---: | ---: | --- |",
        ]
    )
    for hour in report["hourly"]:
        latency = "/".join(
            str(hour[field])
            for field in ("api_latency_mean_ms", "api_latency_p50_ms", "api_latency_p95_ms")
        )
        null_rates = (
            ", ".join(
                f"{field}={value}"
                for field, value in hour["null_rates"].items()
                if value is not None
            )
            or "n/a"
        )
        lines.append(
            "| {hour_start} | {latency} | {state_transitions} | {resurrections} | "
            "{duplicate_rate} | {null_rates_display} |".format_map(
                {**hour, "latency": latency, "null_rates_display": null_rates}
            )
        )
    lines.extend(["", "## Largest durable poll-claim gaps", ""])
    if report["largest_poll_gaps"]:
        lines.extend(
            ["| Token | Previous claim | Next claim | Gap (s) |", "| --- | --- | --- | ---: |"]
        )
        for gap in report["largest_poll_gaps"]:
            lines.append(
                f"| {gap['chain']}:{gap['address']} | {gap['previous_claimed_at']} | "
                f"{gap['claimed_at']} | {gap['gap_seconds']} |"
            )
    else:
        lines.append("No token had two durable poll claims in the reporting window.")
    lines.extend(["", "Metric definitions are included in `24h_report.json`.", ""])
    return "\n".join(lines)
