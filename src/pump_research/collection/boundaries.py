"""Derived collector-run live-work intervals for reporting and research."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from pump_research.persistence.models import CollectorRun


class CollectionBoundaryUnknownError(RuntimeError):
    """A coverage calculation encountered a run with no durable live boundary."""


@dataclass(frozen=True, slots=True)
class CollectorRunBoundary:
    """Lifecycle and live-work bounds for one process invocation."""

    run_id: uuid.UUID
    started_at: datetime
    collection_started_at: datetime | None
    finished_at: datetime | None
    status: str


@dataclass(frozen=True, slots=True)
class LiveCollectionInterval:
    """A known run-level interval during which live worker execution was allowed."""

    run_id: uuid.UUID
    start: datetime
    end: datetime

    @property
    def duration_seconds(self) -> float:
        return (self.end - self.start).total_seconds()


async def load_run_boundaries(
    session: AsyncSession, *, collection_epoch_id: uuid.UUID | None = None
) -> tuple[CollectorRunBoundary, ...]:
    """Load run boundaries in invocation order without inferring missing values."""
    statement = select(CollectorRun).order_by(CollectorRun.started_at, CollectorRun.id)
    if collection_epoch_id is not None:
        statement = statement.where(CollectorRun.collection_epoch_id == collection_epoch_id)
    runs = (await session.execute(statement)).scalars()
    return tuple(
        CollectorRunBoundary(
            run_id=run.id,
            started_at=_utc(run.started_at),
            collection_started_at=(
                _utc(run.collection_started_at) if run.collection_started_at is not None else None
            ),
            finished_at=_utc(run.finished_at) if run.finished_at is not None else None,
            status=run.status,
        )
        for run in runs
    )


def require_known_collection_boundary(
    boundaries: tuple[CollectorRunBoundary, ...], *, context: str
) -> None:
    """Reject canonical coverage calculations with absent historical boundaries."""
    if not boundaries:
        raise CollectionBoundaryUnknownError(f"{context} has no collector runs")
    unknown = [
        str(boundary.run_id)
        for boundary in boundaries
        if boundary.collection_started_at is None
    ]
    if unknown:
        raise CollectionBoundaryUnknownError(
            f"{context} has unknown collection_started_at for collector run(s): "
            + ", ".join(unknown)
        )


def live_collection_intervals(
    boundaries: tuple[CollectorRunBoundary, ...],
    *,
    start: datetime,
    end: datetime,
) -> tuple[LiveCollectionInterval, ...]:
    """Clip known run intervals to a scope while retaining restart gaps."""
    normalized_start, normalized_end = _utc(start), _utc(end)
    if normalized_start >= normalized_end:
        raise ValueError("collection interval scope must have start < end")
    intervals: list[LiveCollectionInterval] = []
    for boundary in boundaries:
        if boundary.collection_started_at is None:
            continue
        interval_start = max(normalized_start, boundary.collection_started_at)
        interval_end = min(normalized_end, boundary.finished_at or normalized_end)
        if interval_end < boundary.collection_started_at:
            raise ValueError(
                f"collector run {boundary.run_id} finished before collection_started_at"
            )
        if interval_start < interval_end:
            intervals.append(LiveCollectionInterval(boundary.run_id, interval_start, interval_end))
    return tuple(intervals)


def live_seconds(intervals: tuple[LiveCollectionInterval, ...]) -> float:
    """Measure the union of live intervals without filling or double-counting gaps."""
    if not intervals:
        return 0.0
    ordered = sorted(intervals, key=lambda item: (item.start, item.end, item.run_id))
    total = 0.0
    current_start, current_end = ordered[0].start, ordered[0].end
    for interval in ordered[1:]:
        if interval.start <= current_end:
            current_end = max(current_end, interval.end)
            continue
        total += (current_end - current_start).total_seconds()
        current_start, current_end = interval.start, interval.end
    return total + (current_end - current_start).total_seconds()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("collector run boundaries must be timezone-aware")
    return value.astimezone(UTC)
