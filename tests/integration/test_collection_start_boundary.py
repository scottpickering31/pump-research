from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
import structlog
from sqlalchemy import func, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.boundaries import CollectionBoundaryUnknownError
from pump_research.collection.runtime import CollectorRuntime
from pump_research.config import Settings
from pump_research.epochs import close_epoch, create_epoch, get_epoch_status, start_epoch
from pump_research.monitoring.status import read_collector_status
from pump_research.persistence.models import CollectorRun
from pump_research.persistence.repositories import (
    CollectorRunNotRunningError,
    CollectorRunRepository,
)
from pump_research.reporting.twenty_four_hour import generate_report
from pump_research.research.sources import PostgresResearchSource, ResearchCoverageUnknownError

NOW = datetime(2026, 9, 4, 12, tzinfo=UTC)


def _settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://unused:unused@localhost/unused",
        environment="test",
        collector_shutdown_grace_seconds=1,
    )


async def _planned_epoch(
    session_factory: async_sessionmaker[AsyncSession], *, number: int = 13
) -> None:
    async with session_factory() as session, session.begin():
        await create_epoch(
            session,
            _settings(),
            epoch_number=number,
            purpose="live collection boundary regression",
            now=NOW,
        )


class BlockingInitializer:
    def __init__(self) -> None:
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.started_at: datetime | None = None
        self.completed_at: datetime | None = None

    async def initialize_epoch_in_session(
        self,
        session: AsyncSession,
        *,
        collection_epoch_id: object,
        epoch_number: int,
        started_at: datetime,
    ) -> int:
        del session, collection_epoch_id, epoch_number
        self.started_at = started_at
        self.entered.set()
        await self.release.wait()
        self.completed_at = datetime.now(UTC)
        return 0


class BoundaryReadingWorker:
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self.entered = asyncio.Event()
        self.visible_boundary: datetime | None = None

    async def run(self, *, run_id: object, shutdown: asyncio.Event) -> None:
        async with self._session_factory() as session:
            self.visible_boundary = await session.scalar(
                select(CollectorRun.collection_started_at).where(CollectorRun.id == run_id)
            )
        self.entered.set()
        await shutdown.wait()

    async def mark_stopped(self, run_id: object) -> None:
        del run_id


class FailingInitializer:
    async def initialize_epoch_in_session(self, *_: object, **__: object) -> int:
        raise RuntimeError("synthetic initializer failure")


@pytest.mark.integration
async def test_slow_initializer_precedes_committed_live_boundary_and_worker(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _planned_epoch(session_factory)
    initializer = BlockingInitializer()
    worker = BoundaryReadingWorker(session_factory)
    runtime = CollectorRuntime(
        session_factory,
        _settings(),
        logger=structlog.get_logger("test.collection-boundary"),
        epoch_number=13,
        worker=worker,
        epoch_initializer=initializer,
    )

    task = asyncio.create_task(runtime.run_until_stopped())
    await asyncio.wait_for(initializer.entered.wait(), timeout=2)
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(CollectorRun)) == 0
    initializer.release.set()
    await asyncio.wait_for(worker.entered.wait(), timeout=2)

    assert initializer.started_at is not None
    assert initializer.completed_at is not None
    assert worker.visible_boundary is not None
    async with session_factory() as session:
        run = await session.scalar(select(CollectorRun))
    assert run is not None
    assert run.started_at == initializer.started_at
    assert run.started_at <= initializer.completed_at <= worker.visible_boundary
    assert run.collection_started_at == worker.visible_boundary

    runtime.request_shutdown("boundary_order_verified")
    startup = await asyncio.wait_for(task, timeout=2)
    assert startup.collection_started_at == worker.visible_boundary


@pytest.mark.integration
async def test_failed_initializer_creates_no_run_or_live_boundary(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _planned_epoch(session_factory)
    runtime = CollectorRuntime(
        session_factory,
        _settings(),
        logger=structlog.get_logger("test.failed-initializer"),
        epoch_number=13,
        epoch_initializer=FailingInitializer(),
    )

    with pytest.raises(RuntimeError, match="synthetic initializer failure"):
        await runtime.start()

    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(CollectorRun)) == 0
        epoch = await get_epoch_status(session, 13)
    assert epoch.status == "planned"
    assert epoch.started_at is None


@pytest.mark.integration
async def test_collection_start_repository_is_one_way_and_idempotent(
    session: AsyncSession,
) -> None:
    repository = CollectorRunRepository()
    async with session.begin():
        run = await repository.start(
            session,
            started_at=NOW,
            collector_version="test",
            configuration_sha256="a" * 64,
            configuration_snapshot={},
        )
        first = await repository.mark_collection_started(
            session, run_id=run.id, collection_started_at=NOW + timedelta(seconds=10)
        )
        replay = await repository.mark_collection_started(
            session, run_id=run.id, collection_started_at=NOW + timedelta(seconds=20)
        )
    assert first == replay == NOW + timedelta(seconds=10)

    with pytest.raises(DBAPIError, match="one-way and immutable"):
        async with session.begin():
            await session.execute(
                update(CollectorRun)
                .where(CollectorRun.id == run.id)
                .values(collection_started_at=NOW + timedelta(seconds=30))
            )
    await session.rollback()

    async with session.begin():
        terminal = await repository.start(
            session,
            started_at=NOW,
            collector_version="test",
            configuration_sha256="b" * 64,
            configuration_snapshot={},
        )
        await repository.finish(
            session, run_id=terminal.id, finished_at=NOW + timedelta(seconds=1), status="failed"
        )
        with pytest.raises(CollectorRunNotRunningError):
            await repository.mark_collection_started(
                session,
                run_id=terminal.id,
                collection_started_at=NOW + timedelta(seconds=2),
            )


@pytest.mark.integration
async def test_status_reports_invocation_and_live_boundaries(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _planned_epoch(session_factory)
    repository = CollectorRunRepository()
    async with session_factory() as session, session.begin():
        epoch = await start_epoch(session, epoch_number=13, now=NOW)
        run = await repository.start(
            session,
            started_at=NOW + timedelta(seconds=1),
            collector_version="test",
            configuration_sha256="a" * 64,
            configuration_snapshot={},
            collection_epoch_id=epoch.id,
        )
        await repository.mark_collection_started(
            session,
            run_id=run.id,
            collection_started_at=NOW + timedelta(minutes=2),
        )

    status = await read_collector_status(session_factory, _settings())

    assert status["collector_run"]["started_at"] == (NOW + timedelta(seconds=1)).isoformat()
    assert status["collector_run"]["collection_started_at"] == (
        NOW + timedelta(minutes=2)
    ).isoformat()
    assert status["collector_run"]["collection_start_boundary_status"] == "known"
    assert status["collection_epoch"]["started_at"] == NOW.isoformat()
    assert status["collection_epoch"]["effective_collection_started_at"] == (
        NOW + timedelta(minutes=2)
    ).isoformat()


@pytest.mark.integration
async def test_epoch_report_uses_distinct_live_intervals_without_bridging_restart_gap(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _planned_epoch(session_factory)
    repository = CollectorRunRepository()
    async with session_factory() as session, session.begin():
        epoch = await start_epoch(session, epoch_number=13, now=NOW)
        first = await repository.start(
            session,
            started_at=NOW,
            collector_version="test",
            configuration_sha256="a" * 64,
            configuration_snapshot={},
            collection_epoch_id=epoch.id,
        )
        await repository.mark_collection_started(
            session, run_id=first.id, collection_started_at=NOW + timedelta(minutes=10)
        )
        await repository.finish(
            session,
            run_id=first.id,
            finished_at=NOW + timedelta(minutes=30),
            status="stopped",
        )
        second = await repository.start(
            session,
            started_at=NOW + timedelta(minutes=50),
            collector_version="test",
            configuration_sha256="b" * 64,
            configuration_snapshot={},
            collection_epoch_id=epoch.id,
        )
        await repository.mark_collection_started(
            session, run_id=second.id, collection_started_at=NOW + timedelta(minutes=60)
        )
        await repository.finish(
            session,
            run_id=second.id,
            finished_at=NOW + timedelta(minutes=90),
            status="stopped",
        )
        await close_epoch(
            session,
            epoch_number=13,
            status="completed",
            reason="two controlled runs",
            now=NOW + timedelta(hours=2),
        )

    report = await generate_report(
        session_factory, epoch_number=13, end_at=NOW + timedelta(hours=2)
    )

    collection = report["validation"]["collection"]
    assert report["window"]["start"] == "2026-09-04T12:10:00Z"
    assert collection["uptime_window_seconds"] == 50 * 60
    assert collection["non_live_gap_seconds"] == 60 * 60
    assert len(collection["live_run_intervals"]) == 2


@pytest.mark.integration
async def test_historical_null_boundary_fails_report_and_research_conservatively(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _planned_epoch(session_factory)
    async with session_factory() as session, session.begin():
        epoch = await start_epoch(session, epoch_number=13, now=NOW)
        run = await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="historical",
            configuration_sha256="a" * 64,
            configuration_snapshot={},
            collection_epoch_id=epoch.id,
        )
        await CollectorRunRepository().finish(
            session,
            run_id=run.id,
            finished_at=NOW + timedelta(hours=1),
            status="stopped",
        )
        await close_epoch(
            session,
            epoch_number=13,
            status="completed",
            reason="historical fixture",
            now=NOW + timedelta(hours=1),
        )

    with pytest.raises(CollectionBoundaryUnknownError, match="unknown collection_started_at"):
        await generate_report(
            session_factory, epoch_number=13, end_at=NOW + timedelta(hours=1)
        )
    with pytest.raises(ResearchCoverageUnknownError, match="unknown collection_started_at"):
        await PostgresResearchSource(session_factory).load_histories(epoch_number=13)
