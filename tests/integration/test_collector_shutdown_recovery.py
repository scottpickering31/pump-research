from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.recovery import reconcile_stale_collector_run
from pump_research.collection.runtime import (
    CollectorAlreadyRunningError,
    acquire_collector_process_lock,
    release_collector_process_lock,
)
from pump_research.config import Settings
from pump_research.epochs import CollectionEpochError, close_epoch, create_epoch, start_epoch
from pump_research.monitoring.status import read_collector_status
from pump_research.persistence.models import (
    CollectorComponentHealth,
    CollectorRun,
    CollectorRunEvent,
)
from pump_research.persistence.repositories import CollectorRunRepository

NOW = datetime(2026, 8, 17, 12, tzinfo=UTC)


def _settings(session_factory: async_sessionmaker[AsyncSession]) -> Settings:
    engine = session_factory.kw["bind"]
    assert engine is not None
    return Settings(
        environment="test", database_url=engine.url.render_as_string(hide_password=False)
    )


async def _seed_stale_epoch2(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> CollectorRun:
    async with session_factory() as session, session.begin():
        epoch = await create_epoch(
            session,
            settings,
            epoch_number=2,
            purpose="isolated stale shutdown recovery test",
            now=NOW - timedelta(hours=8),
        )
        await start_epoch(session, epoch_number=2, now=NOW - timedelta(hours=8))
        run = await CollectorRunRepository().start(
            session,
            started_at=NOW - timedelta(hours=8),
            collector_version="test",
            configuration_sha256="d" * 64,
            configuration_snapshot={},
            collection_epoch_id=epoch.id,
        )
        run.last_heartbeat_at = NOW - timedelta(minutes=5)
        session.add(
            CollectorComponentHealth(
                component_name="scheduled_observation",
                collector_run_id=run.id,
                status="failed",
                last_attempt_at=NOW - timedelta(minutes=5),
                last_success_at=NOW - timedelta(minutes=5, seconds=1),
                detail={"error_type": "BrokenPipeError", "message": "[Errno 32] Broken pipe"},
                updated_at=NOW - timedelta(minutes=5),
            )
        )
        await session.flush()
        return run


@pytest.mark.integration
async def test_stale_reconciliation_requires_free_lock_and_allows_explicit_epoch_close(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = _settings(session_factory)
    run = await _seed_stale_epoch2(session_factory, settings)

    async with session_factory() as session, session.begin():
        with pytest.raises(CollectionEpochError, match="stop the running collector"):
            await close_epoch(
                session,
                epoch_number=2,
                status="completed",
                reason="must not close while durable run is active",
                now=NOW,
            )

    held_lock = await acquire_collector_process_lock(session_factory)
    try:
        with pytest.raises(CollectorAlreadyRunningError):
            await reconcile_stale_collector_run(
                session_factory,
                settings,
                epoch_number=2,
                reason="operator requested Ctrl+C",
                now=NOW,
            )
    finally:
        await release_collector_process_lock(held_lock)

    result = await reconcile_stale_collector_run(
        session_factory,
        settings,
        epoch_number=2,
        reason="operator requested Ctrl+C after verified final backup",
        now=NOW,
    )
    assert result.collector_run_id == run.id
    assert result.status == "stopped"
    assert result.already_reconciled is False

    repeated = await reconcile_stale_collector_run(
        session_factory,
        settings,
        epoch_number=2,
        reason="the repeat cannot create another event",
        now=NOW + timedelta(seconds=1),
    )
    assert repeated.already_reconciled is True

    status = await read_collector_status(session_factory, settings)
    assert status["operational_state"] == "STOPPED"
    assert status["run_lifecycle"] == "GRACEFULLY_STOPPED"
    assert status["actively_collecting"] is False
    assert status["collector_run"]["finished_at"] == NOW.isoformat()
    assert status["collector_run"]["last_heartbeat_at"] == (
        NOW - timedelta(minutes=5)
    ).isoformat()
    assert status["components"]["scheduled_observation"]["status"] == "stopped"

    async with session_factory() as session:
        event_count = await session.scalar(
            select(func.count())
            .select_from(CollectorRunEvent)
            .where(CollectorRunEvent.collector_run_id == run.id)
        )
        terminal_event = await session.scalar(
            select(CollectorRunEvent).where(CollectorRunEvent.collector_run_id == run.id)
        )
    assert event_count == 1
    assert terminal_event is not None
    assert terminal_event.event_type == "stale_reconciled"
    previous_health = terminal_event.detail["previous_component_health"]
    assert isinstance(previous_health, dict)
    scheduled_health = previous_health["scheduled_observation"]
    assert isinstance(scheduled_health, dict)
    assert scheduled_health["status"] == "failed"

    async with session_factory() as session, session.begin():
        closed = await close_epoch(
            session,
            epoch_number=2,
            status="completed",
            reason="successful isolated validation",
            now=NOW + timedelta(minutes=1),
        )
    assert closed.status == "completed"
    assert closed.data_valid is True


@pytest.mark.integration
async def test_normal_graceful_stop_allows_epoch_close_but_stale_run_is_distinct(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = _settings(session_factory)
    live_now = datetime.now(UTC)
    async with session_factory() as session, session.begin():
        epoch = await create_epoch(
            session,
            settings,
            epoch_number=2,
            purpose="graceful terminal status test",
            now=live_now - timedelta(minutes=2),
        )
        await start_epoch(session, epoch_number=2, now=live_now - timedelta(minutes=2))
        run = await CollectorRunRepository().start(
            session,
            started_at=live_now - timedelta(minutes=2),
            collector_version="test",
            configuration_sha256="e" * 64,
            configuration_snapshot={},
            collection_epoch_id=epoch.id,
        )
        run.last_heartbeat_at = live_now - timedelta(seconds=5)

    healthy = await read_collector_status(session_factory, settings)
    assert healthy["operational_state"] == "HEALTHY"
    assert healthy["run_lifecycle"] == "HEALTHY_RUNNING"

    async with session_factory() as session, session.begin():
        await CollectorRunRepository().finish(
            session,
            run_id=run.id,
            finished_at=live_now,
            status="stopped",
            failure_detail={
                "reason": "operator_requested_shutdown",
                "signal": "SIGTERM",
            },
        )
        closed = await close_epoch(
            session,
            epoch_number=2,
            status="completed",
            reason="graceful run completed",
            now=live_now + timedelta(seconds=1),
        )
    assert closed.status == "completed"
