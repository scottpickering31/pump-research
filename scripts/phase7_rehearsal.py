"""Run the write-side Phase 7 scheduler rehearsal on an isolated test database.

This utility deliberately refuses any connected database whose server-reported
name is not an approved test/rehearsal name.  It never opens provider clients.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from datetime import UTC, datetime, timedelta

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from pump_research.collection.runtime import CollectorRuntime
from pump_research.config import get_settings
from pump_research.database import create_database_engine
from pump_research.database_safety import inspect_engine_database_safety
from pump_research.persistence.models import (
    CollectionEpoch,
    CollectionEpochCurrent,
    CollectorRun,
    CoverageDecision,
    LifecycleEvent,
    Observation,
    PollSchedule,
)
from pump_research.scheduling.scheduler import AdaptiveScheduler


async def _run(epoch_number: int) -> dict[str, object]:
    settings = get_settings()
    engine = create_database_engine(settings)
    try:
        safety = await inspect_engine_database_safety(
            engine,
            environment=settings.environment,
            explicit_test_database_url=bool(os.environ.get("PUMP_RESEARCH_TEST_DATABASE_URL")),
        )
        if not safety.destructive_test_operations_permitted:
            raise RuntimeError(
                "CRITICAL DATABASE SAFETY ABORT: Phase 7 rehearsal requires an explicit "
                f"test URL and test environment; connected to {safety.database!r}: "
                f"{safety.reason}"
            )

        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            epoch = await session.scalar(
                select(CollectionEpoch).where(CollectionEpoch.epoch_number == epoch_number)
            )
            if epoch is None:
                raise RuntimeError(f"rehearsal epoch {epoch_number} does not exist")
            before = {
                "observations": int(
                    await session.scalar(select(func.count()).select_from(Observation)) or 0
                ),
                "lifecycle_events": int(
                    await session.scalar(select(func.count()).select_from(LifecycleEvent)) or 0
                ),
                "poll_schedules": int(
                    await session.scalar(select(func.count()).select_from(PollSchedule)) or 0
                ),
                "unmapped_schedules": int(
                    await session.scalar(
                        select(func.count())
                        .select_from(PollSchedule)
                        .where(PollSchedule.coverage_class.is_(None))
                    )
                    or 0
                ),
                "coverage_decisions": int(
                    await session.scalar(select(func.count()).select_from(CoverageDecision)) or 0
                ),
            }

        runtime = CollectorRuntime(
            factory,
            settings,
            logger=structlog.get_logger("phase7.rehearsal"),
            epoch_number=epoch_number,
            epoch_initializer=AdaptiveScheduler(factory, settings),
        )
        runtime.request_shutdown("phase7_rehearsal_requested")
        started = datetime.now(UTC)
        startup = await runtime.run_until_stopped()
        finished = datetime.now(UTC)

        async with factory() as session:
            current = await session.get(CollectionEpochCurrent, startup.collection_epoch_id)
            class_rows = (
                await session.execute(
                    select(PollSchedule.coverage_class, func.count())
                    .group_by(PollSchedule.coverage_class)
                    .order_by(PollSchedule.coverage_class)
                )
            ).all()
            due_first_minute = int(
                await session.scalar(
                    select(func.count())
                    .select_from(PollSchedule)
                    .where(PollSchedule.next_due_at <= started + timedelta(minutes=1))
                )
                or 0
            )
            leased = int(
                await session.scalar(
                    select(func.count())
                    .select_from(PollSchedule)
                    .where(PollSchedule.lease_id.is_not(None))
                )
                or 0
            )
            terminal_run = await session.get(CollectorRun, startup.run_id)
            after = {
                "observations": int(
                    await session.scalar(select(func.count()).select_from(Observation)) or 0
                ),
                "lifecycle_events": int(
                    await session.scalar(select(func.count()).select_from(LifecycleEvent)) or 0
                ),
                "poll_schedules": int(
                    await session.scalar(select(func.count()).select_from(PollSchedule)) or 0
                ),
                "unmapped_schedules": int(
                    await session.scalar(
                        select(func.count())
                        .select_from(PollSchedule)
                        .where(PollSchedule.coverage_class.is_(None))
                    )
                    or 0
                ),
                "coverage_decisions": int(
                    await session.scalar(select(func.count()).select_from(CoverageDecision)) or 0
                ),
            }
        assert current is not None
        assert terminal_run is not None
        if before["observations"] != after["observations"]:
            raise RuntimeError("reconstruction mutated observations")
        if before["lifecycle_events"] != after["lifecycle_events"]:
            raise RuntimeError("reconstruction mutated lifecycle history")
        if before["poll_schedules"] != after["poll_schedules"]:
            raise RuntimeError("reconstruction changed the schedule population")
        return {
            "connected_database": safety.database,
            "epoch_number": epoch_number,
            "epoch_status": current.status,
            "collector_run_status": terminal_run.status,
            "duration_seconds": round((finished - started).total_seconds(), 3),
            "before": before,
            "after": after,
            "coverage_counts": {
                str(coverage_class): int(count) for coverage_class, count in class_rows
            },
            "due_in_first_minute": due_first_minute,
            "leased_schedules": leased,
            "startup_state": {
                "tokens": startup.state.token_count,
                "poll_schedules": startup.state.poll_schedule_count,
                "abandoned_runs_recovered": startup.state.abandoned_runs_recovered,
            },
        }
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--epoch", type=int, required=True)
    args = parser.parse_args()
    print(json.dumps(asyncio.run(_run(args.epoch)), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
