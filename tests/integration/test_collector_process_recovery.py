from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.persistence.models import CollectorRun
from pump_research.persistence.repositories import (
    DexAvailabilityTaskRepository,
    DiscoveryCheckpointRepository,
    TokenRepository,
)
from pump_research.scheduling.policy import LifecycleState
from pump_research.scheduling.scheduler import AdaptiveScheduler

PROJECT_ROOT = Path(__file__).parents[2]


async def _start_collector_subprocess(
    *,
    database_url: str,
) -> tuple[asyncio.subprocess.Process, dict[str, Any]]:
    environment = {
        **os.environ,
        "PYTHONUNBUFFERED": "1",
        "PUMP_RESEARCH_DATABASE_URL": database_url,
        "PUMP_RESEARCH_LOG_JSON": "true",
        "PUMP_RESEARCH_LOG_LEVEL": "INFO",
    }
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-u",
        "-m",
        "pump_research",
        "collector",
        "run",
        cwd=PROJECT_ROOT,
        env=environment,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    assert process.stdout is not None
    try:
        async with asyncio.timeout(10):
            while line := await process.stdout.readline():
                try:
                    payload = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if payload.get("event") == "collector_started":
                    return process, payload
    except BaseException:
        process.kill()
        await process.wait()
        raise
    output = (await process.stdout.read()).decode(errors="replace")
    msg = f"Collector exited before startup evidence; returncode={process.returncode}: {output}"
    raise AssertionError(msg)


@pytest.mark.integration
async def test_physical_stop_restart_and_sigterm_reconstruct_postgres_state(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Hard-kill one real process, restart it, then stop replacement with SIGTERM."""
    engine = session_factory.kw["bind"]
    assert engine is not None
    database_url = engine.url.render_as_string(hide_password=False)
    settings = Settings(database_url=database_url)
    scheduler = AdaptiveScheduler(session_factory, settings)
    tokens = TokenRepository()
    pending_tasks = DexAvailabilityTaskRepository()
    checkpoints = DiscoveryCheckpointRepository()
    now = datetime.now(UTC)
    async with session_factory() as session, session.begin():
        pending_token = await tokens.get_or_create(
            session,
            chain="solana",
            address="restart-pending-token",
            first_discovered_at=now,
        )
        await pending_tasks.create_pending_if_absent(
            session,
            token_id=pending_token.id,
            due_at=now,
        )
        scheduled_token = await tokens.get_or_create(
            session,
            chain="solana",
            address="restart-scheduled-token",
            first_discovered_at=now,
        )
        await scheduler.set_lifecycle_state_in_session(
            session,
            token_id=scheduled_token.id,
            state=LifecycleState.NEW,
            decided_at=now,
            reason_code="restart_test_seed",
        )
        await checkpoints.advance(
            session,
            source_name="restart-test-source",
            checkpoint_value="opaque-checkpoint-before-kill",
            batch_received_at=now,
            coverage_status="best_effort",
            supports_replay=False,
            coverage_note="restart test",
        )

    first_process, first_startup = await _start_collector_subprocess(
        database_url=database_url
    )
    first_state = first_startup["reconstructed_state"]
    assert first_state["token_count"] == 2
    assert first_state["pending_dex_count"] == 1
    assert first_state["discovery_checkpoint_count"] == 1
    assert first_state["poll_schedule_count"] == 1
    assert first_state["abandoned_runs_recovered"] == 0

    # This is an actual SIGKILL: no finally block or run finalization can execute.
    first_process.kill()
    await asyncio.wait_for(first_process.wait(), timeout=5)
    assert first_process.returncode == -signal.SIGKILL

    restarted_process, restarted_startup = await _start_collector_subprocess(
        database_url=database_url
    )
    restarted_state = restarted_startup["reconstructed_state"]
    assert restarted_state["token_count"] == first_state["token_count"]
    assert restarted_state["pending_dex_count"] == first_state["pending_dex_count"]
    assert (
        restarted_state["discovery_checkpoint_count"]
        == first_state["discovery_checkpoint_count"]
    )
    assert restarted_state["poll_schedule_count"] == first_state["poll_schedule_count"]
    assert restarted_state["abandoned_runs_recovered"] == 1

    restarted_process.send_signal(signal.SIGTERM)
    await asyncio.wait_for(restarted_process.wait(), timeout=5)
    assert restarted_process.returncode == 0

    async with session_factory() as session:
        runs = list((await session.execute(select(CollectorRun))).scalars())
    status_counts = {
        status: sum(run.status == status for run in runs)
        for status in ("failed", "cancelled")
    }
    assert status_counts == {"failed": 1, "cancelled": 1}
    failed_run = next(run for run in runs if run.status == "failed")
    cancelled_run = next(run for run in runs if run.status == "cancelled")
    assert failed_run.failure_detail is not None
    assert failed_run.failure_detail["reason"] == "process_terminated_without_finalization"
    assert cancelled_run.failure_detail == {"reason": "SIGTERM"}
