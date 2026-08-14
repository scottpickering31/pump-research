from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

DATABASE_URL = os.environ.get(
    "PUMP_RESEARCH_DATABASE_URL",
    "postgresql+asyncpg://pump_research:pump_research@localhost:5433/pump_research",
)
PROJECT_ROOT = Path(__file__).parents[2]


@pytest.fixture(scope="session", autouse=True)
def apply_migrations() -> None:
    environment = {**os.environ, "PUMP_RESEARCH_DATABASE_URL": DATABASE_URL}
    subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        check=True,
        cwd=PROJECT_ROOT,
        env=environment,
    )


@pytest_asyncio.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = create_async_engine(DATABASE_URL)
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "TRUNCATE poll_batch_outcomes, poll_batch_members, poll_schedule_decisions, "
                "poll_schedules, poll_batches, dex_availability_tasks, "
                "discovery_checkpoint_states, observations, "
                "lifecycle_events, discovery_events, api_request_log, pairs, tokens, "
                "collector_runs CASCADE"
            )
        )

    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest_asyncio.fixture
async def session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[AsyncSession]:
    """A convenience session for persistence tests using the clean database fixture."""
    async with session_factory() as database_session:
        yield database_session
