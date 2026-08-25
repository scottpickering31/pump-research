"""Cross-workflow PostgreSQL lock protocol for schedules and token evidence."""

from __future__ import annotations

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

# Scheduler claims already used this transaction-advisory key for global request
# capacity and claim serialization.  Shared holders may continue concurrently;
# the exclusive Phase 6 holder drains them before taking the Token UPDATE evidence
# fence.  This prevents Schedule -> Token FK / Token -> Schedule cycles without
# weakening that evidence fence or serializing ordinary poll completions.
_SCHEDULE_TOKEN_FK_COORDINATION_LOCK_ID = 7_428_901_163


async def lock_schedule_token_fk_path(session: AsyncSession, *, exclusive: bool) -> None:
    """Gate transactions that acquire Schedule before a token FK dependency.

    Scheduler claim serialization and Phase 6 evidence evaluation use the
    exclusive form.  Scheduler completion and epoch reconstruction use the shared
    form because they may safely run together when schedule rows do not overlap.
    Every caller must acquire this gate before any ``PollSchedule`` row lock.
    """
    lock = (
        func.pg_advisory_xact_lock(_SCHEDULE_TOKEN_FK_COORDINATION_LOCK_ID)
        if exclusive
        else func.pg_advisory_xact_lock_shared(_SCHEDULE_TOKEN_FK_COORDINATION_LOCK_ID)
    )
    await session.execute(select(lock))
