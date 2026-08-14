"""Restart-safe collector process lifecycle and durable state reconstruction."""

from __future__ import annotations

import asyncio
import hashlib
import json
import signal
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import structlog
from sqlalchemy import Select, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research import __version__
from pump_research.config import Settings
from pump_research.persistence.models import (
    CollectorRun,
    DexAvailabilityTask,
    DiscoveryCheckpointState,
    PollSchedule,
    Token,
)
from pump_research.persistence.repositories import CollectorRunRepository


@dataclass(frozen=True, slots=True)
class ReconstructedCollectorState:
    """Durable operational state observed during process startup."""

    token_count: int
    pending_dex_count: int
    discovery_checkpoint_count: int
    poll_schedule_count: int
    leased_pending_dex_count: int
    leased_poll_schedule_count: int
    expired_pending_dex_lease_count: int
    expired_poll_schedule_lease_count: int
    abandoned_runs_recovered: int


@dataclass(frozen=True, slots=True)
class CollectorStartup:
    """One collector invocation and the state it reconstructed."""

    run_id: uuid.UUID
    state: ReconstructedCollectorState


class CollectorRuntime:
    """Own process signals and reconstruct authoritative state from PostgreSQL.

    Collection workers remain injected future concerns. This runtime deliberately
    creates no external API clients and no in-memory authoritative queue.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        *,
        logger: structlog.stdlib.BoundLogger,
    ) -> None:
        self._session_factory = session_factory
        self._logger = logger
        self._run_repository = CollectorRunRepository()
        self._shutdown = asyncio.Event()
        self._shutdown_reason = "shutdown_requested"
        self._configuration_snapshot = _safe_configuration_snapshot(settings)
        self._configuration_sha256 = _snapshot_sha256(self._configuration_snapshot)

    async def start(self) -> CollectorStartup:
        """Recover abandoned run records and read all durable work projections."""
        started_at = datetime.now(UTC)
        async with self._session_factory() as session, session.begin():
            abandoned_runs = list(
                (
                    await session.execute(
                        select(CollectorRun)
                        .where(CollectorRun.status == "running")
                        .with_for_update()
                    )
                ).scalars()
            )
            if abandoned_runs:
                await session.execute(
                    update(CollectorRun)
                    .where(CollectorRun.id.in_([run.id for run in abandoned_runs]))
                    .values(
                        status="failed",
                        finished_at=started_at,
                        failure_detail={
                            "reason": "process_terminated_without_finalization",
                            "recovered_at": started_at.isoformat(),
                        },
                    )
                )
            state = await _read_durable_state(
                session,
                now=started_at,
                abandoned_runs_recovered=len(abandoned_runs),
            )
            run = await self._run_repository.start(
                session,
                started_at=started_at,
                collector_version=__version__,
                configuration_sha256=self._configuration_sha256,
                configuration_snapshot=self._configuration_snapshot,
            )
        return CollectorStartup(run_id=run.id, state=state)

    async def run_until_stopped(self) -> CollectorStartup:
        """Start, report reconstructed state, and wait for a process signal."""
        startup = await self.start()
        installed_signals = self._install_signal_handlers()
        self._logger.info(
            "collector_started",
            run_id=str(startup.run_id),
            reconstructed_state=asdict(startup.state),
        )
        try:
            await self._shutdown.wait()
        finally:
            self._remove_signal_handlers(installed_signals)
        await self._finish(
            startup.run_id,
            status="cancelled",
            failure_detail={"reason": self._shutdown_reason},
        )
        self._logger.info(
            "collector_stopped",
            run_id=str(startup.run_id),
            reason=self._shutdown_reason,
        )
        return startup

    def request_shutdown(self, reason: str = "shutdown_requested") -> None:
        """Request an orderly stop from a signal handler or embedding process."""
        self._shutdown_reason = reason
        self._shutdown.set()

    async def _finish(
        self,
        run_id: uuid.UUID,
        *,
        status: str,
        failure_detail: dict[str, object] | None,
    ) -> None:
        async with self._session_factory() as session, session.begin():
            await self._run_repository.finish(
                session,
                run_id=run_id,
                finished_at=datetime.now(UTC),
                status=status,
                failure_detail=failure_detail,
            )

    def _install_signal_handlers(self) -> tuple[signal.Signals, ...]:
        loop = asyncio.get_running_loop()
        installed: list[signal.Signals] = []
        for process_signal in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(
                    process_signal,
                    self.request_shutdown,
                    process_signal.name,
                )
            except (NotImplementedError, RuntimeError):
                continue
            installed.append(process_signal)
        return tuple(installed)

    def _remove_signal_handlers(self, installed: tuple[signal.Signals, ...]) -> None:
        loop = asyncio.get_running_loop()
        for process_signal in installed:
            loop.remove_signal_handler(process_signal)


async def _read_durable_state(
    session: AsyncSession,
    *,
    now: datetime,
    abandoned_runs_recovered: int,
) -> ReconstructedCollectorState:
    async def count(statement: Select[tuple[int]]) -> int:
        value = await session.scalar(statement)
        return int(value or 0)

    return ReconstructedCollectorState(
        token_count=await count(select(func.count()).select_from(Token)),
        pending_dex_count=await count(
            select(func.count())
            .select_from(DexAvailabilityTask)
            .where(DexAvailabilityTask.state == "PENDING_DEX")
        ),
        discovery_checkpoint_count=await count(
            select(func.count()).select_from(DiscoveryCheckpointState)
        ),
        poll_schedule_count=await count(select(func.count()).select_from(PollSchedule)),
        leased_pending_dex_count=await count(
            select(func.count())
            .select_from(DexAvailabilityTask)
            .where(DexAvailabilityTask.lease_id.is_not(None))
        ),
        leased_poll_schedule_count=await count(
            select(func.count())
            .select_from(PollSchedule)
            .where(PollSchedule.lease_id.is_not(None))
        ),
        expired_pending_dex_lease_count=await count(
            select(func.count())
            .select_from(DexAvailabilityTask)
            .where(
                DexAvailabilityTask.lease_id.is_not(None),
                DexAvailabilityTask.lease_expires_at <= now,
            )
        ),
        expired_poll_schedule_lease_count=await count(
            select(func.count())
            .select_from(PollSchedule)
            .where(
                PollSchedule.lease_id.is_not(None),
                PollSchedule.lease_expires_at <= now,
            )
        ),
        abandoned_runs_recovered=abandoned_runs_recovered,
    )


def _safe_configuration_snapshot(settings: Settings) -> dict[str, object]:
    values = settings.model_dump(
        mode="json",
        exclude={"database_url", "pump_fun_api_token"},
    )
    return {
        "component": "collector_runtime",
        "schema_version": 1,
        "settings": values,
        "excluded_secret_fields": ["database_url", "pump_fun_api_token"],
    }


def _snapshot_sha256(snapshot: dict[str, object]) -> str:
    encoded = json.dumps(
        snapshot,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()
