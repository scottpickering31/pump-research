"""Restart-safe collector process lifecycle and durable state reconstruction."""

from __future__ import annotations

import asyncio
import hashlib
import json
import signal
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Protocol

import structlog
from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, AsyncSession, async_sessionmaker

from pump_research import __version__
from pump_research.config import Settings
from pump_research.epochs import start_epoch_with_result
from pump_research.persistence.models import (
    CollectorRun,
    DexAvailabilityTask,
    DiscoveryCheckpointState,
    PollSchedule,
    Token,
)
from pump_research.persistence.repositories import CollectorRunRepository

_COLLECTOR_PROCESS_LOCK_ID = 7_428_901_164


class CollectorAlreadyRunningError(RuntimeError):
    """Another collector process already owns the database-scoped runtime lock."""


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
    collection_epoch_id: uuid.UUID
    epoch_number: int
    state: ReconstructedCollectorState


class CollectorWorkerProtocol(Protocol):
    """Fixed-task pipeline supervised by the process runtime."""

    async def run(self, *, run_id: uuid.UUID, shutdown: asyncio.Event) -> None:
        """Run until shutdown or raise on an unexpected component failure."""

    async def mark_stopped(self, run_id: uuid.UUID) -> None:
        """Persist final component status after a graceful shutdown."""


class EpochInitializerProtocol(Protocol):
    """One-time operational projection initialization at a new epoch boundary."""

    async def initialize_epoch_in_session(
        self,
        session: AsyncSession,
        *,
        collection_epoch_id: uuid.UUID,
        epoch_number: int,
        started_at: datetime,
    ) -> int:
        """Rebase stale operational work without altering historical facts."""


class CollectorRuntime:
    """Own process signals and reconstruct authoritative state from PostgreSQL.

    The worker is injected so provider adapters remain replaceable; this runtime
    owns only signals, run records, and durable restart reconstruction.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        *,
        logger: structlog.stdlib.BoundLogger,
        epoch_number: int,
        worker: CollectorWorkerProtocol | None = None,
        epoch_initializer: EpochInitializerProtocol | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._logger = logger
        self._worker = worker
        self._epoch_initializer = epoch_initializer
        self._epoch_number = epoch_number
        self._shutdown_grace_seconds = settings.collector_shutdown_grace_seconds
        self._run_repository = CollectorRunRepository()
        self._shutdown = asyncio.Event()
        self._shutdown_reason = "shutdown_requested"
        self._configuration_snapshot = _safe_configuration_snapshot(settings)
        self._configuration_sha256 = _snapshot_sha256(self._configuration_snapshot)

    async def start(self) -> CollectorStartup:
        """Recover abandoned run records and read all durable work projections."""
        started_at = datetime.now(UTC)
        async with self._session_factory() as session, session.begin():
            epoch_start = await start_epoch_with_result(
                session,
                epoch_number=self._epoch_number,
                now=started_at,
            )
            epoch = epoch_start.epoch
            if epoch_start.started_now and self._epoch_initializer is not None:
                await self._epoch_initializer.initialize_epoch_in_session(
                    session,
                    collection_epoch_id=epoch.id,
                    epoch_number=epoch.epoch_number,
                    started_at=started_at,
                )
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
                for abandoned_run in abandoned_runs:
                    await self._run_repository.finish(
                        session,
                        run_id=abandoned_run.id,
                        finished_at=started_at,
                        status="failed",
                        failure_detail={
                            "reason": "process_terminated_without_finalization",
                            "recovered_at": started_at.isoformat(),
                        },
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
                collection_epoch_id=epoch.id,
            )
        return CollectorStartup(
            run_id=run.id,
            collection_epoch_id=epoch.id,
            epoch_number=epoch.epoch_number,
            state=state,
        )

    async def run_until_stopped(self) -> CollectorStartup:
        """Start, report reconstructed state, and wait for a process signal."""
        lock_connection = await self._acquire_process_lock()
        try:
            return await self._run_until_stopped_locked()
        finally:
            await self._release_process_lock(lock_connection)

    async def _run_until_stopped_locked(self) -> CollectorStartup:
        """Run while holding the session-level singleton lock."""
        startup = await self.start()
        installed_signals = self._install_signal_handlers()
        self._logger.info(
            "collector_started",
            run_id=str(startup.run_id),
            collection_epoch_id=str(startup.collection_epoch_id),
            epoch_number=startup.epoch_number,
            reconstructed_state=asdict(startup.state),
        )
        worker = self._worker
        worker_task = (
            asyncio.create_task(worker.run(run_id=startup.run_id, shutdown=self._shutdown))
            if worker is not None
            else None
        )
        shutdown_task = asyncio.create_task(_wait_for_shutdown(self._shutdown))
        terminal_status = "failed"
        terminal_detail: dict[str, object] = {
            "reason": "collector_runtime_terminated_during_supervision"
        }
        pending_error: BaseException | None = None
        graceful_stop = False
        try:
            wait_set = {shutdown_task}
            if worker_task is not None:
                wait_set.add(worker_task)
            done, _ = await asyncio.wait(wait_set, return_when=asyncio.FIRST_COMPLETED)
            if not self._shutdown.is_set() and worker_task is not None and worker_task in done:
                error = worker_task.exception()
                if error is not None:
                    terminal_detail = {
                        "reason": "collector_worker_failed",
                        "error_type": type(error).__name__,
                    }
                    pending_error = error
                else:
                    terminal_detail = {"reason": "collector_worker_stopped_unexpectedly"}
                    pending_error = RuntimeError(
                        "collector worker stopped without a shutdown request"
                    )
            else:
                graceful_stop = True
                terminal_status = "stopped"
                terminal_detail = {
                    "reason": "operator_requested_shutdown",
                    "signal": self._shutdown_reason,
                }

            if graceful_stop and worker_task is not None:
                try:
                    await asyncio.wait_for(worker_task, timeout=self._shutdown_grace_seconds)
                except TimeoutError:
                    terminal_detail["forced_cancel_after_grace_timeout"] = True
                    worker_task.cancel()
                    await asyncio.gather(worker_task, return_exceptions=True)
                except BaseException as error:
                    if _is_shutdown_broken_pipe(error):
                        terminal_detail["suppressed_shutdown_error"] = "BrokenPipeError"
                    else:
                        graceful_stop = False
                        terminal_status = "failed"
                        terminal_detail = {
                            "reason": "collector_worker_failed_during_shutdown",
                            "error_type": type(error).__name__,
                            "signal": self._shutdown_reason,
                        }
                        pending_error = error
            if graceful_stop and worker is not None:
                assert worker is not None
                try:
                    await worker.mark_stopped(startup.run_id)
                except BaseException as error:
                    graceful_stop = False
                    terminal_status = "failed"
                    terminal_detail = {
                        "reason": "component_stop_persistence_failed",
                        "error_type": type(error).__name__,
                        "signal": self._shutdown_reason,
                    }
                    pending_error = error
        except BaseException as error:
            if pending_error is None:
                pending_error = error
                terminal_detail = {
                    "reason": "collector_runtime_supervision_failed",
                    "error_type": type(error).__name__,
                }
        finally:
            shutdown_task.cancel()
            await asyncio.gather(shutdown_task, return_exceptions=True)
            self._remove_signal_handlers(installed_signals)
            await self._finish(
                startup.run_id,
                status=terminal_status,
                failure_detail=terminal_detail,
            )
        if pending_error is not None:
            raise pending_error
        self._logger.info("collector_stopped", run_id=str(startup.run_id), **terminal_detail)
        return startup

    async def _acquire_process_lock(self) -> AsyncConnection:
        return await acquire_collector_process_lock(self._session_factory)

    async def _release_process_lock(self, connection: AsyncConnection) -> None:
        await release_collector_process_lock(connection)

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
            select(func.count()).select_from(PollSchedule).where(PollSchedule.lease_id.is_not(None))
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
        exclude={"database_url", "pumpportal_api_key", "solana_rpc_url"},
    )
    return {
        "component": "collector_runtime",
        "schema_version": 1,
        "settings": values,
        "excluded_secret_fields": ["database_url", "pumpportal_api_key", "solana_rpc_url"],
    }


def _snapshot_sha256(snapshot: dict[str, object]) -> str:
    encoded = json.dumps(
        snapshot,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


async def _wait_for_shutdown(shutdown: asyncio.Event) -> None:
    await shutdown.wait()


async def acquire_collector_process_lock(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncConnection:
    """Acquire the database-scoped singleton lock used by every collector process."""
    bind = session_factory.kw.get("bind")
    if not isinstance(bind, AsyncEngine):
        raise RuntimeError("collector runtime requires an AsyncEngine-bound session factory")
    connection = await bind.connect()
    try:
        acquired = bool(
            await connection.scalar(select(func.pg_try_advisory_lock(_COLLECTOR_PROCESS_LOCK_ID)))
        )
        # Session advisory locks survive transaction boundaries; avoid holding
        # an idle transaction open for the entire collector lifetime.
        await connection.commit()
    except BaseException:
        await connection.close()
        raise
    if not acquired:
        await connection.close()
        raise CollectorAlreadyRunningError(
            "another collector is already running for this PostgreSQL database"
        )
    return connection


async def release_collector_process_lock(connection: AsyncConnection) -> None:
    """Release a collector singleton lock acquired on this exact connection."""
    try:
        await connection.scalar(select(func.pg_advisory_unlock(_COLLECTOR_PROCESS_LOCK_ID)))
        await connection.commit()
    finally:
        await connection.close()


def _is_shutdown_broken_pipe(error: BaseException) -> bool:
    """Return true only when an exception tree consists solely of closed-pipe errors."""
    if isinstance(error, BaseExceptionGroup):
        return bool(error.exceptions) and all(
            _is_shutdown_broken_pipe(child) for child in error.exceptions
        )
    return isinstance(error, BrokenPipeError)
