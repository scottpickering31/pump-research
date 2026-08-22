"""Fixed-task, restart-safe orchestration of the durable collector pipeline."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import partial

import structlog
from sqlalchemy import update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.boosts import BoostCollectionWorkflow
from pump_research.collection.dex_availability import DexAvailabilityWorkflow
from pump_research.collection.discovery import DiscoveryCoordinator
from pump_research.collection.market_context import MarketContextWorkflow
from pump_research.collection.polling import ScheduledObservationWorkflow
from pump_research.collection.security import TokenSecurityWorkflow
from pump_research.config import Settings
from pump_research.discovery.contracts import DiscoverySourceError
from pump_research.market_data.dexscreener import DexScreenerError
from pump_research.market_data.solana_rpc import SolanaRpcError
from pump_research.monitoring.storage import record_storage_sample
from pump_research.persistence.models import CollectorComponentHealth, CollectorRun
from pump_research.scheduling.clock import Clock, SystemClock
from pump_research.scheduling.scheduler import AdaptiveScheduler, PollOutcome
from pump_research.security_enrichment.service import SecurityEnrichmentWorker


class CollectorComponentDegradedError(RuntimeError):
    """A component durably recorded failed work but remains safe to retry."""


class CollectorWorker:
    """Run a constant number of supervised pipeline loops until shutdown.

    PostgreSQL remains authoritative: loops claim bounded work on demand and
    never maintain a token-address queue in memory.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        *,
        discovery: DiscoveryCoordinator,
        availability: DexAvailabilityWorkflow,
        scheduler: AdaptiveScheduler,
        polling: ScheduledObservationWorkflow,
        logger: structlog.stdlib.BoundLogger,
        boosts: BoostCollectionWorkflow | None = None,
        security: TokenSecurityWorkflow | None = None,
        market_context: MarketContextWorkflow | None = None,
        selective_security: SecurityEnrichmentWorker | None = None,
        clock: Clock | None = None,
    ) -> None:
        self._session_factory = session_factory
        self._settings = settings
        self._discovery = discovery
        self._availability = availability
        self._scheduler = scheduler
        self._polling = polling
        self._logger = logger
        self._clock = clock or SystemClock()
        self._boosts = boosts
        self._security = security
        self._market_context = market_context
        self._selective_security = selective_security

    async def run(self, *, run_id: uuid.UUID, shutdown: asyncio.Event) -> None:
        """Run all pipeline components; an unexpected component error stops all."""
        async with asyncio.TaskGroup() as group:
            group.create_task(
                self._loop(
                    "discovery",
                    run_id,
                    shutdown,
                    self._settings.collector_discovery_poll_seconds,
                    lambda: self._discover_once(run_id),
                    expected=(DiscoverySourceError,),
                )
            )
            group.create_task(
                self._loop(
                    "dex_availability",
                    run_id,
                    shutdown,
                    self._settings.collector_reconciliation_poll_seconds,
                    lambda: self._availability_once(run_id),
                    expected=(CollectorComponentDegradedError,),
                )
            )
            # A fixed number of batch workers lets request latency and database
            # persistence overlap. Durable leases and the shared capacity gate
            # remain authoritative; this is never a task or queue per token.
            for worker_index in range(self._settings.scheduler_max_in_flight_batches):
                group.create_task(
                    self._loop(
                        "scheduled_observation",
                        run_id,
                        shutdown,
                        self._settings.collector_scheduler_poll_seconds,
                        lambda: self._poll_once(run_id),
                        expected=(DexScreenerError, CollectorComponentDegradedError),
                    ),
                    name=f"scheduled-observation-{worker_index}",
                )
            group.create_task(self._heartbeat_loop(run_id, shutdown))
            group.create_task(self._storage_telemetry_loop(run_id, shutdown))
            if self._boosts is not None:
                group.create_task(
                    self._loop(
                        "boost_latest",
                        run_id,
                        shutdown,
                        self._settings.boost_latest_poll_seconds,
                        lambda: self._boost_once(run_id, "latest"),
                        expected=(DexScreenerError,),
                    )
                )
                group.create_task(
                    self._loop(
                        "boost_top",
                        run_id,
                        shutdown,
                        self._settings.boost_top_poll_seconds,
                        lambda: self._boost_once(run_id, "top"),
                        expected=(DexScreenerError,),
                    )
                )
            if self._security is not None:
                group.create_task(
                    self._loop(
                        "token_security",
                        run_id,
                        shutdown,
                        self._settings.token_security_poll_seconds,
                        lambda: self._security_once(run_id),
                        expected=(SolanaRpcError,),
                    )
                )
            if self._market_context is not None:
                group.create_task(
                    self._loop(
                        "market_context",
                        run_id,
                        shutdown,
                        self._settings.market_context_interval_seconds,
                        lambda: self._market_context_once(run_id),
                        expected=(),
                    )
                )
            if self._selective_security is not None:
                for worker_index in range(self._settings.security_enrichment_workers):
                    group.create_task(
                        self._loop(
                            "selective_security_enrichment",
                            run_id,
                            shutdown,
                            self._settings.security_enrichment_poll_seconds,
                            partial(self._selective_security_once, run_id, worker_index),
                            expected=(),
                        ),
                        name=f"selective-security-enrichment-{worker_index}",
                    )

    async def _discover_once(self, run_id: uuid.UUID) -> None:
        await self._discovery.run_once(collector_run_id=run_id)

    async def _availability_once(self, run_id: uuid.UUID) -> None:
        result = await self._availability.check_due(
            now=self._clock.now(),
            collector_run_id=run_id,
        )
        if result.failed_tokens:
            raise CollectorComponentDegradedError(
                f"DEX availability failed for {result.failed_tokens} token(s)"
            )

    async def _poll_once(self, run_id: uuid.UUID) -> None:
        claim = await self._scheduler.claim_next_batch(collector_run_id=run_id)
        if claim is not None:
            result = await self._polling.execute(claim, collector_run_id=run_id)
            if result.outcome in {PollOutcome.FAILED, PollOutcome.THROTTLED, PollOutcome.MALFORMED}:
                raise CollectorComponentDegradedError(
                    f"Scheduled observation completed as {result.outcome.value}"
                )

    async def _boost_once(self, run_id: uuid.UUID, feed_kind: str) -> None:
        assert self._boosts is not None
        await self._boosts.collect(feed_kind=feed_kind, collector_run_id=run_id)

    async def _security_once(self, run_id: uuid.UUID) -> None:
        assert self._security is not None
        await self._security.collect_due(collector_run_id=run_id, now=self._clock.now())

    async def _market_context_once(self, run_id: uuid.UUID) -> None:
        assert self._market_context is not None
        await self._market_context.record_closed_bucket(
            collector_run_id=run_id,
            now=self._clock.now(),
        )

    async def _selective_security_once(self, run_id: uuid.UUID, worker_index: int) -> None:
        assert self._selective_security is not None
        await self._selective_security.run_once(
            now=self._clock.now(),
            worker_id=f"phase6-{run_id}-{worker_index}",
            collector_run_id=run_id,
            limit=1,
        )

    async def _loop(
        self,
        component: str,
        run_id: uuid.UUID,
        shutdown: asyncio.Event,
        interval: float,
        action: Callable[[], Awaitable[object]],
        *,
        expected: tuple[type[Exception], ...],
    ) -> None:
        while not shutdown.is_set():
            attempted_at = datetime.now(UTC)
            try:
                await action()
            except expected as error:
                await self._record_component(
                    component, run_id, "degraded", attempted_at, error=error
                )
                self._logger.warning(
                    "collector_component_retryable_error",
                    component=component,
                    error_type=type(error).__name__,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._record_component(component, run_id, "failed", attempted_at, error=error)
                self._logger.exception(
                    "collector_component_failed",
                    component=component,
                    error_type=type(error).__name__,
                )
                raise
            else:
                await self._record_component(component, run_id, "healthy", attempted_at)
            await _wait_for_shutdown(shutdown, interval)

    async def _heartbeat_loop(self, run_id: uuid.UUID, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            now = datetime.now(UTC)
            async with self._session_factory() as session, session.begin():
                await session.execute(
                    update(CollectorRun)
                    .where(CollectorRun.id == run_id)
                    .values(last_heartbeat_at=now)
                )
            await self._record_component("heartbeat", run_id, "healthy", now)
            await _wait_for_shutdown(shutdown, self._settings.collector_heartbeat_seconds)

    async def _storage_telemetry_loop(self, run_id: uuid.UUID, shutdown: asyncio.Event) -> None:
        while not shutdown.is_set():
            attempted_at = datetime.now(UTC)
            try:
                await record_storage_sample(
                    self._session_factory,
                    collector_run_id=run_id,
                    sampled_at=attempted_at,
                )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                await self._record_component(
                    "storage_telemetry", run_id, "degraded", attempted_at, error=error
                )
                self._logger.exception("storage_telemetry_failed", error_type=type(error).__name__)
            else:
                await self._record_component("storage_telemetry", run_id, "healthy", attempted_at)
            await _wait_for_shutdown(shutdown, self._settings.storage_telemetry_interval_seconds)

    async def mark_stopped(self, run_id: uuid.UUID) -> None:
        now = datetime.now(UTC)
        components = [
            "discovery",
            "dex_availability",
            "scheduled_observation",
            "heartbeat",
            "storage_telemetry",
        ]
        if self._boosts is not None:
            components.extend(("boost_latest", "boost_top"))
        if self._security is not None:
            components.append("token_security")
        if self._market_context is not None:
            components.append("market_context")
        if self._selective_security is not None:
            components.append("selective_security_enrichment")
        for component in components:
            await self._record_component(component, run_id, "stopped", now)

    async def _record_component(
        self,
        component: str,
        run_id: uuid.UUID,
        status: str,
        attempted_at: datetime,
        *,
        error: Exception | None = None,
    ) -> None:
        values = {
            "component_name": component,
            "collector_run_id": run_id,
            "status": status,
            "last_attempt_at": attempted_at,
            "last_success_at": attempted_at if status == "healthy" else None,
            "detail": (
                None
                if error is None
                else {"error_type": type(error).__name__, "message": str(error)[:1_000]}
            ),
            "updated_at": attempted_at,
        }
        async with self._session_factory() as session, session.begin():
            updates = {key: value for key, value in values.items() if key != "component_name"}
            if status != "healthy":
                updates["last_success_at"] = CollectorComponentHealth.last_success_at
            await session.execute(
                insert(CollectorComponentHealth)
                .values(**values)
                .on_conflict_do_update(
                    index_elements=[CollectorComponentHealth.component_name],
                    set_=updates,
                )
            )


async def _wait_for_shutdown(shutdown: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(shutdown.wait(), timeout=seconds)
    except TimeoutError:
        return
