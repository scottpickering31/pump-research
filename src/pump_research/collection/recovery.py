"""Explicit, lock-protected recovery of stale collector-run projections."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.collection.runtime import (
    acquire_collector_process_lock,
    release_collector_process_lock,
)
from pump_research.config import Settings
from pump_research.persistence.models import (
    CollectionEpoch,
    CollectionEpochCurrent,
    CollectorComponentHealth,
    CollectorRun,
    CollectorRunEvent,
)
from pump_research.persistence.repositories import CollectorRunRepository


class StaleCollectorReconciliationError(RuntimeError):
    """A stale-run repair was unsafe or inconsistent with durable state."""


@dataclass(frozen=True, slots=True)
class StaleCollectorReconciliation:
    """Audited result of reconciling one dead process's stale projection."""

    epoch_number: int
    collector_run_id: uuid.UUID
    previous_status: str
    status: str
    last_heartbeat_at: datetime | None
    reconciled_at: datetime
    reason: str
    already_reconciled: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "epoch_number": self.epoch_number,
            "collector_run_id": str(self.collector_run_id),
            "previous_status": self.previous_status,
            "status": self.status,
            "last_heartbeat_at": (
                self.last_heartbeat_at.isoformat() if self.last_heartbeat_at else None
            ),
            "reconciled_at": self.reconciled_at.isoformat(),
            "reason": self.reason,
            "already_reconciled": self.already_reconciled,
        }


async def reconcile_stale_collector_run(
    session_factory: async_sessionmaker[AsyncSession],
    settings: Settings,
    *,
    epoch_number: int,
    reason: str,
    now: datetime | None = None,
) -> StaleCollectorReconciliation:
    """Mark a stale running row stopped only while owning the process lock.

    Acquiring the same session advisory lock used by the collector is the
    authoritative proof that no conforming collector process is active. The
    run update, component projection repair, and immutable terminal event commit
    atomically.
    """
    normalized_now = _normalize_now(now)
    normalized_reason = reason.strip()
    if not normalized_reason:
        raise StaleCollectorReconciliationError("a non-empty operator reason is required")
    if len(normalized_reason) > 1_500:
        raise StaleCollectorReconciliationError("operator reason exceeds 1500 characters")

    lock_connection = await acquire_collector_process_lock(session_factory)
    try:
        async with session_factory() as session, session.begin():
            epoch_row = (
                await session.execute(
                    select(CollectionEpoch, CollectionEpochCurrent)
                    .join(
                        CollectionEpochCurrent,
                        CollectionEpochCurrent.collection_epoch_id == CollectionEpoch.id,
                    )
                    .where(CollectionEpoch.epoch_number == epoch_number)
                    .with_for_update(of=CollectionEpochCurrent)
                )
            ).one_or_none()
            if epoch_row is None:
                raise StaleCollectorReconciliationError(
                    f"collection epoch {epoch_number} does not exist"
                )
            epoch, current = epoch_row
            if current.status != "running":
                raise StaleCollectorReconciliationError(
                    f"collection epoch {epoch_number} is {current.status}, not running"
                )
            run = await session.scalar(
                select(CollectorRun)
                .where(CollectorRun.collection_epoch_id == epoch.id)
                .order_by(CollectorRun.started_at.desc())
                .limit(1)
                .with_for_update()
            )
            if run is None:
                raise StaleCollectorReconciliationError(
                    f"collection epoch {epoch_number} has no collector run"
                )
            existing_event = await session.scalar(
                select(CollectorRunEvent).where(
                    CollectorRunEvent.collector_run_id == run.id,
                    CollectorRunEvent.event_type == "stale_reconciled",
                )
            )
            if run.status == "stopped" and existing_event is not None:
                return StaleCollectorReconciliation(
                    epoch_number=epoch_number,
                    collector_run_id=run.id,
                    previous_status="running",
                    status=run.status,
                    last_heartbeat_at=run.last_heartbeat_at,
                    reconciled_at=existing_event.occurred_at,
                    reason=str(existing_event.detail.get("operator_reason", existing_event.reason)),
                    already_reconciled=True,
                )
            if run.status != "running":
                raise StaleCollectorReconciliationError(
                    f"latest collector run is {run.status}, not stale-running"
                )

            heartbeat_basis = run.last_heartbeat_at or run.started_at
            heartbeat_age_seconds = (normalized_now - heartbeat_basis).total_seconds()
            stale_after_seconds = max(30.0, 3 * settings.collector_heartbeat_seconds)
            if heartbeat_age_seconds <= stale_after_seconds:
                raise StaleCollectorReconciliationError(
                    "latest collector heartbeat is not stale; refusing reconciliation"
                )

            components = list(
                (
                    await session.execute(
                        select(CollectorComponentHealth)
                        .where(CollectorComponentHealth.collector_run_id == run.id)
                        .order_by(CollectorComponentHealth.component_name)
                        .with_for_update()
                    )
                ).scalars()
            )
            prior_components = {
                component.component_name: {
                    "status": component.status,
                    "detail": component.detail,
                    "updated_at": component.updated_at.isoformat(),
                }
                for component in components
            }
            detail: dict[str, object] = {
                "reason": "stale_run_reconciled_after_operator_shutdown",
                "operator_reason": normalized_reason,
                "reconciled_at": normalized_now.isoformat(),
                "last_heartbeat_at": (
                    run.last_heartbeat_at.isoformat() if run.last_heartbeat_at else None
                ),
                "finished_at_semantics": (
                    "reconciliation time; exact process exit time is unknown after last heartbeat"
                ),
                "previous_component_health": prior_components,
            }
            await CollectorRunRepository().finish(
                session,
                run_id=run.id,
                finished_at=normalized_now,
                status="stopped",
                failure_detail=detail,
                event_type="stale_reconciled",
            )
            for component in components:
                component.status = "stopped"
                component.detail = {
                    "reason": "stale_run_reconciled_after_operator_shutdown",
                    "previous_status": prior_components[component.component_name]["status"],
                    "reconciled_at": normalized_now.isoformat(),
                }
                component.updated_at = normalized_now

            return StaleCollectorReconciliation(
                epoch_number=epoch_number,
                collector_run_id=run.id,
                previous_status="running",
                status="stopped",
                last_heartbeat_at=run.last_heartbeat_at,
                reconciled_at=normalized_now,
                reason=normalized_reason,
                already_reconciled=False,
            )
    finally:
        await release_collector_process_lock(lock_connection)


def _normalize_now(value: datetime | None) -> datetime:
    result = value or datetime.now(UTC)
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("reconciliation timestamp must be timezone-aware")
    return result.astimezone(UTC)
