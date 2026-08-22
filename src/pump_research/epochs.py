"""Immutable collection-epoch declarations and audited status transitions."""

from __future__ import annotations

import hashlib
import json
import subprocess
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from pump_research.config import Settings
from pump_research.persistence.models import (
    CollectionEpoch,
    CollectionEpochCurrent,
    CollectionEpochEvent,
    CollectorRun,
)

EPOCH0_ID = uuid.UUID("00000000-0000-0000-0000-000000000000")
_TERMINAL_STATUSES = frozenset({"completed", "aborted", "invalid"})


class CollectionEpochError(RuntimeError):
    """An epoch declaration or transition would violate provenance rules."""


@dataclass(frozen=True, slots=True)
class EpochStartResult:
    """Whether this transaction performed the epoch's one-time start transition."""

    epoch: CollectionEpoch
    started_now: bool


@dataclass(frozen=True, slots=True)
class EpochStatus:
    """Joined immutable epoch declaration and rebuildable current status."""

    id: uuid.UUID
    epoch_number: int
    name: str
    purpose: str
    status: str
    data_valid: bool
    invalid_reason: str | None
    started_at: datetime | None
    ended_at: datetime | None
    configuration_sha256: str
    code_revision: str | None
    created_at: datetime

    def as_dict(self) -> dict[str, object]:
        return {
            "id": str(self.id),
            "epoch_number": self.epoch_number,
            "name": self.name,
            "purpose": self.purpose,
            "status": self.status,
            "data_valid": self.data_valid,
            "invalid_reason": self.invalid_reason,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "ended_at": self.ended_at.isoformat() if self.ended_at else None,
            "configuration_sha256": self.configuration_sha256,
            "code_revision": self.code_revision,
            "created_at": self.created_at.isoformat(),
        }


def epoch_configuration(settings: Settings, epoch_number: int) -> tuple[str, dict[str, object]]:
    """Return a recoverable, secret-free immutable configuration document."""
    snapshot: dict[str, object] = {
        "component": "collection_epoch",
        "schema_version": 1,
        "epoch_number": epoch_number,
        "settings": settings.model_dump(
            mode="json",
            exclude={"database_url", "pumpportal_api_key", "solana_rpc_url"},
        ),
        "excluded_secret_fields": ["database_url", "pumpportal_api_key", "solana_rpc_url"],
    }
    encoded = json.dumps(snapshot, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest(), snapshot


def current_code_revision() -> str | None:
    """Best-effort code revision; absence is explicit rather than fabricated."""
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    revision = result.stdout.strip()
    return revision if result.returncode == 0 and revision else None


async def create_epoch(
    session: AsyncSession,
    settings: Settings,
    *,
    epoch_number: int,
    purpose: str,
    name: str | None = None,
    now: datetime | None = None,
) -> EpochStatus:
    """Declare a valid planned epoch; collection does not start here."""
    if epoch_number <= 0:
        raise CollectionEpochError("new research epoch numbers must be positive")
    existing = await session.scalar(
        select(CollectionEpoch).where(CollectionEpoch.epoch_number == epoch_number)
    )
    if existing is not None:
        raise CollectionEpochError(f"collection epoch {epoch_number} already exists")
    occurred_at = _normalize_now(now)
    digest, snapshot = epoch_configuration(settings, epoch_number)
    epoch = CollectionEpoch(
        epoch_number=epoch_number,
        name=name or f"Epoch {epoch_number}",
        purpose=purpose,
        data_valid=True,
        invalid_reason=None,
        configuration_sha256=digest,
        configuration_snapshot=snapshot,
        code_revision=current_code_revision(),
        created_at=occurred_at,
    )
    session.add(epoch)
    await session.flush()
    event = CollectionEpochEvent(
        collection_epoch_id=epoch.id,
        status="planned",
        occurred_at=occurred_at,
        reason="epoch explicitly declared before collection",
        detail={"epoch_number": epoch_number},
        idempotency_key=f"epoch:{epoch_number}:planned:{occurred_at.isoformat()}",
    )
    session.add(event)
    await session.flush()
    session.add(
        CollectionEpochCurrent(
            collection_epoch_id=epoch.id,
            status="planned",
            data_valid=True,
            invalid_reason=None,
            started_at=None,
            ended_at=None,
            latest_event_id=event.id,
            updated_at=occurred_at,
        )
    )
    await session.flush()
    return _status(epoch, "planned", True, None, None, None)


async def start_epoch(
    session: AsyncSession, *, epoch_number: int, now: datetime
) -> CollectionEpoch:
    """Atomically start a planned epoch, or safely attach a restart to it."""
    return (await start_epoch_with_result(session, epoch_number=epoch_number, now=now)).epoch


async def start_epoch_with_result(
    session: AsyncSession, *, epoch_number: int, now: datetime
) -> EpochStartResult:
    """Start an epoch and expose whether one-time operational initialization is due."""
    epoch, current = await _locked_epoch(session, epoch_number)
    if not current.data_valid:
        raise CollectionEpochError(f"collection epoch {epoch_number} is invalid")
    if current.status == "running":
        return EpochStartResult(epoch=epoch, started_now=False)
    if current.status != "planned":
        raise CollectionEpochError(
            f"collection epoch {epoch_number} is {current.status}, not planned or running"
        )
    other = await session.scalar(
        select(CollectionEpochCurrent.collection_epoch_id).where(
            CollectionEpochCurrent.status == "running",
            CollectionEpochCurrent.collection_epoch_id != epoch.id,
        )
    )
    if other is not None:
        raise CollectionEpochError("another collection epoch is already running")
    occurred_at = _normalize_now(now)
    event = CollectionEpochEvent(
        collection_epoch_id=epoch.id,
        status="running",
        occurred_at=occurred_at,
        reason="first collector run started",
        detail={"epoch_number": epoch_number},
        idempotency_key=f"epoch:{epoch_number}:running:{occurred_at.isoformat()}",
    )
    session.add(event)
    await session.flush()
    await session.execute(
        update(CollectionEpochCurrent)
        .where(CollectionEpochCurrent.collection_epoch_id == epoch.id)
        .values(
            status="running",
            started_at=occurred_at,
            latest_event_id=event.id,
            updated_at=occurred_at,
        )
    )
    return EpochStartResult(epoch=epoch, started_now=True)


async def close_epoch(
    session: AsyncSession,
    *,
    epoch_number: int,
    status: str,
    reason: str,
    now: datetime | None = None,
) -> EpochStatus:
    """Explicitly end a running epoch through an immutable terminal event."""
    if status not in _TERMINAL_STATUSES:
        raise CollectionEpochError("close status must be completed, aborted, or invalid")
    epoch, current = await _locked_epoch(session, epoch_number)
    if current.status != "running":
        raise CollectionEpochError(f"collection epoch {epoch_number} is not running")
    active_run = await session.scalar(
        select(CollectorRun.id).where(
            CollectorRun.collection_epoch_id == epoch.id,
            CollectorRun.status == "running",
        )
    )
    if active_run is not None:
        raise CollectionEpochError("stop the running collector before closing its epoch")
    occurred_at = _normalize_now(now)
    event = CollectionEpochEvent(
        collection_epoch_id=epoch.id,
        status=status,
        occurred_at=occurred_at,
        reason=reason,
        detail={"epoch_number": epoch_number},
        idempotency_key=f"epoch:{epoch_number}:{status}:{occurred_at.isoformat()}",
    )
    session.add(event)
    await session.flush()
    await session.execute(
        update(CollectionEpochCurrent)
        .where(CollectionEpochCurrent.collection_epoch_id == epoch.id)
        .values(
            status=status,
            data_valid=status != "invalid",
            invalid_reason=reason if status == "invalid" else None,
            ended_at=occurred_at,
            latest_event_id=event.id,
            updated_at=occurred_at,
        )
    )
    return _status(
        epoch,
        status,
        status != "invalid",
        reason if status == "invalid" else None,
        current.started_at,
        occurred_at,
    )


async def list_epochs(session: AsyncSession) -> list[EpochStatus]:
    """List all epoch declarations with their current audited projection."""
    rows = (
        await session.execute(
            select(CollectionEpoch, CollectionEpochCurrent)
            .join(
                CollectionEpochCurrent,
                CollectionEpochCurrent.collection_epoch_id == CollectionEpoch.id,
            )
            .order_by(CollectionEpoch.epoch_number)
        )
    ).all()
    return [
        _status(
            epoch,
            current.status,
            current.data_valid,
            current.invalid_reason,
            current.started_at,
            current.ended_at,
        )
        for epoch, current in rows
    ]


async def get_epoch_status(session: AsyncSession, epoch_number: int | None = None) -> EpochStatus:
    """Return one requested epoch or the highest numbered declaration."""
    statement = (
        select(CollectionEpoch, CollectionEpochCurrent)
        .join(
            CollectionEpochCurrent,
            CollectionEpochCurrent.collection_epoch_id == CollectionEpoch.id,
        )
        .order_by(CollectionEpoch.epoch_number.desc())
        .limit(1)
    )
    if epoch_number is not None:
        statement = statement.where(CollectionEpoch.epoch_number == epoch_number)
    row = (await session.execute(statement)).one_or_none()
    if row is None:
        requested = "latest" if epoch_number is None else str(epoch_number)
        raise CollectionEpochError(f"collection epoch {requested} does not exist")
    epoch, current = row
    return _status(
        epoch,
        current.status,
        current.data_valid,
        current.invalid_reason,
        current.started_at,
        current.ended_at,
    )


async def _locked_epoch(
    session: AsyncSession, epoch_number: int
) -> tuple[CollectionEpoch, CollectionEpochCurrent]:
    row = (
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
    if row is None:
        raise CollectionEpochError(f"collection epoch {epoch_number} does not exist")
    return row[0], row[1]


def _status(
    epoch: CollectionEpoch,
    status: str,
    data_valid: bool,
    invalid_reason: str | None,
    started_at: datetime | None,
    ended_at: datetime | None,
) -> EpochStatus:
    return EpochStatus(
        id=epoch.id,
        epoch_number=epoch.epoch_number,
        name=epoch.name,
        purpose=epoch.purpose,
        status=status,
        data_valid=data_valid,
        invalid_reason=invalid_reason,
        started_at=started_at,
        ended_at=ended_at,
        configuration_sha256=epoch.configuration_sha256,
        code_revision=epoch.code_revision,
        created_at=epoch.created_at,
    )


def _normalize_now(value: datetime | None) -> datetime:
    result = value or datetime.now(UTC)
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("epoch timestamps must be timezone-aware")
    return result.astimezone(UTC)
