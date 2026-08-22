"""Durable, fail-closed archive catalog and non-destructive retention eligibility."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import cast

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.persistence.models import (
    ArchiveCopyVerification,
    ArchiveRetentionEvaluation,
    ArchiveScope,
    ArchiveScopeEvent,
    CollectionEpoch,
    CollectionEpochCurrent,
)

SUPPORTED_ARCHIVE_SCHEMA_VERSIONS = frozenset({2})
_SCOPE_NAMESPACE = uuid.UUID("5910a50d-65d2-5b1c-af4a-8f46ef68d1df")


class ArchiveCatalogError(RuntimeError):
    """Archive catalog state is conflicting or incomplete."""


class ArchiveBusyError(ArchiveCatalogError):
    """Another worker owns the unexpired claim for this source scope."""


@dataclass(frozen=True, slots=True)
class ArchiveClaim:
    scope_id: uuid.UUID
    identity_sha256: str
    claim_token: uuid.UUID | None
    reusable_manifest_path: str | None
    previous_state: str


def canonical_sha256(value: object) -> str:
    """Hash a JSON-compatible value using the archive's canonical representation."""
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


async def claim_archive_scope(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    identity_sha256: str,
    epoch_id: uuid.UUID,
    start_at: datetime,
    end_at: datetime,
    archive_schema_version: int,
    source_db_schema_revision: str,
    source_scope_snapshot: dict[str, object],
    now: datetime | None = None,
    lease_seconds: int = 900,
) -> ArchiveClaim:
    """Create or exclusively lease one deterministic source scope."""
    current = now or datetime.now(UTC)
    scope_id = uuid.uuid5(_SCOPE_NAMESPACE, identity_sha256)
    claim_token = uuid.uuid4()
    async with session_factory() as session, session.begin():
        await session.execute(
            insert(ArchiveScope)
            .values(
                id=scope_id,
                archive_identity_sha256=identity_sha256,
                collection_epoch_id=epoch_id,
                start_at=start_at,
                end_at=end_at,
                archive_schema_version=archive_schema_version,
                source_db_schema_revision=source_db_schema_revision,
                source_scope_snapshot=source_scope_snapshot,
                state="pending",
                updated_at=current,
            )
            # Both the UUID primary key and identity digest are deterministic.
            # A narrow conflict target does not suppress a concurrent collision
            # on the other constraint. Readback below verifies semantic equality.
            .on_conflict_do_nothing()
        )
        scope = cast(
            ArchiveScope,
            await session.scalar(
                select(ArchiveScope)
                .where(ArchiveScope.archive_identity_sha256 == identity_sha256)
                .with_for_update()
            ),
        )
        if scope is None:
            raise ArchiveCatalogError("archive scope insert/readback failed")
        expected = (
            scope.id == scope_id
            and scope.collection_epoch_id == epoch_id
            and scope.start_at == start_at
            and scope.end_at == end_at
            and scope.archive_schema_version == archive_schema_version
            and scope.source_db_schema_revision == source_db_schema_revision
            and scope.source_scope_snapshot == source_scope_snapshot
        )
        if not expected:
            raise ArchiveCatalogError(
                "deterministic archive identity maps to different source-scope content"
            )
        previous = scope.state
        if scope.state in {"verified", "independently_copied", "retention_eligible"}:
            if not scope.manifest_path:
                raise ArchiveCatalogError("verified archive scope has no manifest path")
            return ArchiveClaim(scope.id, identity_sha256, None, scope.manifest_path, previous)
        if (
            scope.state == "exporting"
            and scope.claim_expires_at is not None
            and scope.claim_expires_at > current
        ):
            raise ArchiveBusyError(
                f"archive scope {identity_sha256} is leased until "
                f"{scope.claim_expires_at.isoformat()}"
            )
        reusable = scope.manifest_path if scope.state == "exported" else None
        scope.state = "exporting"
        scope.claim_token = claim_token
        scope.claim_expires_at = current + timedelta(seconds=lease_seconds)
        scope.failure_detail = None
        scope.updated_at = current
        await _append_event(
            session,
            scope_id=scope.id,
            event_type="claimed",
            occurred_at=current,
            idempotency_key=f"archive:{identity_sha256}:claim:{claim_token}",
            detail={"previous_state": previous, "lease_seconds": lease_seconds},
        )
    return ArchiveClaim(scope_id, identity_sha256, claim_token, reusable, previous)


async def mark_archive_exported(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    claim: ArchiveClaim,
    manifest_path: str,
    manifest_sha256: str,
    aggregate_file_sha256: str,
    source_row_count: int,
    parquet_bytes: int,
    now: datetime | None = None,
) -> None:
    current = now or datetime.now(UTC)
    async with session_factory() as session, session.begin():
        scope = await _claimed_scope(session, claim)
        scope.state = "exported"
        scope.manifest_path = manifest_path
        scope.manifest_sha256 = manifest_sha256
        scope.aggregate_file_sha256 = aggregate_file_sha256
        scope.source_row_count = source_row_count
        scope.parquet_bytes = parquet_bytes
        scope.claim_token = None
        scope.claim_expires_at = None
        scope.updated_at = current
        await _append_event(
            session,
            scope_id=scope.id,
            event_type="exported",
            occurred_at=current,
            idempotency_key=f"archive:{claim.identity_sha256}:exported:{manifest_sha256}",
            detail={
                "manifest_path": manifest_path,
                "manifest_sha256": manifest_sha256,
                "aggregate_file_sha256": aggregate_file_sha256,
                "source_row_count": source_row_count,
                "parquet_bytes": parquet_bytes,
            },
        )


async def mark_archive_verified(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    scope_id: uuid.UUID,
    manifest_sha256: str,
    verification_detail: dict[str, object],
    analytical_reads_passed: bool,
    now: datetime | None = None,
) -> None:
    current = now or datetime.now(UTC)
    async with session_factory() as session, session.begin():
        scope = cast(
            ArchiveScope,
            await session.scalar(
                select(ArchiveScope).where(ArchiveScope.id == scope_id).with_for_update()
            ),
        )
        if scope is None or scope.manifest_sha256 != manifest_sha256:
            raise ArchiveCatalogError("archive verification does not match catalog manifest")
        scope.state = "verified"
        scope.verified_at = current
        scope.verification_detail = verification_detail
        if analytical_reads_passed:
            scope.analytical_reads_verified_at = current
        scope.failure_detail = None
        scope.claim_token = None
        scope.claim_expires_at = None
        scope.updated_at = current
        await _append_event(
            session,
            scope_id=scope.id,
            event_type="verified",
            occurred_at=current,
            idempotency_key=f"archive:{scope.archive_identity_sha256}:verified:{manifest_sha256}",
            detail=verification_detail,
        )


async def mark_archive_failed(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    claim: ArchiveClaim,
    stage: str,
    error: BaseException,
    now: datetime | None = None,
) -> None:
    """Persist bounded failure evidence unless ownership has already moved on."""
    if claim.claim_token is None:
        return
    current = now or datetime.now(UTC)
    detail: dict[str, object] = {
        "stage": stage,
        "error_type": type(error).__name__,
        "message": str(error)[:2048],
    }
    async with session_factory() as session, session.begin():
        scope = cast(
            ArchiveScope,
            await session.scalar(
                select(ArchiveScope).where(ArchiveScope.id == claim.scope_id).with_for_update()
            ),
        )
        if scope is None or scope.claim_token != claim.claim_token:
            return
        scope.state = "failed"
        scope.failure_detail = detail
        scope.claim_token = None
        scope.claim_expires_at = None
        scope.updated_at = current
        await _append_event(
            session,
            scope_id=scope.id,
            event_type="failed",
            occurred_at=current,
            idempotency_key=(f"archive:{claim.identity_sha256}:failed:{claim.claim_token}:{stage}"),
            detail=detail,
        )


async def record_copy_verification(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    scope_id: uuid.UUID,
    copy_role: str,
    provider_kind: str,
    location: str,
    manifest_sha256: str,
    aggregate_file_sha256: str,
    total_bytes: int,
    object_count: int,
    independence_asserted: bool,
    independence_detail: str | None,
    verification_method: str,
    detail: dict[str, object],
    now: datetime | None = None,
) -> None:
    current = now or datetime.now(UTC)
    if copy_role == "secondary" and not independence_asserted:
        raise ArchiveCatalogError("secondary archive copy requires explicit independence assertion")
    async with session_factory() as session, session.begin():
        scope = cast(
            ArchiveScope,
            await session.scalar(
                select(ArchiveScope).where(ArchiveScope.id == scope_id).with_for_update()
            ),
        )
        if scope is None or scope.state not in {
            "verified",
            "independently_copied",
            "retention_eligible",
        }:
            raise ArchiveCatalogError("archive must be verified before a copy can be recorded")
        if (
            scope.manifest_sha256 != manifest_sha256
            or scope.aggregate_file_sha256 != aggregate_file_sha256
        ):
            raise ArchiveCatalogError("archive copy digest differs from canonical archive")
        statement = (
            insert(ArchiveCopyVerification)
            .values(
                archive_scope_id=scope_id,
                copy_role=copy_role,
                provider_kind=provider_kind,
                location=location,
                manifest_sha256=manifest_sha256,
                aggregate_file_sha256=aggregate_file_sha256,
                total_bytes=total_bytes,
                object_count=object_count,
                independence_asserted=independence_asserted,
                independence_detail=independence_detail,
                verification_method=verification_method,
                verified_at=current,
                detail=detail,
            )
            .on_conflict_do_nothing(constraint="uq_archive_copy_verifications_identity")
        )
        await session.execute(statement)
        existing = await session.scalar(
            select(ArchiveCopyVerification).where(
                ArchiveCopyVerification.archive_scope_id == scope_id,
                ArchiveCopyVerification.copy_role == copy_role,
                ArchiveCopyVerification.location == location,
                ArchiveCopyVerification.aggregate_file_sha256 == aggregate_file_sha256,
            )
        )
        if existing is None or existing.manifest_sha256 != manifest_sha256:
            raise ArchiveCatalogError("archive copy idempotency conflict")
        if copy_role == "secondary":
            scope.state = "independently_copied"
            scope.updated_at = current
        await _append_event(
            session,
            scope_id=scope_id,
            event_type="copy_verified",
            occurred_at=current,
            idempotency_key=(
                f"archive:{scope.archive_identity_sha256}:copy:{copy_role}:"
                f"{canonical_sha256([location, aggregate_file_sha256])}"
            ),
            detail={"role": copy_role, "provider": provider_kind, "location": location},
        )


async def evaluate_retention_eligibility(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    scope_id: uuid.UUID,
    minimum_hot_retention_days: int,
    now: datetime | None = None,
) -> dict[str, object]:
    """Calculate and persist metadata eligibility; never remove source data."""
    if minimum_hot_retention_days < 1:
        raise ValueError("minimum hot retention must be at least one day")
    current = now or datetime.now(UTC)
    policy = {
        "schema_version": 1,
        "minimum_hot_retention_days": minimum_hot_retention_days,
        "required_archive_schema_versions": sorted(SUPPORTED_ARCHIVE_SCHEMA_VERSIONS),
        "requires_primary_verified": True,
        "requires_independent_secondary_verified": True,
        "requires_analytical_reads": True,
        "requires_valid_epoch": True,
        "deletion_performed": False,
    }
    policy_sha = canonical_sha256(policy)
    async with session_factory() as session, session.begin():
        scope = cast(
            ArchiveScope,
            await session.scalar(
                select(ArchiveScope).where(ArchiveScope.id == scope_id).with_for_update()
            ),
        )
        if scope is None:
            raise ArchiveCatalogError("archive scope does not exist")
        epoch = cast(
            CollectionEpoch,
            await session.scalar(
                select(CollectionEpoch).where(CollectionEpoch.id == scope.collection_epoch_id)
            ),
        )
        current_epoch = cast(
            CollectionEpochCurrent,
            await session.scalar(
                select(CollectionEpochCurrent).where(
                    CollectionEpochCurrent.collection_epoch_id == scope.collection_epoch_id
                )
            ),
        )
        copies = list(
            (
                await session.execute(
                    select(ArchiveCopyVerification).where(
                        ArchiveCopyVerification.archive_scope_id == scope_id
                    )
                )
            ).scalars()
        )
        primary = any(row.copy_role == "primary" for row in copies)
        secondary = any(
            row.copy_role == "secondary" and row.independence_asserted for row in copies
        )
        coverage = bool((scope.verification_detail or {}).get("source_scope_fully_covered"))
        checks = {
            "archive_verified": scope.verified_at is not None,
            "manifest_verified": bool((scope.verification_detail or {}).get("manifest_verified")),
            "primary_copy_verified": primary,
            "independent_secondary_verified": secondary,
            "archive_schema_supported": (
                scope.archive_schema_version in SUPPORTED_ARCHIVE_SCHEMA_VERSIONS
            ),
            "source_scope_fully_covered": coverage,
            "no_unresolved_integrity_failure": scope.state != "failed",
            "epoch_data_valid": bool(epoch.data_valid and current_epoch.data_valid),
            "epoch_closed": current_epoch.status in {"completed", "aborted", "invalid"},
            "minimum_hot_age_passed": (
                scope.end_at <= current - timedelta(days=minimum_hot_retention_days)
            ),
            "analytical_reads_passed": scope.analytical_reads_verified_at is not None,
        }
        reasons = [name for name, passed in checks.items() if not passed]
        eligible = not reasons
        bucket = current.replace(second=0, microsecond=0)
        identity = canonical_sha256(
            [str(scope_id), bucket.isoformat(), policy_sha, checks, eligible]
        )
        await session.execute(
            insert(ArchiveRetentionEvaluation)
            .values(
                archive_scope_id=scope_id,
                evaluated_at=current,
                eligible=eligible,
                minimum_hot_retention_days=minimum_hot_retention_days,
                policy_sha256=policy_sha,
                policy_snapshot=policy,
                reasons=reasons,
                idempotency_key=identity,
            )
            .on_conflict_do_nothing(constraint="uq_archive_retention_idempotency")
        )
        if eligible:
            scope.state = "retention_eligible"
            scope.updated_at = current
        await _append_event(
            session,
            scope_id=scope_id,
            event_type="retention_evaluated",
            occurred_at=current,
            idempotency_key=f"archive:{scope.archive_identity_sha256}:retention:{identity}",
            detail={"eligible": eligible, "missing": reasons, "checks": checks},
        )
    return {
        "scope_id": str(scope_id),
        "eligible": eligible,
        "checks": checks,
        "missing": reasons,
        "minimum_hot_retention_days": minimum_hot_retention_days,
        "deletion_performed": False,
    }


async def archive_status(
    session_factory: async_sessionmaker[AsyncSession], *, epoch_number: int | None = None
) -> dict[str, object]:
    """Return compact durable archive coverage and failure state."""
    async with session_factory() as session:
        query = select(ArchiveScope, CollectionEpoch.epoch_number).join(
            CollectionEpoch, CollectionEpoch.id == ArchiveScope.collection_epoch_id
        )
        if epoch_number is not None:
            query = query.where(CollectionEpoch.epoch_number == epoch_number)
        raw_rows = (await session.execute(query.order_by(ArchiveScope.end_at.desc()))).all()
        rows = [(row[0], row[1]) for row in raw_rows]
    verified = [scope for scope, _ in rows if scope.verified_at is not None]
    failures = [row for row in rows if row[0].state == "failed"]
    return {
        "scope_count": len(rows),
        "verified_scope_count": len(verified),
        "retention_eligible_scope_count": sum(
            scope.state == "retention_eligible" for scope, _ in rows
        ),
        "archive_storage_bytes": sum(scope.parquet_bytes or 0 for scope in verified),
        "latest_archive": _scope_dict(rows[0]) if rows else None,
        "last_archive_failure": _scope_dict(failures[0]) if failures else None,
        "scopes": [_scope_dict(row) for row in rows],
        "deletion_available": False,
    }


async def _claimed_scope(session: AsyncSession, claim: ArchiveClaim) -> ArchiveScope:
    scope = cast(
        ArchiveScope,
        await session.scalar(
            select(ArchiveScope).where(ArchiveScope.id == claim.scope_id).with_for_update()
        ),
    )
    if scope is None or scope.claim_token != claim.claim_token or scope.state != "exporting":
        raise ArchiveCatalogError("archive claim is no longer owned by this worker")
    return scope


async def _append_event(
    session: AsyncSession,
    *,
    scope_id: uuid.UUID,
    event_type: str,
    occurred_at: datetime,
    idempotency_key: str,
    detail: dict[str, object],
) -> None:
    durable_key = (
        idempotency_key
        if len(idempotency_key) <= 128
        else f"archive-event:{hashlib.sha256(idempotency_key.encode()).hexdigest()}"
    )
    await session.execute(
        insert(ArchiveScopeEvent)
        .values(
            archive_scope_id=scope_id,
            event_type=event_type,
            occurred_at=occurred_at,
            idempotency_key=durable_key,
            detail=detail,
        )
        .on_conflict_do_nothing(constraint="uq_archive_scope_events_idempotency")
    )


def _scope_dict(row: tuple[ArchiveScope, int | None]) -> dict[str, object]:
    scope, epoch_number = row
    return {
        "id": str(scope.id),
        "epoch": epoch_number,
        "state": scope.state,
        "start_at": scope.start_at.isoformat(),
        "end_at": scope.end_at.isoformat(),
        "manifest_path": scope.manifest_path,
        "manifest_sha256": scope.manifest_sha256,
        "parquet_bytes": scope.parquet_bytes,
        "verified_at": scope.verified_at.isoformat() if scope.verified_at else None,
        "failure_detail": scope.failure_detail,
    }
