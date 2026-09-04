"""Repository abstractions with PostgreSQL-backed idempotent writes."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import null, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from pump_research.persistence.models import (
    ApiRequestLog,
    CollectorRun,
    CollectorRunEvent,
    DeduplicationConflict,
    DexAvailabilityTask,
    DiscoveryCheckpointState,
    DiscoveryConnectivityEvent,
    DiscoveryEvent,
    LifecycleEvent,
    LifecycleEvidenceEvaluation,
    LifecyclePolicy,
    Observation,
    Pair,
    Token,
)

_EPOCH0_ID = uuid.UUID("00000000-0000-0000-0000-000000000000")


class PairTokenMismatchError(ValueError):
    """Raised when a canonical pair is associated with another tracked token."""


class LifecyclePolicySnapshotMismatchError(ValueError):
    """Raised when one policy digest is presented with different policy content."""


class CollectorRunTerminalConflictError(RuntimeError):
    """One run was presented with conflicting terminal-state evidence."""


class CollectorRunNotRunningError(RuntimeError):
    """A collector-run lifecycle transition targeted a terminal run."""


@dataclass(slots=True)
class _LifecyclePolicyTransactionCache:
    """Policies already verified inside one database transaction."""

    transaction: object
    snapshots: dict[str, dict[str, object]]


@dataclass(frozen=True, slots=True)
class DexAvailabilityClaim:
    """One leased pending token, safe to process until its lease expires."""

    token_id: uuid.UUID
    chain: str
    address: str
    lease_id: uuid.UUID


def _normalize_utc(value: datetime | None, field_name: str) -> datetime | None:
    """Reject naïve timestamps and normalize aware values to UTC."""
    if value is None:
        return None
    if value.tzinfo is None or value.utcoffset() is None:
        msg = f"{field_name} must be timezone-aware"
        raise ValueError(msg)
    return value.astimezone(UTC)


def _normalize_timestamp_values(
    values: dict[str, object], field_names: tuple[str, ...]
) -> dict[str, object]:
    """Normalize known timestamp fields in flexible repository payloads."""
    normalized = dict(values)
    for field_name in field_names:
        value = normalized.get(field_name)
        if value is not None:
            if not isinstance(value, datetime):
                msg = f"{field_name} must be a datetime or None"
                raise TypeError(msg)
            normalized[field_name] = _normalize_utc(value, field_name)
    return normalized


@dataclass(frozen=True, slots=True)
class ObservationCreate:
    """Normalized fields for one immutable pair observation."""

    pair_id: uuid.UUID
    source_observed_at: datetime | None = None
    source_record_locator: str | None = None
    source_record_sha256: str | None = None
    price_usd: Decimal | None = None
    price_native: Decimal | None = None
    liquidity_usd: Decimal | None = None
    market_cap_usd: Decimal | None = None
    fully_diluted_valuation_usd: Decimal | None = None
    volume_m5_usd: Decimal | None = None
    volume_h1_usd: Decimal | None = None
    volume_h6_usd: Decimal | None = None
    volume_h24_usd: Decimal | None = None
    price_change_m5_pct: Decimal | None = None
    price_change_h1_pct: Decimal | None = None
    price_change_h6_pct: Decimal | None = None
    price_change_h24_pct: Decimal | None = None
    buys_m5: int | None = None
    sells_m5: int | None = None
    buys_h1: int | None = None
    sells_h1: int | None = None
    buys_h6: int | None = None
    sells_h6: int | None = None
    buys_h24: int | None = None
    sells_h24: int | None = None
    liquidity_base: Decimal | None = None
    liquidity_quote: Decimal | None = None


async def _load_by_id[T](session: AsyncSession, model: type[T], identifier: uuid.UUID) -> T:
    result = await session.execute(select(model).where(model.id == identifier))  # type: ignore[attr-defined]
    return result.scalar_one()


def _record_deduplication_conflict(
    session: AsyncSession,
    *,
    record_type: str,
    idempotency_key: str,
    occurred_at: datetime,
) -> None:
    """Preserve a rejected duplicate delivery without duplicating accepted facts."""
    session.add(
        DeduplicationConflict(
            record_type=record_type,
            idempotency_key=idempotency_key,
            occurred_at=occurred_at,
        )
    )


class TokenRepository:
    """Idempotent creation and retrieval of canonical token identities."""

    async def get_or_create(
        self,
        session: AsyncSession,
        *,
        chain: str,
        address: str,
        first_discovered_at: datetime | None,
    ) -> Token:
        first_discovered_at = _normalize_utc(first_discovered_at, "first_discovered_at")
        statement = (
            insert(Token)
            .values(chain=chain, address=address, first_discovered_at=first_discovered_at)
            .on_conflict_do_nothing(index_elements=[Token.chain, Token.address])
            .returning(Token.id)
        )
        identifier = (await session.execute(statement)).scalar_one_or_none()
        if identifier is not None:
            return await _load_by_id(session, Token, identifier)

        result = await session.execute(
            select(Token).where(Token.chain == chain, Token.address == address)
        )
        return result.scalar_one()


class PairRepository:
    """Idempotent creation of canonical pairs without assuming one pair per token."""

    async def get_or_create(
        self,
        session: AsyncSession,
        *,
        token_id: uuid.UUID,
        chain: str,
        address: str,
        dex_identifier: str | None,
        first_discovered_at: datetime | None,
    ) -> Pair:
        first_discovered_at = _normalize_utc(first_discovered_at, "first_discovered_at")
        statement = (
            insert(Pair)
            .values(
                token_id=token_id,
                chain=chain,
                address=address,
                dex_identifier=dex_identifier,
                first_discovered_at=first_discovered_at,
            )
            .on_conflict_do_nothing(index_elements=[Pair.chain, Pair.address])
            .returning(Pair.id)
        )
        identifier = (await session.execute(statement)).scalar_one_or_none()
        pair = (
            await _load_by_id(session, Pair, identifier)
            if identifier is not None
            else (
                await session.execute(
                    select(Pair).where(Pair.chain == chain, Pair.address == address)
                )
            ).scalar_one()
        )
        if pair.token_id != token_id:
            msg = "A canonical pair cannot be reassigned to a different token"
            raise PairTokenMismatchError(msg)
        return pair


class CollectorRunRepository:
    """Create and finish mutable operational collector-run records."""

    async def start(
        self,
        session: AsyncSession,
        *,
        started_at: datetime,
        collector_version: str,
        configuration_sha256: str,
        configuration_snapshot: dict[str, object],
        collection_epoch_id: uuid.UUID = _EPOCH0_ID,
    ) -> CollectorRun:
        normalized_started_at = _normalize_utc(started_at, "started_at")
        assert normalized_started_at is not None
        run = CollectorRun(
            collection_epoch_id=collection_epoch_id,
            started_at=normalized_started_at,
            status="running",
            collector_version=collector_version,
            configuration_sha256=configuration_sha256,
            configuration_snapshot=configuration_snapshot,
        )
        session.add(run)
        await session.flush()
        return run

    async def mark_collection_started(
        self,
        session: AsyncSession,
        *,
        run_id: uuid.UUID,
        collection_started_at: datetime,
    ) -> datetime:
        """Durably open the one-way live-work boundary for one running invocation.

        Retrying after an uncertain commit returns the original boundary. The
        timestamp is never replaced with a later retry time.
        """
        normalized_collection_started_at = _normalize_utc(
            collection_started_at, "collection_started_at"
        )
        assert normalized_collection_started_at is not None
        run = await session.scalar(
            select(CollectorRun).where(CollectorRun.id == run_id).with_for_update()
        )
        if run is None:
            raise LookupError(f"collector run does not exist: {run_id}")
        if run.collection_started_at is not None:
            return run.collection_started_at
        if run.status != "running":
            raise CollectorRunNotRunningError(
                f"collector run {run_id} is {run.status}; live collection cannot begin"
            )
        if normalized_collection_started_at < run.started_at:
            raise ValueError("collection_started_at cannot precede started_at")
        run.collection_started_at = normalized_collection_started_at
        await session.flush()
        return normalized_collection_started_at

    async def finish(
        self,
        session: AsyncSession,
        *,
        run_id: uuid.UUID,
        finished_at: datetime,
        status: str,
        failure_detail: dict[str, object] | None = None,
        event_type: str | None = None,
    ) -> None:
        """Atomically finalize one run and append immutable transition evidence.

        Locking the run makes repeated finalization safe across cleanup paths. A
        retry with the same terminal meaning succeeds; a contradictory terminal
        result is an explicit integrity error.
        """
        normalized_finished_at = _normalize_utc(finished_at, "finished_at")
        assert normalized_finished_at is not None
        if status not in {"stopped", "succeeded", "failed", "cancelled"}:
            raise ValueError(f"invalid terminal collector-run status: {status}")
        resolved_event_type = event_type or ("failed" if status == "failed" else "graceful_stop")
        if resolved_event_type not in {"graceful_stop", "failed", "stale_reconciled"}:
            raise ValueError(f"invalid collector-run event type: {resolved_event_type}")
        detail = failure_detail or {}
        reason = str(detail.get("reason", status))
        run = await session.scalar(
            select(CollectorRun).where(CollectorRun.id == run_id).with_for_update()
        )
        if run is None:
            raise LookupError(f"collector run does not exist: {run_id}")
        if run.status != "running":
            existing = await session.scalar(
                select(CollectorRunEvent).where(
                    CollectorRunEvent.idempotency_key == f"collector-run:{run_id}:terminal"
                )
            )
            if (
                run.status == status
                and run.failure_detail == failure_detail
                and existing is not None
                and existing.event_type == resolved_event_type
                and existing.reason == reason
                and existing.detail == detail
            ):
                return
            raise CollectorRunTerminalConflictError(
                f"collector run {run_id} is already {run.status}; refused conflicting "
                f"terminal status {status}"
            )
        await session.execute(
            update(CollectorRun)
            .where(CollectorRun.id == run_id)
            .values(
                finished_at=normalized_finished_at,
                status=status,
                failure_detail=failure_detail,
            )
        )
        session.add(
            CollectorRunEvent(
                collector_run_id=run_id,
                event_type=resolved_event_type,
                occurred_at=normalized_finished_at,
                reason=reason,
                detail=detail,
                idempotency_key=f"collector-run:{run_id}:terminal",
            )
        )
        await session.flush()


class DiscoveryEventRepository:
    """Append immutable, idempotent discovery-source evidence."""

    async def record(
        self,
        session: AsyncSession,
        **values: object,
    ) -> DiscoveryEvent:
        normalized_values = _normalize_timestamp_values(
            values,
            ("source_event_at", "received_at"),
        )
        statement = (
            insert(DiscoveryEvent)
            .values(**normalized_values)
            .on_conflict_do_nothing(index_elements=[DiscoveryEvent.idempotency_key])
            .returning(DiscoveryEvent.id)
        )
        identifier = (await session.execute(statement)).scalar_one_or_none()
        if identifier is not None:
            return await _load_by_id(session, DiscoveryEvent, identifier)

        key = normalized_values["idempotency_key"]
        received_at = normalized_values["received_at"]
        assert isinstance(key, str)
        assert isinstance(received_at, datetime)
        _record_deduplication_conflict(
            session,
            record_type="discovery_event",
            idempotency_key=key,
            occurred_at=received_at,
        )
        result = await session.execute(
            select(DiscoveryEvent).where(DiscoveryEvent.idempotency_key == key)
        )
        return result.scalar_one()


class DiscoveryCheckpointRepository:
    """Load and advance a source-owned cursor only after its batch is durable."""

    async def get(
        self,
        session: AsyncSession,
        *,
        source_name: str,
    ) -> DiscoveryCheckpointState | None:
        return await session.get(DiscoveryCheckpointState, source_name)

    async def advance(
        self,
        session: AsyncSession,
        *,
        source_name: str,
        checkpoint_value: str,
        batch_received_at: datetime,
        coverage_status: str,
        supports_replay: bool,
        coverage_note: str | None,
    ) -> DiscoveryCheckpointState:
        normalized_received_at = _normalize_utc(batch_received_at, "batch_received_at")
        assert normalized_received_at is not None
        statement = (
            insert(DiscoveryCheckpointState)
            .values(
                source_name=source_name,
                checkpoint_value=checkpoint_value,
                last_batch_received_at=normalized_received_at,
                coverage_status=coverage_status,
                supports_replay=supports_replay,
                coverage_note=coverage_note,
                updated_at=normalized_received_at,
            )
            .on_conflict_do_update(
                index_elements=[DiscoveryCheckpointState.source_name],
                set_={
                    "checkpoint_value": checkpoint_value,
                    "last_batch_received_at": normalized_received_at,
                    "coverage_status": coverage_status,
                    "supports_replay": supports_replay,
                    "coverage_note": coverage_note,
                    "updated_at": normalized_received_at,
                },
                where=(DiscoveryCheckpointState.last_batch_received_at <= normalized_received_at),
            )
            .returning(DiscoveryCheckpointState.source_name)
        )
        advanced_source = (await session.execute(statement)).scalar_one_or_none()
        state = await session.get(DiscoveryCheckpointState, source_name)
        if state is None:
            msg = "Discovery checkpoint was not found after advancement"
            raise RuntimeError(msg)
        if advanced_source is None and state.last_batch_received_at > normalized_received_at:
            return state
        return state


class DiscoveryConnectivityEventRepository:
    """Append idempotent stream connectivity facts without provider coupling."""

    async def record(self, session: AsyncSession, **values: object) -> None:
        normalized_values = _normalize_timestamp_values(values, ("observed_at",))
        gap_id = normalized_values.get("gap_id")
        if isinstance(gap_id, str):
            normalized_values["gap_id"] = uuid.UUID(gap_id)
        await session.execute(
            insert(DiscoveryConnectivityEvent)
            .values(**normalized_values)
            .on_conflict_do_nothing(index_elements=[DiscoveryConnectivityEvent.idempotency_key])
        )


class DexAvailabilityTaskRepository:
    """Durable projection and leases for tokens awaiting first DEX presence."""

    async def create_pending_if_absent(
        self,
        session: AsyncSession,
        *,
        token_id: uuid.UUID,
        due_at: datetime,
    ) -> tuple[DexAvailabilityTask, bool]:
        normalized_due_at = _normalize_utc(due_at, "due_at")
        assert normalized_due_at is not None
        statement = (
            insert(DexAvailabilityTask)
            .values(
                token_id=token_id,
                state="PENDING_DEX",
                next_check_at=normalized_due_at,
                attempt_count=0,
                updated_at=normalized_due_at,
            )
            .on_conflict_do_nothing(index_elements=[DexAvailabilityTask.token_id])
            .returning(DexAvailabilityTask.token_id)
        )
        inserted_token_id = (await session.execute(statement)).scalar_one_or_none()
        task = await session.get(DexAvailabilityTask, token_id)
        if task is None:
            msg = "DEX availability task was not found after creation"
            raise RuntimeError(msg)
        return task, inserted_token_id is not None

    async def claim_due(
        self,
        session: AsyncSession,
        *,
        now: datetime,
        limit: int,
        lease_duration: timedelta,
    ) -> list[DexAvailabilityClaim]:
        """Lease due tasks; expired leases are deliberately reclaimable after crashes."""
        normalized_now = _normalize_utc(now, "now")
        assert normalized_now is not None
        if limit < 1:
            msg = "limit must be at least one"
            raise ValueError(msg)
        if lease_duration <= timedelta(0):
            msg = "lease_duration must be positive"
            raise ValueError(msg)

        result = await session.execute(
            select(DexAvailabilityTask, Token)
            .join(Token, Token.id == DexAvailabilityTask.token_id)
            .where(
                DexAvailabilityTask.state == "PENDING_DEX",
                DexAvailabilityTask.next_check_at <= normalized_now,
                or_(
                    DexAvailabilityTask.lease_id.is_(None),
                    DexAvailabilityTask.lease_expires_at <= normalized_now,
                ),
            )
            .order_by(DexAvailabilityTask.next_check_at, DexAvailabilityTask.token_id)
            .limit(limit)
            .with_for_update(skip_locked=True, of=DexAvailabilityTask)
        )
        rows = result.all()
        if not rows:
            return []

        lease_id = uuid.uuid4()
        expires_at = normalized_now + lease_duration
        claims: list[DexAvailabilityClaim] = []
        for task, token in rows:
            task.lease_id = lease_id
            task.lease_expires_at = expires_at
            task.updated_at = normalized_now
            claims.append(
                DexAvailabilityClaim(
                    token_id=task.token_id,
                    chain=token.chain,
                    address=token.address,
                    lease_id=lease_id,
                )
            )
        await session.flush()
        return claims

    async def complete_with_retry(
        self,
        session: AsyncSession,
        *,
        token_ids: list[uuid.UUID],
        lease_id: uuid.UUID,
        checked_at: datetime,
        retry_at: datetime,
    ) -> list[DexAvailabilityTask]:
        """Release an owned lease while retaining tokens for a later DEX check."""
        return await self._complete(
            session,
            token_ids=token_ids,
            lease_id=lease_id,
            checked_at=checked_at,
            next_state="PENDING_DEX",
            next_check_at=retry_at,
        )

    async def complete_as_new(
        self,
        session: AsyncSession,
        *,
        token_ids: list[uuid.UUID],
        lease_id: uuid.UUID,
        checked_at: datetime,
    ) -> list[DexAvailabilityTask]:
        """Release an owned lease and mark a token as DEX-present."""
        return await self._complete(
            session,
            token_ids=token_ids,
            lease_id=lease_id,
            checked_at=checked_at,
            next_state="NEW",
            next_check_at=checked_at,
        )

    async def get(self, session: AsyncSession, *, token_id: uuid.UUID) -> DexAvailabilityTask:
        """Load the current projection for one token."""
        task = await session.get(DexAvailabilityTask, token_id)
        if task is None:
            msg = "DEX availability task was not found"
            raise LookupError(msg)
        return task

    async def _complete(
        self,
        session: AsyncSession,
        *,
        token_ids: list[uuid.UUID],
        lease_id: uuid.UUID,
        checked_at: datetime,
        next_state: str,
        next_check_at: datetime,
    ) -> list[DexAvailabilityTask]:
        normalized_checked_at = _normalize_utc(checked_at, "checked_at")
        normalized_next_check_at = _normalize_utc(next_check_at, "next_check_at")
        assert normalized_checked_at is not None
        assert normalized_next_check_at is not None
        if not token_ids:
            return []
        result = await session.execute(
            select(DexAvailabilityTask)
            .where(
                DexAvailabilityTask.token_id.in_(token_ids),
                DexAvailabilityTask.lease_id == lease_id,
                DexAvailabilityTask.state == "PENDING_DEX",
            )
            .with_for_update()
        )
        tasks = list(result.scalars())
        if len(tasks) != len(token_ids):
            msg = "DEX availability lease is no longer owned by this worker"
            raise RuntimeError(msg)
        for task in tasks:
            task.state = next_state
            task.next_check_at = normalized_next_check_at
            task.last_checked_at = normalized_checked_at
            task.attempt_count += 1
            task.lease_id = None
            task.lease_expires_at = None
            task.updated_at = normalized_checked_at
        await session.flush()
        return tasks


class ApiRequestLogRepository:
    """Append immutable, idempotent request evidence and raw batch responses."""

    async def record(self, session: AsyncSession, **values: object) -> ApiRequestLog:
        normalized_values = _normalize_timestamp_values(values, ("requested_at", "received_at"))
        statement = (
            insert(ApiRequestLog)
            .values(**normalized_values)
            .on_conflict_do_nothing(index_elements=[ApiRequestLog.idempotency_key])
            .returning(ApiRequestLog.id)
        )
        identifier = (await session.execute(statement)).scalar_one_or_none()
        if identifier is not None:
            return await _load_by_id(session, ApiRequestLog, identifier)

        key = normalized_values["idempotency_key"]
        requested_at = normalized_values["requested_at"]
        assert isinstance(key, str)
        assert isinstance(requested_at, datetime)
        _record_deduplication_conflict(
            session,
            record_type="api_request",
            idempotency_key=key,
            occurred_at=requested_at,
        )
        result = await session.execute(
            select(ApiRequestLog).where(ApiRequestLog.idempotency_key == key)
        )
        return result.scalar_one()


class ObservationRepository:
    """Append normalized facts without deduplicating unchanged market values."""

    async def record_many(
        self,
        session: AsyncSession,
        *,
        api_request: ApiRequestLog,
        observations: list[ObservationCreate],
    ) -> int:
        if api_request.received_at is None:
            msg = "Observations require an API request with a received_at timestamp"
            raise ValueError(msg)
        if not observations:
            return 0

        values = [
            {
                "received_at": api_request.received_at,
                "api_request_log_id": api_request.id,
                **asdict(
                    replace(
                        observation,
                        source_observed_at=_normalize_utc(
                            observation.source_observed_at,
                            "source_observed_at",
                        ),
                    )
                ),
            }
            for observation in observations
        ]
        statement = (
            insert(Observation)
            .values(values)
            .on_conflict_do_nothing(
                index_elements=[
                    Observation.received_at,
                    Observation.api_request_log_id,
                    Observation.pair_id,
                ]
            )
            .returning(Observation.pair_id)
        )
        inserted_pair_ids = set((await session.execute(statement)).scalars().all())
        inserted = len(inserted_pair_ids)
        if inserted != len(values):
            for value in values:
                if value["pair_id"] in inserted_pair_ids:
                    continue
                digest = hashlib.sha256(
                    ":".join(
                        (
                            "observation",
                            api_request.received_at.isoformat(),
                            str(api_request.id),
                            str(value["pair_id"]),
                        )
                    ).encode()
                ).hexdigest()
                _record_deduplication_conflict(
                    session,
                    record_type="observation",
                    idempotency_key=digest,
                    occurred_at=api_request.received_at,
                )
        return inserted


class LifecycleEvidenceEvaluationRepository:
    """Append one reconstructable lifecycle-evidence selection per token response."""

    def __init__(self) -> None:
        self._policies = LifecyclePolicyRepository()

    async def record(
        self,
        session: AsyncSession,
        *,
        input_watermark: datetime,
        token_id: uuid.UUID,
        api_request_log_id: uuid.UUID,
        outcome: str,
        selected_pair_id: uuid.UUID | None,
        selected_observation_id: uuid.UUID | None,
        selected_observation_received_at: datetime | None,
        reason_code: str,
        reason_detail: dict[str, object],
        policy_sha256: str,
        policy_snapshot: dict[str, object],
    ) -> LifecycleEvidenceEvaluation:
        normalized_watermark = _normalize_utc(input_watermark, "input_watermark")
        normalized_observation_at = _normalize_utc(
            selected_observation_received_at,
            "selected_observation_received_at",
        )
        assert normalized_watermark is not None
        await self._policies.ensure(
            session,
            policy_sha256=policy_sha256,
            policy_snapshot=policy_snapshot,
        )
        conflict_columns = [
            LifecycleEvidenceEvaluation.input_watermark,
            LifecycleEvidenceEvaluation.token_id,
            LifecycleEvidenceEvaluation.api_request_log_id,
            LifecycleEvidenceEvaluation.policy_sha256,
        ]
        statement = (
            insert(LifecycleEvidenceEvaluation)
            .values(
                input_watermark=normalized_watermark,
                token_id=token_id,
                api_request_log_id=api_request_log_id,
                outcome=outcome,
                selected_pair_id=selected_pair_id,
                selected_observation_id=selected_observation_id,
                selected_observation_received_at=normalized_observation_at,
                reason_code=reason_code,
                reason_detail=reason_detail,
                policy_sha256=policy_sha256,
                # JSONB encodes Python None as JSON `null` by default. Emit an
                # explicit SQL NULL so normalized-only rows satisfy IS NULL.
                policy_snapshot=null(),
            )
            .on_conflict_do_nothing(index_elements=conflict_columns)
            .returning(LifecycleEvidenceEvaluation.id)
        )
        identifier = (await session.execute(statement)).scalar_one_or_none()
        if identifier is not None:
            result = await session.execute(
                select(LifecycleEvidenceEvaluation).where(
                    LifecycleEvidenceEvaluation.id == identifier,
                    LifecycleEvidenceEvaluation.input_watermark == normalized_watermark,
                )
            )
            return result.scalar_one()
        result = await session.execute(
            select(LifecycleEvidenceEvaluation).where(
                LifecycleEvidenceEvaluation.input_watermark == normalized_watermark,
                LifecycleEvidenceEvaluation.token_id == token_id,
                LifecycleEvidenceEvaluation.api_request_log_id == api_request_log_id,
                LifecycleEvidenceEvaluation.policy_sha256 == policy_sha256,
            )
        )
        return result.scalar_one()

    async def resolve_policy_snapshot(
        self,
        session: AsyncSession,
        evaluation: LifecycleEvidenceEvaluation,
    ) -> dict[str, object]:
        """Resolve and validate the exact immutable policy used by an evaluation."""
        policy = await session.get(LifecyclePolicy, evaluation.policy_sha256)
        if policy is None:
            msg = (
                "Lifecycle evidence references a missing normalized policy: "
                f"{evaluation.policy_sha256}"
            )
            raise LookupError(msg)
        if (
            evaluation.policy_snapshot is not None
            and evaluation.policy_snapshot != policy.policy_snapshot
        ):
            raise LifecyclePolicySnapshotMismatchError(evaluation.policy_sha256)
        return dict(policy.policy_snapshot)


class LifecyclePolicyRepository:
    """Idempotently persist one immutable document for each policy digest."""

    _SESSION_CACHE_KEY = "pump_research.lifecycle_policy_transaction_cache"

    async def ensure(
        self,
        session: AsyncSession,
        *,
        policy_sha256: str,
        policy_snapshot: dict[str, object],
    ) -> None:
        transaction = session.get_transaction()
        cached = session.info.get(self._SESSION_CACHE_KEY)
        if (
            transaction is not None
            and isinstance(cached, _LifecyclePolicyTransactionCache)
            and cached.transaction is transaction
        ):
            cached_snapshot = cached.snapshots.get(policy_sha256)
            if cached_snapshot is not None:
                if cached_snapshot != policy_snapshot:
                    raise LifecyclePolicySnapshotMismatchError(policy_sha256)
                return

        statement = (
            insert(LifecyclePolicy)
            .values(
                policy_sha256=policy_sha256,
                policy_snapshot=policy_snapshot,
            )
            .on_conflict_do_nothing(index_elements=[LifecyclePolicy.policy_sha256])
        )
        await session.execute(statement)
        policy = await session.get(LifecyclePolicy, policy_sha256)
        if policy is None:
            msg = f"Lifecycle policy insert did not resolve: {policy_sha256}"
            raise RuntimeError(msg)
        if policy.policy_snapshot != policy_snapshot:
            raise LifecyclePolicySnapshotMismatchError(policy_sha256)
        current_transaction = session.get_transaction()
        if current_transaction is None:
            msg = "Lifecycle policy write completed without an active transaction"
            raise RuntimeError(msg)
        cached = session.info.get(self._SESSION_CACHE_KEY)
        if (
            not isinstance(cached, _LifecyclePolicyTransactionCache)
            or cached.transaction is not current_transaction
        ):
            cached = _LifecyclePolicyTransactionCache(current_transaction, {})
            session.info[self._SESSION_CACHE_KEY] = cached
        cached.snapshots[policy_sha256] = dict(policy_snapshot)


class LifecycleEventRepository:
    """Append derived lifecycle transitions with durable idempotency."""

    async def record(self, session: AsyncSession, **values: object) -> LifecycleEvent:
        normalized_values = _normalize_timestamp_values(
            values,
            ("decided_at", "input_watermark"),
        )
        statement = (
            insert(LifecycleEvent)
            .values(**normalized_values)
            .on_conflict_do_nothing(index_elements=[LifecycleEvent.idempotency_key])
            .returning(LifecycleEvent.id)
        )
        identifier = (await session.execute(statement)).scalar_one_or_none()
        if identifier is not None:
            return await _load_by_id(session, LifecycleEvent, identifier)

        key = normalized_values["idempotency_key"]
        decided_at = normalized_values["decided_at"]
        assert isinstance(key, str)
        assert isinstance(decided_at, datetime)
        _record_deduplication_conflict(
            session,
            record_type="lifecycle_event",
            idempotency_key=key,
            occurred_at=decided_at,
        )
        result = await session.execute(
            select(LifecycleEvent).where(LifecycleEvent.idempotency_key == key)
        )
        return result.scalar_one()
