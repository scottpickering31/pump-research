"""Bounded PostgreSQL-backed adaptive polling scheduler."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from enum import StrEnum

from sqlalchemy import func, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.market_data.dexscreener import DEX_SCREENER_PROVIDER
from pump_research.persistence.models import (
    PollBatch,
    PollBatchMember,
    PollBatchOutcome,
    PollSchedule,
    PollScheduleDecision,
    Token,
)
from pump_research.persistence.repositories import _normalize_utc
from pump_research.scheduling.clock import Clock, SystemClock
from pump_research.scheduling.policy import AdaptivePollingPolicy, LifecycleState

_CLAIM_ADVISORY_LOCK_ID = 7_428_901_163


class PollOutcome(StrEnum):
    """Persisted outcomes for one scheduler batch."""

    SUCCEEDED = "succeeded"
    EMPTY = "empty"
    PARTIAL = "partial"
    FAILED = "failed"
    THROTTLED = "throttled"
    MALFORMED = "malformed"
    CANCELLED = "cancelled"


class LostPollLeaseError(RuntimeError):
    """A worker attempted to complete work after losing its durable lease."""


@dataclass(frozen=True, slots=True)
class PollMemberClaim:
    """One due token included in a bounded poll batch."""

    token_id: uuid.UUID
    address: str
    lifecycle_state: LifecycleState
    due_at: datetime
    claim_lateness_ms: int
    previous_batch_id: uuid.UUID | None


@dataclass(frozen=True, slots=True)
class PollBatchClaim:
    """A single-chain, API-eligible batch returned without an in-memory queue."""

    batch_id: uuid.UUID
    chain: str
    claimed_at: datetime
    lease_expires_at: datetime
    members: tuple[PollMemberClaim, ...]

    @property
    def token_addresses(self) -> tuple[str, ...]:
        """Return the exact deduplicated request order for the DEX client."""
        return tuple(member.address for member in self.members)


@dataclass(frozen=True, slots=True)
class PollCompletion:
    """Persisted completion and observation-lateness measurements."""

    batch_id: uuid.UUID
    outcome: PollOutcome
    completed_at: datetime
    member_count: int
    observation_lateness_min_ms: int
    observation_lateness_max_ms: int
    observation_lateness_mean_ms: Decimal


class AdaptiveScheduler:
    """Plan adaptive token polls using durable schedules and expiring leases."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._session_factory = session_factory
        self.policy = AdaptivePollingPolicy.from_settings(settings)
        self._clock = clock or SystemClock()

    async def set_lifecycle_state(
        self,
        *,
        token_id: uuid.UUID,
        state: LifecycleState,
        decided_at: datetime | None = None,
        reason_code: str = "lifecycle_state_applied",
    ) -> PollSchedule:
        """Apply one state to future polls and preserve the scheduling decision."""
        effective_at = decided_at or self._clock.now()
        async with self._session_factory() as session, session.begin():
            return await self.set_lifecycle_state_in_session(
                session,
                token_id=token_id,
                state=state,
                decided_at=effective_at,
                reason_code=reason_code,
            )

    async def set_lifecycle_state_in_session(
        self,
        session: AsyncSession,
        *,
        token_id: uuid.UUID,
        state: LifecycleState,
        decided_at: datetime,
        reason_code: str,
    ) -> PollSchedule:
        """Transactional form used when a lifecycle transition is persisted nearby."""
        normalized_decided_at = _normalize_utc(decided_at, "decided_at")
        assert normalized_decided_at is not None
        # The token exists before its schedule. Locking it closes the otherwise
        # unavoidable check-then-insert race between first-time scheduler writers.
        (
            await session.execute(
                select(Token.id).where(Token.id == token_id).with_for_update()
            )
        ).scalar_one()
        task = (
            await session.execute(
                select(PollSchedule)
                .where(PollSchedule.token_id == token_id)
                .with_for_update()
            )
        ).scalar_one_or_none()
        if task is not None and normalized_decided_at < task.state_decided_at:
            return task
        if (
            task is not None
            and normalized_decided_at == task.state_decided_at
            and task.lifecycle_state != state.value
        ):
            msg = "Conflicting lifecycle states cannot share the same decision timestamp"
            raise ValueError(msg)
        if (
            task is not None
            and task.lifecycle_state == state.value
            and task.configuration_sha256 == self.policy.sha256
        ):
            # Repeated evidence for the current state is not a transition. Moving
            # its due time would allow a busy event stream to postpone polling forever.
            return task

        previous_state = task.lifecycle_state if task is not None else None
        previous_due_at = task.next_due_at if task is not None else None
        next_due_at = normalized_decided_at + self.policy.interval_for(state)
        if task is None:
            task = PollSchedule(
                token_id=token_id,
                lifecycle_state=state.value,
                state_decided_at=normalized_decided_at,
                priority=self.policy.priority_for(state),
                next_due_at=next_due_at,
                attempt_count=0,
                configuration_sha256=self.policy.sha256,
                configuration_snapshot=self.policy.snapshot,
                updated_at=normalized_decided_at,
            )
            session.add(task)
        else:
            task.lifecycle_state = state.value
            task.state_decided_at = normalized_decided_at
            task.priority = self.policy.priority_for(state)
            task.next_due_at = next_due_at
            task.configuration_sha256 = self.policy.sha256
            task.configuration_snapshot = self.policy.snapshot
            task.updated_at = normalized_decided_at

        await session.execute(
            insert(PollScheduleDecision)
            .values(
                token_id=token_id,
                idempotency_key=_decision_idempotency_key(
                    token_id=token_id,
                    state=state,
                    decided_at=normalized_decided_at,
                    policy_sha256=self.policy.sha256,
                    reason_code=reason_code,
                ),
                previous_state=previous_state,
                new_state=state.value,
                previous_due_at=previous_due_at,
                new_due_at=next_due_at,
                decided_at=normalized_decided_at,
                reason_code=reason_code,
                configuration_sha256=self.policy.sha256,
                configuration_snapshot=self.policy.snapshot,
            )
            .on_conflict_do_nothing(index_elements=[PollScheduleDecision.idempotency_key])
        )
        await session.flush()
        return task

    async def claim_next_batch(self) -> PollBatchClaim | None:
        """Claim one bounded batch, subject to fairness, capacity, and request budget."""
        now = self._normalized_now()
        async with self._session_factory() as session, session.begin():
            await session.execute(select(func.pg_advisory_xact_lock(_CLAIM_ADVISORY_LOCK_ID)))
            if not await self._has_batch_capacity(session, now=now):
                return None

            eligibility = (
                PollSchedule.next_due_at <= now,
                or_(
                    PollSchedule.lease_id.is_(None),
                    PollSchedule.lease_expires_at <= now,
                ),
            )
            head = (
                await session.execute(
                    select(Token.chain)
                    .join(PollSchedule, PollSchedule.token_id == Token.id)
                    .where(*eligibility)
                    .order_by(
                        PollSchedule.next_due_at,
                        PollSchedule.priority,
                        PollSchedule.token_id,
                    )
                    .limit(1)
                    .with_for_update(skip_locked=True, of=PollSchedule)
                )
            ).scalar_one_or_none()
            if head is None:
                return None

            rows = (
                await session.execute(
                    select(PollSchedule, Token)
                    .join(Token, Token.id == PollSchedule.token_id)
                    .where(Token.chain == head, *eligibility)
                    .order_by(
                        PollSchedule.next_due_at,
                        PollSchedule.priority,
                        PollSchedule.token_id,
                    )
                    .limit(self.policy.batch_size)
                    .with_for_update(skip_locked=True, of=PollSchedule)
                )
            ).all()
            if not rows:
                return None

            batch = PollBatch(
                provider=DEX_SCREENER_PROVIDER,
                chain=head,
                claimed_at=now,
                lease_expires_at=now + self.policy.lease_duration,
                reserved_request_capacity=self.policy.requests_reserved_per_batch,
                configuration_sha256=self.policy.sha256,
                configuration_snapshot=self.policy.snapshot,
            )
            session.add(batch)
            await session.flush()

            members: list[PollMemberClaim] = []
            for task, token in rows:
                previous_batch_id = task.lease_id
                lateness_ms = _lateness_ms(observed_at=now, due_at=task.next_due_at)
                session.add(
                    PollBatchMember(
                        claimed_at=now,
                        batch_id=batch.id,
                        token_id=task.token_id,
                        due_at=task.next_due_at,
                        lifecycle_state=task.lifecycle_state,
                        priority=task.priority,
                        claim_lateness_ms=lateness_ms,
                        previous_batch_id=previous_batch_id,
                    )
                )
                task.lease_id = batch.id
                task.lease_expires_at = batch.lease_expires_at
                task.last_started_at = now
                task.updated_at = now
                members.append(
                    PollMemberClaim(
                        token_id=task.token_id,
                        address=token.address,
                        lifecycle_state=LifecycleState(task.lifecycle_state),
                        due_at=task.next_due_at,
                        claim_lateness_ms=lateness_ms,
                        previous_batch_id=previous_batch_id,
                    )
                )
            await session.flush()
            return PollBatchClaim(
                batch_id=batch.id,
                chain=batch.chain,
                claimed_at=batch.claimed_at,
                lease_expires_at=batch.lease_expires_at,
                members=tuple(members),
            )

    async def complete_batch(
        self,
        *,
        batch_id: uuid.UUID,
        outcome: PollOutcome,
        api_request_log_id: uuid.UUID | None = None,
        failure_detail: dict[str, object] | None = None,
    ) -> PollCompletion:
        """Complete an owned batch atomically and schedule each member's next poll."""
        completed_at = self._normalized_now()
        async with self._session_factory() as session, session.begin():
            batch = (
                await session.execute(
                    select(PollBatch).where(PollBatch.id == batch_id).with_for_update()
                )
            ).scalar_one()
            existing = await session.get(PollBatchOutcome, batch_id)
            if existing is not None:
                return _completion_from_model(existing)

            members = list(
                (
                    await session.execute(
                        select(PollBatchMember)
                        .where(
                            PollBatchMember.batch_id == batch_id,
                            PollBatchMember.claimed_at == batch.claimed_at,
                        )
                        .order_by(PollBatchMember.priority, PollBatchMember.token_id)
                    )
                ).scalars()
            )
            schedules = list(
                (
                    await session.execute(
                        select(PollSchedule)
                        .where(
                            PollSchedule.token_id.in_([member.token_id for member in members])
                        )
                        .with_for_update()
                    )
                ).scalars()
            )
            schedule_by_token = {schedule.token_id: schedule for schedule in schedules}
            if (
                completed_at >= batch.lease_expires_at
                or len(schedule_by_token) != len(members)
                or any(
                    schedule_by_token[member.token_id].lease_id != batch_id
                    for member in members
                )
            ):
                msg = "Poll batch lease expired or was reclaimed by another worker"
                raise LostPollLeaseError(msg)

            lateness_values = [
                _lateness_ms(observed_at=completed_at, due_at=member.due_at)
                for member in members
            ]
            mean_lateness = Decimal(sum(lateness_values)) / Decimal(len(lateness_values))
            completion_model = PollBatchOutcome(
                batch_id=batch_id,
                api_request_log_id=api_request_log_id,
                outcome=outcome.value,
                completed_at=completed_at,
                member_count=len(members),
                observation_lateness_min_ms=min(lateness_values),
                observation_lateness_max_ms=max(lateness_values),
                observation_lateness_mean_ms=mean_lateness,
                failure_detail=failure_detail,
                configuration_sha256=self.policy.sha256,
                configuration_snapshot=self.policy.snapshot,
            )
            session.add(completion_model)
            member_by_token = {member.token_id: member for member in members}
            for token_id, task in schedule_by_token.items():
                state = LifecycleState(task.lifecycle_state)
                task.last_due_at = member_by_token[token_id].due_at
                task.last_completed_at = completed_at
                task.attempt_count += 1
                task.next_due_at = completed_at + self.policy.interval_for(state)
                task.priority = self.policy.priority_for(state)
                task.lease_id = None
                task.lease_expires_at = None
                task.configuration_sha256 = self.policy.sha256
                task.configuration_snapshot = self.policy.snapshot
                task.updated_at = completed_at
            await session.flush()
            return _completion_from_model(completion_model)

    async def _has_batch_capacity(self, session: AsyncSession, *, now: datetime) -> bool:
        in_flight = await session.scalar(
            select(func.count(PollBatch.id))
            .outerjoin(PollBatchOutcome, PollBatchOutcome.batch_id == PollBatch.id)
            .where(
                PollBatchOutcome.batch_id.is_(None),
                PollBatch.lease_expires_at > now,
            )
        )
        if int(in_flight or 0) >= self.policy.max_in_flight_batches:
            return False

        reserved = await session.scalar(
            select(func.coalesce(func.sum(PollBatch.reserved_request_capacity), 0)).where(
                PollBatch.provider == DEX_SCREENER_PROVIDER,
                PollBatch.claimed_at > now - timedelta(minutes=1),
                PollBatch.claimed_at <= now,
            )
        )
        return (
            int(reserved or 0) + self.policy.requests_reserved_per_batch
            <= self.policy.request_budget_per_minute
        )

    def _normalized_now(self) -> datetime:
        now = _normalize_utc(self._clock.now(), "clock.now")
        assert now is not None
        return now


def _decision_idempotency_key(
    *,
    token_id: uuid.UUID,
    state: LifecycleState,
    decided_at: datetime,
    policy_sha256: str,
    reason_code: str,
) -> str:
    encoded = ":".join(
        (
            "adaptive_scheduler",
            str(token_id),
            state.value,
            decided_at.isoformat(),
            policy_sha256,
            reason_code,
        )
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _lateness_ms(*, observed_at: datetime, due_at: datetime) -> int:
    return max(0, int((observed_at - due_at).total_seconds() * 1_000))


def _completion_from_model(model: PollBatchOutcome) -> PollCompletion:
    return PollCompletion(
        batch_id=model.batch_id,
        outcome=PollOutcome(model.outcome),
        completed_at=model.completed_at,
        member_count=model.member_count,
        observation_lateness_min_ms=model.observation_lateness_min_ms,
        observation_lateness_max_ms=model.observation_lateness_max_ms,
        observation_lateness_mean_ms=model.observation_lateness_mean_ms,
    )
