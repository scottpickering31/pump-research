"""Bounded PostgreSQL-backed adaptive polling scheduler."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import cast

import structlog
from sqlalchemy import Select, func, nullsfirst, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.sql.elements import ColumnElement

from pump_research.config import Settings
from pump_research.market_data.dexscreener import DEX_SCREENER_PROVIDER
from pump_research.persistence.models import (
    CollectorRun,
    CoverageDecision,
    CoveragePolicy,
    LifecycleEvent,
    PollBatch,
    PollBatchMember,
    PollBatchOutcome,
    PollSchedule,
    PollScheduleDecision,
    SchedulerCapacityDecision,
    SchedulerPolicy,
    Token,
)
from pump_research.persistence.repositories import _normalize_utc
from pump_research.scheduling.capacity import CapacityMode, CapacityPlan, plan_capacity
from pump_research.scheduling.clock import Clock, SystemClock
from pump_research.scheduling.locks import lock_schedule_token_fk_path
from pump_research.scheduling.policy import (
    AdaptivePollingPolicy,
    CapacityTier,
    CoverageClass,
    LifecycleState,
)


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


class SchedulerCapacityDecisionIntegrityError(RuntimeError):
    """One durable capacity identity maps to non-equivalent decision content."""


class CoverageReconstructionError(RuntimeError):
    """A legacy schedule cannot be mapped without guessing its admission fact."""


class CoverageTransitionProgressError(RuntimeError):
    """A due coverage transition remained due after its transactional refresh."""


@dataclass(frozen=True, slots=True)
class PollMemberClaim:
    """One due token included in a bounded poll batch."""

    token_id: uuid.UUID
    address: str
    lifecycle_state: LifecycleState
    coverage_class: CoverageClass
    due_at: datetime
    claim_lateness_ms: int
    previous_batch_id: uuid.UUID | None
    capacity_decision_id: uuid.UUID
    target_interval_seconds: int
    effective_interval_seconds: int


@dataclass(frozen=True, slots=True)
class PollBatchClaim:
    """A single-chain, API-eligible batch returned without an in-memory queue."""

    batch_id: uuid.UUID
    chain: str
    claimed_at: datetime
    lease_expires_at: datetime
    capacity_decision_id: uuid.UUID
    batch_kind: str
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


@dataclass(frozen=True, slots=True)
class _CapacityDecision:
    id: uuid.UUID
    decided_at: datetime
    plan: CapacityPlan


def _legacy_admissions_statement() -> Select[tuple[uuid.UUID, datetime]]:
    """Resolve the legacy population relationally, without a per-token bind list."""
    return (
        select(
            LifecycleEvent.token_id,
            func.min(LifecycleEvent.input_watermark),
        )
        .join(
            PollSchedule,
            PollSchedule.token_id == LifecycleEvent.token_id,
        )
        .where(
            PollSchedule.admitted_at.is_(None),
            LifecycleEvent.new_state == LifecycleState.NEW.value,
        )
        .group_by(LifecycleEvent.token_id)
    )


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
        self._logger = structlog.get_logger("pump_research.scheduling.capacity")
        self._cached_capacity_bucket: datetime | None = None
        self._cached_capacity_decision: _CapacityDecision | None = None
        self._last_logged_capacity_decision_id: uuid.UUID | None = None
        self._validated_policy = False
        self._validated_coverage_policy = False

    async def set_lifecycle_state(
        self,
        *,
        token_id: uuid.UUID,
        state: LifecycleState,
        decided_at: datetime | None = None,
        admitted_at: datetime | None = None,
        collector_run_id: uuid.UUID | None = None,
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
                admitted_at=admitted_at,
                collector_run_id=collector_run_id,
                reason_code=reason_code,
            )

    async def initialize_epoch_in_session(
        self,
        session: AsyncSession,
        *,
        collection_epoch_id: uuid.UUID,
        epoch_number: int,
        started_at: datetime,
    ) -> int:
        """Reconstruct legacy coverage and deterministically rebase one epoch."""
        normalized_started_at = _normalize_utc(started_at, "started_at")
        assert normalized_started_at is not None
        await lock_schedule_token_fk_path(session, exclusive=False)
        await self._persist_coverage_policy(session)
        schedules = list(
            (
                await session.execute(
                    select(PollSchedule).order_by(PollSchedule.token_id).with_for_update()
                )
            ).scalars()
        )
        missing = [schedule.token_id for schedule in schedules if schedule.admitted_at is None]
        admission_by_token: dict[uuid.UUID, datetime] = {}
        if missing:
            admission_by_token = {
                token_id: admitted_at
                for token_id, admitted_at in (
                    await session.execute(_legacy_admissions_statement())
                ).all()
            }
            unresolved = sorted(
                str(token_id) for token_id in missing if token_id not in admission_by_token
            )
            if unresolved:
                raise CoverageReconstructionError(
                    "legacy schedules lack an immutable NEW admission event: "
                    + ", ".join(unresolved[:10])
                )

        previous: dict[uuid.UUID, tuple[str | None, datetime | None, str]] = {}
        for schedule in schedules:
            state = LifecycleState(schedule.lifecycle_state)
            admitted_at = schedule.admitted_at or admission_by_token[schedule.token_id]
            previous[schedule.token_id] = (
                schedule.coverage_class,
                schedule.next_due_at,
                "epoch_start_legacy_reconstruction"
                if schedule.coverage_class is None
                else "epoch_start_rebase",
            )
            coverage = self.policy.coverage_class_for(
                state,
                admitted_at=admitted_at,
                state_decided_at=schedule.state_decided_at,
                at=normalized_started_at,
            )
            schedule.admitted_at = admitted_at
            schedule.coverage_class = coverage.value
            schedule.coverage_decided_at = normalized_started_at
            schedule.coverage_next_transition_at = self.policy.next_transition_at(
                state,
                admitted_at=admitted_at,
                state_decided_at=schedule.state_decided_at,
                at=normalized_started_at,
            )
            schedule.coverage_policy_sha256 = self.policy.coverage_sha256
            schedule.priority = self.policy.priority_for_coverage(coverage)
            schedule.next_due_at = None
            schedule.lease_id = None
            schedule.lease_expires_at = None
            schedule.updated_at = normalized_started_at
        await session.flush()

        capacity = await self._capacity_decision(session, now=normalized_started_at)
        for schedule in schedules:
            assert schedule.coverage_class is not None
            coverage = CoverageClass(schedule.coverage_class)
            target_interval_seconds = capacity.plan.target_interval_seconds[coverage]
            effective_interval_seconds = capacity.plan.effective_interval_seconds[coverage]
            if coverage is CoverageClass.RETIRED_CONTROL:
                next_due_at = None
            else:
                phase_microseconds = _epoch_phase_microseconds(
                    collection_epoch_id=collection_epoch_id,
                    token_id=schedule.token_id,
                    interval_seconds=effective_interval_seconds,
                )
                next_due_at = normalized_started_at + timedelta(microseconds=phase_microseconds)
            previous_class, previous_due_at, reason_code = previous[schedule.token_id]
            await self._record_coverage_decision(
                session,
                collection_epoch_id=collection_epoch_id,
                collector_run_id=None,
                schedule=schedule,
                previous_coverage_class=previous_class,
                decided_at=normalized_started_at,
                coverage_effective_at=normalized_started_at,
                reason_code=reason_code,
                capacity_decision_id=capacity.id,
                target_interval_seconds=target_interval_seconds,
                effective_interval_seconds=effective_interval_seconds,
                next_due_at=next_due_at,
                detail={"epoch_number": epoch_number},
            )
            session.add(
                PollScheduleDecision(
                    collection_epoch_id=collection_epoch_id,
                    token_id=schedule.token_id,
                    idempotency_key=_epoch_rebase_idempotency_key(
                        collection_epoch_id=collection_epoch_id,
                        token_id=schedule.token_id,
                    ),
                    previous_state=schedule.lifecycle_state,
                    new_state=schedule.lifecycle_state,
                    previous_due_at=previous_due_at,
                    new_due_at=next_due_at,
                    decided_at=normalized_started_at,
                    reason_code=reason_code,
                    capacity_decision_id=capacity.id,
                    target_interval_seconds=target_interval_seconds,
                    effective_interval_seconds=effective_interval_seconds,
                    configuration_sha256=self.policy.sha256,
                    configuration_snapshot=self.policy.snapshot,
                )
            )
            schedule.next_due_at = next_due_at
            schedule.capacity_decision_id = capacity.id
            schedule.target_interval_seconds = target_interval_seconds
            schedule.effective_interval_seconds = effective_interval_seconds
            schedule.configuration_sha256 = self.policy.sha256
            schedule.configuration_snapshot = self.policy.snapshot
            schedule.updated_at = normalized_started_at
        await session.flush()
        return len(schedules)
    async def set_lifecycle_state_in_session(
        self,
        session: AsyncSession,
        *,
        token_id: uuid.UUID,
        state: LifecycleState,
        decided_at: datetime,
        reason_code: str,
        admitted_at: datetime | None = None,
        collector_run_id: uuid.UUID | None = None,
    ) -> PollSchedule:
        """Transactional form used when a lifecycle transition is persisted nearby."""
        normalized_decided_at = _normalize_utc(decided_at, "decided_at")
        assert normalized_decided_at is not None
        # The token exists before its schedule. Locking it closes the otherwise
        # unavoidable check-then-insert race between first-time scheduler writers.
        # Its immutable key needs coordination, not a lock that conflicts with
        # child-row FK checks performed by poll and coverage evidence writers.
        (
            await session.execute(
                select(Token.id)
                .where(Token.id == token_id)
                .with_for_update(key_share=True)
            )
        ).scalar_one()
        task = (
            await session.execute(
                select(PollSchedule).where(PollSchedule.token_id == token_id).with_for_update()
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
        await self._persist_coverage_policy(session)
        previous_state = task.lifecycle_state if task is not None else None
        previous_coverage = task.coverage_class if task is not None else None
        previous_due_at = task.next_due_at if task is not None else None
        if task is None:
            admission = admitted_at or (
                normalized_decided_at if state is LifecycleState.NEW else None
            )
            if admission is None:
                raise CoverageReconstructionError(
                    "an initial non-NEW schedule requires an explicit DEX admission time"
                )
        else:
            if task.admitted_at is None:
                raise CoverageReconstructionError(
                    "legacy coverage must be reconstructed at epoch start before scheduling"
                )
            admission = task.admitted_at

        if (
            task is not None
            and task.lifecycle_state == state.value
            and task.configuration_sha256 == self.policy.sha256
            and task.coverage_policy_sha256 == self.policy.coverage_sha256
        ):
            await self._refresh_one_coverage(
                session,
                schedule=task,
                now=normalized_decided_at,
                reason_code="coverage_time_advanced",
                collector_run_id=collector_run_id,
            )
            return task

        coverage = self.policy.coverage_class_for(
            state,
            admitted_at=admission,
            state_decided_at=normalized_decided_at,
            at=normalized_decided_at,
        )
        if task is None:
            task = PollSchedule(
                token_id=token_id,
                lifecycle_state=state.value,
                state_decided_at=normalized_decided_at,
                admitted_at=admission,
                coverage_class=coverage.value,
                coverage_decided_at=normalized_decided_at,
                coverage_next_transition_at=self.policy.next_transition_at(
                    state,
                    admitted_at=admission,
                    state_decided_at=normalized_decided_at,
                    at=normalized_decided_at,
                ),
                coverage_policy_sha256=self.policy.coverage_sha256,
                priority=self.policy.priority_for_coverage(coverage),
                next_due_at=None,
                attempt_count=0,
                control_scan_count=0,
                configuration_sha256=self.policy.sha256,
                configuration_snapshot=self.policy.snapshot,
                updated_at=normalized_decided_at,
            )
            session.add(task)
        else:
            task.lifecycle_state = state.value
            task.state_decided_at = normalized_decided_at
            task.coverage_class = coverage.value
            task.coverage_decided_at = normalized_decided_at
            task.coverage_next_transition_at = self.policy.next_transition_at(
                state,
                admitted_at=admission,
                state_decided_at=normalized_decided_at,
                at=normalized_decided_at,
            )
            task.coverage_policy_sha256 = self.policy.coverage_sha256
            task.priority = self.policy.priority_for_coverage(coverage)
            if coverage is CoverageClass.RETIRED_CONTROL:
                # Keep the row valid before the population flush used by the
                # capacity planner. Retired work is selected only by control windows.
                task.next_due_at = None
            task.configuration_sha256 = self.policy.sha256
            task.configuration_snapshot = self.policy.snapshot
            task.updated_at = normalized_decided_at

        await session.flush()
        capacity = await self._capacity_decision(session, now=normalized_decided_at)
        target_interval_seconds = capacity.plan.target_interval_seconds[coverage]
        effective_interval_seconds = capacity.plan.effective_interval_seconds[coverage]
        next_due_at = (
            None
            if coverage is CoverageClass.RETIRED_CONTROL
            else normalized_decided_at + timedelta(seconds=effective_interval_seconds)
        )
        task.next_due_at = next_due_at
        task.capacity_decision_id = capacity.id
        task.target_interval_seconds = target_interval_seconds
        task.effective_interval_seconds = effective_interval_seconds
        await self._record_coverage_decision(
            session,
            collection_epoch_id=None,
            collector_run_id=collector_run_id,
            schedule=task,
            previous_coverage_class=previous_coverage,
            decided_at=normalized_decided_at,
            coverage_effective_at=normalized_decided_at,
            reason_code=reason_code,
            capacity_decision_id=capacity.id,
            target_interval_seconds=target_interval_seconds,
            effective_interval_seconds=effective_interval_seconds,
            next_due_at=next_due_at,
            detail={
                "previous_lifecycle_state": previous_state,
                "new_lifecycle_state": state.value,
            },
        )

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
                capacity_decision_id=capacity.id,
                target_interval_seconds=target_interval_seconds,
                effective_interval_seconds=effective_interval_seconds,
                configuration_sha256=self.policy.sha256,
                configuration_snapshot=self.policy.snapshot,
            )
            .on_conflict_do_nothing(index_elements=[PollScheduleDecision.idempotency_key])
        )
        await session.flush()
        return task

    async def claim_next_batch(
        self, *, collector_run_id: uuid.UUID | None = None
    ) -> PollBatchClaim | None:
        """Claim one bounded batch, subject to fairness, capacity, and request budget."""
        now = self._normalized_now()
        async with self._session_factory() as session, session.begin():
            await lock_schedule_token_fk_path(session, exclusive=True)
            await self._advance_coverage_transitions(
                session, now=now, collector_run_id=collector_run_id
            )
            capacity = await self._capacity_decision(session, now=now)
            if not await self._has_batch_capacity(
                session,
                now=now,
                request_budget_per_minute=capacity.plan.available_requests_per_minute,
            ):
                return None

            control_window = _time_bucket(now, timedelta(minutes=1))
            control_claimed = bool(
                await session.scalar(
                    select(func.count())
                    .select_from(PollBatch)
                    .where(PollBatch.control_window_start == control_window)
                )
            )
            if not control_claimed and now >= control_window + timedelta(seconds=30):
                control = await self._claim_control_batch(
                    session,
                    now=now,
                    control_window=control_window,
                    capacity=capacity,
                    collector_run_id=collector_run_id,
                )
                if control is not None:
                    return control

            eligibility = (
                PollSchedule.next_due_at <= now,
                PollSchedule.coverage_class.is_not(None),
                PollSchedule.coverage_class != CoverageClass.RETIRED_CONTROL.value,
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
                        _priority_order(),
                        PollSchedule.next_due_at,
                        PollSchedule.token_id,
                    )
                    .limit(1)
                    .with_for_update(skip_locked=True, of=PollSchedule)
                )
            ).scalar_one_or_none()
            if head is None:
                if control_claimed:
                    return None
                return await self._claim_control_batch(
                    session,
                    now=now,
                    control_window=control_window,
                    capacity=capacity,
                    collector_run_id=collector_run_id,
                )

            rows = (
                await session.execute(
                    select(PollSchedule, Token)
                    .join(Token, Token.id == PollSchedule.token_id)
                    .where(Token.chain == head, *eligibility)
                    .order_by(
                        _priority_order(),
                        PollSchedule.next_due_at,
                        PollSchedule.token_id,
                    )
                    .limit(self.policy.batch_size)
                    .with_for_update(skip_locked=True, of=PollSchedule)
                )
            ).all()
            if not rows:
                return None

            batch = PollBatch(
                collector_run_id=collector_run_id,
                provider=DEX_SCREENER_PROVIDER,
                chain=head,
                claimed_at=now,
                lease_expires_at=now + self.policy.lease_duration,
                reserved_request_capacity=1,
                batch_kind="ordinary",
                control_window_start=None,
                capacity_decision_id=capacity.id,
                configuration_sha256=self.policy.sha256,
                configuration_snapshot=self.policy.snapshot,
            )
            session.add(batch)
            await session.flush()

            members: list[PollMemberClaim] = []
            for task, token in rows:
                previous_batch_id = task.lease_id
                due_at = task.next_due_at
                if due_at is None:
                    raise CoverageReconstructionError(
                        "an ordinary schedule reached claim selection without a due time"
                    )
                lateness_ms = _lateness_ms(observed_at=now, due_at=due_at)
                if task.coverage_class is None:
                    raise CoverageReconstructionError(
                        "an unmapped legacy schedule reached ordinary claim selection"
                    )
                coverage = CoverageClass(task.coverage_class)
                target_interval_seconds = capacity.plan.target_interval_seconds[coverage]
                effective_interval_seconds = capacity.plan.effective_interval_seconds[coverage]
                session.add(
                    PollBatchMember(
                        claimed_at=now,
                        batch_id=batch.id,
                        token_id=task.token_id,
                        due_at=due_at,
                        lifecycle_state=task.lifecycle_state,
                        coverage_class=coverage.value,
                        priority=self.policy.priority_for_coverage(coverage),
                        claim_lateness_ms=lateness_ms,
                        previous_batch_id=previous_batch_id,
                        capacity_decision_id=capacity.id,
                        target_interval_seconds=target_interval_seconds,
                        effective_interval_seconds=effective_interval_seconds,
                    )
                )
                task.lease_id = batch.id
                task.lease_expires_at = batch.lease_expires_at
                task.last_started_at = now
                task.priority = self.policy.priority_for_coverage(coverage)
                task.capacity_decision_id = capacity.id
                task.target_interval_seconds = target_interval_seconds
                task.effective_interval_seconds = effective_interval_seconds
                task.updated_at = now
                members.append(
                    PollMemberClaim(
                        token_id=task.token_id,
                        address=token.address,
                        lifecycle_state=LifecycleState(task.lifecycle_state),
                        coverage_class=coverage,
                        due_at=due_at,
                        claim_lateness_ms=lateness_ms,
                        previous_batch_id=previous_batch_id,
                        capacity_decision_id=capacity.id,
                        target_interval_seconds=target_interval_seconds,
                        effective_interval_seconds=effective_interval_seconds,
                    )
                )
            await session.flush()
            return PollBatchClaim(
                batch_id=batch.id,
                chain=batch.chain,
                claimed_at=batch.claimed_at,
                lease_expires_at=batch.lease_expires_at,
                capacity_decision_id=capacity.id,
                batch_kind="ordinary",
                members=tuple(members),
            )

    async def _claim_control_batch(
        self,
        session: AsyncSession,
        *,
        now: datetime,
        control_window: datetime,
        capacity: _CapacityDecision,
        collector_run_id: uuid.UUID | None,
    ) -> PollBatchClaim | None:
        """Claim at most one deterministic fixed-budget retired batch per minute."""
        existing = await session.scalar(
            select(func.count())
            .select_from(PollBatch)
            .where(PollBatch.control_window_start == control_window)
        )
        if existing:
            return None
        eligibility = (
            PollSchedule.coverage_class == CoverageClass.RETIRED_CONTROL.value,
            or_(
                PollSchedule.lease_id.is_(None),
                PollSchedule.lease_expires_at <= now,
            ),
        )
        order = (
            nullsfirst(PollSchedule.last_control_scan_at),
            PollSchedule.admitted_at,
            PollSchedule.token_id,
        )
        head = await session.scalar(
            select(Token.chain)
            .join(PollSchedule, PollSchedule.token_id == Token.id)
            .where(*eligibility)
            .order_by(*order)
            .limit(1)
            .with_for_update(skip_locked=True, of=PollSchedule)
        )
        if head is None:
            return None
        rows = (
            await session.execute(
                select(PollSchedule, Token)
                .join(Token, Token.id == PollSchedule.token_id)
                .where(Token.chain == head, *eligibility)
                .order_by(*order)
                .limit(self.policy.control_scan_tokens_per_minute)
                .with_for_update(skip_locked=True, of=PollSchedule)
            )
        ).all()
        if not rows:
            return None
        batch = PollBatch(
            collector_run_id=collector_run_id,
            provider=DEX_SCREENER_PROVIDER,
            chain=head,
            claimed_at=now,
            lease_expires_at=now + self.policy.lease_duration,
            reserved_request_capacity=1,
            batch_kind="retired_control",
            control_window_start=control_window,
            capacity_decision_id=capacity.id,
            configuration_sha256=self.policy.sha256,
            configuration_snapshot=self.policy.snapshot,
        )
        session.add(batch)
        await session.flush()
        coverage = CoverageClass.RETIRED_CONTROL
        target = capacity.plan.target_interval_seconds[coverage]
        effective = capacity.plan.effective_interval_seconds[coverage]
        members: list[PollMemberClaim] = []
        for schedule, token in rows:
            previous_batch_id = schedule.lease_id
            session.add(
                PollBatchMember(
                    claimed_at=now,
                    batch_id=batch.id,
                    token_id=schedule.token_id,
                    due_at=now,
                    lifecycle_state=schedule.lifecycle_state,
                    coverage_class=coverage.value,
                    priority=self.policy.priority_for_coverage(coverage),
                    claim_lateness_ms=0,
                    previous_batch_id=previous_batch_id,
                    capacity_decision_id=capacity.id,
                    target_interval_seconds=target,
                    effective_interval_seconds=effective,
                )
            )
            schedule.lease_id = batch.id
            schedule.lease_expires_at = batch.lease_expires_at
            schedule.last_started_at = now
            schedule.capacity_decision_id = capacity.id
            schedule.target_interval_seconds = target
            schedule.effective_interval_seconds = effective
            schedule.updated_at = now
            members.append(
                PollMemberClaim(
                    token_id=schedule.token_id,
                    address=token.address,
                    lifecycle_state=LifecycleState(schedule.lifecycle_state),
                    coverage_class=coverage,
                    due_at=now,
                    claim_lateness_ms=0,
                    previous_batch_id=previous_batch_id,
                    capacity_decision_id=capacity.id,
                    target_interval_seconds=target,
                    effective_interval_seconds=effective,
                )
            )
        await session.flush()
        return PollBatchClaim(
            batch_id=batch.id,
            chain=batch.chain,
            claimed_at=batch.claimed_at,
            lease_expires_at=batch.lease_expires_at,
            capacity_decision_id=capacity.id,
            batch_kind="retired_control",
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
        async with self._session_factory() as session, session.begin():
            return await self.complete_batch_in_session(
                session,
                batch_id=batch_id,
                outcome=outcome,
                api_request_log_id=api_request_log_id,
                failure_detail=failure_detail,
            )

    async def complete_batch_in_session(
        self,
        session: AsyncSession,
        *,
        batch_id: uuid.UUID,
        outcome: PollOutcome,
        api_request_log_id: uuid.UUID | None = None,
        failure_detail: dict[str, object] | None = None,
    ) -> PollCompletion:
        """Transactional form used with observation facts from the claimed batch.

        Callers that write token-referencing facts earlier in the transaction must
        enter ``lock_schedule_token_fk_path`` before those writes. Re-acquiring its
        shared form here is transaction-local and protects direct callers.
        """
        completed_at = self._normalized_now()
        await lock_schedule_token_fk_path(session, exclusive=False)
        batch = (
            await session.execute(
                select(PollBatch).where(PollBatch.id == batch_id).with_for_update()
            )
        ).scalar_one()
        existing = await session.get(PollBatchOutcome, batch_id)
        if existing is not None:
            return _completion_from_model(existing)

        capacity_model = (
            await session.get(SchedulerCapacityDecision, batch.capacity_decision_id)
            if batch.capacity_decision_id is not None
            else None
        )
        effective_intervals = (
            _decision_intervals(capacity_model) if capacity_model is not None else None
        )
        target_intervals = (
            _decision_target_intervals(capacity_model) if capacity_model is not None else None
        )

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
        schedules = await _lock_completion_schedules(
            session,
            token_ids=[member.token_id for member in members],
        )
        schedule_by_token = {schedule.token_id: schedule for schedule in schedules}
        if (
            completed_at >= batch.lease_expires_at
            or len(schedule_by_token) != len(members)
            or any(schedule_by_token[member.token_id].lease_id != batch_id for member in members)
        ):
            msg = "Poll batch lease expired or was reclaimed by another worker"
            raise LostPollLeaseError(msg)

        lateness_values = [
            _lateness_ms(observed_at=completed_at, due_at=member.due_at) for member in members
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
            if task.admitted_at is None or task.coverage_class is None:
                raise CoverageReconstructionError(
                    "an unmapped legacy schedule cannot complete V2 work"
                )
            state = LifecycleState(task.lifecycle_state)
            coverage = self.policy.coverage_class_for(
                state,
                admitted_at=task.admitted_at,
                state_decided_at=task.state_decided_at,
                at=completed_at,
            )
            previous_coverage = task.coverage_class
            target_interval_seconds = (
                target_intervals[coverage]
                if target_intervals is not None
                else self.policy.capacity_target_interval_seconds(coverage, population=1)
            )
            effective_interval_seconds = (
                effective_intervals[coverage]
                if effective_intervals is not None
                else target_interval_seconds
            )
            candidate_active = (
                task.candidate_coverage_expires_at is not None
                and task.candidate_coverage_expires_at > completed_at
                and task.candidate_coverage_interval_seconds is not None
            )
            if candidate_active:
                assert task.candidate_coverage_interval_seconds is not None
                target_interval_seconds = min(
                    target_interval_seconds, task.candidate_coverage_interval_seconds
                )
                effective_interval_seconds = min(
                    effective_interval_seconds, task.candidate_coverage_interval_seconds
                )
            elif task.candidate_coverage_expires_at is not None:
                task.candidate_coverage_expires_at = None
                task.candidate_coverage_interval_seconds = None
                task.candidate_tier_event_id = None
            member = member_by_token[token_id]
            task.last_due_at = member.due_at
            task.last_completed_at = completed_at
            task.attempt_count += 1
            if member.coverage_class == CoverageClass.RETIRED_CONTROL.value:
                task.last_control_scan_at = completed_at
                task.control_scan_count += 1
            task.coverage_class = coverage.value
            task.coverage_decided_at = completed_at
            baseline_transition_at = self.policy.next_transition_at(
                state,
                admitted_at=task.admitted_at,
                state_decided_at=task.state_decided_at,
                at=completed_at,
            )
            task.coverage_next_transition_at = _earliest_timestamp(
                baseline_transition_at,
                task.candidate_coverage_expires_at if candidate_active else None,
            )
            task.coverage_policy_sha256 = self.policy.coverage_sha256
            task.next_due_at = (
                None
                if coverage is CoverageClass.RETIRED_CONTROL
                else completed_at + timedelta(seconds=effective_interval_seconds)
            )
            task.priority = (
                min(self.policy.priority_for_coverage(coverage), 1)
                if candidate_active
                else self.policy.priority_for_coverage(coverage)
            )
            task.lease_id = None
            task.lease_expires_at = None
            task.capacity_decision_id = batch.capacity_decision_id
            task.target_interval_seconds = target_interval_seconds
            task.effective_interval_seconds = effective_interval_seconds
            task.configuration_sha256 = self.policy.sha256
            task.configuration_snapshot = self.policy.snapshot
            task.updated_at = completed_at
            if previous_coverage != coverage.value:
                await self._record_coverage_decision(
                    session,
                    collection_epoch_id=None,
                    collector_run_id=batch.collector_run_id,
                    schedule=task,
                    previous_coverage_class=previous_coverage,
                    decided_at=completed_at,
                    coverage_effective_at=completed_at,
                    reason_code="coverage_boundary_observed_at_completion",
                    capacity_decision_id=batch.capacity_decision_id,
                    target_interval_seconds=target_interval_seconds,
                    effective_interval_seconds=effective_interval_seconds,
                    next_due_at=task.next_due_at,
                    detail={"batch_id": str(batch_id)},
                )
        await session.flush()
        return _completion_from_model(completion_model)

    async def _has_batch_capacity(
        self,
        session: AsyncSession,
        *,
        now: datetime,
        request_budget_per_minute: int,
    ) -> bool:
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
        return int(reserved or 0) + 1 <= request_budget_per_minute

    async def _capacity_decision(
        self,
        session: AsyncSession,
        *,
        now: datetime,
    ) -> _CapacityDecision:
        bucket = _time_bucket(now, self.policy.capacity_refresh)
        cached = self._cached_capacity_decision
        if cached is None or self._cached_capacity_bucket != bucket:
            counts = await self._capacity_counts(session, now=now)
            plan = plan_capacity(self.policy, counts)
            key = _capacity_idempotency_key(
                bucket=bucket,
                policy_sha256=self.policy.sha256,
                plan=plan,
            )
            cached = _CapacityDecision(
                id=uuid.uuid5(uuid.NAMESPACE_URL, key),
                decided_at=bucket,
                plan=plan,
            )
            self._cached_capacity_bucket = bucket
            self._cached_capacity_decision = cached
        await self._persist_capacity_decision(session, cached)
        self._log_capacity_decision(cached)
        return cached

    async def _capacity_counts(
        self,
        session: AsyncSession,
        *,
        now: datetime,
    ) -> dict[CapacityTier, int]:
        del now
        unmapped = int(
            await session.scalar(
                select(func.count())
                .select_from(PollSchedule)
                .where(PollSchedule.coverage_class.is_(None))
            )
            or 0
        )
        if unmapped:
            raise CoverageReconstructionError(
                f"{unmapped} legacy schedules require epoch-start coverage reconstruction"
            )
        rows = (
            await session.execute(
                select(PollSchedule.coverage_class, func.count()).group_by(
                    PollSchedule.coverage_class
                )
            )
        ).all()
        counts = {
            CoverageClass(coverage): int(count) for coverage, count in rows if coverage is not None
        }
        return {tier: counts.get(tier, 0) for tier in CapacityTier}

    async def _advance_coverage_transitions(
        self,
        session: AsyncSession,
        *,
        now: datetime,
        collector_run_id: uuid.UUID | None,
    ) -> int:
        """Advance elapsed age/FADING boundaries before planning or claiming."""
        advanced = 0
        while True:
            schedules = list(
                (
                    await session.execute(
                        select(PollSchedule)
                        .where(
                            PollSchedule.coverage_next_transition_at.is_not(None),
                            PollSchedule.coverage_next_transition_at <= now,
                        )
                        .order_by(
                            PollSchedule.coverage_next_transition_at,
                            PollSchedule.token_id,
                        )
                        .limit(1_000)
                        .with_for_update(skip_locked=True)
                    )
                ).scalars()
            )
            if not schedules:
                break
            for schedule in schedules:
                await self._refresh_one_coverage(
                    session,
                    schedule=schedule,
                    now=now,
                    reason_code="coverage_boundary_elapsed",
                    collector_run_id=collector_run_id,
                )
            advanced += len(schedules)
            await session.flush()
            stalled = list(
                await session.scalars(
                    select(PollSchedule.token_id)
                    .where(
                        PollSchedule.token_id.in_([schedule.token_id for schedule in schedules]),
                        PollSchedule.coverage_next_transition_at.is_not(None),
                        PollSchedule.coverage_next_transition_at <= now,
                    )
                    .order_by(PollSchedule.token_id)
                )
            )
            if stalled:
                raise CoverageTransitionProgressError(
                    "coverage refresh made no progress for due schedules: "
                    + ", ".join(str(token_id) for token_id in stalled[:10])
                )
        if advanced:
            self._cached_capacity_bucket = None
            self._cached_capacity_decision = None
        return advanced

    async def _refresh_one_coverage(
        self,
        session: AsyncSession,
        *,
        schedule: PollSchedule,
        now: datetime,
        reason_code: str,
        collector_run_id: uuid.UUID | None,
    ) -> None:
        """Refresh one projection without changing its lifecycle evidence."""
        if schedule.admitted_at is None or schedule.coverage_class is None:
            raise CoverageReconstructionError(
                "legacy coverage must be reconstructed at epoch start"
            )
        state = LifecycleState(schedule.lifecycle_state)
        coverage = self.policy.coverage_class_for(
            state,
            admitted_at=schedule.admitted_at,
            state_decided_at=schedule.state_decided_at,
            at=now,
        )
        baseline_transition_at = self.policy.next_transition_at(
            state,
            admitted_at=schedule.admitted_at,
            state_decided_at=schedule.state_decided_at,
            at=now,
        )
        candidate_active = (
            schedule.candidate_coverage_expires_at is not None
            and schedule.candidate_coverage_expires_at > now
            and schedule.candidate_coverage_interval_seconds is not None
        )
        candidate_expired = (
            schedule.candidate_coverage_expires_at is not None
            and schedule.candidate_coverage_expires_at <= now
        )
        expected_transition_at = _earliest_timestamp(
            baseline_transition_at,
            schedule.candidate_coverage_expires_at if candidate_active else None,
        )
        if (
            schedule.coverage_class == coverage.value
            and schedule.coverage_policy_sha256 == self.policy.coverage_sha256
            and not candidate_expired
            and schedule.coverage_next_transition_at == expected_transition_at
        ):
            return
        previous = schedule.coverage_class
        effective_at = schedule.coverage_next_transition_at or now
        interval = self.policy.interval_for_coverage(coverage)
        target = int(interval.total_seconds()) if interval is not None else None
        if candidate_expired:
            schedule.candidate_coverage_expires_at = None
            schedule.candidate_coverage_interval_seconds = None
            schedule.candidate_tier_event_id = None
        if candidate_active and schedule.candidate_coverage_interval_seconds is not None:
            target = min(
                target or schedule.candidate_coverage_interval_seconds,
                schedule.candidate_coverage_interval_seconds,
            )
        if interval is None:
            next_due_at = None
        else:
            phase = _coverage_phase_microseconds(
                token_id=schedule.token_id,
                coverage=coverage,
                policy_sha256=self.policy.coverage_sha256,
                interval_seconds=int(interval.total_seconds()),
            )
            next_due_at = now + timedelta(microseconds=phase)
            # A class boundary may cool future work, but it must not make an
            # already-due obligation disappear or jump back into the future.
            if schedule.next_due_at is not None and schedule.next_due_at <= now:
                next_due_at = schedule.next_due_at
        schedule.coverage_class = coverage.value
        schedule.coverage_decided_at = now
        schedule.coverage_next_transition_at = _earliest_timestamp(
            baseline_transition_at,
            schedule.candidate_coverage_expires_at if candidate_active else None,
        )
        schedule.coverage_policy_sha256 = self.policy.coverage_sha256
        schedule.priority = (
            min(self.policy.priority_for_coverage(coverage), 1)
            if candidate_active
            else self.policy.priority_for_coverage(coverage)
        )
        schedule.next_due_at = next_due_at
        schedule.target_interval_seconds = target
        schedule.effective_interval_seconds = target
        schedule.configuration_sha256 = self.policy.sha256
        schedule.configuration_snapshot = self.policy.snapshot
        schedule.updated_at = now
        await self._record_coverage_decision(
            session,
            collection_epoch_id=None,
            collector_run_id=collector_run_id,
            schedule=schedule,
            previous_coverage_class=previous,
            decided_at=now,
            coverage_effective_at=effective_at,
            reason_code=reason_code,
            capacity_decision_id=None,
            target_interval_seconds=target,
            effective_interval_seconds=target,
            next_due_at=next_due_at,
            detail={},
        )

    async def _persist_coverage_policy(self, session: AsyncSession) -> None:
        await session.execute(
            insert(CoveragePolicy)
            .values(
                policy_sha256=self.policy.coverage_sha256,
                policy_snapshot=self.policy.coverage_snapshot,
            )
            .on_conflict_do_nothing(index_elements=[CoveragePolicy.policy_sha256])
        )
        if self._validated_coverage_policy:
            return
        stored = await session.get(CoveragePolicy, self.policy.coverage_sha256)
        if stored is None:
            raise RuntimeError("coverage policy insert did not resolve")
        if stored.policy_snapshot != self.policy.coverage_snapshot:
            raise ValueError("coverage policy digest maps to different content")
        self._validated_coverage_policy = True

    async def _record_coverage_decision(
        self,
        session: AsyncSession,
        *,
        collection_epoch_id: uuid.UUID | None,
        collector_run_id: uuid.UUID | None,
        schedule: PollSchedule,
        previous_coverage_class: str | None,
        decided_at: datetime,
        coverage_effective_at: datetime,
        reason_code: str,
        capacity_decision_id: uuid.UUID | None,
        target_interval_seconds: int | None,
        effective_interval_seconds: int | None,
        next_due_at: datetime | None,
        detail: dict[str, object],
    ) -> None:
        """Persist an idempotent immutable explanation of a coverage change."""
        if schedule.admitted_at is None or schedule.coverage_class is None:
            raise CoverageReconstructionError("coverage decision requires a mapped schedule")
        await self._persist_coverage_policy(session)
        if collection_epoch_id is None and collector_run_id is not None:
            collection_epoch_id = await session.scalar(
                select(CollectorRun.collection_epoch_id).where(CollectorRun.id == collector_run_id)
            )
            if collection_epoch_id is None:
                raise ValueError("coverage decision collector run does not exist")
        key = _coverage_decision_idempotency_key(
            token_id=schedule.token_id,
            previous_coverage_class=previous_coverage_class,
            new_coverage_class=schedule.coverage_class,
            decided_at=decided_at,
            coverage_effective_at=coverage_effective_at,
            policy_sha256=self.policy.coverage_sha256,
            reason_code=reason_code,
        )
        decision_id = uuid.uuid5(uuid.NAMESPACE_URL, key)
        full_detail = {
            "lifecycle_state_decided_at": schedule.state_decided_at.isoformat(),
            "coverage_next_transition_at": (
                schedule.coverage_next_transition_at.isoformat()
                if schedule.coverage_next_transition_at
                else None
            ),
            **detail,
        }
        await session.execute(
            insert(CoverageDecision)
            .values(
                id=decision_id,
                collection_epoch_id=collection_epoch_id,
                collector_run_id=collector_run_id,
                token_id=schedule.token_id,
                idempotency_key=key,
                previous_coverage_class=previous_coverage_class,
                new_coverage_class=schedule.coverage_class,
                lifecycle_state=schedule.lifecycle_state,
                admitted_at=schedule.admitted_at,
                decided_at=decided_at,
                coverage_effective_at=coverage_effective_at,
                reason_code=reason_code,
                policy_sha256=self.policy.coverage_sha256,
                capacity_decision_id=capacity_decision_id,
                target_interval_seconds=target_interval_seconds,
                effective_interval_seconds=effective_interval_seconds,
                next_due_at=next_due_at,
                detail=full_detail,
            )
            .on_conflict_do_nothing()
        )
        stored = await session.scalar(
            select(CoverageDecision).where(
                or_(
                    CoverageDecision.id == decision_id,
                    CoverageDecision.idempotency_key == key,
                )
            )
        )
        if stored is None:
            raise RuntimeError("coverage decision insert did not resolve")
        expected = (
            decision_id,
            key,
            schedule.token_id,
            collection_epoch_id,
            collector_run_id,
            previous_coverage_class,
            schedule.coverage_class,
            schedule.lifecycle_state,
            schedule.admitted_at,
            decided_at,
            coverage_effective_at,
            reason_code,
            self.policy.coverage_sha256,
            target_interval_seconds,
            effective_interval_seconds,
            next_due_at,
            full_detail,
        )
        actual = (
            stored.id,
            stored.idempotency_key,
            stored.token_id,
            stored.collection_epoch_id,
            stored.collector_run_id,
            stored.previous_coverage_class,
            stored.new_coverage_class,
            stored.lifecycle_state,
            stored.admitted_at,
            stored.decided_at,
            stored.coverage_effective_at,
            stored.reason_code,
            stored.policy_sha256,
            stored.target_interval_seconds,
            stored.effective_interval_seconds,
            stored.next_due_at,
            stored.detail,
        )
        if actual != expected:
            raise SchedulerCapacityDecisionIntegrityError(
                "coverage decision identity maps to different semantic content"
            )

    async def _persist_capacity_decision(
        self,
        session: AsyncSession,
        decision: _CapacityDecision,
    ) -> None:
        await session.execute(
            insert(SchedulerPolicy)
            .values(
                policy_sha256=self.policy.sha256,
                policy_snapshot=self.policy.snapshot,
            )
            .on_conflict_do_nothing(index_elements=[SchedulerPolicy.policy_sha256])
        )
        if not self._validated_policy:
            stored_policy = await session.get(SchedulerPolicy, self.policy.sha256)
            if stored_policy is None:
                raise RuntimeError("scheduler policy insert did not resolve")
            if stored_policy.policy_snapshot != self.policy.snapshot:
                raise ValueError("scheduler policy digest maps to different content")
            self._validated_policy = True
        key = _capacity_idempotency_key(
            bucket=decision.decided_at,
            policy_sha256=self.policy.sha256,
            plan=decision.plan,
        )
        snapshot: dict[str, object] = {
            "component": "scheduler_capacity_decision",
            "schema_version": 1,
            "policy_sha256": self.policy.sha256,
            **decision.plan.snapshot,
        }
        await session.execute(
            insert(SchedulerCapacityDecision)
            .values(
                id=decision.id,
                idempotency_key=key,
                decided_at=decision.decided_at,
                capacity_mode=decision.plan.mode.value,
                policy_sha256=self.policy.sha256,
                decision_snapshot=snapshot,
            )
            # ID and idempotency_key encode the same logical identity. An
            # untargeted no-op makes either uniqueness constraint a valid race
            # arbiter; semantic readback below decides whether the no-op was safe.
            .on_conflict_do_nothing()
        )
        stored_rows = list(
            (
                await session.execute(
                    select(SchedulerCapacityDecision).where(
                        or_(
                            SchedulerCapacityDecision.id == decision.id,
                            SchedulerCapacityDecision.idempotency_key == key,
                        )
                    )
                )
            ).scalars()
        )
        expected = {
            "id": decision.id,
            "idempotency_key": key,
            "decided_at": decision.decided_at,
            "capacity_mode": decision.plan.mode.value,
            "policy_sha256": self.policy.sha256,
            "decision_snapshot": snapshot,
        }
        if len(stored_rows) != 1:
            self._raise_capacity_integrity_error(
                expected,
                stored_rows,
                reason="capacity identity resolved to an unexpected number of rows",
            )
        stored = stored_rows[0]
        actual = {
            "id": stored.id,
            "idempotency_key": stored.idempotency_key,
            "decided_at": stored.decided_at,
            "capacity_mode": stored.capacity_mode,
            "policy_sha256": stored.policy_sha256,
            "decision_snapshot": stored.decision_snapshot,
        }
        mismatched_fields = sorted(
            field for field, value in expected.items() if actual[field] != value
        )
        if mismatched_fields:
            self._raise_capacity_integrity_error(
                expected,
                stored_rows,
                reason="capacity identity maps to different semantic content",
                mismatched_fields=mismatched_fields,
            )

    def _raise_capacity_integrity_error(
        self,
        expected: dict[str, object],
        stored_rows: list[SchedulerCapacityDecision],
        *,
        reason: str,
        mismatched_fields: list[str] | None = None,
    ) -> None:
        self._logger.critical(
            "scheduler_capacity_decision_integrity_error",
            reason=reason,
            expected_id=str(expected["id"]),
            expected_idempotency_key=expected["idempotency_key"],
            stored_ids=[str(row.id) for row in stored_rows],
            stored_idempotency_keys=[row.idempotency_key for row in stored_rows],
            mismatched_fields=mismatched_fields or [],
        )
        detail = ", ".join(mismatched_fields or []) or "identity cardinality"
        raise SchedulerCapacityDecisionIntegrityError(
            f"{reason}: {detail}; expected id={expected['id']} "
            f"idempotency_key={expected['idempotency_key']}"
        )

    def _log_capacity_decision(self, decision: _CapacityDecision) -> None:
        if self._last_logged_capacity_decision_id == decision.id:
            return
        self._last_logged_capacity_decision_id = decision.id
        event = {
            "capacity_decision_id": str(decision.id),
            "capacity_mode": decision.plan.mode.value,
            "requested_token_observations_per_minute": (
                decision.plan.requested_token_observations_per_minute
            ),
            "available_token_observations_per_minute": (
                decision.plan.available_token_observations_per_minute
            ),
            "effective_interval_seconds": {
                tier.value: seconds
                for tier, seconds in decision.plan.effective_interval_seconds.items()
            },
        }
        if decision.plan.mode is CapacityMode.CRITICAL:
            self._logger.error("scheduler_capacity_critical", **event)
        elif decision.plan.mode is CapacityMode.DEGRADED:
            self._logger.warning("scheduler_capacity_degraded", **event)
        else:
            self._logger.info("scheduler_capacity_normal", **event)

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


def _capacity_idempotency_key(
    *,
    bucket: datetime,
    policy_sha256: str,
    plan: CapacityPlan,
) -> str:
    encoded = json.dumps(
        {
            "bucket": bucket.isoformat(),
            "policy_sha256": policy_sha256,
            "plan": plan.snapshot,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _epoch_rebase_idempotency_key(*, collection_epoch_id: uuid.UUID, token_id: uuid.UUID) -> str:
    return hashlib.sha256(
        f"epoch-start-rebase:{collection_epoch_id}:{token_id}".encode()
    ).hexdigest()


def _epoch_phase_microseconds(
    *, collection_epoch_id: uuid.UUID, token_id: uuid.UUID, interval_seconds: int
) -> int:
    digest = hashlib.sha256(f"{collection_epoch_id}:{token_id}".encode()).digest()
    return int.from_bytes(digest[:8], "big") % (interval_seconds * 1_000_000)


def _coverage_phase_microseconds(
    *,
    token_id: uuid.UUID,
    coverage: CoverageClass,
    policy_sha256: str,
    interval_seconds: int,
) -> int:
    digest = hashlib.sha256(
        f"coverage-phase:{policy_sha256}:{coverage.value}:{token_id}".encode()
    ).digest()
    return int.from_bytes(digest[:8], "big") % (interval_seconds * 1_000_000)


def _time_bucket(value: datetime, duration: timedelta) -> datetime:
    seconds = int(duration.total_seconds())
    epoch = int(value.timestamp())
    return datetime.fromtimestamp(epoch - (epoch % seconds), tz=UTC)


def _priority_order() -> ColumnElement[int]:
    # The projection normally equals coverage-policy priority. Phase 5 may
    # temporarily raise a candidate to priority 1 without changing lifecycle or
    # coverage class; that finite override is durable and expires explicitly.
    return cast(ColumnElement[int], PollSchedule.priority)


def _earliest_timestamp(*values: datetime | None) -> datetime | None:
    present = [value for value in values if value is not None]
    return min(present) if present else None


def _decision_intervals(
    decision: SchedulerCapacityDecision,
) -> dict[CapacityTier, int]:
    raw = decision.decision_snapshot.get("effective_interval_seconds")
    if not isinstance(raw, dict):
        raise ValueError("capacity decision is missing effective intervals")
    intervals: dict[CapacityTier, int] = {}
    for tier in CapacityTier:
        value = raw.get(tier.value)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"capacity decision has an invalid {tier.value} interval")
        intervals[tier] = value
    return intervals


def _decision_target_intervals(
    decision: SchedulerCapacityDecision,
) -> dict[CapacityTier, int]:
    raw = decision.decision_snapshot.get("target_interval_seconds")
    if not isinstance(raw, dict):
        raise ValueError("capacity decision is missing target intervals")
    intervals: dict[CapacityTier, int] = {}
    for tier in CapacityTier:
        value = raw.get(tier.value)
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise ValueError(f"capacity decision has an invalid {tier.value} target interval")
        intervals[tier] = value
    return intervals


def _coverage_decision_idempotency_key(
    *,
    token_id: uuid.UUID,
    previous_coverage_class: str | None,
    new_coverage_class: str,
    decided_at: datetime,
    coverage_effective_at: datetime,
    policy_sha256: str,
    reason_code: str,
) -> str:
    encoded = json.dumps(
        {
            "component": "coverage_decision",
            "token_id": str(token_id),
            "previous_coverage_class": previous_coverage_class,
            "new_coverage_class": new_coverage_class,
            "decided_at": decided_at.isoformat(),
            "coverage_effective_at": coverage_effective_at.isoformat(),
            "policy_sha256": policy_sha256,
            "reason_code": reason_code,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _lateness_ms(*, observed_at: datetime, due_at: datetime) -> int:
    return max(0, int((observed_at - due_at).total_seconds() * 1_000))


async def _lock_completion_schedules(
    session: AsyncSession,
    *,
    token_ids: list[uuid.UUID],
) -> list[PollSchedule]:
    """Lock every completion schedule in the global token UUID order.

    Expired batches can overlap a reclaimed batch.  SQL ``IN`` input and result
    order are not lock-order guarantees, so the database ORDER BY is mandatory.
    """
    return list(
        (
            await session.scalars(
                select(PollSchedule)
                .where(PollSchedule.token_id.in_(token_ids))
                .order_by(PollSchedule.token_id)
                .with_for_update()
            )
        ).all()
    )


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
