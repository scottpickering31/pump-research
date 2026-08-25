"""Concurrency-safe persistence for candidate evidence and leased enrichment work."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from pump_research.candidates.policy import (
    ORCHESTRATION_RULE_NAME,
    ORCHESTRATION_RULE_VERSION,
    CandidateEvidence,
    CandidatePolicy,
    CandidateTier,
    EvaluationDecision,
    candidate_identity,
)
from pump_research.persistence.models import (
    CandidateCurrentState,
    CandidateEnrichmentTask,
    CandidateEvent,
    CandidatePolicyRecord,
    CandidateTierEvent,
    PollSchedule,
    Token,
)
from pump_research.scheduling.policy import CoverageClass


class CandidateIntegrityError(RuntimeError):
    """A deterministic identity mapped to different semantic content."""


class LostCandidateTaskLeaseError(RuntimeError):
    """A task completion no longer owns its durable lease."""


_CANDIDATE_COVERAGE_BUDGET_LOCK = 7_428_901_166
_CANDIDATE_TASK_BUDGET_LOCK = 7_428_901_167
_TIER3_BUDGET_LOCK = 7_428_901_168


@dataclass(frozen=True, slots=True)
class CandidateTransitionResult:
    candidate_id: uuid.UUID | None
    previous_tier: CandidateTier
    current_tier: CandidateTier
    changed: bool
    coverage_expires_at: datetime | None


@dataclass(frozen=True, slots=True)
class CandidateTaskClaim:
    id: uuid.UUID
    lease_id: uuid.UUID
    candidate_id: uuid.UUID
    token_id: uuid.UUID
    tier: CandidateTier
    analysis_type: str
    input_watermark: datetime
    input_sha256: str
    attempt_number: int


class CandidateRepository:
    """Persist immutable nominations/transitions and rebuildable current state."""

    async def apply_evaluation(
        self,
        session: AsyncSession,
        *,
        collection_epoch_id: uuid.UUID,
        collector_run_id: uuid.UUID | None,
        evidence: CandidateEvidence,
        decision: EvaluationDecision,
        policy: CandidatePolicy,
        now: datetime,
        task_max_attempts: int = 4,
    ) -> CandidateTransitionResult:
        now = _utc(now)
        await self._persist_policy(session, policy)
        token_id = uuid.UUID(evidence.token_id)
        # Lock the canonical token before checking/inserting the projection. This
        # serializes first nomination without relying on an in-memory worker lock.
        await session.scalar(
            select(Token.id).where(Token.id == token_id).with_for_update(key_share=True)
        )
        current = await session.scalar(
            select(CandidateCurrentState)
            .where(
                CandidateCurrentState.collection_epoch_id == collection_epoch_id,
                CandidateCurrentState.token_id == token_id,
            )
            .with_for_update()
        )
        previous = (
            CandidateTier(current.tier) if current is not None else CandidateTier.TIER_0_UNIVERSAL
        )
        if current is not None and evidence.watermark < current.input_watermark:
            return CandidateTransitionResult(
                candidate_id=current.latest_candidate_id,
                previous_tier=previous,
                current_tier=previous,
                changed=False,
                coverage_expires_at=current.coverage_expires_at,
            )

        target = decision.target_tier
        if (
            current is not None
            and evidence.watermark == current.input_watermark
            and _tier_rank(target) < _tier_rank(previous)
        ):
            # Concurrent Phase 6 tasks can finish at one received-at watermark.
            # A transaction that began before a sibling committed may see fewer
            # facts; equal-time incomplete visibility must not undo a promotion.
            return CandidateTransitionResult(
                candidate_id=current.latest_candidate_id,
                previous_tier=previous,
                current_tier=previous,
                changed=False,
                coverage_expires_at=current.coverage_expires_at,
            )
        if target is CandidateTier.TIER_3_DEEP_REVIEW and previous is not target:
            await session.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": _TIER3_BUDGET_LOCK},
            )
            tier3_count = int(
                await session.scalar(
                    select(func.count())
                    .select_from(CandidateCurrentState)
                    .where(CandidateCurrentState.tier == CandidateTier.TIER_3_DEEP_REVIEW.value)
                )
                or 0
            )
            if tier3_count >= policy.max_tier3_candidates:
                target = previous
                decision = replace(
                    decision,
                    target_tier=previous,
                    detail={
                        **decision.detail,
                        "tier3_budget_admitted": False,
                        "tier3_budget_limit": policy.max_tier3_candidates,
                    },
                )
        if (
            not decision.eligible
            and previous is not CandidateTier.TIER_0_UNIVERSAL
            and current is not None
            and current.coverage_expires_at is not None
            and now < current.coverage_expires_at
        ):
            target = previous
        refresh = (
            target is previous
            and target is not CandidateTier.TIER_0_UNIVERSAL
            and decision.eligible
            and current is not None
            and (current.coverage_expires_at is None or current.coverage_expires_at <= now)
            and current.next_evaluation_at <= now
            and current.evidence_sha256 != evidence.sha256
        )
        if target is previous and not refresh:
            return CandidateTransitionResult(
                candidate_id=current.latest_candidate_id if current else None,
                previous_tier=previous,
                current_tier=previous,
                changed=False,
                coverage_expires_at=current.coverage_expires_at if current else None,
            )

        schedule = await session.scalar(
            select(PollSchedule).where(PollSchedule.token_id == token_id).with_for_update()
        )
        coverage_admitted = False
        if target is not CandidateTier.TIER_0_UNIVERSAL and schedule is not None:
            await session.execute(
                text("SELECT pg_advisory_xact_lock(:lock_id)"),
                {"lock_id": _CANDIDATE_COVERAGE_BUDGET_LOCK},
            )
            active_coverage = int(
                await session.scalar(
                    select(func.count())
                    .select_from(PollSchedule)
                    .where(PollSchedule.candidate_coverage_expires_at > now)
                )
                or 0
            )
            already_active = (
                schedule.candidate_coverage_expires_at is not None
                and schedule.candidate_coverage_expires_at > now
            )
            coverage_admitted = already_active or active_coverage < policy.max_active_coverage
        audited_decision = replace(
            decision,
            detail={
                **decision.detail,
                "candidate_coverage_admitted": coverage_admitted,
                "candidate_coverage_limit": policy.max_active_coverage,
            },
        )

        candidate: CandidateEvent | None = None
        if target is not CandidateTier.TIER_0_UNIVERSAL:
            candidate = await self._persist_candidate(
                session,
                collection_epoch_id=collection_epoch_id,
                collector_run_id=collector_run_id,
                evidence=evidence,
                decision=audited_decision,
                policy=policy,
            )
        transition = await self._persist_tier_event(
            session,
            collection_epoch_id=collection_epoch_id,
            collector_run_id=collector_run_id,
            token_id=token_id,
            candidate_id=candidate.id if candidate else None,
            previous=previous,
            target=target,
            evidence=evidence,
            decision=audited_decision,
            policy=policy,
            now=now,
        )
        coverage_expires_at = (
            audited_decision.coverage_until
            if target is not CandidateTier.TIER_0_UNIVERSAL and coverage_admitted
            else None
        )
        next_evaluation_at = coverage_expires_at or now + timedelta(minutes=1)
        if current is None:
            current = CandidateCurrentState(
                collection_epoch_id=collection_epoch_id,
                token_id=token_id,
                tier=target.value,
                latest_candidate_id=candidate.id if candidate else None,
                latest_tier_event_id=transition.id,
                tier_since=now,
                coverage_expires_at=coverage_expires_at,
                next_evaluation_at=next_evaluation_at,
                input_watermark=evidence.watermark,
                evidence_sha256=evidence.sha256,
                policy_sha256=policy.sha256,
                updated_at=now,
            )
            session.add(current)
        else:
            current.tier = target.value
            current.latest_candidate_id = candidate.id if candidate else current.latest_candidate_id
            current.latest_tier_event_id = transition.id
            current.tier_since = now
            current.coverage_expires_at = coverage_expires_at
            current.next_evaluation_at = next_evaluation_at
            current.input_watermark = evidence.watermark
            current.evidence_sha256 = evidence.sha256
            current.policy_sha256 = policy.sha256
            current.updated_at = now
        if schedule is not None:
            if target is CandidateTier.TIER_0_UNIVERSAL:
                schedule.candidate_coverage_expires_at = None
                schedule.candidate_coverage_interval_seconds = None
                schedule.candidate_tier_event_id = None
                schedule.coverage_next_transition_at = now
            elif coverage_expires_at is not None:
                interval_seconds = int(policy.coverage_interval.total_seconds())
                schedule.candidate_coverage_expires_at = coverage_expires_at
                schedule.candidate_coverage_interval_seconds = interval_seconds
                schedule.candidate_tier_event_id = transition.id
                schedule.coverage_next_transition_at = min(
                    value
                    for value in (schedule.coverage_next_transition_at, coverage_expires_at)
                    if value is not None
                )
                accelerated_due = now + timedelta(seconds=interval_seconds)
                if schedule.coverage_class == CoverageClass.RETIRED_CONTROL.value:
                    # Retired work is selected only through the fixed-budget control
                    # rotation. The candidate overlay remains durable, but it cannot
                    # place a retired row onto the ordinary next-due queue.
                    schedule.next_due_at = None
                elif schedule.next_due_at is None or accelerated_due < schedule.next_due_at:
                    schedule.next_due_at = accelerated_due
                schedule.priority = min(schedule.priority, 1)
                schedule.target_interval_seconds = min(
                    schedule.target_interval_seconds or interval_seconds, interval_seconds
                )
                schedule.effective_interval_seconds = min(
                    schedule.effective_interval_seconds or interval_seconds, interval_seconds
                )
            elif (
                schedule.candidate_coverage_expires_at is not None
                and schedule.candidate_coverage_expires_at <= now
            ):
                schedule.candidate_coverage_expires_at = None
                schedule.candidate_coverage_interval_seconds = None
                schedule.candidate_tier_event_id = None
                schedule.coverage_next_transition_at = now
            schedule.updated_at = now
        if candidate is not None:
            for analysis_type in _task_types_for(target):
                await self._create_task(
                    session,
                    candidate=candidate,
                    tier=target,
                    analysis_type=analysis_type,
                    max_attempts=task_max_attempts,
                    now=now,
                )
        await session.flush()
        return CandidateTransitionResult(
            candidate_id=candidate.id if candidate else None,
            previous_tier=previous,
            current_tier=target,
            changed=True,
            coverage_expires_at=coverage_expires_at,
        )

    async def claim_tasks(
        self,
        session: AsyncSession,
        *,
        now: datetime,
        worker_id: str,
        collector_run_id: uuid.UUID | None,
        limit: int,
        tasks_per_minute: int,
        lease_duration: timedelta,
        analysis_types: tuple[str, ...] | None = None,
    ) -> tuple[CandidateTaskClaim, ...]:
        """Claim without exceeding the independent candidate task/minute budget."""
        now = _utc(now)
        minute_start = now.replace(second=0, microsecond=0)
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {"lock_id": _CANDIDATE_TASK_BUDGET_LOCK},
        )
        already_claimed = int(
            await session.scalar(
                select(func.count())
                .select_from(CandidateEnrichmentTask)
                .where(CandidateEnrichmentTask.claimed_at >= minute_start)
            )
            or 0
        )
        available = max(0, min(limit, tasks_per_minute - already_claimed))
        if not available:
            return ()
        eligible = [
            CandidateEnrichmentTask.not_before <= now,
            CandidateEnrichmentTask.attempt_count < CandidateEnrichmentTask.max_attempts,
            or_(
                CandidateEnrichmentTask.status.in_(("pending", "retry", "deferred")),
                (
                    (CandidateEnrichmentTask.status == "claimed")
                    & (CandidateEnrichmentTask.lease_expires_at <= now)
                ),
            ),
        ]
        if analysis_types is not None:
            eligible.append(CandidateEnrichmentTask.analysis_type.in_(analysis_types))
        rows = list(
            (
                await session.execute(
                    select(CandidateEnrichmentTask)
                    .where(*eligible)
                    .order_by(
                        CandidateEnrichmentTask.not_before,
                        CandidateEnrichmentTask.created_at,
                        CandidateEnrichmentTask.id,
                    )
                    .limit(available)
                    .with_for_update(skip_locked=True)
                )
            ).scalars()
        )
        claims: list[CandidateTaskClaim] = []
        for task in rows:
            lease_id = uuid.uuid4()
            task.status = "claimed"
            task.lease_id = lease_id
            task.lease_expires_at = now + lease_duration
            task.claimed_at = now
            task.claimed_by = worker_id
            task.collector_run_id = collector_run_id
            task.attempt_count += 1
            task.updated_at = now
            claims.append(
                CandidateTaskClaim(
                    id=task.id,
                    lease_id=lease_id,
                    candidate_id=task.candidate_id,
                    token_id=task.token_id,
                    tier=CandidateTier(task.tier),
                    analysis_type=task.analysis_type,
                    input_watermark=task.input_watermark,
                    input_sha256=task.input_sha256,
                    attempt_number=task.attempt_count,
                )
            )
        await session.flush()
        return tuple(claims)

    async def complete_task(
        self,
        session: AsyncSession,
        *,
        claim: CandidateTaskClaim,
        completed_at: datetime,
        outcome: str,
        evidence_generated_at: datetime | None,
        evidence_received_at: datetime | None,
        fresh_until: datetime | None,
        result_identity: str | None,
        result_sha256: str | None,
    ) -> None:
        task = await session.scalar(
            select(CandidateEnrichmentTask)
            .where(CandidateEnrichmentTask.id == claim.id)
            .with_for_update()
        )
        if task is None:
            raise LostCandidateTaskLeaseError("candidate enrichment task does not exist")
        if task.status == "succeeded":
            expected = (outcome, result_identity, result_sha256)
            actual = (task.outcome, task.result_identity, task.result_sha256)
            if actual != expected:
                raise CandidateIntegrityError(
                    "idempotent task completion maps to different result content"
                )
            return
        if task.lease_id != claim.lease_id:
            raise LostCandidateTaskLeaseError("candidate enrichment task lease is no longer owned")
        task.status = "succeeded"
        task.completed_at = _utc(completed_at)
        task.outcome = outcome
        task.evidence_generated_at = evidence_generated_at
        task.evidence_received_at = evidence_received_at
        task.fresh_until = fresh_until
        task.result_identity = result_identity
        task.result_sha256 = result_sha256
        task.lease_id = None
        task.lease_expires_at = None
        task.updated_at = task.completed_at

    async def fail_task(
        self,
        session: AsyncSession,
        *,
        claim: CandidateTaskClaim,
        failed_at: datetime,
        failure_detail: dict[str, object],
        retry_delay: timedelta,
    ) -> None:
        task = await self._owned_task(session, claim)
        failed_at = _utc(failed_at)
        terminal = task.attempt_count >= task.max_attempts
        task.status = "failed" if terminal else "retry"
        task.failure_detail = failure_detail
        task.not_before = failed_at if terminal else failed_at + retry_delay
        task.completed_at = failed_at if terminal else None
        task.lease_id = None
        task.lease_expires_at = None
        task.updated_at = failed_at

    async def defer_task(
        self,
        session: AsyncSession,
        *,
        claim: CandidateTaskClaim,
        deferred_at: datetime,
        not_before: datetime,
        reason: dict[str, object],
    ) -> None:
        """Release a lease without charging an attempt when a lower budget is full."""
        task = await self._owned_task(session, claim)
        task.status = "deferred"
        task.not_before = _utc(not_before)
        task.failure_detail = reason
        task.attempt_count = max(0, task.attempt_count - 1)
        task.lease_id = None
        task.lease_expires_at = None
        task.updated_at = _utc(deferred_at)

    async def _owned_task(
        self, session: AsyncSession, claim: CandidateTaskClaim
    ) -> CandidateEnrichmentTask:
        task = await session.scalar(
            select(CandidateEnrichmentTask)
            .where(
                CandidateEnrichmentTask.id == claim.id,
                CandidateEnrichmentTask.lease_id == claim.lease_id,
            )
            .with_for_update()
        )
        if task is None:
            raise LostCandidateTaskLeaseError("candidate enrichment task lease is no longer owned")
        return task

    async def _persist_policy(self, session: AsyncSession, policy: CandidatePolicy) -> None:
        await session.execute(
            insert(CandidatePolicyRecord)
            .values(
                policy_sha256=policy.sha256,
                policy_name=ORCHESTRATION_RULE_NAME,
                policy_version=ORCHESTRATION_RULE_VERSION,
                policy_snapshot=policy.snapshot,
            )
            .on_conflict_do_nothing(index_elements=[CandidatePolicyRecord.policy_sha256])
        )

    async def _persist_candidate(
        self,
        session: AsyncSession,
        *,
        collection_epoch_id: uuid.UUID,
        collector_run_id: uuid.UUID | None,
        evidence: CandidateEvidence,
        decision: EvaluationDecision,
        policy: CandidatePolicy,
    ) -> CandidateEvent:
        candidate_id_text, key = candidate_identity(
            epoch_id=str(collection_epoch_id),
            evidence=evidence,
            policy_sha256=policy.sha256,
            reason=decision.reason.value,
        )
        candidate_id = uuid.UUID(candidate_id_text)
        values = {
            "id": candidate_id,
            "idempotency_key": key,
            "token_id": uuid.UUID(evidence.token_id),
            "collection_epoch_id": collection_epoch_id,
            "collector_run_id": collector_run_id,
            "candidate_at": evidence.evaluated_at,
            "trigger_type": decision.reason.value,
            "trigger_version": ORCHESTRATION_RULE_VERSION,
            "feature_set_name": "market-v1",
            "feature_set_version": "1.0.0",
            "input_watermark": evidence.watermark,
            "lifecycle_state": evidence.lifecycle_state,
            "coverage_class": evidence.coverage_class,
            "evidence_sha256": evidence.sha256,
            "evidence_snapshot": evidence.identity_payload,
            "source_fact_ids": {
                "observation_id": evidence.observation_id,
                "boost_event_id": evidence.boost_event_id,
                "security_snapshot_id": evidence.security_snapshot_id,
                "holder_snapshot_id": evidence.holder_snapshot_id,
                "trader_snapshot_id": evidence.trader_snapshot_id,
            },
            "policy_sha256": policy.sha256,
        }
        await session.execute(insert(CandidateEvent).values(**values).on_conflict_do_nothing())
        row = await session.get(CandidateEvent, candidate_id)
        if row is None:
            row = await session.scalar(
                select(CandidateEvent).where(CandidateEvent.idempotency_key == key)
            )
        if row is None or not _candidate_equal(row, values):
            raise CandidateIntegrityError(
                "candidate deterministic identity maps to different semantic content"
            )
        return row

    async def _persist_tier_event(
        self,
        session: AsyncSession,
        *,
        collection_epoch_id: uuid.UUID,
        collector_run_id: uuid.UUID | None,
        token_id: uuid.UUID,
        candidate_id: uuid.UUID | None,
        previous: CandidateTier,
        target: CandidateTier,
        evidence: CandidateEvidence,
        decision: EvaluationDecision,
        policy: CandidatePolicy,
        now: datetime,
    ) -> CandidateTierEvent:
        identity = _digest(
            {
                "epoch": str(collection_epoch_id),
                "token": str(token_id),
                "previous": previous.value,
                "target": target.value,
                "watermark": evidence.watermark.isoformat(),
                "evidence": evidence.sha256,
                "policy": policy.sha256,
            }
        )
        event_id = uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research:candidate-tier:{identity}")
        values = {
            "id": event_id,
            "idempotency_key": identity,
            "candidate_id": candidate_id,
            "token_id": token_id,
            "collection_epoch_id": collection_epoch_id,
            "collector_run_id": collector_run_id,
            "previous_tier": previous.value,
            "new_tier": target.value,
            "decided_at": now,
            "input_watermark": evidence.watermark,
            "reason_code": decision.reason.value,
            "reason_detail": decision.detail,
            "transition_version": ORCHESTRATION_RULE_VERSION,
            "policy_sha256": policy.sha256,
            "evidence_sha256": evidence.sha256,
        }
        await session.execute(insert(CandidateTierEvent).values(**values).on_conflict_do_nothing())
        row = await session.get(CandidateTierEvent, event_id)
        if row is None or not _tier_equal(row, values):
            raise CandidateIntegrityError(
                "candidate tier identity maps to different semantic content"
            )
        return row

    async def _create_task(
        self,
        session: AsyncSession,
        *,
        candidate: CandidateEvent,
        tier: CandidateTier,
        analysis_type: str,
        max_attempts: int,
        now: datetime,
    ) -> None:
        semantic = _digest(
            {
                "candidate": str(candidate.id),
                "tier": tier.value,
                "analysis_type": analysis_type,
                "input": candidate.evidence_sha256,
            }
        )
        task_id = uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research:candidate-task:{semantic}")
        await session.execute(
            insert(CandidateEnrichmentTask)
            .values(
                id=task_id,
                semantic_key=semantic,
                candidate_id=candidate.id,
                token_id=candidate.token_id,
                collection_epoch_id=candidate.collection_epoch_id,
                tier=tier.value,
                analysis_type=analysis_type,
                input_watermark=candidate.input_watermark,
                input_sha256=candidate.evidence_sha256,
                created_at=now,
                not_before=now,
                status="pending",
                attempt_count=0,
                max_attempts=max_attempts,
                updated_at=now,
            )
            .on_conflict_do_nothing()
        )


def _task_types_for(tier: CandidateTier) -> tuple[str, ...]:
    if tier is CandidateTier.TIER_1_INTERESTING:
        return ("BASIC_SECURITY_REFRESH", "METADATA_REFRESH")
    if tier is CandidateTier.TIER_2_INVESTIGATE:
        return (
            "HOLDER_SNAPSHOT",
            "TRADER_DISTRIBUTION",
            "CREATOR_HISTORY",
            "LIQUIDITY_EVENT_ANALYSIS",
        )
    if tier is CandidateTier.TIER_3_DEEP_REVIEW:
        return ("WALLET_CLUSTER_ANALYSIS", "FUNDING_GRAPH_ANALYSIS")
    return ()


def _tier_rank(tier: CandidateTier) -> int:
    return {
        CandidateTier.TIER_0_UNIVERSAL: 0,
        CandidateTier.TIER_1_INTERESTING: 1,
        CandidateTier.TIER_2_INVESTIGATE: 2,
        CandidateTier.TIER_3_DEEP_REVIEW: 3,
        CandidateTier.TIER_4_PRETRADE: 4,
    }[tier]


def _candidate_equal(row: CandidateEvent, values: dict[str, object]) -> bool:
    keys = (
        "idempotency_key",
        "token_id",
        "collection_epoch_id",
        "collector_run_id",
        "candidate_at",
        "trigger_type",
        "trigger_version",
        "feature_set_name",
        "feature_set_version",
        "input_watermark",
        "lifecycle_state",
        "coverage_class",
        "evidence_sha256",
        "evidence_snapshot",
        "source_fact_ids",
        "policy_sha256",
    )
    return all(getattr(row, key) == values[key] for key in keys)


def _tier_equal(row: CandidateTierEvent, values: dict[str, object]) -> bool:
    keys = (
        "idempotency_key",
        "candidate_id",
        "token_id",
        "collection_epoch_id",
        "collector_run_id",
        "previous_tier",
        "new_tier",
        "decided_at",
        "input_watermark",
        "reason_code",
        "reason_detail",
        "transition_version",
        "policy_sha256",
        "evidence_sha256",
    )
    return all(getattr(row, key) == values[key] for key in keys)


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
