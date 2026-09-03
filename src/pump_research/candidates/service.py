"""Application service around deterministic policy and durable persistence."""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.candidates.policy import CandidateEvidence, CandidatePolicy, CandidateTier
from pump_research.candidates.repository import (
    CandidateRepository,
    CandidateTaskClaim,
    CandidateTransitionResult,
)
from pump_research.persistence.models import (
    BoostEvent,
    BoostObservation,
    CandidateCurrentState,
    CandidateEvent,
    CollectorRun,
    HolderSnapshot,
    LifecycleEvidenceEvaluation,
    Observation,
    Pair,
    PollSchedule,
    SecurityFeatureSnapshot,
    Token,
    TokenSecuritySnapshot,
    TraderDistributionSnapshot,
)
from pump_research.scheduling.locks import lock_schedule_token_fk_path

_BOOST_WAKEUP_BUDGET_LOCK = 7_428_901_165


class CandidateOrchestrationService:
    """Evaluate supplied as-of facts; provider collection remains elsewhere."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        policy: CandidatePolicy,
        *,
        task_lease: timedelta,
        task_max_attempts: int,
    ) -> None:
        self._session_factory = session_factory
        self.policy = policy
        self._task_lease = task_lease
        self._task_max_attempts = task_max_attempts
        self._repository = CandidateRepository()

    async def evaluate(
        self,
        *,
        collection_epoch_id: uuid.UUID,
        collector_run_id: uuid.UUID | None,
        evidence: CandidateEvidence,
    ) -> CandidateTransitionResult:
        """Nominate/promote/demote atomically from facts received by the watermark."""
        async with self._session_factory() as session, session.begin():
            current_tier_raw = await session.scalar(
                select(CandidateCurrentState.tier).where(
                    CandidateCurrentState.collection_epoch_id == collection_epoch_id,
                    CandidateCurrentState.token_id == uuid.UUID(evidence.token_id),
                )
            )
            current_tier = (
                CandidateTier(current_tier_raw)
                if current_tier_raw is not None
                else CandidateTier.TIER_0_UNIVERSAL
            )
            decision = self.policy.evaluate(evidence, current_tier=current_tier)
            return await self._repository.apply_evaluation(
                session,
                collection_epoch_id=collection_epoch_id,
                collector_run_id=collector_run_id,
                evidence=evidence,
                decision=decision,
                policy=self.policy,
                now=evidence.evaluated_at,
                task_max_attempts=self._task_max_attempts,
            )

    async def claim_tasks(
        self,
        *,
        now: datetime,
        worker_id: str,
        collector_run_id: uuid.UUID | None,
        limit: int,
        analysis_types: tuple[str, ...] | None = None,
    ) -> tuple[CandidateTaskClaim, ...]:
        async with self._session_factory() as session, session.begin():
            return await self._repository.claim_tasks(
                session,
                now=now,
                worker_id=worker_id,
                collector_run_id=collector_run_id,
                limit=limit,
                tasks_per_minute=self.policy.tasks_per_minute,
                lease_duration=self._task_lease,
                analysis_types=analysis_types,
            )

    async def complete_task(
        self,
        claim: CandidateTaskClaim,
        *,
        completed_at: datetime,
        outcome: str,
        evidence_generated_at: datetime | None = None,
        evidence_received_at: datetime | None = None,
        fresh_until: datetime | None = None,
        result_identity: str | None = None,
        result_sha256: str | None = None,
    ) -> None:
        async with self._session_factory() as session, session.begin():
            await self._repository.complete_task(
                session,
                claim=claim,
                completed_at=completed_at,
                outcome=outcome,
                evidence_generated_at=evidence_generated_at,
                evidence_received_at=evidence_received_at,
                fresh_until=fresh_until,
                result_identity=result_identity,
                result_sha256=result_sha256,
            )

    async def fail_task(
        self,
        claim: CandidateTaskClaim,
        *,
        failed_at: datetime,
        failure_detail: dict[str, object],
        retry_delay: timedelta,
    ) -> None:
        async with self._session_factory() as session, session.begin():
            await self._repository.fail_task(
                session,
                claim=claim,
                failed_at=failed_at,
                failure_detail=failure_detail,
                retry_delay=retry_delay,
            )

    async def defer_task(
        self,
        claim: CandidateTaskClaim,
        *,
        deferred_at: datetime,
        not_before: datetime,
        reason: dict[str, object],
    ) -> None:
        async with self._session_factory() as session, session.begin():
            await self._repository.defer_task(
                session,
                claim=claim,
                deferred_at=deferred_at,
                not_before=not_before,
                reason=reason,
            )

    async def evaluate_request_in_session(
        self,
        session: AsyncSession,
        *,
        api_request_log_id: uuid.UUID,
        collector_run_id: uuid.UUID,
    ) -> tuple[CandidateTransitionResult, ...]:
        """Evaluate each lifecycle-selected observation from one durable request."""
        run = await session.get(CollectorRun, collector_run_id)
        if run is None or run.collection_epoch_id is None:
            return ()
        selections = list(
            (
                await session.execute(
                    select(LifecycleEvidenceEvaluation, Observation, PollSchedule)
                    .join(
                        Observation,
                        (Observation.id == LifecycleEvidenceEvaluation.selected_observation_id)
                        & (
                            Observation.received_at
                            == LifecycleEvidenceEvaluation.selected_observation_received_at
                        ),
                    )
                    .join(Pair, Pair.id == Observation.pair_id)
                    .join(PollSchedule, PollSchedule.token_id == Pair.token_id)
                    .where(
                        LifecycleEvidenceEvaluation.api_request_log_id == api_request_log_id,
                        LifecycleEvidenceEvaluation.outcome == "selected",
                    )
                    .order_by(LifecycleEvidenceEvaluation.token_id)
                )
            ).all()
        )
        results: list[CandidateTransitionResult] = []
        for selected, observation, schedule in selections:
            current = await session.scalar(
                select(CandidateCurrentState).where(
                    CandidateCurrentState.collection_epoch_id == run.collection_epoch_id,
                    CandidateCurrentState.token_id == selected.token_id,
                )
            )
            evidence = await self._build_evidence(
                session,
                observation=observation,
                schedule=schedule,
                watermark=selected.input_watermark,
                boost_after=(
                    current.input_watermark if current is not None else schedule.admitted_at
                ),
            )
            tier = CandidateTier(current.tier) if current else CandidateTier.TIER_0_UNIVERSAL
            decision = self.policy.evaluate(evidence, current_tier=tier)
            results.append(
                await self._repository.apply_evaluation(
                    session,
                    collection_epoch_id=run.collection_epoch_id,
                    collector_run_id=collector_run_id,
                    evidence=evidence,
                    decision=decision,
                    policy=self.policy,
                    now=selected.input_watermark,
                    task_max_attempts=self._task_max_attempts,
                )
            )
        return tuple(results)

    async def evaluate_boost_observation_in_session(
        self,
        session: AsyncSession,
        *,
        boost_observation_id: uuid.UUID,
        collector_run_id: uuid.UUID,
    ) -> CandidateTransitionResult | None:
        """Wake a tracked retired token subject to a hard global per-minute gate."""
        # Boost feeds call this after taking that source/token's boost lock. The
        # feed workflow confines the whole path to one token per transaction, and
        # every boost wake-up takes this global gate before CandidateRepository's
        # tier-3 and coverage budget locks. No candidate path that owns either
        # downstream budget lock later requests this gate or another token's feed
        # enrichment lock, so the nested global lock cannot close a lock cycle.
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {"lock_id": _BOOST_WAKEUP_BUDGET_LOCK},
        )
        boost = await session.get(BoostObservation, boost_observation_id)
        run = await session.get(CollectorRun, collector_run_id)
        if boost is None or run is None or run.collection_epoch_id is None:
            return None
        minute_start = boost.received_at.replace(second=0, microsecond=0)
        recent = int(
            await session.scalar(
                select(func.count())
                .select_from(CandidateEvent)
                .where(
                    CandidateEvent.trigger_type == "BOOST_ACTIVITY",
                    CandidateEvent.candidate_at >= minute_start,
                    CandidateEvent.candidate_at < minute_start + timedelta(minutes=1),
                )
            )
            or 0
        )
        if recent >= self.policy.boost_wakeups_per_minute:
            return None
        schedule = await session.get(PollSchedule, boost.token_id)
        observation = await session.scalar(
            select(Observation)
            .join(Pair, Pair.id == Observation.pair_id)
            .where(
                Pair.token_id == boost.token_id,
                Observation.received_at <= boost.received_at,
            )
            .order_by(Observation.received_at.desc(), Observation.id.desc())
            .limit(1)
        )
        if schedule is None or observation is None:
            return None
        boost_event = await session.scalar(
            select(BoostEvent)
            .where(BoostEvent.boost_observation_id == boost.id)
            .order_by(BoostEvent.decided_at, BoostEvent.id)
            .limit(1)
        )
        if boost_event is None:
            return None
        evidence = await self._build_evidence(
            session,
            observation=observation,
            schedule=schedule,
            watermark=boost.received_at,
            explicit_boost_event=boost_event,
            boost_after=None,
        )
        current = await session.scalar(
            select(CandidateCurrentState).where(
                CandidateCurrentState.collection_epoch_id == run.collection_epoch_id,
                CandidateCurrentState.token_id == boost.token_id,
            )
        )
        tier = CandidateTier(current.tier) if current else CandidateTier.TIER_0_UNIVERSAL
        decision = self.policy.evaluate(evidence, current_tier=tier)
        return await self._repository.apply_evaluation(
            session,
            collection_epoch_id=run.collection_epoch_id,
            collector_run_id=collector_run_id,
            evidence=evidence,
            decision=decision,
            policy=self.policy,
            now=boost.received_at,
            task_max_attempts=self._task_max_attempts,
        )

    async def evaluate_security_token(
        self,
        *,
        collection_epoch_id: uuid.UUID,
        collector_run_id: uuid.UUID | None,
        token_id: uuid.UUID,
        evaluated_at: datetime,
    ) -> CandidateTransitionResult | None:
        """Re-evaluate Tier 2/3 from newly received Phase 6 evidence."""
        async with self._session_factory() as session, session.begin():
            # Scheduler transactions that already own PollSchedule and will emit
            # token-referencing evidence hold the shared side of this gate. Drain
            # them before taking Token UPDATE so the immutable-evidence fence
            # cannot form Schedule -> Token / Token -> Schedule.
            await lock_schedule_token_fk_path(session, exclusive=True)
            # Serialize the evidence read itself, not merely the eventual tier
            # write. Otherwise workers that finish at one received-at watermark
            # can each evaluate a different partial commit set.
            await session.scalar(select(Token.id).where(Token.id == token_id).with_for_update())
            schedule = await session.get(PollSchedule, token_id)
            observation = await session.scalar(
                select(Observation)
                .join(Pair, Pair.id == Observation.pair_id)
                .where(
                    Pair.token_id == token_id,
                    Observation.received_at <= evaluated_at,
                )
                .order_by(Observation.received_at.desc(), Observation.id.desc())
                .limit(1)
            )
            if schedule is None or observation is None:
                return None
            current = await session.scalar(
                select(CandidateCurrentState).where(
                    CandidateCurrentState.collection_epoch_id == collection_epoch_id,
                    CandidateCurrentState.token_id == token_id,
                )
            )
            if current is None:
                return None
            evidence = await self._build_evidence(
                session,
                observation=observation,
                schedule=schedule,
                watermark=evaluated_at,
                boost_after=current.input_watermark,
            )
            tier = CandidateTier(current.tier)
            decision = self.policy.evaluate(evidence, current_tier=tier)
            return await self._repository.apply_evaluation(
                session,
                collection_epoch_id=collection_epoch_id,
                collector_run_id=collector_run_id,
                evidence=evidence,
                decision=decision,
                policy=self.policy,
                now=evaluated_at,
                task_max_attempts=self._task_max_attempts,
            )

    async def _build_evidence(
        self,
        session: AsyncSession,
        *,
        observation: Observation,
        schedule: PollSchedule,
        watermark: datetime,
        explicit_boost_event: BoostEvent | None = None,
        boost_after: datetime | None = None,
    ) -> CandidateEvidence:
        security = await session.scalar(
            select(TokenSecuritySnapshot)
            .where(
                TokenSecuritySnapshot.token_id == schedule.token_id,
                TokenSecuritySnapshot.received_at <= watermark,
            )
            .order_by(TokenSecuritySnapshot.received_at.desc(), TokenSecuritySnapshot.id.desc())
            .limit(1)
        )
        holder = await session.scalar(
            select(HolderSnapshot)
            .where(
                HolderSnapshot.token_id == schedule.token_id,
                HolderSnapshot.received_at <= watermark,
                HolderSnapshot.acquisition_mode == "historically_available",
            )
            .order_by(HolderSnapshot.received_at.desc(), HolderSnapshot.id.desc())
            .limit(1)
        )
        trader = await session.scalar(
            select(TraderDistributionSnapshot)
            .where(
                TraderDistributionSnapshot.token_id == schedule.token_id,
                TraderDistributionSnapshot.received_at <= watermark,
                TraderDistributionSnapshot.acquisition_mode == "historically_available",
            )
            .order_by(
                TraderDistributionSnapshot.received_at.desc(),
                TraderDistributionSnapshot.id.desc(),
            )
            .limit(1)
        )
        security_features = await session.scalar(
            select(SecurityFeatureSnapshot)
            .where(
                SecurityFeatureSnapshot.token_id == schedule.token_id,
                SecurityFeatureSnapshot.received_at <= watermark,
                SecurityFeatureSnapshot.acquisition_mode == "historically_available",
            )
            .order_by(
                SecurityFeatureSnapshot.received_at.desc(),
                SecurityFeatureSnapshot.id.desc(),
            )
            .limit(1)
        )
        boost_event = explicit_boost_event
        if boost_event is None:
            after = boost_after or schedule.admitted_at
            assert after is not None
            boost_event = await session.scalar(
                select(BoostEvent)
                .where(
                    BoostEvent.token_id == schedule.token_id,
                    BoostEvent.decided_at <= watermark,
                    BoostEvent.decided_at > after,
                )
                .order_by(BoostEvent.decided_at.desc(), BoostEvent.id.desc())
                .limit(1)
            )
        assert schedule.admitted_at is not None
        return CandidateEvidence(
            token_id=str(schedule.token_id),
            evaluated_at=watermark,
            watermark=watermark,
            lifecycle_state=schedule.lifecycle_state,
            coverage_class=schedule.coverage_class or "LEGACY_UNMAPPED",
            admitted_at=schedule.admitted_at,
            observation_id=str(observation.id),
            observation_received_at=observation.received_at,
            liquidity_usd=observation.liquidity_usd,
            volume_m5_usd=observation.volume_m5_usd,
            buys_m5=observation.buys_m5,
            sells_m5=observation.sells_m5,
            boost_event_id=str(boost_event.id) if boost_event else None,
            boost_received_at=boost_event.decided_at if boost_event else None,
            security_snapshot_id=str(security.id) if security else None,
            security_received_at=security.received_at if security else None,
            coverage_resurrection=(schedule.coverage_class == "RETIRED_CONTROL"),
            holder_snapshot_id=str(holder.id) if holder else None,
            holder_received_at=holder.received_at if holder else None,
            holder_top10_pct=holder.top_10_pct if holder else None,
            trader_snapshot_id=str(trader.id) if trader else None,
            trader_received_at=trader.received_at if trader else None,
            unique_traders=trader.unique_traders if trader else None,
            total_trades=trader.total_trades if trader else None,
            wallet_evidence_received_at=(
                security_features.received_at if security_features else None
            ),
            common_funder_share_pct=_decimal_feature(security_features, "common_funder_share"),
            liquidity_evidence_received_at=(
                security_features.received_at if security_features else None
            ),
            liquidity_removal_pct=_decimal_feature(
                security_features, "liquidity_removal_recent_pct"
            ),
        )


def _decimal_feature(snapshot: SecurityFeatureSnapshot | None, field: str) -> Decimal | None:
    if snapshot is None:
        return None
    value = snapshot.values.get(field)
    return Decimal(str(value)) if value is not None else None
