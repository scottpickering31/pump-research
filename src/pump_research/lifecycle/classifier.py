"""Transactional lifecycle derivation from deterministically selected pair evidence."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.lifecycle.evidence import RawObservationEvidence
from pump_research.lifecycle.evidence_selection import (
    EvidenceSelectionOutcome,
    HighestLiquidityEvidencePolicy,
    LifecycleEvidenceCandidate,
)
from pump_research.lifecycle.policy import LifecyclePolicy, LifecycleTransitionRule
from pump_research.persistence.models import ApiRequestLog, Observation, Pair, PollSchedule, Token
from pump_research.persistence.repositories import (
    LifecycleEventRepository,
    LifecycleEvidenceEvaluationRepository,
    _normalize_utc,
)
from pump_research.scheduling.clock import Clock, SystemClock
from pump_research.scheduling.policy import LifecycleState
from pump_research.scheduling.scheduler import AdaptiveScheduler


@dataclass(frozen=True, slots=True)
class LifecycleTransition:
    """The persisted result of applying one lifecycle rule."""

    event_id: uuid.UUID
    token_id: uuid.UUID
    observation_id: uuid.UUID
    observation_received_at: datetime
    decided_at: datetime
    previous_state: LifecycleState
    new_state: LifecycleState
    rule: LifecycleTransitionRule


@dataclass(frozen=True, slots=True)
class LifecycleRequestEvaluation:
    """Derived outcomes for every token represented in one API response."""

    transitions: tuple[LifecycleTransition, ...]
    selection_failures: tuple[dict[str, object], ...]


@dataclass(frozen=True, slots=True)
class _TokenEvaluation:
    transition: LifecycleTransition | None
    selection_failure: dict[str, object] | None


class LifecycleClassifier:
    """Select one pair deterministically, then apply configured lifecycle rules."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        *,
        clock: Clock | None = None,
        scheduler: AdaptiveScheduler | None = None,
    ) -> None:
        self._session_factory = session_factory
        self.policy = LifecyclePolicy.from_settings(settings)
        self.evidence_policy = HighestLiquidityEvidencePolicy()
        self._clock = clock or SystemClock()
        self._events = LifecycleEventRepository()
        self._evidence_evaluations = LifecycleEvidenceEvaluationRepository()
        self._scheduler = scheduler or AdaptiveScheduler(
            session_factory, settings, clock=self._clock
        )

    async def evaluate_observation(
        self,
        *,
        observation_id: uuid.UUID,
        received_at: datetime,
    ) -> LifecycleTransition | None:
        """Evaluate the complete token/request candidate set containing an observation."""
        observation_received_at = _normalize_utc(received_at, "received_at")
        assert observation_received_at is not None
        async with self._session_factory() as session, session.begin():
            return await self.evaluate_observation_in_session(
                session,
                observation_id=observation_id,
                received_at=observation_received_at,
            )

    async def evaluate_observation_in_session(
        self,
        session: AsyncSession,
        *,
        observation_id: uuid.UUID,
        received_at: datetime,
    ) -> LifecycleTransition | None:
        """Compatibility entry point that still evaluates all same-response pairs."""
        observation_received_at = _normalize_utc(received_at, "received_at")
        assert observation_received_at is not None
        observation, pair = await self._load_observation_and_pair(
            session,
            observation_id=observation_id,
            received_at=observation_received_at,
        )
        result = await self._evaluate_token_request_in_session(
            session,
            token_id=pair.token_id,
            api_request_log_id=observation.api_request_log_id,
        )
        return result.transition

    async def evaluate_request_in_session(
        self,
        session: AsyncSession,
        *,
        api_request_log_id: uuid.UUID,
    ) -> LifecycleRequestEvaluation:
        """Select and evaluate once per represented token, never once per pair."""
        token_ids = list(
            (
                await session.execute(
                    select(Pair.token_id)
                    .join(Observation, Observation.pair_id == Pair.id)
                    .where(Observation.api_request_log_id == api_request_log_id)
                    .distinct()
                    .order_by(Pair.token_id)
                )
            ).scalars()
        )
        transitions: list[LifecycleTransition] = []
        failures: list[dict[str, object]] = []
        for token_id in token_ids:
            result = await self._evaluate_token_request_in_session(
                session,
                token_id=token_id,
                api_request_log_id=api_request_log_id,
            )
            if result.transition is not None:
                transitions.append(result.transition)
            if result.selection_failure is not None:
                failures.append(result.selection_failure)
        return LifecycleRequestEvaluation(tuple(transitions), tuple(failures))

    async def _evaluate_token_request_in_session(
        self,
        session: AsyncSession,
        *,
        token_id: uuid.UUID,
        api_request_log_id: uuid.UUID,
    ) -> _TokenEvaluation:
        rows = list(
            (
                await session.execute(
                    select(Observation, Pair)
                    .join(Pair, Pair.id == Observation.pair_id)
                    .where(
                        Observation.api_request_log_id == api_request_log_id,
                        Pair.token_id == token_id,
                    )
                    .order_by(Pair.chain, Pair.address)
                )
            ).all()
        )
        if not rows:
            msg = "Lifecycle evaluation requires at least one persisted pair observation"
            raise LookupError(msg)
        watermarks = {observation.received_at for observation, _ in rows}
        if len(watermarks) != 1:
            msg = "One API response produced inconsistent observation knowledge timestamps"
            raise ValueError(msg)
        input_watermark = watermarks.pop()
        candidates = tuple(
            LifecycleEvidenceCandidate(
                observation_id=observation.id,
                observation_received_at=observation.received_at,
                pair_id=pair.id,
                chain=pair.chain,
                pair_address=pair.address,
                dex_identifier=pair.dex_identifier,
                liquidity_usd=observation.liquidity_usd,
                volume_m5_usd=observation.volume_m5_usd,
                volume_h1_usd=observation.volume_h1_usd,
            )
            for observation, pair in rows
        )
        selection = self.evidence_policy.select(candidates)
        selected = selection.selected
        evidence_evaluation = await self._evidence_evaluations.record(
            session,
            input_watermark=input_watermark,
            token_id=token_id,
            api_request_log_id=api_request_log_id,
            outcome=selection.outcome.value,
            selected_pair_id=selected.pair_id if selected else None,
            selected_observation_id=selected.observation_id if selected else None,
            selected_observation_received_at=(
                selected.observation_received_at if selected else None
            ),
            reason_code=selection.reason_code,
            reason_detail=selection.reason_detail,
            policy_sha256=self.evidence_policy.sha256,
            policy_snapshot=self.evidence_policy.snapshot,
        )
        if selection.outcome is EvidenceSelectionOutcome.FAILED:
            return _TokenEvaluation(
                transition=None,
                selection_failure={
                    "token_id": str(token_id),
                    "evidence_evaluation_id": str(evidence_evaluation.id),
                    "reason_code": selection.reason_code,
                },
            )
        assert selected is not None

        # Keep the lock order aligned with AdaptiveScheduler: Token then PollSchedule.
        await session.execute(select(Token.id).where(Token.id == token_id).with_for_update())
        schedule = (
            await session.execute(
                select(PollSchedule).where(PollSchedule.token_id == token_id).with_for_update()
            )
        ).scalar_one_or_none()
        if schedule is None or selected.observation_received_at < schedule.state_decided_at:
            return _TokenEvaluation(None, None)

        current_state = LifecycleState(schedule.lifecycle_state)
        evidence = RawObservationEvidence(
            observation_id=selected.observation_id,
            received_at=selected.observation_received_at,
            pair_id=selected.pair_id,
            api_request_log_id=api_request_log_id,
            volume_m5_usd=selected.volume_m5_usd,
            volume_h1_usd=selected.volume_h1_usd,
            liquidity_usd=selected.liquidity_usd,
        )
        evaluation = self.policy.evaluate(state=current_state, observation=evidence)
        if evaluation is None:
            return _TokenEvaluation(None, None)

        decided_at = self._normalized_now()
        if decided_at < evidence.received_at:
            msg = "Lifecycle decision cannot predate the observation knowledge time"
            raise ValueError(msg)
        collector_run_id = await session.scalar(
            select(ApiRequestLog.collector_run_id).where(ApiRequestLog.id == api_request_log_id)
        )
        event = await self._events.record(
            session,
            collector_run_id=collector_run_id,
            token_id=token_id,
            idempotency_key=_transition_idempotency_key(
                token_id=token_id,
                evidence_evaluation_id=evidence_evaluation.id,
                rule=evaluation.rule,
                policy_sha256=self.policy.sha256,
            ),
            previous_state=evaluation.previous_state.value,
            new_state=evaluation.new_state.value,
            decided_at=decided_at,
            input_watermark=evidence.received_at,
            reason_code=evaluation.rule.value,
            reason_detail={
                "what_happened": (
                    f"{evaluation.previous_state.value} -> {evaluation.new_state.value}"
                ),
                "rule": evaluation.rule.value,
                "input_values": evaluation.input_values,
                "thresholds": evaluation.thresholds,
                "observation": {
                    "id": str(evidence.observation_id),
                    "received_at": evidence.received_at.isoformat(),
                    "pair_id": str(evidence.pair_id),
                    "api_request_log_id": str(evidence.api_request_log_id),
                },
                "lifecycle_evidence": {
                    "evaluation_id": str(evidence_evaluation.id),
                    "policy_sha256": evidence_evaluation.policy_sha256,
                    "selected_observation_id": str(evidence.observation_id),
                    "selected_pair_id": str(evidence.pair_id),
                    "api_request_log_id": str(evidence.api_request_log_id),
                },
            },
            lifecycle_evidence_evaluation_id=evidence_evaluation.id,
            lifecycle_evidence_input_watermark=evidence_evaluation.input_watermark,
            configuration_sha256=self.policy.sha256,
            configuration_snapshot=self.policy.snapshot,
        )
        await self._scheduler.set_lifecycle_state_in_session(
            session,
            token_id=token_id,
            state=evaluation.new_state,
            decided_at=decided_at,
            collector_run_id=collector_run_id,
            reason_code=evaluation.rule.value,
        )
        return _TokenEvaluation(
            transition=LifecycleTransition(
                event_id=event.id,
                token_id=token_id,
                observation_id=evidence.observation_id,
                observation_received_at=evidence.received_at,
                decided_at=decided_at,
                previous_state=evaluation.previous_state,
                new_state=evaluation.new_state,
                rule=evaluation.rule,
            ),
            selection_failure=None,
        )

    async def _load_observation_and_pair(
        self,
        session: AsyncSession,
        *,
        observation_id: uuid.UUID,
        received_at: datetime,
    ) -> tuple[Observation, Pair]:
        result = await session.execute(
            select(Observation, Pair)
            .join(Pair, Pair.id == Observation.pair_id)
            .where(
                Observation.id == observation_id,
                Observation.received_at == received_at,
            )
        )
        row = result.one_or_none()
        if row is None:
            msg = "Observation was not found for its received_at partition key"
            raise LookupError(msg)
        return row._t

    def _normalized_now(self) -> datetime:
        now = _normalize_utc(self._clock.now(), "clock.now")
        assert now is not None
        return now


def _transition_idempotency_key(
    *,
    token_id: uuid.UUID,
    evidence_evaluation_id: uuid.UUID,
    rule: LifecycleTransitionRule,
    policy_sha256: str,
) -> str:
    encoded = ":".join(
        (
            "lifecycle_classifier",
            str(token_id),
            str(evidence_evaluation_id),
            rule.value,
            policy_sha256,
        )
    ).encode()
    return hashlib.sha256(encoded).hexdigest()
