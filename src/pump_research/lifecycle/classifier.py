"""Transactional lifecycle classification from immutable market observations."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.lifecycle.evidence import RawObservationEvidence
from pump_research.lifecycle.policy import LifecyclePolicy, LifecycleTransitionRule
from pump_research.persistence.models import Observation, Pair, PollSchedule, Token
from pump_research.persistence.repositories import LifecycleEventRepository, _normalize_utc
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


class LifecycleClassifier:
    """Apply configured lifecycle rules without mutating raw observations."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        *,
        clock: Clock | None = None,
    ) -> None:
        self._session_factory = session_factory
        self.policy = LifecyclePolicy.from_settings(settings)
        self._clock = clock or SystemClock()
        self._events = LifecycleEventRepository()
        self._scheduler = AdaptiveScheduler(session_factory, settings, clock=self._clock)

    async def evaluate_observation(
        self,
        *,
        observation_id: uuid.UUID,
        received_at: datetime,
    ) -> LifecycleTransition | None:
        """Classify one already-persisted observation exactly once.

        ``received_at`` is part of the partitioned observation key.  Requiring it
        makes lookup unambiguous and binds the decision to collector knowledge
        time rather than an inferred source timestamp.
        """
        observation_received_at = _normalize_utc(received_at, "received_at")
        assert observation_received_at is not None
        async with self._session_factory() as session, session.begin():
            observation, pair = await self._load_observation_and_pair(
                session,
                observation_id=observation_id,
                received_at=observation_received_at,
            )
            # Keep the lock order aligned with AdaptiveScheduler: Token then
            # PollSchedule. This avoids a lifecycle worker deadlocking a scheduler.
            await session.execute(
                select(Token.id).where(Token.id == pair.token_id).with_for_update()
            )
            schedule = (
                await session.execute(
                    select(PollSchedule)
                    .where(PollSchedule.token_id == pair.token_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if schedule is None:
                return None
            if observation.received_at < schedule.state_decided_at:
                # This fact was known before the current state existed, so using it
                # now would make the transition retrospective/look-ahead biased.
                return None

            current_state = LifecycleState(schedule.lifecycle_state)
            evidence = RawObservationEvidence(
                observation_id=observation.id,
                received_at=observation.received_at,
                pair_id=observation.pair_id,
                api_request_log_id=observation.api_request_log_id,
                volume_m5_usd=observation.volume_m5_usd,
                volume_h1_usd=observation.volume_h1_usd,
                liquidity_usd=observation.liquidity_usd,
            )
            evaluation = self.policy.evaluate(
                state=current_state,
                observation=evidence,
            )
            if evaluation is None:
                return None

            decided_at = self._normalized_now()
            if decided_at < observation.received_at:
                msg = "Lifecycle decision cannot predate the observation knowledge time"
                raise ValueError(msg)
            event = await self._events.record(
                session,
                token_id=pair.token_id,
                idempotency_key=_transition_idempotency_key(
                    token_id=pair.token_id,
                    observation_id=observation.id,
                    observation_received_at=observation.received_at,
                    rule=evaluation.rule,
                    policy_sha256=self.policy.sha256,
                ),
                previous_state=evaluation.previous_state.value,
                new_state=evaluation.new_state.value,
                decided_at=decided_at,
                input_watermark=observation.received_at,
                reason_code=evaluation.rule.value,
                reason_detail={
                    "what_happened": (
                        f"{evaluation.previous_state.value} -> "
                        f"{evaluation.new_state.value}"
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
                },
                configuration_sha256=self.policy.sha256,
                configuration_snapshot=self.policy.snapshot,
            )
            await self._scheduler.set_lifecycle_state_in_session(
                session,
                token_id=pair.token_id,
                state=evaluation.new_state,
                decided_at=decided_at,
                reason_code=evaluation.rule.value,
            )
            return LifecycleTransition(
                event_id=event.id,
                token_id=pair.token_id,
                observation_id=observation.id,
                observation_received_at=observation.received_at,
                decided_at=decided_at,
                previous_state=evaluation.previous_state,
                new_state=evaluation.new_state,
                rule=evaluation.rule,
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
    observation_id: uuid.UUID,
    observation_received_at: datetime,
    rule: LifecycleTransitionRule,
    policy_sha256: str,
) -> str:
    encoded = ":".join(
        (
            "lifecycle_classifier",
            str(token_id),
            str(observation_id),
            observation_received_at.isoformat(),
            rule.value,
            policy_sha256,
        )
    ).encode()
    return hashlib.sha256(encoded).hexdigest()
