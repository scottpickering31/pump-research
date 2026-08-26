from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.lifecycle.classifier import LifecycleClassifier
from pump_research.lifecycle.policy import LifecycleTransitionRule
from pump_research.persistence.models import (
    CoverageDecision,
    LifecycleEvent,
    Observation,
    PollSchedule,
    PollScheduleDecision,
    Token,
)
from pump_research.persistence.repositories import (
    ApiRequestLogRepository,
    ObservationCreate,
    ObservationRepository,
    PairRepository,
    TokenRepository,
)
from pump_research.scheduling.policy import LifecycleState
from pump_research.scheduling.scheduler import AdaptiveScheduler

NOW = datetime(2026, 8, 14, 12, 0, tzinfo=UTC)


@dataclass(slots=True)
class FakeClock:
    """A deterministic wall clock for lifecycle decision tests."""

    current: datetime = NOW

    def now(self) -> datetime:
        return self.current

    def advance(self, seconds: int = 1) -> None:
        self.current += timedelta(seconds=seconds)


def _settings(**overrides: object) -> Settings:
    values: dict[str, object] = {
        "database_url": (
            "postgresql+asyncpg://researcher:password@localhost:5433/pump_research"
        ),
    }
    values.update(overrides)
    return Settings.model_validate(values)


async def _create_scheduled_observation(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    scheduler: AdaptiveScheduler,
    clock: FakeClock,
    state: LifecycleState,
    address: str,
    price_usd: Decimal | None = None,
    volume_m5_usd: Decimal | None = None,
    volume_h1_usd: Decimal | None = None,
    liquidity_usd: Decimal | None = None,
    received_at: datetime | None = None,
) -> tuple[Token, Observation]:
    token_repository = TokenRepository()
    pair_repository = PairRepository()
    request_repository = ApiRequestLogRepository()
    observation_repository = ObservationRepository()
    async with session_factory() as session, session.begin():
        token = await token_repository.get_or_create(
            session,
            chain="solana",
            address=address,
            first_discovered_at=clock.now(),
        )
        pair = await pair_repository.get_or_create(
            session,
            token_id=token.id,
            chain="solana",
            address=f"pair-{address}",
            dex_identifier="test-dex",
            first_discovered_at=clock.now(),
        )
        await scheduler.set_lifecycle_state_in_session(
            session,
            token_id=token.id,
            state=state,
            decided_at=clock.now(),
            admitted_at=clock.now(),
            reason_code="test_initial_state",
        )
        if received_at is None:
            # A transition cannot share a decision timestamp with the state it
            # replaces. Advance the fake wall clock to model fact availability.
            clock.advance()
            observation_received_at = clock.now()
        else:
            observation_received_at = received_at
        request = await request_repository.record(
            session,
            idempotency_key=f"request-{address}-{observation_received_at.isoformat()}",
            provider="test",
            endpoint="/test",
            requested_at=observation_received_at,
            received_at=observation_received_at,
            outcome="succeeded",
            http_status_code=200,
            request_payload={"address": address},
            response_payload={"pair": pair.address},
        )
        inserted = await observation_repository.record_many(
            session,
            api_request=request,
            observations=[
                ObservationCreate(
                    pair_id=pair.id,
                    price_usd=price_usd,
                    volume_m5_usd=volume_m5_usd,
                    volume_h1_usd=volume_h1_usd,
                    liquidity_usd=liquidity_usd,
                )
            ],
        )
        assert inserted == 1
        observation = (
            await session.execute(
                select(Observation).where(
                    Observation.received_at == observation_received_at,
                    Observation.api_request_log_id == request.id,
                    Observation.pair_id == pair.id,
                )
            )
        ).scalar_one()
    return token, observation


@pytest.mark.integration
@pytest.mark.parametrize(
    (
        "previous_state,fields,expected_state,rule,expected_inputs,expected_thresholds"
    ),
    [
        (
            LifecycleState.NEW,
            {"volume_m5_usd": Decimal("100")},
            LifecycleState.ACTIVE,
            LifecycleTransitionRule.NEW_TO_ACTIVE,
            {"volume_m5_usd": "100"},
            {"min_volume_m5_usd": "100"},
        ),
        (
            LifecycleState.NEW,
            {"volume_m5_usd": Decimal("99"), "liquidity_usd": Decimal("1000")},
            LifecycleState.WATCH,
            LifecycleTransitionRule.NEW_TO_WATCH,
            {"volume_m5_usd": "99", "liquidity_usd": "1000"},
            {"max_volume_m5_usd_exclusive": "100", "min_liquidity_usd": "1000"},
        ),
        (
            LifecycleState.ACTIVE,
            {"volume_m5_usd": Decimal("25")},
            LifecycleState.FADING,
            LifecycleTransitionRule.ACTIVE_TO_FADING,
            {"volume_m5_usd": "25"},
            {"max_volume_m5_usd": "25"},
        ),
        (
            LifecycleState.WATCH,
            {"volume_m5_usd": Decimal("10")},
            LifecycleState.FADING,
            LifecycleTransitionRule.WATCH_TO_FADING,
            {"volume_m5_usd": "10"},
            {"max_volume_m5_usd": "10"},
        ),
        (
            LifecycleState.FADING,
            {"volume_h1_usd": Decimal("10"), "liquidity_usd": Decimal("100")},
            LifecycleState.DORMANT,
            LifecycleTransitionRule.FADING_TO_DORMANT,
            {"volume_h1_usd": "10", "liquidity_usd": "100"},
            {"max_volume_h1_usd": "10", "max_liquidity_usd": "100"},
        ),
        (
            LifecycleState.DORMANT,
            {"volume_m5_usd": Decimal("100"), "liquidity_usd": Decimal("500")},
            LifecycleState.RESURRECTED,
            LifecycleTransitionRule.DORMANT_TO_RESURRECTED,
            {"volume_m5_usd": "100", "liquidity_usd": "500"},
            {"min_volume_m5_usd": "100", "min_liquidity_usd": "500"},
        ),
    ],
)
async def test_supported_transitions_record_complete_reconstructable_evidence(
    session_factory: async_sessionmaker[AsyncSession],
    previous_state: LifecycleState,
    fields: dict[str, Decimal],
    expected_state: LifecycleState,
    rule: LifecycleTransitionRule,
    expected_inputs: dict[str, str],
    expected_thresholds: dict[str, str],
) -> None:
    clock = FakeClock()
    settings = _settings()
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    clock.advance()
    token, observation = await _create_scheduled_observation(
        session_factory,
        scheduler=scheduler,
        clock=clock,
        state=previous_state,
        address=f"transition-{rule.value}",
        volume_m5_usd=fields.get("volume_m5_usd"),
        volume_h1_usd=fields.get("volume_h1_usd"),
        liquidity_usd=fields.get("liquidity_usd"),
    )
    classifier = LifecycleClassifier(session_factory, settings, clock=clock)

    transition = await classifier.evaluate_observation(
        observation_id=observation.id,
        received_at=observation.received_at,
    )

    assert transition is not None
    assert transition.previous_state is previous_state
    assert transition.new_state is expected_state
    assert transition.rule is rule
    assert transition.observation_received_at == observation.received_at
    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token.id)
        event = (
            await session.execute(
                select(LifecycleEvent).where(LifecycleEvent.id == transition.event_id)
            )
        ).scalar_one()
        token_count = await session.scalar(select(func.count()).select_from(Token))

    assert schedule is not None
    assert schedule.lifecycle_state == expected_state.value
    assert event.previous_state == previous_state.value
    assert event.new_state == expected_state.value
    assert event.decided_at == clock.now()
    assert event.input_watermark == observation.received_at
    assert event.reason_code == rule.value
    assert event.reason_detail is not None
    detail = event.reason_detail
    assert detail["what_happened"] == (
        f"{previous_state.value} -> {expected_state.value}"
    )
    assert detail["rule"] == rule.value
    assert detail["input_values"] == expected_inputs
    assert detail["thresholds"] == expected_thresholds
    observation_detail = detail["observation"]
    assert isinstance(observation_detail, dict)
    assert observation_detail["id"] == str(observation.id)
    assert event.configuration_sha256 == classifier.policy.sha256
    assert event.configuration_snapshot == classifier.policy.snapshot
    assert token_count == 1


@pytest.mark.integration
async def test_scheduled_request_transitions_own_one_capacity_identity(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """All lifecycle transitions in one scheduled request share its capacity owner."""
    clock = FakeClock()
    settings = _settings()
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    tokens: list[Token] = []
    pairs = []
    token_repository = TokenRepository()
    pair_repository = PairRepository()
    request_repository = ApiRequestLogRepository()
    observation_repository = ObservationRepository()
    async with session_factory() as session, session.begin():
        for index in range(2):
            token = await token_repository.get_or_create(
                session,
                chain="solana",
                address=f"scheduled-capacity-owner-{index}",
                first_discovered_at=clock.now(),
            )
            pair = await pair_repository.get_or_create(
                session,
                token_id=token.id,
                chain="solana",
                address=f"scheduled-capacity-owner-pair-{index}",
                dex_identifier="test-dex",
                first_discovered_at=clock.now(),
            )
            await scheduler.set_lifecycle_state_in_session(
                session,
                token_id=token.id,
                state=LifecycleState.NEW,
                decided_at=clock.now(),
                admitted_at=clock.now(),
                reason_code="scheduled_capacity_owner_setup",
            )
            tokens.append(token)
            pairs.append(pair)

    clock.advance()
    async with session_factory() as session, session.begin():
        request = await request_repository.record(
            session,
            idempotency_key="scheduled-capacity-owner-request",
            provider="test",
            endpoint="/test",
            requested_at=clock.now(),
            received_at=clock.now(),
            outcome="succeeded",
            http_status_code=200,
            request_payload={},
            response_payload={},
        )
        inserted = await observation_repository.record_many(
            session,
            api_request=request,
            observations=[
                ObservationCreate(pair_id=pair.id, volume_m5_usd=Decimal("100"))
                for pair in pairs
            ],
        )
        assert inserted == 2

    original_persist = scheduler._persist_capacity_decision
    attempted_ids = []

    async def persist_and_replace_process_cache(
        session: AsyncSession,
        decision: Any,
    ) -> None:
        attempted_ids.append(decision.id)
        await original_persist(session, decision)
        scheduler._cached_capacity_bucket = None
        scheduler._cached_capacity_decision = None

    scheduler._persist_capacity_decision = persist_and_replace_process_cache  # type: ignore[method-assign]
    classifier = LifecycleClassifier(
        session_factory,
        settings,
        clock=clock,
        scheduler=scheduler,
    )
    async with session_factory() as session, session.begin():
        evaluation = await classifier.evaluate_request_in_session(
            session,
            api_request_log_id=request.id,
        )

    assert len(evaluation.transitions) == 2
    assert len(set(attempted_ids)) == 1
    token_ids = [token.id for token in tokens]
    async with session_factory() as session:
        schedule_ids = set(
            await session.scalars(
                select(PollSchedule.capacity_decision_id).where(
                    PollSchedule.token_id.in_(token_ids)
                )
            )
        )
        coverage_ids = set(
            await session.scalars(
                select(CoverageDecision.capacity_decision_id).where(
                    CoverageDecision.token_id.in_(token_ids),
                    CoverageDecision.reason_code == LifecycleTransitionRule.NEW_TO_ACTIVE.value,
                )
            )
        )
        schedule_decision_ids = set(
            await session.scalars(
                select(PollScheduleDecision.capacity_decision_id).where(
                    PollScheduleDecision.token_id.in_(token_ids),
                    PollScheduleDecision.reason_code == LifecycleTransitionRule.NEW_TO_ACTIVE.value,
                )
            )
        )
    assert schedule_ids == set(attempted_ids)
    assert coverage_ids == set(attempted_ids)
    assert schedule_decision_ids == set(attempted_ids)


@pytest.mark.integration
async def test_lifecycle_derivation_never_mutates_or_stores_state_on_raw_observation(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Raw facts and derived state remain separate before and after classification."""
    clock = FakeClock()
    settings = _settings()
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    clock.advance()
    token, observation = await _create_scheduled_observation(
        session_factory,
        scheduler=scheduler,
        clock=clock,
        state=LifecycleState.NEW,
        address="raw-derived-separation",
        price_usd=Decimal("0.000012345678901234"),
        volume_m5_usd=Decimal("100"),
        volume_h1_usd=Decimal("250"),
        liquidity_usd=Decimal("5000"),
    )
    raw_snapshot = (
        observation.id,
        observation.received_at,
        observation.pair_id,
        observation.api_request_log_id,
        observation.price_usd,
        observation.volume_m5_usd,
        observation.volume_h1_usd,
        observation.liquidity_usd,
    )
    classifier = LifecycleClassifier(session_factory, settings, clock=clock)

    transition = await classifier.evaluate_observation(
        observation_id=observation.id,
        received_at=observation.received_at,
    )

    assert transition is not None
    assert {"lifecycle_state", "trading_score", "opportunity_score"}.isdisjoint(
        Observation.__table__.columns.keys()
    )
    async with session_factory() as session:
        reloaded = (
            await session.execute(
                select(Observation).where(
                    Observation.id == observation.id,
                    Observation.received_at == observation.received_at,
                )
            )
        ).scalar_one()
        event = await session.get(LifecycleEvent, transition.event_id)
        schedule = await session.get(PollSchedule, token.id)

    assert (
        reloaded.id,
        reloaded.received_at,
        reloaded.pair_id,
        reloaded.api_request_log_id,
        reloaded.price_usd,
        reloaded.volume_m5_usd,
        reloaded.volume_h1_usd,
        reloaded.liquidity_usd,
    ) == raw_snapshot
    assert event is not None
    assert event.previous_state == LifecycleState.NEW.value
    assert event.new_state == LifecycleState.ACTIVE.value
    assert schedule is not None
    assert schedule.lifecycle_state == LifecycleState.ACTIVE.value


@pytest.mark.integration
async def test_transition_uses_configured_threshold_and_persists_full_policy(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    settings = _settings(lifecycle_new_to_active_min_volume_m5_usd=Decimal("42.5"))
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    clock.advance()
    _, observation = await _create_scheduled_observation(
        session_factory,
        scheduler=scheduler,
        clock=clock,
        state=LifecycleState.NEW,
        address="configured-threshold",
        volume_m5_usd=Decimal("42.5"),
    )
    classifier = LifecycleClassifier(session_factory, settings, clock=clock)

    transition = await classifier.evaluate_observation(
        observation_id=observation.id,
        received_at=observation.received_at,
    )

    assert transition is not None
    async with session_factory() as session:
        event = await session.get(LifecycleEvent, transition.event_id)
    assert event is not None
    assert event.reason_detail is not None
    thresholds = event.reason_detail["thresholds"]
    assert isinstance(thresholds, dict)
    assert thresholds["min_volume_m5_usd"] == "42.5"
    snapshot_thresholds = event.configuration_snapshot["thresholds"]
    assert isinstance(snapshot_thresholds, dict)
    assert snapshot_thresholds["new_to_active_min_volume_m5_usd"] == "42.5"


@pytest.mark.integration
async def test_missing_or_insufficient_inputs_do_not_transition_or_delete_token(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    settings = _settings()
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    clock.advance()
    token, observation = await _create_scheduled_observation(
        session_factory,
        scheduler=scheduler,
        clock=clock,
        state=LifecycleState.NEW,
        address="retained-no-transition",
        volume_m5_usd=None,
        liquidity_usd=Decimal("1000000"),
    )
    classifier = LifecycleClassifier(session_factory, settings, clock=clock)

    transition = await classifier.evaluate_observation(
        observation_id=observation.id,
        received_at=observation.received_at,
    )

    assert transition is None
    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token.id)
        event_count = await session.scalar(select(func.count()).select_from(LifecycleEvent))
        token_count = await session.scalar(select(func.count()).select_from(Token))
    assert schedule is not None
    assert schedule.lifecycle_state == LifecycleState.NEW.value
    assert event_count == 0
    assert token_count == 1


@pytest.mark.integration
async def test_same_observation_is_idempotent_and_creates_one_transition(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    settings = _settings()
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    clock.advance()
    token, observation = await _create_scheduled_observation(
        session_factory,
        scheduler=scheduler,
        clock=clock,
        state=LifecycleState.NEW,
        address="idempotent-transition",
        volume_m5_usd=Decimal("100"),
    )
    classifier = LifecycleClassifier(session_factory, settings, clock=clock)

    first = await classifier.evaluate_observation(
        observation_id=observation.id,
        received_at=observation.received_at,
    )
    second = await classifier.evaluate_observation(
        observation_id=observation.id,
        received_at=observation.received_at,
    )

    assert first is not None
    assert second is None
    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token.id)
        matching_events = await session.scalar(
            select(func.count())
            .select_from(LifecycleEvent)
            .where(LifecycleEvent.reason_code == LifecycleTransitionRule.NEW_TO_ACTIVE.value)
        )
    assert schedule is not None
    assert schedule.lifecycle_state == LifecycleState.ACTIVE.value
    assert matching_events == 1


@pytest.mark.integration
async def test_observation_known_before_current_state_cannot_retroactively_transition(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    clock = FakeClock()
    settings = _settings()
    scheduler = AdaptiveScheduler(session_factory, settings, clock=clock)
    token, observation = await _create_scheduled_observation(
        session_factory,
        scheduler=scheduler,
        clock=clock,
        state=LifecycleState.NEW,
        address="stale-observation",
        volume_m5_usd=Decimal("100"),
        received_at=clock.now(),
    )
    clock.advance()
    await scheduler.set_lifecycle_state(
        token_id=token.id,
        state=LifecycleState.ACTIVE,
        decided_at=clock.now(),
        reason_code="later-active-state",
    )
    classifier = LifecycleClassifier(session_factory, settings, clock=clock)

    transition = await classifier.evaluate_observation(
        observation_id=observation.id,
        received_at=observation.received_at,
    )

    assert transition is None
    async with session_factory() as session:
        schedule = await session.get(PollSchedule, token.id)
        event_count = await session.scalar(select(func.count()).select_from(LifecycleEvent))
    assert schedule is not None
    assert schedule.lifecycle_state == LifecycleState.ACTIVE.value
    assert event_count == 0
