from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from pump_research.candidates.policy import (
    CandidateEvidence,
    CandidatePolicy,
    CandidateTier,
    TransitionReason,
    budget_projection,
    candidate_identity,
    select_boost_wakeups,
)
from pump_research.candidates.simulation import model_candidate_spikes
from pump_research.config import Settings

NOW = datetime(2026, 8, 21, 12, tzinfo=UTC)


def _policy() -> CandidatePolicy:
    return CandidatePolicy.from_settings(
        Settings(database_url="postgresql+asyncpg://unused:unused@localhost/unused")
    )


def _evidence(**changes: object) -> CandidateEvidence:
    values: dict[str, object] = {
        "token_id": "10000000-0000-0000-0000-000000000001",
        "evaluated_at": NOW,
        "watermark": NOW,
        "lifecycle_state": "NEW",
        "coverage_class": "EARLY",
        "admitted_at": NOW - timedelta(minutes=3),
        "observation_id": "20000000-0000-0000-0000-000000000001",
        "observation_received_at": NOW,
        "liquidity_usd": Decimal("20000"),
        "volume_m5_usd": Decimal("2000"),
        "buys_m5": 20,
        "sells_m5": 5,
    }
    values.update(changes)
    return CandidateEvidence(**values)  # type: ignore[arg-type]


def test_rule_is_deterministic_and_selective() -> None:
    policy = _policy()
    evidence = _evidence()
    first = policy.evaluate(evidence, current_tier=CandidateTier.TIER_0_UNIVERSAL)
    second = policy.evaluate(evidence, current_tier=CandidateTier.TIER_0_UNIVERSAL)
    assert first == second
    assert first.eligible
    assert first.target_tier is CandidateTier.TIER_1_INTERESTING
    assert first.reason is TransitionReason.MARKET_ACTIVITY

    ordinary = policy.evaluate(
        _evidence(liquidity_usd=Decimal("999"), buys_m5=1, sells_m5=0),
        current_tier=CandidateTier.TIER_0_UNIVERSAL,
    )
    assert not ordinary.eligible
    assert ordinary.target_tier is CandidateTier.TIER_0_UNIVERSAL


def test_tier_two_requires_fresh_security_and_existing_candidate() -> None:
    policy = _policy()
    evidence = _evidence(
        liquidity_usd=Decimal("100000"),
        volume_m5_usd=Decimal("20000"),
        buys_m5=60,
        sells_m5=10,
        security_snapshot_id="30000000-0000-0000-0000-000000000001",
        security_received_at=NOW - timedelta(minutes=5),
    )
    initial = policy.evaluate(evidence, current_tier=CandidateTier.TIER_0_UNIVERSAL)
    promoted = policy.evaluate(evidence, current_tier=CandidateTier.TIER_1_INTERESTING)
    assert initial.target_tier is CandidateTier.TIER_1_INTERESTING
    assert promoted.target_tier is CandidateTier.TIER_2_INVESTIGATE


def test_future_received_fact_is_rejected() -> None:
    with pytest.raises(ValueError, match="cannot exceed"):
        _evidence(boost_received_at=NOW + timedelta(seconds=1))


def test_identity_and_budget_are_stable_and_core_protected() -> None:
    policy = _policy()
    evidence = _evidence()
    first = candidate_identity(
        epoch_id="epoch", evidence=evidence, policy_sha256=policy.sha256, reason="MARKET_ACTIVITY"
    )
    second = candidate_identity(
        epoch_id="epoch", evidence=evidence, policy_sha256=policy.sha256, reason="MARKET_ACTIVITY"
    )
    assert first == second
    projected = budget_projection(policy, 120)
    assert projected == {
        "requested_candidate_tasks_per_minute": 120,
        "admitted_candidate_tasks_per_minute": 12,
        "deferred_candidate_tasks_per_minute": 108,
        "core_requests_displaced_per_minute": 0,
    }


def test_simultaneous_boost_wakeups_are_deterministic_and_bounded() -> None:
    events = tuple(
        _evidence(
            token_id=f"10000000-0000-0000-0000-{index:012d}",
            boost_event_id=f"boost-{index}",
            boost_received_at=NOW - timedelta(milliseconds=index % 2),
        )
        for index in reversed(range(10))
    )
    first = select_boost_wakeups(events, budget=5)
    second = select_boost_wakeups(tuple(reversed(events)), budget=5)
    assert first == second
    assert len(first) == 5


def test_candidate_spikes_plateau_below_safe_dex_budget() -> None:
    projections = model_candidate_spikes(_policy(), normal_candidates_per_minute=1)
    assert [item.multiplier for item in projections] == [1, 2, 5, 10]
    assert max(item.admitted_tasks_per_minute for item in projections) == 12
    assert max(item.candidate_coverage_tokens for item in projections) == 100
    assert max(item.maximum_total_dex_requests_per_minute for item in projections) < 192
    assert {item.core_requests_displaced_per_minute for item in projections} == {0}


def test_tier3_is_deep_review_not_pretrade_and_later_evidence_can_demote() -> None:
    policy = _policy()
    deep = policy.evaluate(
        _evidence(
            holder_snapshot_id="holder",
            holder_received_at=NOW,
            holder_top10_pct=Decimal("65"),
        ),
        current_tier=CandidateTier.TIER_2_INVESTIGATE,
    )
    assert deep.target_tier is CandidateTier.TIER_3_DEEP_REVIEW
    assert deep.reason is TransitionReason.SECURITY_CHANGE

    cooled_at = NOW + timedelta(hours=2)
    cooled = policy.evaluate(
        _evidence(
            evaluated_at=cooled_at,
            watermark=cooled_at,
            observation_received_at=cooled_at,
            liquidity_usd=Decimal("100"),
            volume_m5_usd=Decimal("0"),
            buys_m5=0,
            sells_m5=0,
        ),
        current_tier=CandidateTier.TIER_3_DEEP_REVIEW,
    )
    assert cooled.target_tier is CandidateTier.TIER_0_UNIVERSAL
