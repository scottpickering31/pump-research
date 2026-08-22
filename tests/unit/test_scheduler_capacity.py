from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pump_research.config import Settings
from pump_research.scheduling.capacity import CapacityMode, plan_capacity
from pump_research.scheduling.policy import (
    AdaptivePollingPolicy,
    CoverageClass,
    LifecycleState,
)
from pump_research.scheduling.simulation import simulate_coverage_populations

NOW = datetime(2026, 8, 17, 12, 0, tzinfo=UTC)


def _policy(**overrides: object) -> AdaptivePollingPolicy:
    values: dict[str, object] = {
        "database_url": "postgresql+asyncpg://researcher:password@localhost/pump_research",
        "dex_screener_requests_per_minute": 240,
        "scheduler_batch_size": 30,
        "scheduler_capacity_headroom_ratio": 0.20,
        "scheduler_reserved_requests_per_minute": 14,
    }
    values.update(overrides)
    return AdaptivePollingPolicy.from_settings(Settings.model_validate(values))


def test_ordinary_coverage_path_uses_stable_admission_time() -> None:
    policy = _policy()
    expected = (
        (0, CoverageClass.INITIAL, 15),
        (119, CoverageClass.INITIAL, 15),
        (120, CoverageClass.EARLY, 30),
        (599, CoverageClass.EARLY, 30),
        (600, CoverageClass.MATURE, 300),
        (3_599, CoverageClass.MATURE, 300),
        (3_600, CoverageClass.COOLED, 1_800),
        (21_600, CoverageClass.LONG_TAIL_DAY, 7_200),
        (86_400, CoverageClass.LONG_TAIL_WEEK, 43_200),
        (604_800, CoverageClass.RETIRED_CONTROL, 0),
    )
    for age, coverage, interval in expected:
        actual = policy.coverage_class_for(
            LifecycleState.NEW,
            admitted_at=NOW,
            state_decided_at=NOW + timedelta(seconds=age),
            at=NOW + timedelta(seconds=age),
        )
        target = policy.interval_for_coverage(actual)
        assert actual is coverage
        assert (int(target.total_seconds()) if target else 0) == interval


def test_ordinary_uninteresting_path_has_finite_sixty_five_observations() -> None:
    policy = _policy()
    bands = (
        (policy.new_initial_duration.total_seconds(), 15),
        ((policy.early_until - policy.new_initial_duration).total_seconds(), 30),
        ((policy.mature_until - policy.early_until).total_seconds(), 300),
        ((policy.cooled_until - policy.mature_until).total_seconds(), 1_800),
        ((policy.long_tail_day_until - policy.cooled_until).total_seconds(), 7_200),
        ((policy.retire_after - policy.long_tail_day_until).total_seconds(), 43_200),
    )
    assert sum(int(duration / interval) for duration, interval in bands) == 65


def test_protected_and_fading_coverage_override_admission_age() -> None:
    policy = _policy()
    old_admission = NOW - timedelta(days=30)
    assert policy.coverage_class_for(
        LifecycleState.ACTIVE,
        admitted_at=old_admission,
        state_decided_at=NOW,
        at=NOW,
    ) is CoverageClass.PROTECTED_ACTIVE
    assert policy.coverage_class_for(
        LifecycleState.WATCH,
        admitted_at=old_admission,
        state_decided_at=NOW,
        at=NOW,
    ) is CoverageClass.PROTECTED_WATCH
    assert policy.coverage_class_for(
        LifecycleState.FADING,
        admitted_at=old_admission,
        state_decided_at=NOW,
        at=NOW + timedelta(minutes=29),
    ) is CoverageClass.FADING_TAIL
    assert policy.coverage_class_for(
        LifecycleState.FADING,
        admitted_at=old_admission,
        state_decided_at=NOW,
        at=NOW + timedelta(minutes=30),
    ) is CoverageClass.FADING_COOL
    assert policy.coverage_class_for(
        LifecycleState.FADING,
        admitted_at=old_admission,
        state_decided_at=NOW,
        at=NOW + timedelta(hours=6),
    ) is CoverageClass.RETIRED_CONTROL
    assert policy.coverage_class_for(
        LifecycleState.DORMANT,
        admitted_at=old_admission,
        state_decided_at=NOW,
        at=NOW,
    ) is CoverageClass.RETIRED_CONTROL


def test_small_load_achieves_targets_and_preserves_request_reserve() -> None:
    plan = plan_capacity(
        _policy(),
        {
            CoverageClass.PROTECTED_ACTIVE: 5,
            CoverageClass.PROTECTED_RESURRECTED: 2,
            CoverageClass.PROTECTED_WATCH: 5,
            CoverageClass.INITIAL: 10,
            CoverageClass.EARLY: 20,
            CoverageClass.FADING_TAIL: 30,
            CoverageClass.RETIRED_CONTROL: 10,
        },
    )
    assert plan.mode is CapacityMode.NORMAL
    assert plan.effective_interval_seconds == plan.target_interval_seconds
    assert plan.safe_requests_per_minute == 192
    assert plan.reserved_requests_per_minute == 14
    assert plan.available_requests_per_minute == 178
    assert plan.effective_requests_per_minute < 178


def test_active_overload_degrades_protected_fairly_and_remains_critical() -> None:
    plan = plan_capacity(
        _policy(scheduler_reserved_requests_per_minute=0),
        {
            CoverageClass.PROTECTED_ACTIVE: 1_000,
            CoverageClass.PROTECTED_RESURRECTED: 100,
        },
    )
    assert plan.mode is CapacityMode.CRITICAL
    assert (
        plan.effective_interval_seconds[CoverageClass.PROTECTED_ACTIVE]
        == plan.effective_interval_seconds[CoverageClass.PROTECTED_RESURRECTED]
    )
    assert plan.effective_requests_per_minute <= 192


def test_headroom_uses_configured_ceiling_not_provider_maximum() -> None:
    plan = plan_capacity(
        _policy(
            dex_screener_requests_per_minute=100,
            scheduler_reserved_requests_per_minute=10,
        ),
        {},
    )
    assert plan.safe_requests_per_minute == 80
    assert plan.available_requests_per_minute == 70
    assert plan.available_token_observations_per_minute == 2_100


def test_population_simulation_plateaus_and_preserves_all_reserves() -> None:
    results = simulate_coverage_populations(
        _policy(),
        (10_000, 50_000, 100_000, 500_000, 1_000_000),
    )
    assert [result.cumulative_population for result in results] == [
        10_000,
        50_000,
        100_000,
        500_000,
        1_000_000,
    ]
    assert all(result.mode == "NORMAL" for result in results)
    assert all(result.unbounded_backlog is False for result in results)
    assert all(result.total_requests_with_reserve_per_minute < 192 for result in results)
    assert results[-1].requested_observations_per_minute == (
        results[-2].requested_observations_per_minute
    )
    assert results[-1].core_requests_per_minute == results[-2].core_requests_per_minute
    assert results[-1].control_demand_per_minute <= 2
    assert results[-1].counts_by_coverage[CoverageClass.RETIRED_CONTROL] > (
        results[-2].counts_by_coverage[CoverageClass.RETIRED_CONTROL]
    )


def test_degraded_plan_is_deterministic_and_every_populated_class_gets_rate() -> None:
    counts = {
        CoverageClass.PROTECTED_ACTIVE: 500,
        CoverageClass.PROTECTED_WATCH: 5_000,
        CoverageClass.INITIAL: 10_000,
        CoverageClass.EARLY: 50_000,
        CoverageClass.LONG_TAIL_WEEK: 100_000,
        CoverageClass.RETIRED_CONTROL: 1_000_000,
    }
    first = plan_capacity(_policy(), counts)
    second = plan_capacity(_policy(), counts)
    assert first == second
    assert first.mode is CapacityMode.CRITICAL
    assert first.effective_requests_per_minute <= first.available_requests_per_minute
    for coverage, count in counts.items():
        assert count > 0
        assert first.effective_interval_seconds[coverage] > 0
