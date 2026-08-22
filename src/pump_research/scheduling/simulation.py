"""Deterministic steady-state simulation for the finite coverage policy."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from pump_research.scheduling.capacity import CapacityPlan, plan_capacity
from pump_research.scheduling.policy import AdaptivePollingPolicy, CoverageClass


@dataclass(frozen=True, slots=True)
class CoverageSimulationResult:
    """One cumulative-population demand projection."""

    cumulative_population: int
    arrival_rate_per_minute: float
    counts_by_coverage: Mapping[CoverageClass, int]
    requested_observations_per_minute: float
    effective_observations_per_minute: float
    core_requests_per_minute: float
    average_batch_occupancy_pct: float
    total_requests_with_reserve_per_minute: float
    protected_demand_per_minute: float
    ordinary_demand_per_minute: float
    fading_tail_demand_per_minute: float
    control_demand_per_minute: float
    safe_requests_per_minute: int
    scheduled_requests_per_minute: int
    capacity_utilization_pct: float
    mode: str
    unbounded_backlog: bool
    effective_interval_seconds: Mapping[CoverageClass, int]


def simulate_coverage_populations(
    policy: AdaptivePollingPolicy,
    populations: Iterable[int],
    *,
    admitted_tokens_per_minute: float = 28.0,
    protected_active_resurrected: int = 120,
    protected_watch: int = 100,
    pessimistic_fading: bool = True,
    fading_transition_fraction: float = 0.40,
) -> tuple[CoverageSimulationResult, ...]:
    """Project finite-path steady demand at cumulative population milestones.

    Every modeled token occupies exactly one coverage class. The ordinary age
    pipeline fills at the configured admission rate after reserving protected and
    conservative FADING cohorts. Retired population then grows after the
    seven-day pipeline, while aggregate control demand remains fixed.
    """
    if admitted_tokens_per_minute <= 0:
        raise ValueError("admission rate must be positive")
    if protected_active_resurrected < 0 or protected_watch < 0:
        raise ValueError("protected populations cannot be negative")
    if not 0 <= fading_transition_fraction <= 1:
        raise ValueError("FADING transition fraction must be between zero and one")
    results: list[CoverageSimulationResult] = []
    for population in populations:
        if population < 0:
            raise ValueError("cumulative population cannot be negative")
        counts = {coverage: 0 for coverage in CoverageClass}
        remaining = population
        counts[CoverageClass.PROTECTED_ACTIVE] = min(remaining, protected_active_resurrected)
        remaining -= counts[CoverageClass.PROTECTED_ACTIVE]
        counts[CoverageClass.PROTECTED_WATCH] = min(remaining, protected_watch)
        remaining -= counts[CoverageClass.PROTECTED_WATCH]
        if pessimistic_fading:
            fading_rate = admitted_tokens_per_minute * fading_transition_fraction
            counts[CoverageClass.FADING_TAIL] = min(
                remaining,
                round(fading_rate * policy.fading_fast_duration.total_seconds() / 60),
            )
            remaining -= counts[CoverageClass.FADING_TAIL]
            counts[CoverageClass.FADING_COOL] = min(
                remaining,
                round(
                    fading_rate
                    * (policy.fading_total_duration - policy.fading_fast_duration).total_seconds()
                    / 60
                ),
            )
            remaining -= counts[CoverageClass.FADING_COOL]
        ordinary_counts = _ordinary_counts(
            policy,
            cumulative_population=remaining,
            rate=admitted_tokens_per_minute,
        )
        for coverage in (
            CoverageClass.INITIAL,
            CoverageClass.EARLY,
            CoverageClass.MATURE,
            CoverageClass.COOLED,
            CoverageClass.LONG_TAIL_DAY,
            CoverageClass.LONG_TAIL_WEEK,
            CoverageClass.RETIRED_CONTROL,
        ):
            counts[coverage] = ordinary_counts[coverage]
        plan = plan_capacity(policy, counts)
        protected = _demand(
            plan,
            (
                CoverageClass.PROTECTED_ACTIVE,
                CoverageClass.PROTECTED_RESURRECTED,
                CoverageClass.PROTECTED_WATCH,
            ),
        )
        fading = _demand(
            plan,
            (CoverageClass.FADING_TAIL, CoverageClass.FADING_COOL),
        )
        control = _demand(plan, (CoverageClass.RETIRED_CONTROL,))
        ordinary = plan.requested_token_observations_per_minute - (protected + fading + control)
        non_control_effective = plan.effective_token_observations_per_minute - control
        control_requests = 1.0 if counts[CoverageClass.RETIRED_CONTROL] else 0.0
        core_requests = non_control_effective / policy.batch_size + control_requests
        occupancy = (
            100.0
            * plan.effective_token_observations_per_minute
            / (core_requests * policy.batch_size)
            if core_requests
            else 0.0
        )
        results.append(
            CoverageSimulationResult(
                cumulative_population=population,
                arrival_rate_per_minute=admitted_tokens_per_minute,
                counts_by_coverage=counts,
                requested_observations_per_minute=(plan.requested_token_observations_per_minute),
                effective_observations_per_minute=(plan.effective_token_observations_per_minute),
                core_requests_per_minute=core_requests,
                average_batch_occupancy_pct=occupancy,
                total_requests_with_reserve_per_minute=(
                    core_requests + policy.reserved_requests_per_minute
                ),
                protected_demand_per_minute=protected,
                ordinary_demand_per_minute=ordinary,
                fading_tail_demand_per_minute=fading,
                control_demand_per_minute=control,
                safe_requests_per_minute=plan.safe_requests_per_minute,
                scheduled_requests_per_minute=plan.available_requests_per_minute,
                capacity_utilization_pct=plan.capacity_utilization_pct,
                mode=plan.mode.value,
                unbounded_backlog=(core_requests > plan.available_requests_per_minute + 1e-9),
                effective_interval_seconds=plan.effective_interval_seconds,
            )
        )
    return tuple(results)


def _ordinary_counts(
    policy: AdaptivePollingPolicy,
    *,
    cumulative_population: int,
    rate: float,
) -> dict[CoverageClass, int]:
    """Fill the finite arrival-age pipeline from youngest to oldest."""
    counts = {coverage: 0 for coverage in CoverageClass}
    remaining = cumulative_population
    bands = (
        (CoverageClass.INITIAL, 0.0, policy.new_initial_duration.total_seconds()),
        (
            CoverageClass.EARLY,
            policy.new_initial_duration.total_seconds(),
            policy.early_until.total_seconds(),
        ),
        (
            CoverageClass.MATURE,
            policy.early_until.total_seconds(),
            policy.mature_until.total_seconds(),
        ),
        (
            CoverageClass.COOLED,
            policy.mature_until.total_seconds(),
            policy.cooled_until.total_seconds(),
        ),
        (
            CoverageClass.LONG_TAIL_DAY,
            policy.cooled_until.total_seconds(),
            policy.long_tail_day_until.total_seconds(),
        ),
        (
            CoverageClass.LONG_TAIL_WEEK,
            policy.long_tail_day_until.total_seconds(),
            policy.retire_after.total_seconds(),
        ),
    )
    for coverage, start, end in bands:
        band_capacity = round(rate * (end - start) / 60)
        count = min(remaining, band_capacity)
        counts[coverage] = count
        remaining -= count
        if remaining <= 0:
            break
    counts[CoverageClass.RETIRED_CONTROL] = max(0, remaining)
    return counts


def _demand(plan: CapacityPlan, classes: tuple[CoverageClass, ...]) -> float:
    return sum(
        plan.token_counts[coverage] * 60 / plan.target_interval_seconds[coverage]
        for coverage in classes
    )
