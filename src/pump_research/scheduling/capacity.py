"""Pure capacity planning for bounded, priority-aware polling cadences."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum

from pump_research.scheduling.policy import (
    AdaptivePollingPolicy,
    CapacityTier,
    CoverageClass,
    capacity_weights,
)

_CRITICAL_LOWER_TIER_FAIRNESS_RATIO = 0.05
_LOWER_TIER_WEIGHTS: Mapping[CapacityTier, float] = capacity_weights()
_PROTECTED_TIERS = (
    CoverageClass.PROTECTED_ACTIVE,
    CoverageClass.PROTECTED_RESURRECTED,
)
_CONTROL_TIER = CoverageClass.RETIRED_CONTROL
_LOWER_TIERS = tuple(
    tier for tier in CoverageClass if tier not in (*_PROTECTED_TIERS, _CONTROL_TIER)
)


class CapacityMode(StrEnum):
    """Operational interpretation of requested load versus safe capacity."""

    NORMAL = "NORMAL"
    DEGRADED = "DEGRADED"
    CRITICAL = "CRITICAL"


@dataclass(frozen=True, slots=True)
class CapacityPlan:
    """One deterministic capacity calculation from policy and population counts."""

    mode: CapacityMode
    token_counts: Mapping[CapacityTier, int]
    target_interval_seconds: Mapping[CapacityTier, int]
    effective_interval_seconds: Mapping[CapacityTier, int]
    requested_token_observations_per_minute: float
    available_token_observations_per_minute: int
    effective_token_observations_per_minute: float
    requested_requests_per_minute: float
    safe_requests_per_minute: int
    reserved_requests_per_minute: int
    available_requests_per_minute: int
    effective_requests_per_minute: float
    capacity_utilization_pct: float
    effective_capacity_utilization_pct: float
    degraded_schedule_count: int
    degraded_schedule_pct: float

    @property
    def snapshot(self) -> dict[str, object]:
        """Return the dynamic facts needed to reconstruct this decision."""
        return {
            "mode": self.mode.value,
            "token_counts": {tier.value: self.token_counts[tier] for tier in CapacityTier},
            "target_interval_seconds": {
                tier.value: self.target_interval_seconds[tier] for tier in CapacityTier
            },
            "effective_interval_seconds": {
                tier.value: self.effective_interval_seconds[tier] for tier in CapacityTier
            },
            "requested_token_observations_per_minute": round(
                self.requested_token_observations_per_minute, 6
            ),
            "available_token_observations_per_minute": (
                self.available_token_observations_per_minute
            ),
            "effective_token_observations_per_minute": round(
                self.effective_token_observations_per_minute, 6
            ),
            "requested_requests_per_minute": round(self.requested_requests_per_minute, 6),
            "safe_requests_per_minute": self.safe_requests_per_minute,
            "reserved_requests_per_minute": self.reserved_requests_per_minute,
            "available_requests_per_minute": self.available_requests_per_minute,
            "effective_requests_per_minute": round(self.effective_requests_per_minute, 6),
            "capacity_utilization_pct": round(self.capacity_utilization_pct, 6),
            "effective_capacity_utilization_pct": round(self.effective_capacity_utilization_pct, 6),
            "degraded_schedule_count": self.degraded_schedule_count,
            "degraded_schedule_pct": round(self.degraded_schedule_pct, 6),
        }


def plan_capacity(
    policy: AdaptivePollingPolicy,
    token_counts: Mapping[CapacityTier, int],
) -> CapacityPlan:
    """Fit deterministic effective rates inside the configured safe request budget.

    ACTIVE and RESURRECTED receive their full targets whenever their combined
    requested load fits. Remaining capacity is shared among lower tiers using
    weighted max-min allocation, which gives every populated tier a finite rate.
    If protected demand alone overloads capacity, it receives 95% and lower tiers
    share 5% to preserve the no-starvation invariant.
    """
    counts = {tier: int(token_counts.get(tier, 0)) for tier in CapacityTier}
    if any(count < 0 for count in counts.values()):
        raise ValueError("capacity token counts cannot be negative")
    targets = {
        tier: policy.capacity_target_interval_seconds(tier, population=counts[tier])
        for tier in CapacityTier
    }
    requested_rates = {tier: 60.0 / targets[tier] for tier in CapacityTier}
    requested = sum(counts[tier] * requested_rates[tier] for tier in CapacityTier)
    safe_requests = max(
        1,
        math.floor(policy.request_budget_per_minute * (1.0 - policy.capacity_headroom_ratio)),
    )
    scheduled_requests = safe_requests - policy.reserved_requests_per_minute
    if scheduled_requests <= 0:
        raise ValueError("scheduler request reserve leaves no scheduled capacity")
    available = scheduled_requests * policy.batch_size

    effective_rates = dict(requested_rates)
    protected_requested = sum(counts[tier] * requested_rates[tier] for tier in _PROTECTED_TIERS)
    lower_populated = any(counts[tier] for tier in (*_LOWER_TIERS, _CONTROL_TIER))
    control_requested = counts[_CONTROL_TIER] * requested_rates[_CONTROL_TIER]
    if requested <= available:
        mode = CapacityMode.NORMAL
    elif protected_requested >= available:
        mode = CapacityMode.CRITICAL
        protected_budget = available * (
            1.0 - _CRITICAL_LOWER_TIER_FAIRNESS_RATIO if lower_populated else 1.0
        )
        lower_budget = available - protected_budget
        control_budget = min(control_requested, lower_budget)
        effective_rates[_CONTROL_TIER] = (
            control_budget / counts[_CONTROL_TIER]
            if counts[_CONTROL_TIER]
            else requested_rates[_CONTROL_TIER]
        )
        effective_rates.update(
            _weighted_rates(
                tiers=_PROTECTED_TIERS,
                counts=counts,
                requested_rates=requested_rates,
                weights={tier: 1.0 for tier in _PROTECTED_TIERS},
                budget=protected_budget,
            )
        )
        effective_rates.update(
            _weighted_rates(
                tiers=_LOWER_TIERS,
                counts=counts,
                requested_rates=requested_rates,
                weights=_LOWER_TIER_WEIGHTS,
                budget=lower_budget - control_budget,
            )
        )
    else:
        mode = CapacityMode.DEGRADED
        lower_budget = available - protected_requested
        control_budget = min(control_requested, lower_budget)
        effective_rates[_CONTROL_TIER] = (
            control_budget / counts[_CONTROL_TIER]
            if counts[_CONTROL_TIER]
            else requested_rates[_CONTROL_TIER]
        )
        effective_rates.update(
            _weighted_rates(
                tiers=_LOWER_TIERS,
                counts=counts,
                requested_rates=requested_rates,
                weights=_LOWER_TIER_WEIGHTS,
                budget=lower_budget - control_budget,
            )
        )

    effective_intervals = {
        tier: _interval_for_rate(
            requested_interval=targets[tier],
            effective_rate=effective_rates[tier],
            populated=counts[tier] > 0,
        )
        for tier in CapacityTier
    }
    effective = sum(counts[tier] * 60.0 / effective_intervals[tier] for tier in CapacityTier)
    # Upward interval rounding must make this true even at fractional boundaries.
    if effective > available + 1e-9:
        raise AssertionError("rounded capacity plan exceeds the safe token budget")
    degraded_count = sum(
        counts[tier] for tier in CapacityTier if effective_intervals[tier] > targets[tier]
    )
    total_count = sum(counts.values())
    return CapacityPlan(
        mode=mode,
        token_counts=counts,
        target_interval_seconds=targets,
        effective_interval_seconds=effective_intervals,
        requested_token_observations_per_minute=requested,
        available_token_observations_per_minute=available,
        effective_token_observations_per_minute=effective,
        requested_requests_per_minute=requested / policy.batch_size,
        safe_requests_per_minute=safe_requests,
        reserved_requests_per_minute=policy.reserved_requests_per_minute,
        available_requests_per_minute=scheduled_requests,
        effective_requests_per_minute=effective / policy.batch_size,
        capacity_utilization_pct=100.0 * requested / available,
        effective_capacity_utilization_pct=100.0 * effective / available,
        degraded_schedule_count=degraded_count,
        degraded_schedule_pct=(100.0 * degraded_count / total_count if total_count else 0.0),
    )


def _weighted_rates(
    *,
    tiers: tuple[CapacityTier, ...],
    counts: Mapping[CapacityTier, int],
    requested_rates: Mapping[CapacityTier, float],
    weights: Mapping[CapacityTier, float],
    budget: float,
) -> dict[CapacityTier, float]:
    """Allocate a finite fair rate to every populated tier within ``budget``."""
    result = {tier: requested_rates[tier] for tier in tiers}
    populated = tuple(tier for tier in tiers if counts[tier] > 0)
    if not populated:
        return result
    if budget <= 0:
        # This branch is unreachable with validated positive capacity and the
        # critical fairness reserve, but fail loudly if those invariants change.
        raise ValueError("a populated capacity tier requires a positive budget")
    requested = sum(counts[tier] * requested_rates[tier] for tier in populated)
    if requested <= budget:
        return result

    low, high = 0.0, max(requested_rates[tier] / weights[tier] for tier in populated)
    for _ in range(80):
        level = (low + high) / 2.0
        demand = sum(
            counts[tier] * min(requested_rates[tier], level * weights[tier]) for tier in populated
        )
        if demand <= budget:
            low = level
        else:
            high = level
    for tier in populated:
        result[tier] = min(requested_rates[tier], low * weights[tier])
    return result


def _interval_for_rate(*, requested_interval: int, effective_rate: float, populated: bool) -> int:
    if not populated or effective_rate >= 60.0 / requested_interval:
        return requested_interval
    if effective_rate <= 0:
        raise ValueError("a populated tier must receive a positive effective rate")
    return max(requested_interval, math.ceil((60.0 / effective_rate) - 1e-12))
