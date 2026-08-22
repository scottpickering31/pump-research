"""Versioned coverage policy separated from market lifecycle classification."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from types import MappingProxyType

from pump_research.config import Settings


class LifecycleState(StrEnum):
    """Market-behaviour states produced by the unchanged lifecycle classifier."""

    NEW = "NEW"
    ACTIVE = "ACTIVE"
    WATCH = "WATCH"
    FADING = "FADING"
    DORMANT = "DORMANT"
    RESURRECTED = "RESURRECTED"


class CoverageClass(StrEnum):
    """Operational observation classes independent of lifecycle state."""

    PROTECTED_ACTIVE = "PROTECTED_ACTIVE"
    PROTECTED_RESURRECTED = "PROTECTED_RESURRECTED"
    PROTECTED_WATCH = "PROTECTED_WATCH"
    INITIAL = "INITIAL"
    EARLY = "EARLY"
    MATURE = "MATURE"
    FADING_TAIL = "FADING_TAIL"
    FADING_COOL = "FADING_COOL"
    COOLED = "COOLED"
    LONG_TAIL_DAY = "LONG_TAIL_DAY"
    LONG_TAIL_WEEK = "LONG_TAIL_WEEK"
    RETIRED_CONTROL = "RETIRED_CONTROL"


# Capacity planning uses coverage classes directly. Keep this public alias so
# callers do not need a second enum whose values silently recouple to lifecycle.
CapacityTier = CoverageClass


_PRIORITIES: Mapping[CoverageClass, int] = MappingProxyType(
    {
        CoverageClass.PROTECTED_ACTIVE: 0,
        CoverageClass.PROTECTED_RESURRECTED: 0,
        CoverageClass.PROTECTED_WATCH: 1,
        CoverageClass.INITIAL: 2,
        CoverageClass.EARLY: 3,
        CoverageClass.MATURE: 4,
        CoverageClass.FADING_TAIL: 5,
        CoverageClass.FADING_COOL: 6,
        CoverageClass.COOLED: 7,
        CoverageClass.LONG_TAIL_DAY: 8,
        CoverageClass.LONG_TAIL_WEEK: 9,
        CoverageClass.RETIRED_CONTROL: 10,
    }
)


@dataclass(frozen=True, slots=True)
class AdaptivePollingPolicy:
    """Configured finite coverage path and bounded-work settings."""

    intervals: Mapping[CoverageClass, timedelta]
    new_initial_duration: timedelta
    early_until: timedelta
    mature_until: timedelta
    cooled_until: timedelta
    long_tail_day_until: timedelta
    retire_after: timedelta
    fading_fast_duration: timedelta
    fading_total_duration: timedelta
    batch_size: int
    lease_duration: timedelta
    max_in_flight_batches: int
    request_budget_per_minute: int
    request_attempts_per_batch: int
    reserved_requests_per_minute: int
    control_scan_tokens_per_minute: int
    capacity_headroom_ratio: float
    capacity_refresh: timedelta

    @classmethod
    def from_settings(cls, settings: Settings) -> AdaptivePollingPolicy:
        """Build and validate a policy from application settings."""
        policy = cls(
            intervals=MappingProxyType(
                {
                    CoverageClass.PROTECTED_ACTIVE: timedelta(
                        seconds=settings.scheduler_active_interval_seconds
                    ),
                    CoverageClass.PROTECTED_RESURRECTED: timedelta(
                        seconds=settings.scheduler_resurrected_interval_seconds
                    ),
                    CoverageClass.PROTECTED_WATCH: timedelta(
                        seconds=settings.scheduler_watch_interval_seconds
                    ),
                    CoverageClass.INITIAL: timedelta(
                        seconds=settings.scheduler_new_initial_interval_seconds
                    ),
                    CoverageClass.EARLY: timedelta(seconds=settings.scheduler_new_interval_seconds),
                    CoverageClass.MATURE: timedelta(
                        seconds=settings.scheduler_mature_interval_seconds
                    ),
                    CoverageClass.FADING_TAIL: timedelta(
                        seconds=settings.scheduler_fading_interval_seconds
                    ),
                    CoverageClass.FADING_COOL: timedelta(
                        seconds=settings.scheduler_fading_tail_cool_interval_seconds
                    ),
                    CoverageClass.COOLED: timedelta(
                        seconds=settings.scheduler_cooled_interval_seconds
                    ),
                    CoverageClass.LONG_TAIL_DAY: timedelta(
                        seconds=settings.scheduler_long_tail_day_interval_seconds
                    ),
                    CoverageClass.LONG_TAIL_WEEK: timedelta(
                        seconds=settings.scheduler_long_tail_week_interval_seconds
                    ),
                    # RETIRED_CONTROL has a population-derived rotation interval.
                    CoverageClass.RETIRED_CONTROL: timedelta(seconds=60),
                }
            ),
            new_initial_duration=timedelta(seconds=settings.scheduler_new_initial_duration_seconds),
            early_until=timedelta(seconds=settings.scheduler_early_until_seconds),
            mature_until=timedelta(seconds=settings.scheduler_mature_until_seconds),
            cooled_until=timedelta(seconds=settings.scheduler_cooled_until_seconds),
            long_tail_day_until=timedelta(seconds=settings.scheduler_long_tail_day_until_seconds),
            retire_after=timedelta(seconds=settings.scheduler_retire_after_seconds),
            fading_fast_duration=timedelta(
                seconds=settings.scheduler_fading_tail_fast_duration_seconds
            ),
            fading_total_duration=timedelta(
                seconds=settings.scheduler_fading_tail_total_duration_seconds
            ),
            batch_size=settings.scheduler_batch_size,
            lease_duration=timedelta(seconds=settings.scheduler_lease_seconds),
            max_in_flight_batches=settings.scheduler_max_in_flight_batches,
            request_budget_per_minute=settings.dex_screener_requests_per_minute,
            request_attempts_per_batch=settings.dex_screener_max_attempts,
            reserved_requests_per_minute=settings.scheduler_reserved_requests_per_minute,
            control_scan_tokens_per_minute=(settings.scheduler_control_scan_tokens_per_minute),
            capacity_headroom_ratio=settings.scheduler_capacity_headroom_ratio,
            capacity_refresh=timedelta(seconds=settings.scheduler_capacity_refresh_seconds),
        )
        policy._validate()
        return policy

    def _validate(self) -> None:
        boundaries = (
            self.new_initial_duration,
            self.early_until,
            self.mature_until,
            self.cooled_until,
            self.long_tail_day_until,
            self.retire_after,
        )
        if any(left >= right for left, right in zip(boundaries, boundaries[1:], strict=False)):
            raise ValueError("coverage age boundaries must increase strictly")
        if self.fading_fast_duration >= self.fading_total_duration:
            raise ValueError("FADING fast duration must be shorter than its total tail")
        safe_requests = math.floor(
            self.request_budget_per_minute * (1.0 - self.capacity_headroom_ratio)
        )
        if self.reserved_requests_per_minute >= safe_requests:
            raise ValueError("scheduler request reserve must be below the safe request budget")

    def coverage_class_for(
        self,
        state: LifecycleState,
        *,
        admitted_at: datetime,
        state_decided_at: datetime,
        at: datetime,
    ) -> CoverageClass:
        """Derive coverage deterministically from admission, lifecycle, and time."""
        if at < admitted_at:
            raise ValueError("coverage time cannot predate DEX admission")
        if state is LifecycleState.ACTIVE:
            return CoverageClass.PROTECTED_ACTIVE
        if state is LifecycleState.RESURRECTED:
            return CoverageClass.PROTECTED_RESURRECTED
        if state is LifecycleState.WATCH:
            return CoverageClass.PROTECTED_WATCH
        if state is LifecycleState.DORMANT:
            return CoverageClass.RETIRED_CONTROL
        if state is LifecycleState.FADING:
            fading_age = at - state_decided_at
            if fading_age < timedelta(0):
                raise ValueError("coverage time cannot predate lifecycle decision")
            if fading_age < self.fading_fast_duration:
                return CoverageClass.FADING_TAIL
            if fading_age < self.fading_total_duration:
                return CoverageClass.FADING_COOL
            return CoverageClass.RETIRED_CONTROL

        age = at - admitted_at
        if age < self.new_initial_duration:
            return CoverageClass.INITIAL
        if age < self.early_until:
            return CoverageClass.EARLY
        if age < self.mature_until:
            return CoverageClass.MATURE
        if age < self.cooled_until:
            return CoverageClass.COOLED
        if age < self.long_tail_day_until:
            return CoverageClass.LONG_TAIL_DAY
        if age < self.retire_after:
            return CoverageClass.LONG_TAIL_WEEK
        return CoverageClass.RETIRED_CONTROL

    def next_transition_at(
        self,
        state: LifecycleState,
        *,
        admitted_at: datetime,
        state_decided_at: datetime,
        at: datetime,
    ) -> datetime | None:
        """Return the next deterministic wall-time coverage boundary."""
        coverage = self.coverage_class_for(
            state,
            admitted_at=admitted_at,
            state_decided_at=state_decided_at,
            at=at,
        )
        if coverage is CoverageClass.FADING_TAIL:
            return state_decided_at + self.fading_fast_duration
        if coverage is CoverageClass.FADING_COOL:
            return state_decided_at + self.fading_total_duration
        boundaries = {
            CoverageClass.INITIAL: admitted_at + self.new_initial_duration,
            CoverageClass.EARLY: admitted_at + self.early_until,
            CoverageClass.MATURE: admitted_at + self.mature_until,
            CoverageClass.COOLED: admitted_at + self.cooled_until,
            CoverageClass.LONG_TAIL_DAY: admitted_at + self.long_tail_day_until,
            CoverageClass.LONG_TAIL_WEEK: admitted_at + self.retire_after,
        }
        return boundaries.get(coverage)

    def interval_for_coverage(self, coverage: CoverageClass) -> timedelta | None:
        """Return an ordinary target interval; retired scans use a fixed budget."""
        if coverage is CoverageClass.RETIRED_CONTROL:
            return None
        return self.intervals[coverage]

    def capacity_target_interval_seconds(self, coverage: CoverageClass, *, population: int) -> int:
        """Return a target used by aggregate capacity planning."""
        if coverage is CoverageClass.RETIRED_CONTROL:
            if population <= 0:
                return 60
            return max(
                60,
                math.ceil(60 * population / self.control_scan_tokens_per_minute),
            )
        return int(self.intervals[coverage].total_seconds())

    def priority_for_coverage(self, coverage: CoverageClass) -> int:
        """Return persisted claim precedence for a coverage class."""
        return _PRIORITIES[coverage]

    def interval_for(self, state: LifecycleState) -> timedelta:
        """Compatibility helper for lifecycle-oriented callers and fixtures."""
        mapping = {
            LifecycleState.NEW: CoverageClass.INITIAL,
            LifecycleState.ACTIVE: CoverageClass.PROTECTED_ACTIVE,
            LifecycleState.WATCH: CoverageClass.PROTECTED_WATCH,
            LifecycleState.FADING: CoverageClass.FADING_TAIL,
            LifecycleState.DORMANT: CoverageClass.RETIRED_CONTROL,
            LifecycleState.RESURRECTED: CoverageClass.PROTECTED_RESURRECTED,
        }
        return self.intervals[mapping[state]]

    @property
    def target_intervals(self) -> Mapping[CoverageClass, timedelta]:
        """Return static targets; control rotation is population-derived in plans."""
        return self.intervals

    @property
    def coverage_snapshot(self) -> dict[str, object]:
        """Return the immutable contents controlling class derivation."""
        return {
            "component": "coverage_scheduler",
            "schema_version": 1,
            "ordinary_age_path": [
                {
                    "class": CoverageClass.INITIAL.value,
                    "until_seconds": int(self.new_initial_duration.total_seconds()),
                    "interval_seconds": int(self.intervals[CoverageClass.INITIAL].total_seconds()),
                },
                {
                    "class": CoverageClass.EARLY.value,
                    "until_seconds": int(self.early_until.total_seconds()),
                    "interval_seconds": int(self.intervals[CoverageClass.EARLY].total_seconds()),
                },
                {
                    "class": CoverageClass.MATURE.value,
                    "until_seconds": int(self.mature_until.total_seconds()),
                    "interval_seconds": int(self.intervals[CoverageClass.MATURE].total_seconds()),
                },
                {
                    "class": CoverageClass.COOLED.value,
                    "until_seconds": int(self.cooled_until.total_seconds()),
                    "interval_seconds": int(self.intervals[CoverageClass.COOLED].total_seconds()),
                },
                {
                    "class": CoverageClass.LONG_TAIL_DAY.value,
                    "until_seconds": int(self.long_tail_day_until.total_seconds()),
                    "interval_seconds": int(
                        self.intervals[CoverageClass.LONG_TAIL_DAY].total_seconds()
                    ),
                },
                {
                    "class": CoverageClass.LONG_TAIL_WEEK.value,
                    "until_seconds": int(self.retire_after.total_seconds()),
                    "interval_seconds": int(
                        self.intervals[CoverageClass.LONG_TAIL_WEEK].total_seconds()
                    ),
                },
            ],
            "new_initial_duration_seconds": int(self.new_initial_duration.total_seconds()),
            "fading_tail": {
                "fast_duration_seconds": int(self.fading_fast_duration.total_seconds()),
                "total_duration_seconds": int(self.fading_total_duration.total_seconds()),
                "fast_interval_seconds": int(
                    self.intervals[CoverageClass.FADING_TAIL].total_seconds()
                ),
                "cool_interval_seconds": int(
                    self.intervals[CoverageClass.FADING_COOL].total_seconds()
                ),
            },
            "protected_interval_seconds": {
                coverage.value: int(self.intervals[coverage].total_seconds())
                for coverage in (
                    CoverageClass.PROTECTED_ACTIVE,
                    CoverageClass.PROTECTED_RESURRECTED,
                    CoverageClass.PROTECTED_WATCH,
                )
            },
            "priority": {coverage.value: priority for coverage, priority in _PRIORITIES.items()},
            "control_scan_tokens_per_minute": self.control_scan_tokens_per_minute,
        }

    @property
    def coverage_sha256(self) -> str:
        """Return a stable digest of only coverage classification behavior."""
        return _sha256(self.coverage_snapshot)

    @property
    def snapshot(self) -> dict[str, object]:
        """Return the complete reconstructable scheduler/capacity policy."""
        return {
            **self.coverage_snapshot,
            "component": "adaptive_scheduler",
            "schema_version": 3,
            "batch_size": self.batch_size,
            "lease_seconds": int(self.lease_duration.total_seconds()),
            "max_in_flight_batches": self.max_in_flight_batches,
            "request_budget_per_minute": self.request_budget_per_minute,
            "request_attempts_per_batch": self.request_attempts_per_batch,
            "capacity_headroom_ratio": self.capacity_headroom_ratio,
            "reserved_requests_per_minute": self.reserved_requests_per_minute,
            "capacity_refresh_seconds": int(self.capacity_refresh.total_seconds()),
            "critical_lower_tier_fairness_ratio": 0.05,
            "capacity_allocation_weights": {
                coverage.value: weight for coverage, weight in _CAPACITY_WEIGHTS.items()
            },
        }

    @property
    def sha256(self) -> str:
        """Return a stable digest of the complete policy snapshot."""
        return _sha256(self.snapshot)


_CAPACITY_WEIGHTS: Mapping[CoverageClass, float] = MappingProxyType(
    {
        CoverageClass.PROTECTED_WATCH: 64.0,
        CoverageClass.INITIAL: 32.0,
        CoverageClass.EARLY: 16.0,
        CoverageClass.MATURE: 8.0,
        CoverageClass.FADING_TAIL: 4.0,
        CoverageClass.FADING_COOL: 2.0,
        CoverageClass.COOLED: 2.0,
        CoverageClass.LONG_TAIL_DAY: 1.0,
        CoverageClass.LONG_TAIL_WEEK: 0.5,
        CoverageClass.RETIRED_CONTROL: 0.25,
    }
)


def capacity_weights() -> Mapping[CoverageClass, float]:
    """Expose immutable allocation weights to the pure capacity planner."""
    return _CAPACITY_WEIGHTS


def _sha256(document: dict[str, object]) -> str:
    encoded = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()
