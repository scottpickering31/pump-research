"""Versioned adaptive polling policy with no market classification logic."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from types import MappingProxyType

from pump_research.config import Settings


class LifecycleState(StrEnum):
    """Lifecycle states eligible for recurring market observation."""

    NEW = "NEW"
    ACTIVE = "ACTIVE"
    WATCH = "WATCH"
    FADING = "FADING"
    DORMANT = "DORMANT"
    RESURRECTED = "RESURRECTED"


_PRIORITIES: Mapping[LifecycleState, int] = MappingProxyType(
    {
        LifecycleState.RESURRECTED: 0,
        LifecycleState.NEW: 1,
        LifecycleState.ACTIVE: 2,
        LifecycleState.WATCH: 3,
        LifecycleState.FADING: 4,
        LifecycleState.DORMANT: 5,
    }
)


@dataclass(frozen=True, slots=True)
class AdaptivePollingPolicy:
    """Configured interval and bounded-work settings used by the scheduler."""

    intervals: Mapping[LifecycleState, timedelta]
    batch_size: int
    lease_duration: timedelta
    max_in_flight_batches: int
    request_budget_per_minute: int
    requests_reserved_per_batch: int

    @classmethod
    def from_settings(cls, settings: Settings) -> AdaptivePollingPolicy:
        """Build a policy from validated application settings."""
        return cls(
            intervals=MappingProxyType(
                {
                    LifecycleState.NEW: timedelta(
                        seconds=settings.scheduler_new_interval_seconds
                    ),
                    LifecycleState.ACTIVE: timedelta(
                        seconds=settings.scheduler_active_interval_seconds
                    ),
                    LifecycleState.WATCH: timedelta(
                        seconds=settings.scheduler_watch_interval_seconds
                    ),
                    LifecycleState.FADING: timedelta(
                        seconds=settings.scheduler_fading_interval_seconds
                    ),
                    LifecycleState.DORMANT: timedelta(
                        seconds=settings.scheduler_dormant_interval_seconds
                    ),
                    LifecycleState.RESURRECTED: timedelta(
                        seconds=settings.scheduler_resurrected_interval_seconds
                    ),
                }
            ),
            batch_size=settings.scheduler_batch_size,
            lease_duration=timedelta(seconds=settings.scheduler_lease_seconds),
            max_in_flight_batches=settings.scheduler_max_in_flight_batches,
            request_budget_per_minute=settings.dex_screener_requests_per_minute,
            requests_reserved_per_batch=settings.dex_screener_max_attempts,
        )

    def interval_for(self, state: LifecycleState) -> timedelta:
        """Return the configured interval for a lifecycle state."""
        return self.intervals[state]

    def priority_for(self, state: LifecycleState) -> int:
        """Return the state priority; due time remains the primary fairness key."""
        return _PRIORITIES[state]

    @property
    def snapshot(self) -> dict[str, object]:
        """Return the complete reconstructable configuration used by decisions."""
        return {
            "component": "adaptive_scheduler",
            "schema_version": 1,
            "interval_seconds": {
                state.value: int(interval.total_seconds())
                for state, interval in self.intervals.items()
            },
            "priority": {state.value: priority for state, priority in _PRIORITIES.items()},
            "batch_size": self.batch_size,
            "lease_seconds": int(self.lease_duration.total_seconds()),
            "max_in_flight_batches": self.max_in_flight_batches,
            "request_budget_per_minute": self.request_budget_per_minute,
            "requests_reserved_per_batch": self.requests_reserved_per_batch,
        }

    @property
    def sha256(self) -> str:
        """Return a stable digest of the complete policy snapshot."""
        encoded = json.dumps(
            self.snapshot,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        return hashlib.sha256(encoded).hexdigest()
