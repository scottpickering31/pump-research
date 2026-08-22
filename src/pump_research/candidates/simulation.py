"""Closed-form resource stress model for candidate orchestration."""

from __future__ import annotations

from dataclasses import dataclass

from pump_research.candidates.policy import CandidatePolicy, budget_projection


@dataclass(frozen=True, slots=True)
class CandidateLoadProjection:
    multiplier: int
    requested_tasks_per_minute: int
    admitted_tasks_per_minute: int
    deferred_tasks_per_minute: int
    candidate_coverage_tokens: int
    candidate_coverage_observations_per_minute: float
    maximum_total_dex_requests_per_minute: float
    safe_dex_requests_per_minute: int
    core_requests_displaced_per_minute: int


def model_candidate_spikes(
    policy: CandidatePolicy,
    *,
    normal_candidates_per_minute: int,
    core_and_universal_dex_requests_per_minute: float = 147.9,
    safe_dex_requests_per_minute: int = 192,
    addresses_per_request: int = 30,
) -> tuple[CandidateLoadProjection, ...]:
    """Model 1x/2x/5x/10x while candidate work sheds before core work."""
    results: list[CandidateLoadProjection] = []
    interval = policy.coverage_interval.total_seconds()
    maximum_extra_observations = policy.max_active_coverage * 60 / interval
    for multiplier in (1, 2, 5, 10):
        requested = normal_candidates_per_minute * multiplier * 2
        task_budget = budget_projection(policy, requested)
        candidate_tokens = min(
            policy.max_active_coverage,
            normal_candidates_per_minute * multiplier * int(policy.tier1_ttl.total_seconds() / 60),
        )
        observations = min(candidate_tokens * 60 / interval, maximum_extra_observations)
        total_requests = core_and_universal_dex_requests_per_minute + (
            observations / addresses_per_request
        )
        results.append(
            CandidateLoadProjection(
                multiplier=multiplier,
                requested_tasks_per_minute=requested,
                admitted_tasks_per_minute=task_budget["admitted_candidate_tasks_per_minute"],
                deferred_tasks_per_minute=task_budget["deferred_candidate_tasks_per_minute"],
                candidate_coverage_tokens=candidate_tokens,
                candidate_coverage_observations_per_minute=round(observations, 3),
                maximum_total_dex_requests_per_minute=round(total_requests, 3),
                safe_dex_requests_per_minute=safe_dex_requests_per_minute,
                core_requests_displaced_per_minute=0,
            )
        )
    return tuple(results)
