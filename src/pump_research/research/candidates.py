"""Outcome-independent deterministic candidate/reference timestamps."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta

from pump_research.research.contracts import CandidateTimestampPolicy, TokenHistory, utc


@dataclass(frozen=True, slots=True)
class CandidateTimestamp:
    id: str
    token_id: str
    token_address: str
    epoch_number: int
    admission_at: datetime
    decision_at: datetime
    reason: str
    policy_sha256: str


def generate_candidate_timestamps(
    history: TokenHistory,
    policy: CandidateTimestampPolicy | None = None,
    *,
    scope_start_at: datetime | None = None,
    scope_end_at: datetime | None = None,
) -> tuple[CandidateTimestamp, ...]:
    candidate_policy = policy or CandidateTimestampPolicy()
    admission = history.admission_at
    if admission is None:
        return ()
    points: list[tuple[datetime, str]] = []
    if candidate_policy.mode == "fixed_age":
        points = [
            (admission + timedelta(seconds=offset), f"fixed_age_{offset}s")
            for offset in candidate_policy.offsets_seconds
        ]
    elif candidate_policy.mode == "observation":
        points = [
            (utc(item.received_at), "observation_received")
            for item in history.observations
            if utc(item.received_at) >= admission
        ]
    elif candidate_policy.mode == "lifecycle":
        points = [
            (utc(item.decided_at), f"lifecycle_{item.new_state.lower()}")
            for item in history.lifecycle
            if item.new_state in candidate_policy.lifecycle_states
            and utc(item.decided_at) >= admission
        ]
    else:  # pragma: no cover - Literal plus defensive runtime check
        raise ValueError(f"unsupported candidate mode: {candidate_policy.mode}")
    start = utc(scope_start_at) if scope_start_at else None
    end = utc(scope_end_at) if scope_end_at else None
    deduplicated = sorted(set(points), key=lambda item: (item[0], item[1]))
    return tuple(
        CandidateTimestamp(
            id=_candidate_id(
                history.epoch_id,
                history.token_id,
                decision_at,
                reason,
                candidate_policy.sha256,
            ),
            token_id=history.token_id,
            token_address=history.address,
            epoch_number=history.epoch_number,
            admission_at=admission,
            decision_at=decision_at,
            reason=reason,
            policy_sha256=candidate_policy.sha256,
        )
        for decision_at, reason in deduplicated
        if (start is None or decision_at >= start) and (end is None or decision_at < end)
    )


def deterministic_stratum(candidate_id: str, strata: int) -> int:
    """Stable cohort sampling primitive that never uses a future outcome."""
    if strata <= 0:
        raise ValueError("strata must be positive")
    return int(hashlib.sha256(candidate_id.encode()).hexdigest()[:16], 16) % strata


def _candidate_id(
    epoch_id: str, token_id: str, decision_at: datetime, reason: str, policy_sha256: str
) -> str:
    payload = "|".join((epoch_id, token_id, utc(decision_at).isoformat(), reason, policy_sha256))
    return hashlib.sha256(payload.encode()).hexdigest()
