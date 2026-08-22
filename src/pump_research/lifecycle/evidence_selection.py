"""Deterministic V1 selection of one pair observation for lifecycle evidence."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum


class EvidenceSelectionOutcome(StrEnum):
    """Result of applying the lifecycle-evidence selection policy."""

    SELECTED = "selected"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class LifecycleEvidenceCandidate:
    """One immutable pair observation available to the selector."""

    observation_id: uuid.UUID
    observation_received_at: datetime
    pair_id: uuid.UUID
    chain: str
    pair_address: str
    dex_identifier: str | None
    liquidity_usd: Decimal | None
    volume_m5_usd: Decimal | None
    volume_h1_usd: Decimal | None


@dataclass(frozen=True, slots=True)
class EvidenceSelectionResult:
    """Deterministic selected candidate or an explicit derivation failure."""

    outcome: EvidenceSelectionOutcome
    selected: LifecycleEvidenceCandidate | None
    reason_code: str
    reason_detail: dict[str, object]


class HighestLiquidityEvidencePolicy:
    """V1: choose greatest reported USD liquidity, then canonical address."""

    @property
    def snapshot(self) -> dict[str, object]:
        """Return the complete fixed policy needed for later reconstruction."""
        return {
            "component": "lifecycle_evidence_selector",
            "policy_name": "highest_reported_liquidity_usd",
            "schema_version": 1,
            "candidate_scope": "one token within one DEX API response",
            "required_candidate_fields": {
                "one_candidate": [],
                "multiple_candidates": ["liquidity_usd_for_every_candidate"],
            },
            "ranking": [
                "liquidity_usd DESC",
                "chain ASC",
                "pair_address ASC",
            ],
            "dex_preference": None,
            "aggregation": None,
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

    def select(self, candidates: tuple[LifecycleEvidenceCandidate, ...]) -> EvidenceSelectionResult:
        """Select independently of provider array order or fail explicitly."""
        ordered = tuple(sorted(candidates, key=_canonical_pair_key))
        candidate_details = [_candidate_detail(candidate) for candidate in ordered]
        if not ordered:
            return EvidenceSelectionResult(
                outcome=EvidenceSelectionOutcome.FAILED,
                selected=None,
                reason_code="no_candidate_pair_observations",
                reason_detail={"candidates": candidate_details},
            )

        if len(ordered) == 1:
            selected = ordered[0]
            return EvidenceSelectionResult(
                outcome=EvidenceSelectionOutcome.SELECTED,
                selected=selected,
                reason_code="only_candidate_pair",
                reason_detail={
                    "candidates": candidate_details,
                    "selected_pair_address": selected.pair_address,
                    "selected_liquidity_usd": _decimal_text(selected.liquidity_usd),
                    "liquidity_tie_count": 1,
                },
            )

        incomplete = [
            candidate.pair_address for candidate in ordered if candidate.liquidity_usd is None
        ]
        if incomplete:
            return EvidenceSelectionResult(
                outcome=EvidenceSelectionOutcome.FAILED,
                selected=None,
                reason_code="candidate_missing_required_liquidity_usd",
                reason_detail={
                    "candidates": candidate_details,
                    "incomplete_pair_addresses": incomplete,
                },
            )

        greatest_liquidity = max(
            candidate.liquidity_usd for candidate in ordered if candidate.liquidity_usd is not None
        )
        tied = tuple(
            candidate for candidate in ordered if candidate.liquidity_usd == greatest_liquidity
        )
        selected = tied[0]
        return EvidenceSelectionResult(
            outcome=EvidenceSelectionOutcome.SELECTED,
            selected=selected,
            reason_code=(
                "highest_reported_liquidity_usd"
                if len(tied) == 1
                else "highest_reported_liquidity_usd_canonical_address_tiebreak"
            ),
            reason_detail={
                "candidates": candidate_details,
                "selected_pair_address": selected.pair_address,
                "selected_liquidity_usd": _decimal_text(selected.liquidity_usd),
                "liquidity_tie_count": len(tied),
            },
        )


def _canonical_pair_key(candidate: LifecycleEvidenceCandidate) -> tuple[str, str]:
    return candidate.chain, candidate.pair_address


def _candidate_detail(candidate: LifecycleEvidenceCandidate) -> dict[str, object]:
    return {
        "observation_id": str(candidate.observation_id),
        "observation_received_at": candidate.observation_received_at.isoformat(),
        "pair_id": str(candidate.pair_id),
        "chain": candidate.chain,
        "pair_address": candidate.pair_address,
        "dex_identifier": candidate.dex_identifier,
        "liquidity_usd": _decimal_text(candidate.liquidity_usd),
        "volume_m5_usd": _decimal_text(candidate.volume_m5_usd),
        "volume_h1_usd": _decimal_text(candidate.volume_h1_usd),
    }


def _decimal_text(value: Decimal | None) -> str | None:
    if value is None:
        return None
    normalized = value.normalize() if value else Decimal(0)
    return format(normalized, "f")
