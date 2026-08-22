"""Transparent orchestration rules; these are explicitly not trading signals."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import StrEnum
from typing import cast

from pump_research.config import Settings

ORCHESTRATION_RULE_NAME = "ORCHESTRATION_RULE_V1_NOT_TRADING_SIGNAL"
ORCHESTRATION_RULE_VERSION = "1.1.0"


class CandidateTier(StrEnum):
    """Extra-analysis eligibility, independent of lifecycle and coverage."""

    TIER_0_UNIVERSAL = "TIER_0_UNIVERSAL"
    TIER_1_INTERESTING = "TIER_1_INTERESTING"
    TIER_2_INVESTIGATE = "TIER_2_INVESTIGATE"
    TIER_3_DEEP_REVIEW = "TIER_3_DEEP_REVIEW"
    TIER_4_PRETRADE = "TIER_4_PRETRADE"


class TransitionReason(StrEnum):
    """Fact-oriented reasons with no profit interpretation."""

    MARKET_ACTIVITY = "MARKET_ACTIVITY"
    WATCH_STATE = "WATCH_STATE"
    BOOST_ACTIVITY = "BOOST_ACTIVITY"
    SECURITY_CHANGE = "SECURITY_CHANGE"
    COVERAGE_RESURRECTION = "COVERAGE_RESURRECTION"
    EVIDENCE_STALE = "EVIDENCE_STALE"
    ACTIVITY_COOLING = "ACTIVITY_COOLING"


@dataclass(frozen=True, slots=True)
class CandidateEvidence:
    """Compact as-of input. All fields were received by ``watermark``."""

    token_id: str
    evaluated_at: datetime
    watermark: datetime
    lifecycle_state: str
    coverage_class: str
    admitted_at: datetime
    observation_id: str | None
    observation_received_at: datetime | None
    liquidity_usd: Decimal | None
    volume_m5_usd: Decimal | None
    buys_m5: int | None
    sells_m5: int | None
    boost_event_id: str | None = None
    boost_received_at: datetime | None = None
    security_snapshot_id: str | None = None
    security_received_at: datetime | None = None
    coverage_resurrection: bool = False
    holder_snapshot_id: str | None = None
    holder_received_at: datetime | None = None
    holder_top10_pct: Decimal | None = None
    trader_snapshot_id: str | None = None
    trader_received_at: datetime | None = None
    unique_traders: int | None = None
    total_trades: int | None = None
    wallet_evidence_received_at: datetime | None = None
    common_funder_share_pct: Decimal | None = None
    liquidity_evidence_received_at: datetime | None = None
    liquidity_removal_pct: Decimal | None = None

    def __post_init__(self) -> None:
        for name in ("evaluated_at", "watermark", "admitted_at"):
            value = getattr(self, name)
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError(f"{name} must be timezone-aware")
        if self.watermark > self.evaluated_at:
            raise ValueError("input watermark cannot be after evaluation time")
        for name in (
            "observation_received_at",
            "boost_received_at",
            "security_received_at",
            "holder_received_at",
            "trader_received_at",
            "wallet_evidence_received_at",
            "liquidity_evidence_received_at",
        ):
            value = getattr(self, name)
            if value is not None and value > self.watermark:
                raise ValueError(f"{name} cannot exceed the input watermark")

    @property
    def transaction_count_m5(self) -> int | None:
        if self.buys_m5 is None or self.sells_m5 is None:
            return None
        return self.buys_m5 + self.sells_m5

    @property
    def volume_liquidity_ratio(self) -> Decimal | None:
        if self.volume_m5_usd is None or not self.liquidity_usd:
            return None
        return self.volume_m5_usd / self.liquidity_usd

    @property
    def identity_payload(self) -> dict[str, object]:
        payload = asdict(self)
        return cast(dict[str, object], _jsonable(payload))

    @property
    def sha256(self) -> str:
        return _digest(self.identity_payload)


@dataclass(frozen=True, slots=True)
class EvaluationDecision:
    """One deterministic target tier and finite candidate-coverage TTL."""

    eligible: bool
    target_tier: CandidateTier
    reason: TransitionReason
    coverage_until: datetime | None
    evidence_sha256: str
    detail: dict[str, object]


@dataclass(frozen=True, slots=True)
class CandidatePolicy:
    """Versioned operational-selectivity policy, never a BUY/SELL rule."""

    minimum_liquidity_usd: Decimal
    minimum_transactions_m5: int
    minimum_volume_liquidity_ratio: Decimal
    tier2_minimum_liquidity_usd: Decimal
    tier2_minimum_transactions_m5: int
    tier2_minimum_volume_liquidity_ratio: Decimal
    tier1_ttl: timedelta
    tier2_ttl: timedelta
    coverage_interval: timedelta
    security_freshness: timedelta
    tasks_per_minute: int
    expensive_slots_per_minute: int
    boost_wakeups_per_minute: int
    max_active_coverage: int
    tier3_ttl: timedelta
    max_tier3_candidates: int
    tier3_holder_top10_pct: Decimal
    tier3_max_unique_traders: int
    tier3_min_total_trades: int
    tier3_common_funder_share_pct: Decimal
    tier3_liquidity_removal_pct: Decimal

    @classmethod
    def from_settings(cls, settings: Settings) -> CandidatePolicy:
        return cls(
            minimum_liquidity_usd=settings.candidate_min_liquidity_usd,
            minimum_transactions_m5=settings.candidate_min_transactions_m5,
            minimum_volume_liquidity_ratio=settings.candidate_min_volume_liquidity_ratio,
            tier2_minimum_liquidity_usd=settings.candidate_tier2_min_liquidity_usd,
            tier2_minimum_transactions_m5=settings.candidate_tier2_min_transactions_m5,
            tier2_minimum_volume_liquidity_ratio=(
                settings.candidate_tier2_min_volume_liquidity_ratio
            ),
            tier1_ttl=timedelta(seconds=settings.candidate_tier1_coverage_seconds),
            tier2_ttl=timedelta(seconds=settings.candidate_tier2_coverage_seconds),
            coverage_interval=timedelta(seconds=settings.candidate_coverage_interval_seconds),
            security_freshness=timedelta(seconds=settings.candidate_security_freshness_seconds),
            tasks_per_minute=settings.candidate_tasks_per_minute,
            expensive_slots_per_minute=settings.candidate_expensive_slots_per_minute,
            boost_wakeups_per_minute=settings.candidate_boost_wakeups_per_minute,
            max_active_coverage=settings.candidate_max_active_coverage,
            tier3_ttl=timedelta(seconds=settings.security_tier3_ttl_seconds),
            max_tier3_candidates=settings.security_max_tier3_candidates,
            tier3_holder_top10_pct=settings.security_tier3_holder_top10_pct,
            tier3_max_unique_traders=settings.security_tier3_max_unique_traders,
            tier3_min_total_trades=settings.security_tier3_min_total_trades,
            tier3_common_funder_share_pct=settings.security_tier3_common_funder_share,
            tier3_liquidity_removal_pct=settings.security_tier3_liquidity_removal_pct,
        )

    @property
    def snapshot(self) -> dict[str, object]:
        return {
            "name": ORCHESTRATION_RULE_NAME,
            "version": ORCHESTRATION_RULE_VERSION,
            "purpose": "resource_orchestration_not_trading_signal",
            "minimum_liquidity_usd": str(self.minimum_liquidity_usd),
            "minimum_transactions_m5": self.minimum_transactions_m5,
            "minimum_volume_liquidity_ratio": str(self.minimum_volume_liquidity_ratio),
            "tier2_minimum_liquidity_usd": str(self.tier2_minimum_liquidity_usd),
            "tier2_minimum_transactions_m5": self.tier2_minimum_transactions_m5,
            "tier2_minimum_volume_liquidity_ratio": str(self.tier2_minimum_volume_liquidity_ratio),
            "tier1_ttl_seconds": int(self.tier1_ttl.total_seconds()),
            "tier2_ttl_seconds": int(self.tier2_ttl.total_seconds()),
            "coverage_interval_seconds": int(self.coverage_interval.total_seconds()),
            "security_freshness_seconds": int(self.security_freshness.total_seconds()),
            "tasks_per_minute": self.tasks_per_minute,
            "expensive_slots_per_minute": self.expensive_slots_per_minute,
            "boost_wakeups_per_minute": self.boost_wakeups_per_minute,
            "max_active_coverage": self.max_active_coverage,
            "tier3_ttl_seconds": int(self.tier3_ttl.total_seconds()),
            "max_tier3_candidates": self.max_tier3_candidates,
            "tier3_holder_top10_pct": str(self.tier3_holder_top10_pct),
            "tier3_max_unique_traders": self.tier3_max_unique_traders,
            "tier3_min_total_trades": self.tier3_min_total_trades,
            "tier3_common_funder_share_pct": str(self.tier3_common_funder_share_pct),
            "tier3_liquidity_removal_pct": str(self.tier3_liquidity_removal_pct),
            "capacity_precedence": [
                "core_collection",
                "universal_enrichment",
                "candidate_enrichment",
            ],
        }

    @property
    def sha256(self) -> str:
        return _digest(self.snapshot)

    def evaluate(
        self, evidence: CandidateEvidence, *, current_tier: CandidateTier
    ) -> EvaluationDecision:
        """Evaluate only contemporaneously available facts."""
        transactions = evidence.transaction_count_m5
        ratio = evidence.volume_liquidity_ratio
        market_ok = (
            evidence.liquidity_usd is not None
            and evidence.liquidity_usd >= self.minimum_liquidity_usd
            and transactions is not None
            and transactions >= self.minimum_transactions_m5
            and ratio is not None
            and ratio >= self.minimum_volume_liquidity_ratio
        )
        security_fresh = (
            evidence.security_received_at is not None
            and evidence.evaluated_at - evidence.security_received_at <= self.security_freshness
        )
        tier2_market = (
            evidence.liquidity_usd is not None
            and evidence.liquidity_usd >= self.tier2_minimum_liquidity_usd
            and transactions is not None
            and transactions >= self.tier2_minimum_transactions_m5
            and ratio is not None
            and ratio >= self.tier2_minimum_volume_liquidity_ratio
        )
        tier3_reasons = {
            "holder_concentration": (
                evidence.holder_top10_pct is not None
                and evidence.holder_top10_pct >= self.tier3_holder_top10_pct
            ),
            "few_traders": (
                evidence.unique_traders is not None
                and evidence.total_trades is not None
                and evidence.total_trades >= self.tier3_min_total_trades
                and evidence.unique_traders <= self.tier3_max_unique_traders
            ),
            "common_funder": (
                evidence.common_funder_share_pct is not None
                and evidence.common_funder_share_pct >= self.tier3_common_funder_share_pct
            ),
            "liquidity_removal": (
                evidence.liquidity_removal_pct is not None
                and evidence.liquidity_removal_pct >= self.tier3_liquidity_removal_pct
            ),
        }
        if current_tier in {
            CandidateTier.TIER_2_INVESTIGATE,
            CandidateTier.TIER_3_DEEP_REVIEW,
        } and any(tier3_reasons.values()):
            return EvaluationDecision(
                eligible=True,
                target_tier=CandidateTier.TIER_3_DEEP_REVIEW,
                reason=TransitionReason.SECURITY_CHANGE,
                coverage_until=evidence.evaluated_at + self.tier3_ttl,
                evidence_sha256=evidence.sha256,
                detail={
                    "deep_review_eligible": True,
                    "deep_review_reasons": sorted(
                        name for name, matched in tier3_reasons.items() if matched
                    ),
                },
            )
        if evidence.boost_event_id is not None:
            reason = TransitionReason.BOOST_ACTIVITY
            target = CandidateTier.TIER_1_INTERESTING
        elif evidence.coverage_resurrection:
            reason = TransitionReason.COVERAGE_RESURRECTION
            target = CandidateTier.TIER_1_INTERESTING
        elif evidence.lifecycle_state == "WATCH" and market_ok:
            reason = TransitionReason.WATCH_STATE
            target = CandidateTier.TIER_1_INTERESTING
        elif market_ok:
            reason = TransitionReason.MARKET_ACTIVITY
            target = CandidateTier.TIER_1_INTERESTING
        else:
            return EvaluationDecision(
                eligible=False,
                target_tier=CandidateTier.TIER_0_UNIVERSAL,
                reason=(
                    TransitionReason.EVIDENCE_STALE
                    if current_tier is not CandidateTier.TIER_0_UNIVERSAL
                    else TransitionReason.ACTIVITY_COOLING
                ),
                coverage_until=None,
                evidence_sha256=evidence.sha256,
                detail={"market_activity_eligible": False},
            )
        if (
            current_tier
            in {
                CandidateTier.TIER_1_INTERESTING,
                CandidateTier.TIER_2_INVESTIGATE,
            }
            and tier2_market
            and security_fresh
        ):
            target = CandidateTier.TIER_2_INVESTIGATE
        ttl = self.tier2_ttl if target is CandidateTier.TIER_2_INVESTIGATE else self.tier1_ttl
        return EvaluationDecision(
            eligible=True,
            target_tier=target,
            reason=reason,
            coverage_until=evidence.evaluated_at + ttl,
            evidence_sha256=evidence.sha256,
            detail={
                "market_activity_eligible": market_ok,
                "tier2_market_eligible": tier2_market,
                "security_fresh": security_fresh,
                "transaction_count_m5": transactions,
                "volume_liquidity_ratio": str(ratio) if ratio is not None else None,
            },
        )


def candidate_identity(
    *, epoch_id: str, evidence: CandidateEvidence, policy_sha256: str, reason: str
) -> tuple[str, str]:
    """Return deterministic UUID text and semantic idempotency key."""
    semantic = {
        "epoch_id": epoch_id,
        "token_id": evidence.token_id,
        "watermark": evidence.watermark.isoformat(),
        "evidence_sha256": evidence.sha256,
        "policy_sha256": policy_sha256,
        "reason": reason,
    }
    key = _digest(semantic)
    import uuid

    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research:candidate:{key}")), key


def budget_projection(policy: CandidatePolicy, requested_tasks_per_minute: int) -> dict[str, int]:
    """Candidate work sheds load at its own gate and cannot consume core budget."""
    admitted = min(requested_tasks_per_minute, policy.tasks_per_minute)
    return {
        "requested_candidate_tasks_per_minute": requested_tasks_per_minute,
        "admitted_candidate_tasks_per_minute": admitted,
        "deferred_candidate_tasks_per_minute": requested_tasks_per_minute - admitted,
        "core_requests_displaced_per_minute": 0,
    }


def select_boost_wakeups(
    evidence: tuple[CandidateEvidence, ...], *, budget: int
) -> tuple[CandidateEvidence, ...]:
    """Select a deterministic bounded prefix; the remainder stays deferred."""
    if budget < 0:
        raise ValueError("boost wake-up budget cannot be negative")
    eligible = (item for item in evidence if item.boost_event_id is not None)
    return tuple(
        sorted(
            eligible,
            key=lambda item: (
                item.boost_received_at or item.watermark,
                item.boost_event_id or "",
                item.token_id,
            ),
        )[:budget]
    )


def _digest(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _jsonable(value: object) -> object:
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value
