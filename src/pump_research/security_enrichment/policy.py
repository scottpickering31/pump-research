"""Versioned Phase 6 freshness, traversal, provider-budget, and Tier 3 policy."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import timedelta

from pump_research.config import Settings

SECURITY_POLICY_NAME = "SELECTIVE_SECURITY_ENRICHMENT_V1_NON_TRADING"
SECURITY_POLICY_VERSION = "1.0.0"


@dataclass(frozen=True, slots=True)
class SecurityEnrichmentPolicy:
    provider_requests_per_minute: int
    transaction_history_requests_per_minute: int
    holder_requests_per_minute: int
    wallet_graph_requests_per_minute: int
    max_pages_per_task: int
    page_size: int
    max_wallets_per_candidate: int
    max_edges_per_candidate: int
    max_funding_hops: int
    holder_ttl: timedelta
    trader_ttl: timedelta
    creator_ttl: timedelta
    liquidity_ttl: timedelta
    wallet_graph_ttl: timedelta
    funding_ttl: timedelta

    @classmethod
    def from_settings(cls, settings: Settings) -> SecurityEnrichmentPolicy:
        return cls(
            provider_requests_per_minute=settings.security_indexer_requests_per_minute,
            transaction_history_requests_per_minute=(
                settings.security_transaction_history_requests_per_minute
            ),
            holder_requests_per_minute=settings.security_holder_requests_per_minute,
            wallet_graph_requests_per_minute=(settings.security_wallet_graph_requests_per_minute),
            max_pages_per_task=settings.security_max_pages_per_task,
            page_size=settings.security_page_size,
            max_wallets_per_candidate=settings.security_max_wallets_per_candidate,
            max_edges_per_candidate=settings.security_max_edges_per_candidate,
            max_funding_hops=settings.security_max_funding_hops,
            holder_ttl=timedelta(seconds=settings.security_holder_ttl_seconds),
            trader_ttl=timedelta(seconds=settings.security_trader_ttl_seconds),
            creator_ttl=timedelta(seconds=settings.security_creator_ttl_seconds),
            liquidity_ttl=timedelta(seconds=settings.security_liquidity_ttl_seconds),
            wallet_graph_ttl=timedelta(seconds=settings.security_wallet_graph_ttl_seconds),
            funding_ttl=timedelta(seconds=settings.security_funding_ttl_seconds),
        )

    @property
    def snapshot(self) -> dict[str, object]:
        return {
            "name": SECURITY_POLICY_NAME,
            "version": SECURITY_POLICY_VERSION,
            "purpose": "selective_security_research_not_trading",
            "provider_requests_per_minute": self.provider_requests_per_minute,
            "transaction_history_requests_per_minute": (
                self.transaction_history_requests_per_minute
            ),
            "holder_requests_per_minute": self.holder_requests_per_minute,
            "wallet_graph_requests_per_minute": self.wallet_graph_requests_per_minute,
            "max_pages_per_task": self.max_pages_per_task,
            "page_size": self.page_size,
            "max_wallets_per_candidate": self.max_wallets_per_candidate,
            "max_edges_per_candidate": self.max_edges_per_candidate,
            "max_funding_hops": self.max_funding_hops,
            "freshness_seconds": {
                "HOLDER_SNAPSHOT": int(self.holder_ttl.total_seconds()),
                "TRADER_DISTRIBUTION": int(self.trader_ttl.total_seconds()),
                "CREATOR_HISTORY": int(self.creator_ttl.total_seconds()),
                "LIQUIDITY_EVENT_ANALYSIS": int(self.liquidity_ttl.total_seconds()),
                "WALLET_CLUSTER_ANALYSIS": int(self.wallet_graph_ttl.total_seconds()),
                "FUNDING_GRAPH_ANALYSIS": int(self.funding_ttl.total_seconds()),
            },
            "capacity_precedence": [
                "core_dex_collection",
                "universal_enrichment",
                "tier2_security",
                "tier3_deep_review",
            ],
        }

    @property
    def sha256(self) -> str:
        return hashlib.sha256(
            json.dumps(self.snapshot, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()

    def ttl_for(self, analysis_type: str) -> timedelta:
        return {
            "HOLDER_SNAPSHOT": self.holder_ttl,
            "TRADER_DISTRIBUTION": self.trader_ttl,
            "CREATOR_HISTORY": self.creator_ttl,
            "LIQUIDITY_EVENT_ANALYSIS": self.liquidity_ttl,
            "WALLET_CLUSTER_ANALYSIS": self.wallet_graph_ttl,
            "FUNDING_GRAPH_ANALYSIS": self.funding_ttl,
        }[analysis_type]

    def request_limit_for(self, analysis_type: str) -> int:
        if analysis_type == "HOLDER_SNAPSHOT":
            return self.holder_requests_per_minute
        if analysis_type in {"WALLET_CLUSTER_ANALYSIS", "FUNDING_GRAPH_ANALYSIS"}:
            return self.wallet_graph_requests_per_minute
        if analysis_type in {"TRADER_DISTRIBUTION", "LIQUIDITY_EVENT_ANALYSIS"}:
            return self.transaction_history_requests_per_minute
        return self.provider_requests_per_minute


def model_security_load(
    policy: SecurityEnrichmentPolicy,
    *,
    tier2_candidates_per_minute: float,
    tier3_candidates_per_minute: float,
) -> dict[str, float]:
    tier2_requests = tier2_candidates_per_minute * 4
    tier3_requests = tier3_candidates_per_minute * 2
    requested = tier2_requests + tier3_requests
    remaining = float(policy.provider_requests_per_minute)
    admitted_holder = min(
        tier2_candidates_per_minute,
        float(policy.holder_requests_per_minute),
        remaining,
    )
    remaining -= admitted_holder
    admitted_transaction = min(
        tier2_candidates_per_minute * 2,
        float(policy.transaction_history_requests_per_minute),
        remaining,
    )
    remaining -= admitted_transaction
    admitted_creator = min(tier2_candidates_per_minute, remaining)
    remaining -= admitted_creator
    admitted_wallet = min(
        tier3_requests,
        float(policy.wallet_graph_requests_per_minute),
        remaining,
    )
    admitted = admitted_holder + admitted_transaction + admitted_creator + admitted_wallet
    return {
        "requested_requests_per_minute": requested,
        "admitted_requests_per_minute": admitted,
        "deferred_requests_per_minute": max(0.0, requested - admitted),
        "admitted_holder_requests_per_minute": admitted_holder,
        "admitted_transaction_requests_per_minute": admitted_transaction,
        "admitted_creator_requests_per_minute": admitted_creator,
        "admitted_wallet_graph_requests_per_minute": admitted_wallet,
        "core_dex_requests_displaced_per_minute": 0.0,
    }
