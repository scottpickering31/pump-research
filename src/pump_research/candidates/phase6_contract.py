"""Non-executable Phase 6 task contracts; no provider integration lives here."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True, slots=True)
class FutureAnalysisContract:
    analysis_type: str
    minimum_tier: str
    required_inputs: tuple[str, ...]
    expected_output_facts: tuple[str, ...]
    freshness: timedelta
    cache_identity: str


PHASE6_TASK_CONTRACTS: tuple[FutureAnalysisContract, ...] = (
    FutureAnalysisContract(
        "HOLDER_SNAPSHOT",
        "TIER_2_INVESTIGATE",
        ("token_address", "input_watermark", "mint_supply_identity"),
        ("holder_count", "top_holder_concentration", "source_provenance"),
        timedelta(minutes=10),
        "token+slot+provider_schema",
    ),
    FutureAnalysisContract(
        "TRADER_DISTRIBUTION",
        "TIER_2_INVESTIGATE",
        ("token_address", "pair_identity", "closed_time_range"),
        ("unique_traders", "size_distribution", "trade_concentration"),
        timedelta(minutes=5),
        "pair+range+provider_schema",
    ),
    FutureAnalysisContract(
        "CREATOR_HISTORY",
        "TIER_2_INVESTIGATE",
        ("token_address", "creator_fact", "input_watermark"),
        ("prior_launch_facts", "provenance"),
        timedelta(hours=24),
        "creator+input_watermark+source_schema",
    ),
    FutureAnalysisContract(
        "LIQUIDITY_EVENT_ANALYSIS",
        "TIER_2_INVESTIGATE",
        ("pair_identity", "closed_slot_range"),
        ("lp_add_remove_events", "authority_facts", "provenance"),
        timedelta(minutes=5),
        "pair+slot_range+decoder_version",
    ),
    FutureAnalysisContract(
        "WALLET_CLUSTER_ANALYSIS",
        "TIER_3_DEEP_REVIEW",
        ("candidate_id", "bounded_wallet_set", "input_watermark"),
        ("relationship_edges", "method_version", "unknown_reasons"),
        timedelta(hours=1),
        "wallet_set_hash+watermark+method_version",
    ),
    FutureAnalysisContract(
        "FUNDING_GRAPH_ANALYSIS",
        "TIER_3_DEEP_REVIEW",
        ("candidate_id", "bounded_wallet_set", "maximum_graph_depth"),
        ("funding_edges", "truncation_evidence", "source_provenance"),
        timedelta(hours=1),
        "wallet_set_hash+slot+depth+method_version",
    ),
)
