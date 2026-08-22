from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest

from pump_research.archival import archive_family_names
from pump_research.config import Settings
from pump_research.market_data.solana_rpc import SolanaRpcClient
from pump_research.security_enrichment.analysis import (
    build_holder_metrics,
    build_security_features,
    build_trader_metrics,
    cluster_wallet_edges,
)
from pump_research.security_enrichment.contracts import (
    EvidenceCompleteness,
    HolderAccountFact,
    ProviderPageRequest,
    TradeFact,
    TradeSide,
    WalletEdgeFact,
    WalletRelationshipType,
)
from pump_research.security_enrichment.policy import (
    SecurityEnrichmentPolicy,
    model_security_load,
)
from pump_research.security_enrichment.provider import StandardSolanaHolderProvider

NOW = datetime(2026, 8, 22, 12, tzinfo=UTC)


def _trade(index: int, wallet: str, *, side: TradeSide = TradeSide.BUY) -> TradeFact:
    return TradeFact(
        signature=f"sig-{index}",
        source_slot=index,
        source_event_at=NOW - timedelta(seconds=index),
        received_at=NOW,
        wallet=wallet,
        side=side,
        notional_usd=Decimal("10"),
        sequence=index,
    )


def _edge(left: str, right: str, kind: WalletRelationshipType) -> WalletEdgeFact:
    return WalletEdgeFact(
        wallet_a=left,
        wallet_b=right,
        relationship_type=kind,
        first_observed_at=NOW - timedelta(minutes=1),
        evidence_received_at=NOW,
        strength_count=2,
        source_fact_ids=(f"{left}:{right}",),
    )


def test_top_twenty_snapshot_does_not_claim_full_hhi() -> None:
    accounts = (
        HolderAccountFact("pool", "pool-owner", Decimal("400"), is_known_pool=True),
        HolderAccountFact("one", "wallet-a", Decimal("300")),
        HolderAccountFact("two", "wallet-a", Decimal("100")),
        HolderAccountFact("three", "wallet-b", Decimal("50"), is_creator=True),
    )
    partial = build_holder_metrics(
        accounts,
        mint_supply_raw=Decimal("1000"),
        holder_count=None,
        completeness=EvidenceCompleteness.TOP_20_TOKEN_ACCOUNTS,
    )
    assert partial.top_1_pct == Decimal("40")
    assert partial.largest_non_pool_holder_pct == Decimal("40")
    assert partial.creator_holder_pct == Decimal("5")
    assert partial.covered_supply_pct == Decimal("85")
    assert partial.hhi is None

    complete = build_holder_metrics(
        accounts,
        mint_supply_raw=Decimal("1000"),
        holder_count=3,
        completeness=EvidenceCompleteness.FULL_DISTRIBUTION,
    )
    assert complete.hhi == Decimal("0.3225")


def test_unknown_holder_supply_remains_unknown_not_zero() -> None:
    metrics = build_holder_metrics(
        (),
        mint_supply_raw=None,
        holder_count=None,
        completeness=EvidenceCompleteness.UNKNOWN,
    )
    assert metrics.top_10_pct is None
    assert metrics.covered_supply_pct is None


def test_synthetic_organic_and_coordinated_trading_are_structurally_distinct() -> None:
    organic = build_trader_metrics(tuple(_trade(i, f"wallet-{i % 700}") for i in range(1000)))
    coordinated = build_trader_metrics(tuple(_trade(i, f"wallet-{i % 15}") for i in range(1000)))
    concentrated = build_trader_metrics(tuple(_trade(i, f"wallet-{i % 3}") for i in range(1000)))

    assert organic.unique_traders == 700
    assert coordinated.unique_traders == 15
    assert concentrated.unique_traders == 3
    assert organic.repeat_trader_ratio is not None
    assert coordinated.repeat_trader_ratio is not None
    assert organic.top_10_trader_volume_share is not None
    assert coordinated.top_10_trader_volume_share is not None
    assert concentrated.top_10_trader_volume_share is not None
    assert organic.repeat_trader_ratio < coordinated.repeat_trader_ratio
    assert organic.top_10_trader_volume_share < coordinated.top_10_trader_volume_share
    assert coordinated.top_10_trader_volume_share < concentrated.top_10_trader_volume_share


def test_graph_clustering_is_explainable_order_independent_and_bounded() -> None:
    edges = (
        _edge("a", "b", WalletRelationshipType.COMMON_FUNDER),
        _edge("b", "c", WalletRelationshipType.DIRECT_TRANSFER),
        _edge("x", "y", WalletRelationshipType.LP_LINK),
    )
    forward = cluster_wallet_edges(edges)
    reverse = cluster_wallet_edges(tuple(reversed(edges)))
    assert forward == reverse
    assert [item.members for item in forward] == [("a", "b", "c"), ("x", "y")]
    assert forward[0].explanation == ("COMMON_FUNDER", "DIRECT_TRANSFER")


def test_security_v1_preserves_unknowns_and_input_identity() -> None:
    features = build_security_features(
        generated_at=NOW,
        holder=None,
        trader=None,
        creator_hold_pct=Decimal("70"),
        creator_prior_collapse_rate=None,
        wallet_cluster_count=1,
        largest_cluster_trade_share=None,
        common_funder_share=Decimal("80"),
        synchronized_trade_score=Decimal("12"),
        liquidity_removal_recent_pct=Decimal("75"),
        liquidity_change_velocity=None,
        creator_transfer_activity=1,
        security_snapshot_age_seconds=None,
        input_fact_ids=("fact-b", "fact-a"),
    )
    assert features.values["holder_top10_pct"] is None
    assert features.values["creator_hold_pct"] == Decimal("70")
    assert features.values["liquidity_removal_recent_pct"] == Decimal("75")
    assert len(features.input_sha256) == 64


def test_security_v1_represents_synthetic_manipulation_structures_without_a_score() -> None:
    organic_traders = build_trader_metrics(
        tuple(_trade(i, f"organic-{i % 700}") for i in range(1000))
    )
    coordinated_traders = build_trader_metrics(
        tuple(_trade(i, f"coordinated-{i % 15}") for i in range(1000))
    )
    organic = build_security_features(
        generated_at=NOW,
        holder=None,
        trader=organic_traders,
        creator_hold_pct=Decimal("1"),
        creator_prior_collapse_rate=None,
        wallet_cluster_count=700,
        largest_cluster_trade_share=None,
        common_funder_share=Decimal("0"),
        synchronized_trade_score=Decimal("0"),
        liquidity_removal_recent_pct=Decimal("0"),
        liquidity_change_velocity=None,
        creator_transfer_activity=0,
        security_snapshot_age_seconds=Decimal("60"),
        input_fact_ids=("organic-facts",),
    )
    coordinated = build_security_features(
        generated_at=NOW,
        holder=None,
        trader=coordinated_traders,
        creator_hold_pct=Decimal("55"),
        creator_prior_collapse_rate=None,
        wallet_cluster_count=1,
        largest_cluster_trade_share=None,
        common_funder_share=Decimal("100"),
        synchronized_trade_score=Decimal("75"),
        liquidity_removal_recent_pct=Decimal("80"),
        liquidity_change_velocity=None,
        creator_transfer_activity=3,
        security_snapshot_age_seconds=Decimal("60"),
        input_fact_ids=("coordinated-facts",),
    )

    assert organic.values["unique_trader_count"] == 700
    assert coordinated.values["unique_trader_count"] == 15
    organic_share = organic.values["top10_trader_volume_share"]
    coordinated_share = coordinated.values["top10_trader_volume_share"]
    assert isinstance(organic_share, Decimal)
    assert isinstance(coordinated_share, Decimal)
    assert organic_share < coordinated_share
    assert organic.values["common_funder_share"] == Decimal("0")
    assert coordinated.values["common_funder_share"] == Decimal("100")
    assert coordinated.values["creator_hold_pct"] == Decimal("55")
    assert coordinated.values["liquidity_removal_recent_pct"] == Decimal("80")
    assert "risk_score" not in organic.values
    assert "risk_score" not in coordinated.values


def test_provider_budget_degrades_deep_work_without_displacing_core() -> None:
    settings = Settings(
        database_url="postgresql+asyncpg://unused:unused@localhost/unused",
        solana_rpc_requests_per_minute=30,
    )
    policy = SecurityEnrichmentPolicy.from_settings(settings)
    loads = [
        model_security_load(
            policy,
            tier2_candidates_per_minute=float(multiplier),
            tier3_candidates_per_minute=float(multiplier) / 4,
        )
        for multiplier in (1, 2, 5, 10, 25)
    ]
    assert [item["admitted_requests_per_minute"] for item in loads][-1] == 6
    assert all(item["core_dex_requests_displaced_per_minute"] == 0 for item in loads)
    assert loads[-1]["deferred_requests_per_minute"] > 0


def test_all_phase6_immutable_families_are_archivable() -> None:
    families = set(archive_family_names())
    assert {
        "security_provider_budget_reservations",
        "security_provider_requests",
        "holder_snapshots",
        "holder_balance_facts",
        "trader_distribution_snapshots",
        "creator_relationship_events",
        "creator_history_snapshots",
        "liquidity_event_evidence",
        "wallet_relationship_edges",
        "funding_relationship_evidence",
        "wallet_cluster_snapshots",
        "security_feature_snapshots",
        "security_enrichment_policies",
    } <= families


@pytest.mark.asyncio
async def test_standard_rpc_holder_adapter_preserves_top20_and_missing_owner() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        result: dict[str, object]
        if payload["method"] == "getTokenLargestAccounts":
            result = {
                "context": {"slot": 50},
                "value": [
                    {"address": "account-a", "amount": "600", "decimals": 6},
                    {"address": "account-b", "amount": "200", "decimals": 6},
                ],
            }
        else:
            result = {
                "context": {"slot": 51},
                "value": [
                    {"data": {"parsed": {"info": {"owner": "wallet-a"}}}},
                    None,
                ],
            }
        return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": result})

    settings = Settings(
        database_url="postgresql+asyncpg://unused:unused@localhost/unused",
        solana_rpc_requests_per_minute=30,
    )
    http = httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="https://rpc.test")
    rpc = SolanaRpcClient(settings, http_client=http)
    provider = StandardSolanaHolderProvider(rpc)
    page = await provider.fetch_holders(
        ProviderPageRequest(
            token_address="mint",
            candidate_id="candidate",
            input_watermark=NOW,
            cursor=None,
            limit=20,
            mint_supply_raw=Decimal("1000"),
        )
    )
    await http.aclose()

    assert page.envelope.availability.value == "partial"
    assert page.envelope.completeness is EvidenceCompleteness.TOP_20_TOKEN_ACCOUNTS
    assert page.envelope.source_slot == 50
    assert page.accounts[0].owner_wallet == "wallet-a"
    assert page.accounts[1].owner_wallet is None
