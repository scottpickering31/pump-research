"""Transparent deterministic concentration, graph, and security-v1 analysis."""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from decimal import Decimal
from statistics import median

from pump_research.security_enrichment.contracts import (
    EvidenceCompleteness,
    HolderAccountFact,
    TradeFact,
    TradeSide,
    WalletEdgeFact,
)

SECURITY_FEATURE_SET_NAME = "security-v1"
SECURITY_FEATURE_SET_VERSION = "1.0.0"
CLUSTER_ALGORITHM_VERSION = "explainable-connected-components-v1"


@dataclass(frozen=True, slots=True)
class HolderMetrics:
    holder_count: int | None
    top_1_pct: Decimal | None
    top_5_pct: Decimal | None
    top_10_pct: Decimal | None
    top_20_pct: Decimal | None
    largest_holder_pct: Decimal | None
    largest_non_pool_holder_pct: Decimal | None
    creator_holder_pct: Decimal | None
    hhi: Decimal | None
    covered_supply_pct: Decimal | None
    completeness: EvidenceCompleteness


@dataclass(frozen=True, slots=True)
class TraderMetrics:
    total_trades: int
    buy_trades: int
    sell_trades: int
    unique_buyers: int | None
    unique_sellers: int | None
    unique_traders: int | None
    volume_usd: Decimal | None
    median_trade_usd: Decimal | None
    p90_trade_usd: Decimal | None
    p95_trade_usd: Decimal | None
    largest_trade_usd: Decimal | None
    top_1_trader_volume_share: Decimal | None
    top_5_trader_volume_share: Decimal | None
    top_10_trader_volume_share: Decimal | None
    repeat_trader_ratio: Decimal | None
    buy_sell_wallet_overlap: Decimal | None


@dataclass(frozen=True, slots=True)
class WalletClusterResult:
    cluster_id: str
    members: tuple[str, ...]
    input_edge_sha256: str
    explanation: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SecurityFeatureValues:
    values: dict[str, object]
    schema_sha256: str
    input_sha256: str


def build_holder_metrics(
    accounts: tuple[HolderAccountFact, ...],
    *,
    mint_supply_raw: Decimal | None,
    holder_count: int | None,
    completeness: EvidenceCompleteness,
) -> HolderMetrics:
    """Aggregate token accounts by owner without hiding pool exclusions."""
    if mint_supply_raw is None or mint_supply_raw <= 0:
        return HolderMetrics(
            holder_count, None, None, None, None, None, None, None, None, None, completeness
        )
    by_owner: dict[str, Decimal] = defaultdict(Decimal)
    non_pool_by_owner: dict[str, Decimal] = defaultdict(Decimal)
    creator = Decimal(0)
    for account in accounts:
        owner = account.owner_wallet or f"unknown-token-account:{account.token_account}"
        by_owner[owner] += account.raw_balance
        if not account.is_known_pool:
            non_pool_by_owner[owner] += account.raw_balance
        if account.is_creator:
            creator += account.raw_balance
    balances = sorted(by_owner.values(), reverse=True)

    def top(n: int) -> Decimal:
        return _ratio(sum(balances[:n], Decimal(0)), mint_supply_raw)

    non_pool = max(non_pool_by_owner.values(), default=Decimal(0))
    covered = sum(balances, Decimal(0))
    # HHI is meaningful only when the source claims a complete distribution.
    hhi = None
    if completeness is EvidenceCompleteness.FULL_DISTRIBUTION:
        hhi = sum(((value / mint_supply_raw) ** 2 for value in balances), Decimal(0))
    return HolderMetrics(
        holder_count=holder_count,
        top_1_pct=top(1),
        top_5_pct=top(5),
        top_10_pct=top(10),
        top_20_pct=top(20),
        largest_holder_pct=top(1),
        largest_non_pool_holder_pct=_ratio(non_pool, mint_supply_raw),
        creator_holder_pct=_ratio(creator, mint_supply_raw) if creator else None,
        hhi=hhi,
        covered_supply_pct=_ratio(covered, mint_supply_raw),
        completeness=completeness,
    )


def build_trader_metrics(trades: tuple[TradeFact, ...]) -> TraderMetrics:
    buys = tuple(item for item in trades if item.side is TradeSide.BUY)
    sells = tuple(item for item in trades if item.side is TradeSide.SELL)
    known = tuple(item for item in trades if item.wallet is not None)
    buyer_wallets = {item.wallet for item in buys if item.wallet is not None}
    seller_wallets = {item.wallet for item in sells if item.wallet is not None}
    trader_wallets = {item.wallet for item in known}
    notionals = sorted(item.notional_usd for item in trades if item.notional_usd is not None)
    by_wallet: dict[str, Decimal] = defaultdict(Decimal)
    counts: dict[str, int] = defaultdict(int)
    for item in known:
        assert item.wallet is not None
        counts[item.wallet] += 1
        if item.notional_usd is not None:
            by_wallet[item.wallet] += item.notional_usd
    total_volume = sum(notionals, Decimal(0)) if notionals else None

    def volume_share(n: int) -> Decimal | None:
        if total_volume is None or total_volume <= 0:
            return None
        return _ratio(sum(sorted(by_wallet.values(), reverse=True)[:n], Decimal(0)), total_volume)

    repeated = sum(count for count in counts.values() if count > 1)
    overlap_denominator = len(buyer_wallets | seller_wallets)
    return TraderMetrics(
        total_trades=len(trades),
        buy_trades=len(buys),
        sell_trades=len(sells),
        unique_buyers=len(buyer_wallets) if all(item.wallet is not None for item in buys) else None,
        unique_sellers=len(seller_wallets)
        if all(item.wallet is not None for item in sells)
        else None,
        unique_traders=len(trader_wallets) if len(known) == len(trades) else None,
        volume_usd=total_volume,
        median_trade_usd=Decimal(median(notionals)) if notionals else None,
        p90_trade_usd=_percentile(notionals, Decimal("0.90")),
        p95_trade_usd=_percentile(notionals, Decimal("0.95")),
        largest_trade_usd=notionals[-1] if notionals else None,
        top_1_trader_volume_share=volume_share(1),
        top_5_trader_volume_share=volume_share(5),
        top_10_trader_volume_share=volume_share(10),
        repeat_trader_ratio=(Decimal(repeated) / Decimal(len(known)) if known else None),
        buy_sell_wallet_overlap=(
            Decimal(len(buyer_wallets & seller_wallets)) / Decimal(overlap_denominator)
            if overlap_denominator
            else None
        ),
    )


def cluster_wallet_edges(edges: tuple[WalletEdgeFact, ...]) -> tuple[WalletClusterResult, ...]:
    """Connected components over factual edges; no probabilistic identity inference."""
    parents: dict[str, str] = {}

    def find(value: str) -> str:
        parents.setdefault(value, value)
        if parents[value] != value:
            parents[value] = find(parents[value])
        return parents[value]

    def union(left: str, right: str) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            low, high = sorted((left_root, right_root))
            parents[high] = low

    ordered = sorted(
        edges,
        key=lambda edge: (
            edge.relationship_type.value,
            min(edge.wallet_a, edge.wallet_b),
            max(edge.wallet_a, edge.wallet_b),
            edge.evidence_received_at,
        ),
    )
    for edge in ordered:
        union(edge.wallet_a, edge.wallet_b)
    members: dict[str, set[str]] = defaultdict(set)
    for wallet in parents:
        members[find(wallet)].add(wallet)
    edge_hash = _digest(
        [
            {
                "a": min(edge.wallet_a, edge.wallet_b),
                "b": max(edge.wallet_a, edge.wallet_b),
                "type": edge.relationship_type.value,
                "count": edge.strength_count,
                "facts": sorted(edge.source_fact_ids),
            }
            for edge in ordered
        ]
    )
    output: list[WalletClusterResult] = []
    for values in sorted(tuple(sorted(item)) for item in members.values()):
        relevant = tuple(
            edge for edge in ordered if edge.wallet_a in values and edge.wallet_b in values
        )
        identity = _digest(
            {"algorithm": CLUSTER_ALGORITHM_VERSION, "members": values, "edges": edge_hash}
        )
        output.append(
            WalletClusterResult(
                cluster_id=identity,
                members=values,
                input_edge_sha256=edge_hash,
                explanation=tuple(sorted({edge.relationship_type.value for edge in relevant})),
            )
        )
    return tuple(output)


def build_security_features(
    *,
    generated_at: datetime,
    holder: HolderMetrics | None,
    trader: TraderMetrics | None,
    creator_hold_pct: Decimal | None,
    creator_prior_collapse_rate: Decimal | None,
    wallet_cluster_count: int | None,
    largest_cluster_trade_share: Decimal | None,
    common_funder_share: Decimal | None,
    synchronized_trade_score: Decimal | None,
    liquidity_removal_recent_pct: Decimal | None,
    liquidity_change_velocity: Decimal | None,
    creator_transfer_activity: int | None,
    security_snapshot_age_seconds: Decimal | None,
    input_fact_ids: tuple[str, ...],
) -> SecurityFeatureValues:
    generated_at = generated_at.astimezone(UTC)
    unique = trader.unique_traders if trader else None
    values: dict[str, object] = {
        "feature_set": SECURITY_FEATURE_SET_NAME,
        "feature_version": SECURITY_FEATURE_SET_VERSION,
        "generated_at": generated_at.isoformat(),
        "holder_top10_pct": holder.top_10_pct if holder else None,
        "holder_hhi": holder.hhi if holder else None,
        "creator_hold_pct": creator_hold_pct,
        "creator_prior_collapse_rate": creator_prior_collapse_rate,
        "unique_trader_count": unique,
        "trades_per_unique_trader": (
            Decimal(trader.total_trades) / Decimal(unique)
            if trader is not None and unique
            else None
        ),
        "top10_trader_volume_share": (trader.top_10_trader_volume_share if trader else None),
        "repeat_trade_ratio": trader.repeat_trader_ratio if trader else None,
        "wallet_cluster_count": wallet_cluster_count,
        "largest_cluster_trade_share": largest_cluster_trade_share,
        "common_funder_share": common_funder_share,
        "synchronized_trade_score": synchronized_trade_score,
        "liquidity_removal_recent_pct": liquidity_removal_recent_pct,
        "liquidity_change_velocity": liquidity_change_velocity,
        "creator_transfer_activity": creator_transfer_activity,
        "security_snapshot_age_seconds": security_snapshot_age_seconds,
    }
    schema = {
        key: type(value).__name__ if value is not None else "nullable"
        for key, value in values.items()
    }
    return SecurityFeatureValues(
        values=values,
        schema_sha256=_digest(schema),
        input_sha256=_digest(sorted(input_fact_ids)),
    )


def _ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
    return numerator * Decimal(100) / denominator


def _percentile(values: list[Decimal], percentile: Decimal) -> Decimal | None:
    if not values:
        return None
    index = max(0, math.ceil(float(percentile) * len(values)) - 1)
    return values[index]


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def holder_metrics_dict(value: HolderMetrics) -> dict[str, object]:
    return asdict(value)
