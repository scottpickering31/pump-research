"""Canonical no-future-information state reconstruction."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from pump_research.research.contracts import (
    BoostFact,
    CandidateFact,
    CandidateTierFact,
    CoverageFact,
    FeatureSetContract,
    LifecycleFact,
    MarketContextFact,
    MetadataFact,
    ObservationFact,
    PairFact,
    SecurityFact,
    SelectiveSecurityFact,
    TokenHistory,
    utc,
)


class ResearchContractError(RuntimeError):
    """Raised when source facts violate the strict research contract."""


@dataclass(frozen=True, slots=True)
class TokenStateAsOf:
    epoch_id: str
    epoch_number: int
    token_id: str
    chain: str
    address: str
    decision_at: datetime
    identity_known: bool
    admission_at: datetime | None
    lifecycle: LifecycleFact | None
    coverage: CoverageFact | None
    candidate: CandidateFact | None
    candidate_tier: CandidateTierFact | None
    selected_pair_id: str | None
    current_observation: ObservationFact | None
    observation_history: tuple[ObservationFact, ...]
    pair_fact: PairFact | None
    boost: BoostFact | None
    first_boost_received_at: datetime | None
    previous_boost: BoostFact | None
    metadata: MetadataFact | None
    security: SecurityFact | None
    holder_snapshot: SelectiveSecurityFact | None
    trader_distribution: SelectiveSecurityFact | None
    creator_history: SelectiveSecurityFact | None
    security_features: SelectiveSecurityFact | None
    liquidity_events: tuple[SelectiveSecurityFact, ...]
    wallet_edges: tuple[SelectiveSecurityFact, ...]
    wallet_clusters: tuple[SelectiveSecurityFact, ...]
    funding_relationships: tuple[SelectiveSecurityFact, ...]
    context: MarketContextFact | None
    feature_set_identifier: str
    feature_set_sha256: str
    availability_watermark: datetime | None


def get_token_state_as_of(
    history: TokenHistory,
    timestamp: datetime,
    feature_set: FeatureSetContract | None = None,
    *,
    allow_invalid_epoch: bool = False,
) -> TokenStateAsOf:
    """Return only facts that the system had received or derived by ``timestamp``."""
    decision_at = utc(timestamp, "decision timestamp")
    contract = feature_set or FeatureSetContract()
    if not history.epoch_data_valid and not allow_invalid_epoch:
        raise ResearchContractError(
            f"epoch {history.epoch_number} is invalid and excluded from canonical research"
        )
    _validate_history(history)
    discoveries = tuple(
        sorted(
            (item for item in history.discoveries if utc(item.received_at) <= decision_at),
            key=lambda item: (utc(item.received_at), item.id),
        )
    )
    visible_observations = tuple(
        sorted(
            (item for item in history.observations if utc(item.received_at) <= decision_at),
            key=lambda item: (utc(item.received_at), item.id),
        )
    )
    lifecycle = _latest_lifecycle(history.lifecycle, decision_at)
    admission_candidates = [
        utc(item.decided_at)
        for item in history.lifecycle
        if item.new_state == "NEW"
        and utc(item.decided_at) <= decision_at
        and utc(item.input_watermark) <= utc(item.decided_at)
    ]
    admission_at = min(admission_candidates) if admission_candidates else None
    coverage = _latest(
        (
            item
            for item in history.coverage
            if utc(item.decided_at) <= decision_at and utc(item.effective_at) <= decision_at
        ),
        lambda item: (utc(item.effective_at), utc(item.decided_at), item.id),
    )
    candidate = _latest(
        (
            item
            for item in history.candidates
            if utc(item.candidate_at) <= decision_at and utc(item.input_watermark) <= decision_at
        ),
        lambda item: (utc(item.candidate_at), item.id),
    )
    candidate_tier = _latest(
        (
            item
            for item in history.candidate_tiers
            if utc(item.decided_at) <= decision_at and utc(item.input_watermark) <= decision_at
        ),
        lambda item: (utc(item.decided_at), item.id),
    )
    pair_id = _select_pair(visible_observations)
    pair_history = tuple(item for item in visible_observations if item.pair_id == pair_id)
    current = pair_history[-1] if pair_history else None
    pair_fact = _latest(
        (
            item
            for item in history.pair_facts
            if item.pair_id == pair_id and utc(item.received_at) <= decision_at
        ),
        lambda item: (utc(item.received_at), item.id),
    )
    visible_boosts = tuple(
        sorted(
            (item for item in history.boosts if utc(item.received_at) <= decision_at),
            key=lambda item: (utc(item.received_at), item.id),
        )
    )
    boost = visible_boosts[-1] if visible_boosts else None
    previous_boost = visible_boosts[-2] if len(visible_boosts) > 1 else None
    metadata = _latest_received(history.metadata, decision_at)
    security = _latest_received(history.security, decision_at)
    deep_security = tuple(
        item
        for item in history.selective_security
        if item.acquisition_mode == "historically_available"
        and utc(item.received_at) <= decision_at
    )

    def latest_security(family: str) -> SelectiveSecurityFact | None:
        return _latest(
            (item for item in deep_security if item.family == family),
            lambda item: (utc(item.received_at), item.id),
        )

    def security_events(family: str) -> tuple[SelectiveSecurityFact, ...]:
        return tuple(
            sorted(
                (item for item in deep_security if item.family == family),
                key=lambda item: (utc(item.received_at), item.id),
            )
        )

    context = _latest(
        (
            item
            for item in history.context
            if utc(item.received_at) <= decision_at and utc(item.bucket_end) <= decision_at
        ),
        lambda item: (utc(item.received_at), utc(item.bucket_end), item.id),
    )
    known_times: list[datetime] = [item.received_at for item in discoveries]
    known_times.extend(item.received_at for item in pair_history)
    if lifecycle is not None:
        known_times.append(lifecycle.decided_at)
    if candidate is not None:
        known_times.append(candidate.candidate_at)
    if candidate_tier is not None:
        known_times.append(candidate_tier.decided_at)
    holder_snapshot = latest_security("holder_snapshots")
    trader_distribution = latest_security("trader_distribution_snapshots")
    creator_history = latest_security("creator_history_snapshots")
    security_features = latest_security("security_feature_snapshots")
    liquidity_events = security_events("liquidity_event_evidence")
    wallet_edges = security_events("wallet_relationship_edges")
    wallet_clusters = security_events("wallet_cluster_snapshots")
    funding_relationships = security_events("funding_relationship_evidence")
    for item in (pair_fact, boost, metadata, security, context):
        if item is not None:
            known_times.append(item.received_at)
    known_times.extend(item.received_at for item in deep_security)
    identity_known = bool(
        discoveries or visible_observations or lifecycle or candidate or candidate_tier
    )
    return TokenStateAsOf(
        epoch_id=history.epoch_id,
        epoch_number=history.epoch_number,
        token_id=history.token_id,
        chain=history.chain,
        address=history.address,
        decision_at=decision_at,
        identity_known=identity_known,
        admission_at=admission_at,
        lifecycle=lifecycle,
        coverage=coverage,
        candidate=candidate,
        candidate_tier=candidate_tier,
        selected_pair_id=pair_id,
        current_observation=current,
        observation_history=pair_history,
        pair_fact=pair_fact,
        boost=boost,
        first_boost_received_at=(utc(visible_boosts[0].received_at) if visible_boosts else None),
        previous_boost=previous_boost,
        metadata=metadata,
        security=security,
        holder_snapshot=holder_snapshot,
        trader_distribution=trader_distribution,
        creator_history=creator_history,
        security_features=security_features,
        liquidity_events=liquidity_events,
        wallet_edges=wallet_edges,
        wallet_clusters=wallet_clusters,
        funding_relationships=funding_relationships,
        context=context,
        feature_set_identifier=contract.identifier,
        feature_set_sha256=contract.sha256,
        availability_watermark=max(known_times) if known_times else None,
    )


def _validate_history(history: TokenHistory) -> None:
    for observation in history.observations:
        utc(observation.received_at, "observation.received_at")
        if observation.source_observed_at is not None:
            utc(observation.source_observed_at, "observation.source_observed_at")
    for item in history.lifecycle:
        decided = utc(item.decided_at, "lifecycle.decided_at")
        watermark = utc(item.input_watermark, "lifecycle.input_watermark")
        if watermark > decided:
            raise ResearchContractError("lifecycle input watermark is after its decision")
    for context_item in history.context:
        received = utc(context_item.received_at, "context.received_at")
        bucket_end = utc(context_item.bucket_end, "context.bucket_end")
        if received < bucket_end:
            raise ResearchContractError("market context was received before its bucket closed")
    for candidate in history.candidates:
        if utc(candidate.input_watermark) > utc(candidate.candidate_at):
            raise ResearchContractError("candidate input watermark is after its decision")
    for tier in history.candidate_tiers:
        if utc(tier.input_watermark) > utc(tier.decided_at):
            raise ResearchContractError("candidate tier input watermark is after its decision")
    for fact in history.selective_security:
        utc(fact.received_at, f"{fact.family}.received_at")
        if fact.acquisition_mode not in {
            "historically_available",
            "retrospectively_reconstructed",
        }:
            raise ResearchContractError(
                f"unknown selective-security acquisition mode: {fact.acquisition_mode}"
            )


def _select_pair(observations: tuple[ObservationFact, ...]) -> str | None:
    latest: dict[str, ObservationFact] = {}
    for observation in observations:
        latest[observation.pair_id] = observation
    if not latest:
        return None

    def rank(item: ObservationFact) -> tuple[int, Decimal, datetime, str]:
        liquidity = item.liquidity_usd
        return (
            liquidity is not None,
            liquidity if liquidity is not None else Decimal("-1"),
            utc(item.received_at),
            item.pair_id,
        )

    return max(latest.values(), key=rank).pair_id


def _latest_lifecycle(
    items: tuple[LifecycleFact, ...], decision_at: datetime
) -> LifecycleFact | None:
    return _latest(
        (
            item
            for item in items
            if utc(item.decided_at) <= decision_at
            and utc(item.input_watermark) <= utc(item.decided_at)
        ),
        lambda item: (utc(item.decided_at), item.id),
    )


def _latest_received[T: BoostFact | MetadataFact | SecurityFact](
    items: tuple[T, ...], decision_at: datetime
) -> T | None:
    return _latest(
        (item for item in items if utc(item.received_at) <= decision_at),
        lambda item: (utc(item.received_at), item.id),
    )


def _latest[T](items: Iterable[T], key: Callable[[T], Any]) -> T | None:
    materialized = list(items)
    return max(materialized, key=key) if materialized else None
