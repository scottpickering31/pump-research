"""Concurrency-safe append-only persistence for selective security evidence."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from pump_research.candidates.repository import CandidateTaskClaim
from pump_research.persistence.models import (
    CandidateEnrichmentTask,
    CandidateEvent,
    CreatorHistorySnapshot,
    CreatorRelationshipEvent,
    FundingRelationshipEvidence,
    HolderBalanceFact,
    HolderSnapshot,
    LiquidityEventEvidence,
    Pair,
    SecurityEnrichmentPolicyRecord,
    SecurityFeatureSnapshot,
    SecurityProviderBudgetReservation,
    SecurityProviderRequest,
    Token,
    TokenSecurityTask,
    TraderDistributionSnapshot,
    WalletClusterSnapshot,
    WalletRelationshipEdge,
)
from pump_research.security_enrichment.analysis import (
    CLUSTER_ALGORITHM_VERSION,
    SECURITY_FEATURE_SET_NAME,
    SECURITY_FEATURE_SET_VERSION,
    HolderMetrics,
    SecurityFeatureValues,
    TraderMetrics,
    WalletClusterResult,
)
from pump_research.security_enrichment.contracts import (
    AcquisitionMode,
    CreatorHistoryFact,
    CreatorRelationshipFact,
    EvidenceEnvelope,
    FundingFact,
    HolderAccountFact,
    LiquidityEventFact,
    WalletEdgeFact,
)
from pump_research.security_enrichment.policy import (
    SECURITY_POLICY_NAME,
    SECURITY_POLICY_VERSION,
    SecurityEnrichmentPolicy,
)

_SCHEMA_VERSION = 1
_PROVIDER_BUDGET_LOCK = 7_428_901_260


class SecurityEvidenceIntegrityError(RuntimeError):
    """A deterministic identity mapped to different evidence content."""


@dataclass(frozen=True, slots=True)
class SecurityTaskContext:
    claim: CandidateTaskClaim
    candidate_id: uuid.UUID
    token_id: uuid.UUID
    token_address: str
    collection_epoch_id: uuid.UUID
    collector_run_id: uuid.UUID | None
    pair_id: uuid.UUID | None
    pair_address: str | None


class SecurityEnrichmentRepository:
    async def load_context(
        self, session: AsyncSession, claim: CandidateTaskClaim
    ) -> SecurityTaskContext:
        row = (
            await session.execute(
                select(CandidateEnrichmentTask, CandidateEvent, Token)
                .join(CandidateEvent, CandidateEvent.id == CandidateEnrichmentTask.candidate_id)
                .join(Token, Token.id == CandidateEnrichmentTask.token_id)
                .where(CandidateEnrichmentTask.id == claim.id)
            )
        ).one_or_none()
        if row is None:
            raise SecurityEvidenceIntegrityError("candidate task context does not exist")
        task, candidate, token = row
        pair = await session.scalar(
            select(Pair)
            .where(Pair.token_id == token.id)
            .order_by(Pair.first_discovered_at.asc().nullslast(), Pair.id)
            .limit(1)
        )
        return SecurityTaskContext(
            claim=claim,
            candidate_id=candidate.id,
            token_id=token.id,
            token_address=token.address,
            collection_epoch_id=task.collection_epoch_id,
            collector_run_id=task.collector_run_id,
            pair_id=pair.id if pair else None,
            pair_address=pair.address if pair else None,
        )

    async def ensure_policy(self, session: AsyncSession, policy: SecurityEnrichmentPolicy) -> None:
        await session.execute(
            insert(SecurityEnrichmentPolicyRecord)
            .values(
                policy_sha256=policy.sha256,
                policy_name=SECURITY_POLICY_NAME,
                policy_version=SECURITY_POLICY_VERSION,
                policy_snapshot=policy.snapshot,
            )
            .on_conflict_do_nothing(index_elements=[SecurityEnrichmentPolicyRecord.policy_sha256])
        )

    async def reserve_provider_request(
        self,
        session: AsyncSession,
        *,
        task_id: uuid.UUID,
        provider: str,
        budget_class: str,
        page_identity: str,
        now: datetime,
        global_limit: int,
        class_limit: int,
    ) -> bool:
        """Reserve before I/O; a crash safely consumes this minute's capacity."""
        now = _utc(now)
        minute = now.replace(second=0, microsecond=0)
        semantic = _digest(
            {
                "task": str(task_id),
                "provider": provider,
                "class": budget_class,
                "page": page_identity,
            }
        )
        existing = await session.scalar(
            select(SecurityProviderBudgetReservation.id).where(
                SecurityProviderBudgetReservation.semantic_key == semantic
            )
        )
        if existing is not None:
            return True
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {"lock_id": _PROVIDER_BUDGET_LOCK},
        )
        if provider == "solana_rpc" and budget_class == "HOLDER_SNAPSHOT":
            universal_due = await session.scalar(
                select(TokenSecurityTask.token_id)
                .where(
                    TokenSecurityTask.next_due_at.is_not(None),
                    TokenSecurityTask.next_due_at <= now + timedelta(minutes=1),
                )
                .limit(1)
            )
            if universal_due is not None:
                return False
        global_used = int(
            await session.scalar(
                select(func.count())
                .select_from(SecurityProviderBudgetReservation)
                .where(
                    SecurityProviderBudgetReservation.provider == provider,
                    SecurityProviderBudgetReservation.reserved_at >= minute,
                    SecurityProviderBudgetReservation.reserved_at < minute + timedelta(minutes=1),
                )
            )
            or 0
        )
        class_used = int(
            await session.scalar(
                select(func.count())
                .select_from(SecurityProviderBudgetReservation)
                .where(
                    SecurityProviderBudgetReservation.provider == provider,
                    SecurityProviderBudgetReservation.budget_class == budget_class,
                    SecurityProviderBudgetReservation.reserved_at >= minute,
                    SecurityProviderBudgetReservation.reserved_at < minute + timedelta(minutes=1),
                )
            )
            or 0
        )
        if global_used >= global_limit or class_used >= class_limit:
            return False
        await session.execute(
            insert(SecurityProviderBudgetReservation)
            .values(
                id=uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research:security-budget:{semantic}"),
                semantic_key=semantic,
                candidate_task_id=task_id,
                provider=provider,
                budget_class=budget_class,
                reserved_at=now,
            )
            .on_conflict_do_nothing()
        )
        return True

    async def record_provider_request(
        self,
        session: AsyncSession,
        *,
        context: SecurityTaskContext,
        method: str,
        requested_at: datetime,
        envelope: EvidenceEnvelope,
        request_payload: dict[str, object],
    ) -> SecurityProviderRequest:
        payload = _jsonable(envelope.raw_payload)
        payload_sha = _digest(payload) if payload is not None else None
        semantic = _digest(
            {
                "task": str(context.claim.id),
                "provider": envelope.provider,
                "method": method,
                "cursor": envelope.page_cursor,
                "received_at": envelope.received_at.isoformat(),
                "payload": payload_sha,
                "failure": envelope.failure_code,
            }
        )
        values: dict[str, object] = {
            "id": uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research:security-request:{semantic}"),
            "semantic_key": semantic,
            "candidate_task_id": context.claim.id,
            "candidate_id": context.candidate_id,
            "token_id": context.token_id,
            "collection_epoch_id": context.collection_epoch_id,
            "collector_run_id": context.collector_run_id,
            "provider": envelope.provider,
            "method": method,
            "provider_schema_version": envelope.provider_schema_version,
            "requested_at": _utc(requested_at),
            "received_at": envelope.received_at,
            "outcome": envelope.availability.value,
            "completeness": envelope.completeness.value,
            "acquisition_mode": envelope.acquisition_mode.value,
            "source_observed_at": envelope.source_observed_at,
            "source_slot": envelope.source_slot,
            "page_cursor": envelope.page_cursor,
            "next_cursor": envelope.next_cursor,
            "http_status_code": envelope.http_status_code,
            "request_payload": _jsonable(request_payload),
            "response_payload": payload,
            "response_payload_sha256": payload_sha,
            "failure_detail": (
                {"failure_code": envelope.failure_code}
                if envelope.failure_code is not None
                else None
            ),
            "schema_version": _SCHEMA_VERSION,
        }
        return await _insert_verified(
            session, SecurityProviderRequest, values, semantic_field="semantic_key"
        )

    async def record_holder_snapshot(
        self,
        session: AsyncSession,
        *,
        context: SecurityTaskContext,
        request: SecurityProviderRequest,
        envelope: EvidenceEnvelope,
        accounts: tuple[HolderAccountFact, ...],
        metrics: HolderMetrics,
        mint_supply_raw: Decimal | None,
        page_count: int,
        policy: SecurityEnrichmentPolicy,
    ) -> HolderSnapshot:
        await self.ensure_policy(session, policy)
        input_sha = _digest(
            {
                "request": str(request.id),
                "accounts": [_jsonable(asdict(item)) for item in accounts],
            }
        )
        semantic = _digest(
            {"task": str(context.claim.id), "input": input_sha, "policy": policy.sha256}
        )
        previous_count = await session.scalar(
            select(HolderSnapshot.holder_count)
            .where(
                HolderSnapshot.token_id == context.token_id,
                HolderSnapshot.received_at < envelope.received_at,
                HolderSnapshot.holder_count.is_not(None),
                HolderSnapshot.acquisition_mode == envelope.acquisition_mode.value,
            )
            .order_by(HolderSnapshot.received_at.desc(), HolderSnapshot.id.desc())
            .limit(1)
        )
        holder_growth = (
            metrics.holder_count - int(previous_count)
            if metrics.holder_count is not None and previous_count is not None
            else None
        )
        values: dict[str, object] = {
            "id": uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research:holder:{semantic}"),
            "semantic_key": semantic,
            "token_id": context.token_id,
            "candidate_id": context.candidate_id,
            "candidate_task_id": context.claim.id,
            "provider_request_id": request.id,
            "collection_epoch_id": context.collection_epoch_id,
            "source_observed_at": envelope.source_observed_at,
            "received_at": envelope.received_at,
            "availability": envelope.availability.value,
            "acquisition_mode": envelope.acquisition_mode.value,
            "source_slot": envelope.source_slot,
            "mint_supply_raw": mint_supply_raw,
            **{key: value for key, value in asdict(metrics).items() if key != "completeness"},
            "completeness": metrics.completeness.value,
            "holder_growth": holder_growth,
            "page_count": page_count,
            "truncated": envelope.next_cursor is not None,
            "input_sha256": input_sha,
            "policy_sha256": policy.sha256,
            "schema_version": _SCHEMA_VERSION,
        }
        snapshot = await _insert_verified(
            session, HolderSnapshot, values, semantic_field="semantic_key"
        )
        for rank, account in enumerate(
            sorted(accounts, key=lambda item: (-item.raw_balance, item.token_account)), start=1
        ):
            fact_semantic = _digest(
                {"snapshot": str(snapshot.id), "account": account.token_account}
            )
            balance_pct = (
                account.raw_balance * Decimal(100) / mint_supply_raw
                if mint_supply_raw is not None and mint_supply_raw > 0
                else None
            )
            await _insert_verified(
                session,
                HolderBalanceFact,
                {
                    "id": uuid.uuid5(
                        uuid.NAMESPACE_URL, f"pump-research:holder-fact:{fact_semantic}"
                    ),
                    "holder_snapshot_id": snapshot.id,
                    "rank": rank,
                    "token_account": account.token_account,
                    "owner_wallet": account.owner_wallet,
                    "raw_balance": account.raw_balance,
                    "balance_pct": balance_pct,
                    "is_known_pool": account.is_known_pool,
                    "is_creator": account.is_creator,
                    "exclusion_reason": account.exclusion_reason,
                    "source_fact_identity": f"{request.id}:{account.token_account}",
                },
                identity_field="id",
            )
        return snapshot

    async def record_trader_snapshot(
        self,
        session: AsyncSession,
        *,
        context: SecurityTaskContext,
        request: SecurityProviderRequest,
        envelope: EvidenceEnvelope,
        window_start: datetime,
        window_end: datetime,
        metrics: TraderMetrics,
        source_fact_ids: tuple[str, ...],
        page_count: int,
        policy: SecurityEnrichmentPolicy,
    ) -> TraderDistributionSnapshot:
        await self.ensure_policy(session, policy)
        input_sha = _digest(sorted(source_fact_ids))
        semantic = _digest(
            {
                "task": str(context.claim.id),
                "window": [_utc(window_start).isoformat(), _utc(window_end).isoformat()],
                "input": input_sha,
            }
        )
        values: dict[str, object] = {
            "id": uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research:traders:{semantic}"),
            "semantic_key": semantic,
            "token_id": context.token_id,
            "candidate_id": context.candidate_id,
            "candidate_task_id": context.claim.id,
            "provider_request_id": request.id,
            "collection_epoch_id": context.collection_epoch_id,
            "window_start": _utc(window_start),
            "window_end": _utc(window_end),
            "source_observed_at": envelope.source_observed_at,
            "received_at": envelope.received_at,
            "availability": envelope.availability.value,
            "completeness": envelope.completeness.value,
            "acquisition_mode": envelope.acquisition_mode.value,
            **asdict(metrics),
            "new_wallet_ratio": None,
            "page_count": page_count,
            "source_fact_ids": list(source_fact_ids),
            "input_sha256": input_sha,
            "policy_sha256": policy.sha256,
            "schema_version": _SCHEMA_VERSION,
        }
        return await _insert_verified(
            session, TraderDistributionSnapshot, values, semantic_field="semantic_key"
        )

    async def record_creator(
        self,
        session: AsyncSession,
        *,
        context: SecurityTaskContext,
        request: SecurityProviderRequest,
        envelope: EvidenceEnvelope,
        relationships: tuple[CreatorRelationshipFact, ...],
        history: CreatorHistoryFact | None,
        policy: SecurityEnrichmentPolicy,
    ) -> CreatorHistorySnapshot | None:
        await self.ensure_policy(session, policy)
        for relationship in relationships:
            semantic = _digest(
                {
                    "token": str(context.token_id),
                    "wallet": relationship.creator_wallet,
                    "type": relationship.relationship_type,
                    "source": relationship.source_fact_identity,
                    "received": envelope.received_at.isoformat(),
                }
            )
            await _insert_verified(
                session,
                CreatorRelationshipEvent,
                {
                    "id": uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research:creator-link:{semantic}"),
                    "semantic_key": semantic,
                    "token_id": context.token_id,
                    "candidate_id": context.candidate_id,
                    "provider_request_id": request.id,
                    "creator_wallet": relationship.creator_wallet,
                    "relationship_type": relationship.relationship_type,
                    "source_event_at": relationship.first_linked_at,
                    "received_at": envelope.received_at,
                    "acquisition_mode": envelope.acquisition_mode.value,
                    "source_fact_identity": relationship.source_fact_identity,
                    "schema_version": _SCHEMA_VERSION,
                },
                semantic_field="semantic_key",
            )
        if history is None:
            return None
        input_sha = _digest(_jsonable(asdict(history)))
        semantic = _digest(
            {"task": str(context.claim.id), "input": input_sha, "policy": policy.sha256}
        )
        return await _insert_verified(
            session,
            CreatorHistorySnapshot,
            {
                "id": uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research:creator:{semantic}"),
                "semantic_key": semantic,
                "token_id": context.token_id,
                "candidate_id": context.candidate_id,
                "candidate_task_id": context.claim.id,
                "provider_request_id": request.id,
                "collection_epoch_id": context.collection_epoch_id,
                "creator_wallet": history.creator_wallet,
                "as_of": _utc(history.as_of),
                "received_at": envelope.received_at,
                "availability": envelope.availability.value,
                "acquisition_mode": envelope.acquisition_mode.value,
                "prior_token_count": history.prior_token_count,
                "prior_tracked_launches": history.prior_tracked_launches,
                "prior_collapse_count": history.prior_collapse_count,
                "prior_large_winner_count": history.prior_large_winner_count,
                "median_survival_seconds": history.median_survival_seconds,
                "mean_survival_seconds": history.mean_survival_seconds,
                "launches_last_30d": history.launches_last_30d,
                "creator_hold_pct": history.creator_hold_pct,
                "source_token_ids": list(history.source_token_ids),
                "input_sha256": input_sha,
                "policy_sha256": policy.sha256,
                "schema_version": _SCHEMA_VERSION,
            },
            semantic_field="semantic_key",
        )

    async def record_liquidity_events(
        self,
        session: AsyncSession,
        *,
        context: SecurityTaskContext,
        request: SecurityProviderRequest,
        envelope: EvidenceEnvelope,
        events: tuple[LiquidityEventFact, ...],
    ) -> tuple[LiquidityEventEvidence, ...]:
        output: list[LiquidityEventEvidence] = []
        for event in events:
            semantic = _digest(
                {
                    "token": str(context.token_id),
                    "pair": event.pair_address,
                    "type": event.event_type.value,
                    "signature": event.signature,
                    "source_at": event.source_event_at.isoformat()
                    if event.source_event_at
                    else None,
                    "request": str(request.id),
                }
            )
            output.append(
                await _insert_verified(
                    session,
                    LiquidityEventEvidence,
                    {
                        "id": uuid.uuid5(
                            uuid.NAMESPACE_URL,
                            f"pump-research:liquidity-event:{semantic}",
                        ),
                        "semantic_key": semantic,
                        "token_id": context.token_id,
                        "pair_id": context.pair_id,
                        "candidate_id": context.candidate_id,
                        "provider_request_id": request.id,
                        "event_type": event.event_type.value,
                        "source_event_at": event.source_event_at,
                        "received_at": envelope.received_at,
                        "availability": envelope.availability.value,
                        "acquisition_mode": envelope.acquisition_mode.value,
                        "source_slot": envelope.source_slot,
                        "signature": event.signature,
                        "base_delta": event.base_delta,
                        "quote_delta": event.quote_delta,
                        "liquidity_usd_before": event.liquidity_usd_before,
                        "liquidity_usd_after": event.liquidity_usd_after,
                        "removal_pct": event.removal_pct,
                        "lp_wallet": event.lp_wallet,
                        "source_fact_ids": [str(request.id)],
                        "decoder_version": "provider-neutral-liquidity-v1",
                        "schema_version": _SCHEMA_VERSION,
                    },
                    semantic_field="semantic_key",
                )
            )
        return tuple(output)

    async def record_wallet_edges(
        self,
        session: AsyncSession,
        *,
        context: SecurityTaskContext,
        request: SecurityProviderRequest,
        envelope: EvidenceEnvelope,
        edges: tuple[WalletEdgeFact, ...],
    ) -> tuple[WalletRelationshipEdge, ...]:
        output: list[WalletRelationshipEdge] = []
        for edge in edges:
            wallet_a, wallet_b = sorted((edge.wallet_a, edge.wallet_b))
            semantic = _digest(
                {
                    "candidate": str(context.candidate_id),
                    "a": wallet_a,
                    "b": wallet_b,
                    "type": edge.relationship_type.value,
                    "facts": sorted(edge.source_fact_ids),
                }
            )
            output.append(
                await _insert_verified(
                    session,
                    WalletRelationshipEdge,
                    {
                        "id": uuid.uuid5(
                            uuid.NAMESPACE_URL, f"pump-research:wallet-edge:{semantic}"
                        ),
                        "semantic_key": semantic,
                        "token_id": context.token_id,
                        "candidate_id": context.candidate_id,
                        "provider_request_id": request.id,
                        "wallet_a": wallet_a,
                        "wallet_b": wallet_b,
                        "relationship_type": edge.relationship_type.value,
                        "first_observed_at": edge.first_observed_at,
                        "evidence_received_at": edge.evidence_received_at,
                        "strength_count": edge.strength_count,
                        "acquisition_mode": envelope.acquisition_mode.value,
                        "source_fact_ids": list(edge.source_fact_ids),
                        "method_version": "bounded-wallet-evidence-v1",
                        "schema_version": _SCHEMA_VERSION,
                    },
                    semantic_field="semantic_key",
                )
            )
        return tuple(output)

    async def record_funding(
        self,
        session: AsyncSession,
        *,
        context: SecurityTaskContext,
        request: SecurityProviderRequest,
        envelope: EvidenceEnvelope,
        relationships: tuple[FundingFact, ...],
    ) -> tuple[FundingRelationshipEvidence, ...]:
        output: list[FundingRelationshipEvidence] = []
        for item in relationships:
            semantic = _digest(
                {
                    "candidate": str(context.candidate_id),
                    "wallet": item.wallet,
                    "source": item.funding_source,
                    "signature": item.source_signature,
                    "hop": item.hop_depth,
                }
            )
            output.append(
                await _insert_verified(
                    session,
                    FundingRelationshipEvidence,
                    {
                        "id": uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research:funding:{semantic}"),
                        "semantic_key": semantic,
                        "token_id": context.token_id,
                        "candidate_id": context.candidate_id,
                        "provider_request_id": request.id,
                        "wallet": item.wallet,
                        "funding_source": item.funding_source,
                        "funding_at": item.funding_at,
                        "received_at": item.received_at,
                        "amount_lamports": item.amount_lamports,
                        "hop_depth": item.hop_depth,
                        "source_signature": item.source_signature,
                        "completeness": item.completeness.value,
                        "acquisition_mode": envelope.acquisition_mode.value,
                        "schema_version": _SCHEMA_VERSION,
                    },
                    semantic_field="semantic_key",
                )
            )
        return tuple(output)

    async def record_clusters(
        self,
        session: AsyncSession,
        *,
        context: SecurityTaskContext,
        received_at: datetime,
        acquisition_mode: AcquisitionMode,
        clusters: tuple[WalletClusterResult, ...],
    ) -> tuple[WalletClusterSnapshot, ...]:
        output: list[WalletClusterSnapshot] = []
        for cluster in clusters:
            semantic = _digest(
                {
                    "candidate": str(context.candidate_id),
                    "cluster": cluster.cluster_id,
                    "received": _utc(received_at).isoformat(),
                }
            )
            output.append(
                await _insert_verified(
                    session,
                    WalletClusterSnapshot,
                    {
                        "id": uuid.uuid5(
                            uuid.NAMESPACE_URL, f"pump-research:wallet-cluster:{semantic}"
                        ),
                        "semantic_key": semantic,
                        "cluster_id": cluster.cluster_id,
                        "token_id": context.token_id,
                        "candidate_id": context.candidate_id,
                        "generated_at": _utc(received_at),
                        "received_at": _utc(received_at),
                        "acquisition_mode": acquisition_mode.value,
                        "algorithm_version": CLUSTER_ALGORITHM_VERSION,
                        "input_edge_sha256": cluster.input_edge_sha256,
                        "members": list(cluster.members),
                        "explanation": list(cluster.explanation),
                        "schema_version": _SCHEMA_VERSION,
                    },
                    semantic_field="semantic_key",
                )
            )
        return tuple(output)

    async def record_security_features(
        self,
        session: AsyncSession,
        *,
        context: SecurityTaskContext,
        received_at: datetime,
        acquisition_mode: AcquisitionMode,
        features: SecurityFeatureValues,
        input_fact_ids: tuple[str, ...],
        policy: SecurityEnrichmentPolicy,
    ) -> SecurityFeatureSnapshot:
        await self.ensure_policy(session, policy)
        semantic = _digest(
            {
                "candidate": str(context.candidate_id),
                "received": _utc(received_at).isoformat(),
                "input": features.input_sha256,
                "schema": features.schema_sha256,
                "policy": policy.sha256,
            }
        )
        return await _insert_verified(
            session,
            SecurityFeatureSnapshot,
            {
                "id": uuid.uuid5(uuid.NAMESPACE_URL, f"pump-research:security-features:{semantic}"),
                "semantic_key": semantic,
                "token_id": context.token_id,
                "candidate_id": context.candidate_id,
                "collection_epoch_id": context.collection_epoch_id,
                "generated_at": _utc(received_at),
                "received_at": _utc(received_at),
                "acquisition_mode": acquisition_mode.value,
                "feature_set_name": SECURITY_FEATURE_SET_NAME,
                "feature_set_version": SECURITY_FEATURE_SET_VERSION,
                "values": _jsonable(features.values),
                "input_fact_ids": list(input_fact_ids),
                "input_sha256": features.input_sha256,
                "schema_sha256": features.schema_sha256,
                "policy_sha256": policy.sha256,
                "schema_version": _SCHEMA_VERSION,
            },
            semantic_field="semantic_key",
        )


async def _insert_verified[T](
    session: AsyncSession,
    model: type[T],
    values: dict[str, object],
    *,
    semantic_field: str | None = None,
    identity_field: str | None = None,
) -> T:
    await session.execute(insert(model).values(**values).on_conflict_do_nothing())
    lookup_field = semantic_field or identity_field
    if lookup_field is None:
        raise ValueError("verified insert requires an identity field")
    row = await session.scalar(
        select(model).where(getattr(model, lookup_field) == values[lookup_field])
    )
    if row is None:
        raise SecurityEvidenceIntegrityError("security evidence insert/readback failed")
    for field, expected in values.items():
        if getattr(row, field) != expected:
            raise SecurityEvidenceIntegrityError(
                f"security evidence identity maps to different {field}"
            )
    return row


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(_jsonable(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _jsonable(value: object) -> object:
    if isinstance(value, datetime):
        return _utc(value).isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    if hasattr(value, "value"):
        return _jsonable(value.value)
    return value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
