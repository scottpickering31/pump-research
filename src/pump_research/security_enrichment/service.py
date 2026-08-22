"""Budgeted Phase 6 task executor with fail-closed pagination and restart safety."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.candidates.repository import CandidateTaskClaim
from pump_research.candidates.service import CandidateOrchestrationService
from pump_research.persistence.models import (
    CreatorHistorySnapshot,
    FundingRelationshipEvidence,
    HolderSnapshot,
    LiquidityEventEvidence,
    SecurityFeatureSnapshot,
    TokenSecuritySnapshot,
    TraderDistributionSnapshot,
    WalletClusterSnapshot,
    WalletRelationshipEdge,
)
from pump_research.security_enrichment.analysis import (
    HolderMetrics,
    TraderMetrics,
    build_holder_metrics,
    build_security_features,
    build_trader_metrics,
    cluster_wallet_edges,
)
from pump_research.security_enrichment.contracts import (
    AcquisitionMode,
    EvidenceAvailability,
    EvidenceCompleteness,
    EvidenceEnvelope,
    ProviderPageRequest,
    TradeFact,
    WalletRelationshipType,
)
from pump_research.security_enrichment.policy import SecurityEnrichmentPolicy
from pump_research.security_enrichment.provider import (
    SecurityEnrichmentProvider,
    SecurityProviderError,
    failed_envelope,
)
from pump_research.security_enrichment.repository import (
    SecurityEnrichmentRepository,
    SecurityTaskContext,
)


class SecurityEnrichmentWorker:
    """Execute only already-leased candidate tasks; never creates per-token tasks."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        candidate_service: CandidateOrchestrationService,
        provider: SecurityEnrichmentProvider,
        policy: SecurityEnrichmentPolicy,
    ) -> None:
        self._sessions = session_factory
        self._candidates = candidate_service
        self._provider = provider
        self.policy = policy
        self._repository = SecurityEnrichmentRepository()

    async def run_once(
        self,
        *,
        now: datetime,
        worker_id: str,
        collector_run_id: uuid.UUID | None,
        limit: int = 4,
    ) -> tuple[CandidateTaskClaim, ...]:
        claims = await self._candidates.claim_tasks(
            now=now,
            worker_id=worker_id,
            collector_run_id=collector_run_id,
            limit=limit,
            analysis_types=(
                "HOLDER_SNAPSHOT",
                "TRADER_DISTRIBUTION",
                "CREATOR_HISTORY",
                "LIQUIDITY_EVENT_ANALYSIS",
                "WALLET_CLUSTER_ANALYSIS",
                "FUNDING_GRAPH_ANALYSIS",
            ),
        )
        for claim in claims:
            await self.process_claim(claim, now=now)
        return claims

    async def process_claim(self, claim: CandidateTaskClaim, *, now: datetime) -> None:
        now = _utc(now)
        async with self._sessions() as session:
            context = await self._repository.load_context(session, claim)
        admitted = await self._reserve(
            context,
            page_identity=f"attempt:{claim.attempt_number}:page:0",
            now=now,
        )
        if not admitted:
            await self._candidates.defer_task(
                claim,
                deferred_at=now,
                not_before=now + timedelta(minutes=1),
                reason={
                    "code": "phase6_provider_budget_or_universal_precedence_deferred",
                    "provider": self._provider.name,
                    "analysis_type": claim.analysis_type,
                },
            )
            return
        try:
            result_identity, result_sha, received_at, outcome = await self._dispatch(
                context, now=now
            )
        except (SecurityProviderError, TypeError, ValueError) as error:
            envelope = failed_envelope(
                provider=self._provider.name,
                received_at=datetime.now(UTC),
                failure_code=type(error).__name__,
            )
            async with self._sessions() as session, session.begin():
                await self._repository.record_provider_request(
                    session,
                    context=context,
                    method=claim.analysis_type,
                    requested_at=now,
                    envelope=envelope,
                    request_payload={"task": str(claim.id)},
                )
            await self._candidates.fail_task(
                claim,
                failed_at=envelope.received_at,
                failure_detail={
                    "code": "security_provider_failure",
                    "provider": self._provider.name,
                    "error_type": type(error).__name__,
                },
                retry_delay=timedelta(minutes=1),
            )
            return
        await self._candidates.complete_task(
            claim,
            completed_at=received_at,
            outcome=outcome,
            evidence_generated_at=received_at,
            evidence_received_at=received_at,
            fresh_until=received_at + self.policy.ttl_for(claim.analysis_type),
            result_identity=result_identity,
            result_sha256=result_sha,
        )
        await self._candidates.evaluate_security_token(
            collection_epoch_id=context.collection_epoch_id,
            collector_run_id=context.collector_run_id,
            token_id=context.token_id,
            evaluated_at=received_at,
        )

    async def _reserve(
        self, context: SecurityTaskContext, *, page_identity: str, now: datetime
    ) -> bool:
        async with self._sessions() as session, session.begin():
            return await self._repository.reserve_provider_request(
                session,
                task_id=context.claim.id,
                provider=self._provider.name,
                budget_class=context.claim.analysis_type,
                page_identity=page_identity,
                now=now,
                global_limit=self.policy.provider_requests_per_minute,
                class_limit=self.policy.request_limit_for(context.claim.analysis_type),
            )

    async def _dispatch(
        self, context: SecurityTaskContext, *, now: datetime
    ) -> tuple[str, str, datetime, str]:
        request = ProviderPageRequest(
            token_address=context.token_address,
            candidate_id=str(context.candidate_id),
            input_watermark=context.claim.input_watermark,
            cursor=None,
            limit=self.policy.page_size,
            window_start=context.claim.input_watermark - timedelta(hours=1),
            window_end=context.claim.input_watermark,
            maximum_hops=self.policy.max_funding_hops,
        )
        analysis = context.claim.analysis_type
        if analysis == "HOLDER_SNAPSHOT":
            return await self._holder(context, request, now=now)
        if analysis == "TRADER_DISTRIBUTION":
            return await self._traders(context, request, now=now)
        if analysis == "CREATOR_HISTORY":
            return await self._creator(context, request, now=now)
        if analysis == "LIQUIDITY_EVENT_ANALYSIS":
            return await self._liquidity(context, request, now=now)
        if analysis == "WALLET_CLUSTER_ANALYSIS":
            return await self._wallet_edges(context, request, now=now)
        if analysis == "FUNDING_GRAPH_ANALYSIS":
            return await self._funding(context, request, now=now)
        raise SecurityProviderError(f"unsupported Phase 6 analysis type: {analysis}")

    async def _holder(
        self, context: SecurityTaskContext, request: ProviderPageRequest, *, now: datetime
    ) -> tuple[str, str, datetime, str]:
        # Phase 2 supply is already available and avoids a third holder RPC call.
        async with self._sessions() as session:
            supply = await session.scalar(
                select(TokenSecuritySnapshot.raw_supply)
                .where(
                    TokenSecuritySnapshot.token_id == context.token_id,
                    TokenSecuritySnapshot.received_at <= context.claim.input_watermark,
                )
                .order_by(
                    TokenSecuritySnapshot.received_at.desc(),
                    TokenSecuritySnapshot.id.desc(),
                )
                .limit(1)
            )
        request = replace(request, mint_supply_raw=supply)
        page = await self._provider.fetch_holders(request)
        metrics = build_holder_metrics(
            page.accounts,
            mint_supply_raw=page.mint_supply_raw,
            holder_count=page.holder_count,
            completeness=page.envelope.completeness,
        )
        async with self._sessions() as session, session.begin():
            provider_request = await self._repository.record_provider_request(
                session,
                context=context,
                method="HOLDER_SNAPSHOT",
                requested_at=now,
                envelope=page.envelope,
                request_payload={"token": context.token_address, "cursor": None},
            )
            snapshot = None
            if page.envelope.availability in {
                EvidenceAvailability.AVAILABLE,
                EvidenceAvailability.PARTIAL,
            }:
                snapshot = await self._repository.record_holder_snapshot(
                    session,
                    context=context,
                    request=provider_request,
                    envelope=page.envelope,
                    accounts=page.accounts,
                    metrics=metrics,
                    mint_supply_raw=page.mint_supply_raw,
                    page_count=1,
                    policy=self.policy,
                )
            feature = await self._refresh_features(
                session, context=context, received_at=page.envelope.received_at
            )
        return _result(snapshot.id if snapshot else provider_request.id, feature.id, page.envelope)

    async def _traders(
        self, context: SecurityTaskContext, request: ProviderPageRequest, *, now: datetime
    ) -> tuple[str, str, datetime, str]:
        trades: list[TradeFact] = []
        pages = 0
        last_request = None
        last_envelope = None
        cursor: str | None = None
        while pages < self.policy.max_pages_per_task:
            page_request = replace(request, cursor=cursor)
            page = await self._provider.fetch_traders(page_request)
            pages += 1
            by_signature = {item.signature: item for item in trades}
            for item in page.trades:
                previous = by_signature.get(item.signature)
                if previous is not None and previous != item:
                    raise SecurityProviderError(
                        "duplicate transaction signature maps to different parsed facts"
                    )
                if previous is None:
                    trades.append(item)
                    by_signature[item.signature] = item
            async with self._sessions() as session, session.begin():
                last_request = await self._repository.record_provider_request(
                    session,
                    context=context,
                    method="TRADER_DISTRIBUTION",
                    requested_at=now,
                    envelope=page.envelope,
                    request_payload={"token": context.token_address, "cursor": cursor},
                )
            last_envelope = page.envelope
            cursor = page.envelope.next_cursor
            if cursor is None:
                break
            if pages < self.policy.max_pages_per_task and not await self._reserve(
                context,
                page_identity=f"attempt:{context.claim.attempt_number}:page:{pages}",
                now=now,
            ):
                last_envelope = _partial(last_envelope, "provider_budget_mid_pagination")
                break
        assert last_request is not None and last_envelope is not None
        if cursor is not None:
            last_envelope = _partial(last_envelope, "maximum_pages_or_budget")
        metrics = build_trader_metrics(tuple(trades))
        source_ids = tuple(sorted({item.signature for item in trades}))
        async with self._sessions() as session, session.begin():
            snapshot = None
            if last_envelope.availability in {
                EvidenceAvailability.AVAILABLE,
                EvidenceAvailability.PARTIAL,
            }:
                snapshot = await self._repository.record_trader_snapshot(
                    session,
                    context=context,
                    request=last_request,
                    envelope=last_envelope,
                    window_start=request.window_start or context.claim.input_watermark,
                    window_end=request.window_end or context.claim.input_watermark,
                    metrics=metrics,
                    source_fact_ids=source_ids,
                    page_count=pages,
                    policy=self.policy,
                )
            feature = await self._refresh_features(
                session, context=context, received_at=last_envelope.received_at
            )
        return _result(snapshot.id if snapshot else last_request.id, feature.id, last_envelope)

    async def _creator(
        self, context: SecurityTaskContext, request: ProviderPageRequest, *, now: datetime
    ) -> tuple[str, str, datetime, str]:
        page = await self._provider.fetch_creator(request)
        async with self._sessions() as session, session.begin():
            provider_request = await self._repository.record_provider_request(
                session,
                context=context,
                method="CREATOR_HISTORY",
                requested_at=now,
                envelope=page.envelope,
                request_payload={"token": context.token_address},
            )
            snapshot = await self._repository.record_creator(
                session,
                context=context,
                request=provider_request,
                envelope=page.envelope,
                relationships=page.relationships,
                history=page.history,
                policy=self.policy,
            )
            feature = await self._refresh_features(
                session, context=context, received_at=page.envelope.received_at
            )
        identity = snapshot.id if snapshot is not None else provider_request.id
        return _result(identity, feature.id, page.envelope)

    async def _liquidity(
        self, context: SecurityTaskContext, request: ProviderPageRequest, *, now: datetime
    ) -> tuple[str, str, datetime, str]:
        page = await self._provider.fetch_liquidity(request)
        async with self._sessions() as session, session.begin():
            provider_request = await self._repository.record_provider_request(
                session,
                context=context,
                method="LIQUIDITY_EVENT_ANALYSIS",
                requested_at=now,
                envelope=page.envelope,
                request_payload={"pair": context.pair_address},
            )
            events = await self._repository.record_liquidity_events(
                session,
                context=context,
                request=provider_request,
                envelope=page.envelope,
                events=page.events,
            )
            feature = await self._refresh_features(
                session, context=context, received_at=page.envelope.received_at
            )
        identity = events[-1].id if events else provider_request.id
        return _result(identity, feature.id, page.envelope)

    async def _wallet_edges(
        self, context: SecurityTaskContext, request: ProviderPageRequest, *, now: datetime
    ) -> tuple[str, str, datetime, str]:
        page = await self._provider.fetch_wallet_edges(request)
        bounded = tuple(page.edges[: self.policy.max_edges_per_candidate])
        wallet_count = len(
            {wallet for edge in bounded for wallet in (edge.wallet_a, edge.wallet_b)}
        )
        if wallet_count > self.policy.max_wallets_per_candidate:
            allowed = set(
                sorted({wallet for edge in bounded for wallet in (edge.wallet_a, edge.wallet_b)})[
                    : self.policy.max_wallets_per_candidate
                ]
            )
            bounded = tuple(
                edge for edge in bounded if edge.wallet_a in allowed and edge.wallet_b in allowed
            )
            page = type(page)(_partial(page.envelope, "wallet_or_edge_bound"), bounded)
        async with self._sessions() as session, session.begin():
            provider_request = await self._repository.record_provider_request(
                session,
                context=context,
                method="WALLET_CLUSTER_ANALYSIS",
                requested_at=now,
                envelope=page.envelope,
                request_payload={"wallet_limit": self.policy.max_wallets_per_candidate},
            )
            await self._repository.record_wallet_edges(
                session,
                context=context,
                request=provider_request,
                envelope=page.envelope,
                edges=bounded,
            )
            clusters = await self._repository.record_clusters(
                session,
                context=context,
                received_at=page.envelope.received_at,
                acquisition_mode=page.envelope.acquisition_mode,
                clusters=cluster_wallet_edges(bounded),
            )
            feature = await self._refresh_features(
                session, context=context, received_at=page.envelope.received_at
            )
        identity = clusters[-1].id if clusters else provider_request.id
        return _result(identity, feature.id, page.envelope)

    async def _funding(
        self, context: SecurityTaskContext, request: ProviderPageRequest, *, now: datetime
    ) -> tuple[str, str, datetime, str]:
        page = await self._provider.fetch_funding(request)
        bounded = tuple(
            item
            for item in page.relationships[: self.policy.max_wallets_per_candidate * 2]
            if item.hop_depth <= self.policy.max_funding_hops
        )
        async with self._sessions() as session, session.begin():
            provider_request = await self._repository.record_provider_request(
                session,
                context=context,
                method="FUNDING_GRAPH_ANALYSIS",
                requested_at=now,
                envelope=page.envelope,
                request_payload={"maximum_hops": self.policy.max_funding_hops},
            )
            relationships = await self._repository.record_funding(
                session,
                context=context,
                request=provider_request,
                envelope=page.envelope,
                relationships=bounded,
            )
            feature = await self._refresh_features(
                session, context=context, received_at=page.envelope.received_at
            )
        identity = relationships[-1].id if relationships else provider_request.id
        return _result(identity, feature.id, page.envelope)

    async def _refresh_features(
        self,
        session: AsyncSession,
        *,
        context: SecurityTaskContext,
        received_at: datetime,
    ) -> SecurityFeatureSnapshot:
        historical = "historically_available"
        holder = await session.scalar(
            select(HolderSnapshot)
            .where(
                HolderSnapshot.token_id == context.token_id,
                HolderSnapshot.received_at <= received_at,
                HolderSnapshot.acquisition_mode == historical,
            )
            .order_by(HolderSnapshot.received_at.desc(), HolderSnapshot.id.desc())
            .limit(1)
        )
        trader = await session.scalar(
            select(TraderDistributionSnapshot)
            .where(
                TraderDistributionSnapshot.token_id == context.token_id,
                TraderDistributionSnapshot.received_at <= received_at,
                TraderDistributionSnapshot.acquisition_mode == historical,
            )
            .order_by(
                TraderDistributionSnapshot.received_at.desc(),
                TraderDistributionSnapshot.id.desc(),
            )
            .limit(1)
        )
        creator = await session.scalar(
            select(CreatorHistorySnapshot)
            .where(
                CreatorHistorySnapshot.token_id == context.token_id,
                CreatorHistorySnapshot.received_at <= received_at,
                CreatorHistorySnapshot.acquisition_mode == historical,
            )
            .order_by(
                CreatorHistorySnapshot.received_at.desc(),
                CreatorHistorySnapshot.id.desc(),
            )
            .limit(1)
        )
        recent_cutoff = received_at - timedelta(minutes=15)
        liquidity_events = list(
            (
                await session.execute(
                    select(LiquidityEventEvidence).where(
                        LiquidityEventEvidence.candidate_id == context.candidate_id,
                        LiquidityEventEvidence.received_at >= recent_cutoff,
                        LiquidityEventEvidence.received_at <= received_at,
                        LiquidityEventEvidence.acquisition_mode == historical,
                    )
                )
            ).scalars()
        )
        removal_values = [
            item.removal_pct for item in liquidity_events if item.removal_pct is not None
        ]
        removal = max(removal_values) if removal_values else None
        clusters = list(
            (
                await session.execute(
                    select(WalletClusterSnapshot).where(
                        WalletClusterSnapshot.candidate_id == context.candidate_id,
                        WalletClusterSnapshot.received_at <= received_at,
                        WalletClusterSnapshot.acquisition_mode == historical,
                    )
                )
            ).scalars()
        )
        funding = list(
            (
                await session.execute(
                    select(FundingRelationshipEvidence).where(
                        FundingRelationshipEvidence.candidate_id == context.candidate_id,
                        FundingRelationshipEvidence.received_at <= received_at,
                        FundingRelationshipEvidence.acquisition_mode == historical,
                    )
                )
            ).scalars()
        )
        synchronous_edges = list(
            (
                await session.execute(
                    select(WalletRelationshipEdge).where(
                        WalletRelationshipEdge.candidate_id == context.candidate_id,
                        WalletRelationshipEdge.evidence_received_at >= recent_cutoff,
                        WalletRelationshipEdge.evidence_received_at <= received_at,
                        WalletRelationshipEdge.acquisition_mode == historical,
                        WalletRelationshipEdge.relationship_type
                        == WalletRelationshipType.CO_TRADE_TIMING.value,
                    )
                )
            ).scalars()
        )
        synchronized = sum(item.strength_count for item in synchronous_edges)
        security = await session.scalar(
            select(TokenSecuritySnapshot)
            .where(
                TokenSecuritySnapshot.token_id == context.token_id,
                TokenSecuritySnapshot.received_at <= received_at,
            )
            .order_by(TokenSecuritySnapshot.received_at.desc(), TokenSecuritySnapshot.id.desc())
            .limit(1)
        )
        holder_metrics = _holder_metrics(holder)
        trader_metrics = _trader_metrics(trader)
        prior_rate = None
        if creator and creator.prior_tracked_launches:
            prior_rate = (
                Decimal(creator.prior_collapse_count or 0)
                * Decimal(100)
                / Decimal(creator.prior_tracked_launches)
            )
        sources: dict[str, int] = {}
        for item in funding:
            sources[item.funding_source] = sources.get(item.funding_source, 0) + 1
        common_funder = (
            Decimal(max(sources.values())) * Decimal(100) / Decimal(len(funding))
            if funding and sources
            else None
        )
        input_ids = tuple(
            sorted(
                str(item)
                for item in (
                    holder.id if holder else None,
                    trader.id if trader else None,
                    creator.id if creator else None,
                    security.id if security else None,
                    *(event.id for event in liquidity_events),
                    *(cluster.id for cluster in clusters),
                    *(item.id for item in funding),
                    *(edge.id for edge in synchronous_edges),
                )
                if item is not None
            )
        )
        features = build_security_features(
            generated_at=received_at,
            holder=holder_metrics,
            trader=trader_metrics,
            creator_hold_pct=creator.creator_hold_pct if creator else None,
            creator_prior_collapse_rate=prior_rate,
            wallet_cluster_count=len(clusters),
            largest_cluster_trade_share=None,
            common_funder_share=common_funder,
            synchronized_trade_score=Decimal(synchronized) if synchronized else None,
            liquidity_removal_recent_pct=removal,
            liquidity_change_velocity=None,
            creator_transfer_activity=None,
            security_snapshot_age_seconds=(
                Decimal((received_at - security.received_at).total_seconds()) if security else None
            ),
            input_fact_ids=input_ids,
        )
        return await self._repository.record_security_features(
            session,
            context=context,
            received_at=received_at,
            acquisition_mode=AcquisitionMode.HISTORICALLY_AVAILABLE,
            features=features,
            input_fact_ids=input_ids,
            policy=self.policy,
        )


def _holder_metrics(value: HolderSnapshot | None) -> HolderMetrics | None:
    if value is None or value.availability not in {"available", "partial"}:
        return None
    return HolderMetrics(
        holder_count=value.holder_count,
        top_1_pct=value.top_1_pct,
        top_5_pct=value.top_5_pct,
        top_10_pct=value.top_10_pct,
        top_20_pct=value.top_20_pct,
        largest_holder_pct=value.largest_holder_pct,
        largest_non_pool_holder_pct=value.largest_non_pool_holder_pct,
        creator_holder_pct=value.creator_holder_pct,
        hhi=value.hhi,
        covered_supply_pct=value.covered_supply_pct,
        completeness=EvidenceCompleteness(value.completeness),
    )


def _trader_metrics(value: TraderDistributionSnapshot | None) -> TraderMetrics | None:
    if value is None or value.availability not in {"available", "partial"}:
        return None
    return TraderMetrics(
        total_trades=value.total_trades,
        buy_trades=value.buy_trades,
        sell_trades=value.sell_trades,
        unique_buyers=value.unique_buyers,
        unique_sellers=value.unique_sellers,
        unique_traders=value.unique_traders,
        volume_usd=value.volume_usd,
        median_trade_usd=value.median_trade_usd,
        p90_trade_usd=value.p90_trade_usd,
        p95_trade_usd=value.p95_trade_usd,
        largest_trade_usd=value.largest_trade_usd,
        top_1_trader_volume_share=value.top_1_trader_volume_share,
        top_5_trader_volume_share=value.top_5_trader_volume_share,
        top_10_trader_volume_share=value.top_10_trader_volume_share,
        repeat_trader_ratio=value.repeat_trader_ratio,
        buy_sell_wallet_overlap=value.buy_sell_wallet_overlap,
    )


def _partial(envelope: EvidenceEnvelope, code: str) -> EvidenceEnvelope:
    return EvidenceEnvelope(
        provider=envelope.provider,
        provider_schema_version=envelope.provider_schema_version,
        source_observed_at=envelope.source_observed_at,
        received_at=envelope.received_at,
        availability=EvidenceAvailability.PARTIAL,
        completeness=EvidenceCompleteness.PARTIAL_PAGINATION,
        acquisition_mode=envelope.acquisition_mode,
        source_slot=envelope.source_slot,
        page_cursor=envelope.page_cursor,
        next_cursor=envelope.next_cursor or code,
        failure_code=code,
        raw_payload=envelope.raw_payload,
    )


def _result(
    evidence_id: uuid.UUID,
    feature_id: uuid.UUID,
    envelope: EvidenceEnvelope,
) -> tuple[str, str, datetime, str]:
    identity = f"{evidence_id}:{feature_id}"
    digest = hashlib.sha256(identity.encode()).hexdigest()
    return identity, digest, envelope.received_at, envelope.availability.value


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
