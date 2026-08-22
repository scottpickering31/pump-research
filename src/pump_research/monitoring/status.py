"""Read-only durable runtime status for the collector command line."""

from __future__ import annotations

import shutil
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import case, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.config import Settings
from pump_research.market_data.dexscreener import DEX_SCREENER_PROVIDER
from pump_research.persistence.models import (
    ApiRequestLog,
    ArchiveScope,
    BackupVerification,
    BoostObservation,
    CandidateCurrentState,
    CandidateEnrichmentTask,
    CandidateEvent,
    CandidateTierEvent,
    CollectionEpoch,
    CollectionEpochCurrent,
    CollectorComponentHealth,
    CollectorRun,
    DexAvailabilityTask,
    HolderSnapshot,
    MarketContextSnapshot,
    Observation,
    PollBatchMember,
    PollSchedule,
    SecurityProviderRequest,
    StorageSample,
    Token,
    TokenMetadataEvent,
    TokenSecuritySnapshot,
    TokenSecurityTask,
    TraderDistributionSnapshot,
    WalletClusterSnapshot,
    WalletRelationshipEdge,
)
from pump_research.scheduling.capacity import plan_capacity
from pump_research.scheduling.policy import (
    AdaptivePollingPolicy,
    CapacityTier,
    CoverageClass,
    LifecycleState,
)


async def read_collector_status(
    session_factory: async_sessionmaker[AsyncSession], settings: Settings
) -> dict[str, Any]:
    """Return JSON-serializable status entirely derived from PostgreSQL."""
    now = datetime.now(UTC)
    host_filesystem = _host_filesystem_status(Path.cwd())
    minute_ago, hour_ago = now - timedelta(minutes=1), now - timedelta(hours=1)
    async with session_factory() as session:
        latest = await session.scalar(
            select(CollectorRun).order_by(CollectorRun.started_at.desc()).limit(1)
        )
        epoch_statement = (
            select(CollectionEpoch, CollectionEpochCurrent)
            .join(
                CollectionEpochCurrent,
                CollectionEpochCurrent.collection_epoch_id == CollectionEpoch.id,
            )
            .order_by(
                case(
                    (CollectionEpochCurrent.status == "running", 0),
                    (CollectionEpochCurrent.status == "planned", 1),
                    else_=2,
                ),
                CollectionEpoch.epoch_number.desc(),
            )
            .limit(1)
        )
        epoch_row = (await session.execute(epoch_statement)).one_or_none()
        health_rows = (
            []
            if latest is None
            else list(
                (
                    await session.execute(
                        select(CollectorComponentHealth)
                        .where(CollectorComponentHealth.collector_run_id == latest.id)
                        .order_by(CollectorComponentHealth.component_name)
                    )
                ).scalars()
            )
        )
        lifecycle_rows = await session.execute(
            select(PollSchedule.lifecycle_state, func.count()).group_by(
                PollSchedule.lifecycle_state
            )
        )
        coverage_rows = await session.execute(
            select(PollSchedule.coverage_class, func.count()).group_by(PollSchedule.coverage_class)
        )
        recent_requests = await session.scalar(
            select(func.count())
            .select_from(ApiRequestLog)
            .where(
                ApiRequestLog.provider == DEX_SCREENER_PROVIDER,
                ApiRequestLog.requested_at >= minute_ago,
            )
        )
        status_counts = await session.execute(
            select(
                func.count().filter(ApiRequestLog.http_status_code == 429),
                func.count().filter(ApiRequestLog.http_status_code >= 500),
            )
            .select_from(ApiRequestLog)
            .where(
                ApiRequestLog.provider == DEX_SCREENER_PROVIDER,
                ApiRequestLog.requested_at >= hour_ago,
            )
        )
        http_429s, http_5xx = status_counts.one()
        occupancy = await _recent_batch_occupancy(session, minute_ago)
        lateness = await session.execute(
            select(
                func.avg(PollBatchMember.claim_lateness_ms),
                func.percentile_cont(0.95).within_group(PollBatchMember.claim_lateness_ms),
            ).where(PollBatchMember.claimed_at >= hour_ago)
        )
        lateness_mean, lateness_p95 = lateness.one()
        lifecycle_lateness_rows = (
            await session.execute(
                select(
                    PollBatchMember.lifecycle_state,
                    func.percentile_cont(0.50).within_group(PollBatchMember.claim_lateness_ms),
                    func.percentile_cont(0.95).within_group(PollBatchMember.claim_lateness_ms),
                )
                .where(PollBatchMember.claimed_at >= hour_ago)
                .group_by(PollBatchMember.lifecycle_state)
            )
        ).all()
        coverage_lateness_rows = (
            await session.execute(
                select(
                    PollBatchMember.coverage_class,
                    func.percentile_cont(0.50).within_group(PollBatchMember.claim_lateness_ms),
                    func.percentile_cont(0.95).within_group(PollBatchMember.claim_lateness_ms),
                )
                .where(
                    PollBatchMember.claimed_at >= hour_ago,
                    PollBatchMember.coverage_class.is_not(None),
                )
                .group_by(PollBatchMember.coverage_class)
            )
        ).all()
        overdue_rows = (
            await session.execute(
                select(
                    PollSchedule.lifecycle_state,
                    func.count(),
                    func.max(func.extract("epoch", now - PollSchedule.next_due_at)),
                )
                .where(PollSchedule.next_due_at <= now)
                .group_by(PollSchedule.lifecycle_state)
            )
        ).all()
        coverage_overdue_rows = (
            await session.execute(
                select(
                    PollSchedule.coverage_class,
                    func.count(),
                    func.max(func.extract("epoch", now - PollSchedule.next_due_at)),
                )
                .where(
                    PollSchedule.next_due_at <= now,
                    PollSchedule.coverage_class.is_not(None),
                )
                .group_by(PollSchedule.coverage_class)
            )
        ).all()
        recent_control_scans = await session.scalar(
            select(func.count())
            .select_from(PollBatchMember)
            .where(
                PollBatchMember.claimed_at >= minute_ago,
                PollBatchMember.coverage_class == CoverageClass.RETIRED_CONTROL.value,
            )
        )
        db_size = await session.scalar(text("SELECT pg_database_size(current_database())"))
        token_count = await session.scalar(select(func.count()).select_from(Token))
        pending_count = await session.scalar(
            select(func.count())
            .select_from(DexAvailabilityTask)
            .where(DexAvailabilityTask.state == "PENDING_DEX")
        )
        observation_count = await session.scalar(select(func.count()).select_from(Observation))
        enrichment_counts = {
            "boost_observations": int(
                await session.scalar(select(func.count()).select_from(BoostObservation)) or 0
            ),
            "metadata_events": int(
                await session.scalar(select(func.count()).select_from(TokenMetadataEvent)) or 0
            ),
            "security_snapshots": int(
                await session.scalar(select(func.count()).select_from(TokenSecuritySnapshot)) or 0
            ),
            "market_context_snapshots": int(
                await session.scalar(select(func.count()).select_from(MarketContextSnapshot)) or 0
            ),
            "security_tasks_due": int(
                await session.scalar(
                    select(func.count())
                    .select_from(TokenSecurityTask)
                    .where(TokenSecurityTask.next_due_at <= now)
                )
                or 0
            ),
        }
        enrichment_request_rows = (
            await session.execute(
                select(ApiRequestLog.provider, func.count())
                .where(
                    ApiRequestLog.requested_at >= minute_ago,
                    or_(
                        ApiRequestLog.endpoint.in_(
                            ("/token-boosts/latest/v1", "/token-boosts/top/v1")
                        ),
                        ApiRequestLog.provider == "solana_rpc",
                    ),
                )
                .group_by(ApiRequestLog.provider)
            )
        ).all()
        candidate_tier_rows = (
            await session.execute(
                select(CandidateCurrentState.tier, func.count()).group_by(
                    CandidateCurrentState.tier
                )
            )
        ).all()
        candidate_recent_events = int(
            await session.scalar(
                select(func.count())
                .select_from(CandidateEvent)
                .where(CandidateEvent.candidate_at >= hour_ago)
            )
            or 0
        )
        candidate_transition_counts = (
            await session.execute(
                select(
                    func.count().filter(CandidateTierEvent.new_tier != "TIER_0_UNIVERSAL"),
                    func.count().filter(CandidateTierEvent.new_tier == "TIER_0_UNIVERSAL"),
                    func.count().filter(CandidateTierEvent.reason_code == "BOOST_ACTIVITY"),
                ).where(CandidateTierEvent.decided_at >= hour_ago)
            )
        ).one()
        candidate_promotions, candidate_demotions, boost_wakeups = (
            int(value or 0) for value in candidate_transition_counts
        )
        candidate_backlog, candidate_oldest, candidate_failures = (
            await session.execute(
                select(
                    func.count().filter(
                        CandidateEnrichmentTask.status.in_(("pending", "retry", "claimed"))
                    ),
                    func.min(CandidateEnrichmentTask.created_at).filter(
                        CandidateEnrichmentTask.status.in_(("pending", "retry", "claimed"))
                    ),
                    func.count().filter(CandidateEnrichmentTask.status == "failed"),
                )
            )
        ).one()
        candidate_tasks_recent = int(
            await session.scalar(
                select(func.count())
                .select_from(CandidateEnrichmentTask)
                .where(CandidateEnrichmentTask.claimed_at >= minute_ago)
            )
            or 0
        )
        candidate_coverage_count = int(
            await session.scalar(
                select(func.count())
                .select_from(PollSchedule)
                .where(PollSchedule.candidate_coverage_expires_at > now)
            )
            or 0
        )
        candidate_coverage_expired = int(
            await session.scalar(
                select(func.count())
                .select_from(PollSchedule)
                .where(PollSchedule.candidate_coverage_expires_at <= now)
            )
            or 0
        )
        tier2_types = (
            "HOLDER_SNAPSHOT",
            "TRADER_DISTRIBUTION",
            "CREATOR_HISTORY",
            "LIQUIDITY_EVENT_ANALYSIS",
        )
        tier3_types = ("WALLET_CLUSTER_ANALYSIS", "FUNDING_GRAPH_ANALYSIS")
        phase6_backlog_rows = (
            await session.execute(
                select(
                    func.count().filter(
                        CandidateEnrichmentTask.analysis_type.in_(tier2_types),
                        CandidateEnrichmentTask.status.in_(("pending", "retry", "claimed")),
                    ),
                    func.count().filter(
                        CandidateEnrichmentTask.analysis_type.in_(tier3_types),
                        CandidateEnrichmentTask.status.in_(("pending", "retry", "claimed")),
                    ),
                )
            )
        ).one()
        latest_holder_at = await session.scalar(select(func.max(HolderSnapshot.received_at)))
        latest_trader_at = await session.scalar(
            select(func.max(TraderDistributionSnapshot.received_at))
        )
        provider_rates = (
            await session.execute(
                select(SecurityProviderRequest.provider, func.count())
                .where(SecurityProviderRequest.requested_at >= minute_ago)
                .group_by(SecurityProviderRequest.provider)
            )
        ).all()
        provider_health = (
            await session.execute(
                select(
                    func.count().filter(SecurityProviderRequest.http_status_code == 429),
                    func.count().filter(SecurityProviderRequest.outcome == "partial"),
                    func.count().filter(SecurityProviderRequest.outcome == "failed"),
                ).where(SecurityProviderRequest.requested_at >= hour_ago)
            )
        ).one()
        wallet_edges_recent = int(
            await session.scalar(
                select(func.count())
                .select_from(WalletRelationshipEdge)
                .where(WalletRelationshipEdge.evidence_received_at >= hour_ago)
            )
            or 0
        )
        clusters_recent = int(
            await session.scalar(
                select(func.count())
                .select_from(WalletClusterSnapshot)
                .where(WalletClusterSnapshot.received_at >= hour_ago)
            )
            or 0
        )
        latest_storage = await session.scalar(
            select(StorageSample).order_by(StorageSample.sampled_at.desc()).limit(1)
        )
        storage_window_start = (
            None
            if latest_storage is None
            else await session.scalar(
                select(StorageSample)
                .where(
                    StorageSample.collection_epoch_id == latest_storage.collection_epoch_id,
                    StorageSample.sampled_at >= hour_ago,
                    StorageSample.sampled_at < latest_storage.sampled_at,
                )
                .order_by(StorageSample.sampled_at)
                .limit(1)
            )
        )
        storage_contributors: list[tuple[str, int]] = []
        if latest_storage is not None and storage_window_start is not None:
            storage_contributors = [
                (str(family), int(delta or 0))
                for family, delta in (
                    await session.execute(
                        text("""
                        SELECT latest.relation_family,
                               sum(latest.total_bytes - earlier.total_bytes)::bigint AS delta
                        FROM storage_relation_samples latest
                        JOIN storage_relation_samples earlier
                          ON earlier.relation_name = latest.relation_name
                         AND earlier.storage_sample_id = :earlier_id
                        WHERE latest.storage_sample_id = :latest_id
                        GROUP BY latest.relation_family
                        ORDER BY delta DESC, latest.relation_family
                        LIMIT 10
                        """),
                        {
                            "earlier_id": storage_window_start.id,
                            "latest_id": latest_storage.id,
                        },
                    )
                ).all()
            ]
        backup_rows = (
            []
            if epoch_row is None
            else list(
                (
                    await session.execute(
                        select(BackupVerification)
                        .where(BackupVerification.collection_epoch_id == epoch_row[0].id)
                        .order_by(BackupVerification.verified_at.desc())
                    )
                ).scalars()
            )
        )
        archive_rows = (
            []
            if epoch_row is None
            else list(
                (
                    await session.execute(
                        select(ArchiveScope)
                        .where(ArchiveScope.collection_epoch_id == epoch_row[0].id)
                        .order_by(ArchiveScope.end_at.desc())
                    )
                ).scalars()
            )
        )

    states = {state: int(count) for state, count in lifecycle_rows}
    policy = AdaptivePollingPolicy.from_settings(settings)
    coverage_counts = {coverage.value: 0 for coverage in CoverageClass}
    coverage_counts["LEGACY_UNMAPPED"] = 0
    for coverage, count in coverage_rows:
        coverage_counts[coverage if coverage is not None else "LEGACY_UNMAPPED"] = int(count)
    tier_counts = {tier: coverage_counts.get(tier.value, 0) for tier in CapacityTier}
    capacity = plan_capacity(policy, tier_counts)
    recent_lateness_by_state = {
        state: {
            "p50_ms": round(float(p50), 3),
            "p95_ms": round(float(p95), 3),
        }
        for state, p50, p95 in lifecycle_lateness_rows
    }
    overdue_by_state = {
        state: {
            "count": int(count),
            "max_overdue_seconds": round(float(maximum), 3),
        }
        for state, count, maximum in overdue_rows
    }
    recent_lateness_by_coverage = {
        coverage: {
            "p50_ms": round(float(p50), 3),
            "p95_ms": round(float(p95), 3),
        }
        for coverage, p50, p95 in coverage_lateness_rows
    }
    overdue_by_coverage = {
        coverage: {
            "count": int(count),
            "max_overdue_seconds": round(float(maximum), 3),
        }
        for coverage, count, maximum in coverage_overdue_rows
    }
    states["PENDING_DEX"] = int(pending_count or 0)
    epoch = epoch_row[0] if epoch_row is not None else None
    epoch_current = epoch_row[1] if epoch_row is not None else None
    storage_status = _storage_status(latest_storage, storage_window_start, storage_contributors)
    heartbeat_stale_after_seconds = max(30.0, 3 * settings.collector_heartbeat_seconds)
    heartbeat_age_seconds = (
        None
        if latest is None or latest.last_heartbeat_at is None
        else max(0.0, (now - latest.last_heartbeat_at).total_seconds())
    )
    if latest is None:
        operational_state = "UNKNOWN"
    elif latest.status == "failed":
        operational_state = "FAILED"
    elif latest.status == "running" and (
        heartbeat_age_seconds is None or heartbeat_age_seconds > heartbeat_stale_after_seconds
    ):
        operational_state = "STALE"
    elif latest.status == "running":
        operational_state = "HEALTHY"
    else:
        operational_state = "STOPPED"
    if operational_state == "HEALTHY":
        run_lifecycle = "HEALTHY_RUNNING"
    elif operational_state == "STALE":
        run_lifecycle = "STALE_OR_CRASHED"
    elif latest is not None and latest.status in {"stopped", "cancelled", "succeeded"}:
        run_lifecycle = "GRACEFULLY_STOPPED"
    elif operational_state == "FAILED":
        run_lifecycle = "FAILED"
    else:
        run_lifecycle = "UNKNOWN"
    actively_collecting = operational_state == "HEALTHY"
    continuity_warning = (
        "epoch is marked running but its latest collector run is not healthy"
        if epoch_current is not None
        and epoch_current.status == "running"
        and not actively_collecting
        else None
    )
    return {
        "checked_at": now.isoformat(),
        "operational_state": operational_state,
        "run_lifecycle": run_lifecycle,
        "actively_collecting": actively_collecting,
        "continuity_warning": continuity_warning,
        "collector_run": None
        if latest is None
        else {
            "id": str(latest.id),
            "status": latest.status,
            "started_at": latest.started_at.isoformat(),
            "finished_at": latest.finished_at.isoformat() if latest.finished_at else None,
            "last_heartbeat_at": (
                latest.last_heartbeat_at.isoformat() if latest.last_heartbeat_at else None
            ),
            "heartbeat_age_seconds": (
                round(heartbeat_age_seconds, 3) if heartbeat_age_seconds is not None else None
            ),
            "heartbeat_stale_after_seconds": heartbeat_stale_after_seconds,
            "failure_detail": latest.failure_detail,
            "uptime_seconds": max(
                0, int(((latest.finished_at or now) - latest.started_at).total_seconds())
            ),
        },
        "collection_epoch": None
        if epoch is None or epoch_current is None
        else {
            "number": epoch.epoch_number,
            "id": str(epoch.id),
            "status": epoch_current.status,
            "started_at": (
                epoch_current.started_at.isoformat() if epoch_current.started_at else None
            ),
            "ended_at": epoch_current.ended_at.isoformat() if epoch_current.ended_at else None,
            "data_valid": epoch_current.data_valid,
            "invalid_reason": epoch_current.invalid_reason,
        },
        "components": {
            row.component_name: {
                "status": row.status,
                "last_success_at": row.last_success_at.isoformat() if row.last_success_at else None,
                "detail": row.detail,
            }
            for row in health_rows
        },
        "discovery_connectivity": next(
            (row.status for row in health_rows if row.component_name == "discovery"), "unknown"
        ),
        "tokens_discovered": int(token_count or 0),
        "pending_dex": int(pending_count or 0),
        "tokens_by_lifecycle_state": states,
        "tokens_by_coverage_class": coverage_counts,
        "observations_written": int(observation_count or 0),
        "phase2_enrichment": {
            **enrichment_counts,
            "requests_recent_minute_by_provider": {
                provider: int(count) for provider, count in enrichment_request_rows
            },
            "boost_feed_max_requests_per_minute": round(
                60 / settings.boost_latest_poll_seconds + 60 / settings.boost_top_poll_seconds,
                3,
            ),
            "solana_rpc_request_ceiling_per_minute": (settings.solana_rpc_requests_per_minute),
        },
        "candidate_orchestration": {
            "candidate_counts_by_tier": {tier: int(count) for tier, count in candidate_tier_rows},
            "candidate_events_recent_hour": candidate_recent_events,
            "candidate_promotions_recent_hour": candidate_promotions,
            "candidate_demotions_recent_hour": candidate_demotions,
            "candidate_task_backlog": int(candidate_backlog or 0),
            "candidate_task_oldest_age_seconds": (
                max(0.0, (now - candidate_oldest).total_seconds())
                if candidate_oldest is not None
                else None
            ),
            "candidate_budget_tasks_per_minute": settings.candidate_tasks_per_minute,
            "candidate_budget_utilization_pct": round(
                100 * candidate_tasks_recent / settings.candidate_tasks_per_minute, 3
            ),
            "boost_wakeups_recent_hour": boost_wakeups,
            "boost_wakeup_budget_per_minute": settings.candidate_boost_wakeups_per_minute,
            "candidate_coverage_count": candidate_coverage_count,
            "candidate_coverage_expirations_pending": candidate_coverage_expired,
            "candidate_failures_total": int(candidate_failures or 0),
        },
        "selective_security_enrichment": {
            "provider_configuration": {
                "mode": "MODE_A_STANDARD_RPC_ONLY",
                "standard_solana_rpc": "CONFIGURED_NOT_ACCEPTANCE_PROVEN",
                "advanced_indexer": (
                    "UNAVAILABLE_ADAPTER_NOT_IMPLEMENTED"
                    if settings.security_indexer_url is not None
                    else "UNCONFIGURED"
                ),
                "advanced_indexer_adapter_active": False,
                "unavailable_evidence_is_negative": False,
            },
            "tier2_backlog": int(phase6_backlog_rows[0] or 0),
            "tier3_backlog": int(phase6_backlog_rows[1] or 0),
            "holder_snapshot_age_seconds": (
                max(0.0, (now - latest_holder_at).total_seconds())
                if latest_holder_at is not None
                else None
            ),
            "trader_snapshot_age_seconds": (
                max(0.0, (now - latest_trader_at).total_seconds())
                if latest_trader_at is not None
                else None
            ),
            "deep_review_count": int(
                next(
                    (count for tier, count in candidate_tier_rows if tier == "TIER_3_DEEP_REVIEW"),
                    0,
                )
            ),
            "provider_request_rates": {provider: int(count) for provider, count in provider_rates},
            "provider_429s_recent_hour": int(provider_health[0] or 0),
            "partial_enrichment_count_recent_hour": int(provider_health[1] or 0),
            "failed_enrichment_count_recent_hour": int(provider_health[2] or 0),
            "wallet_edges_recent_hour": wallet_edges_recent,
            "clusters_recent_hour": clusters_recent,
            "security_evidence_freshness_seconds": {
                "holder": (
                    max(0.0, (now - latest_holder_at).total_seconds())
                    if latest_holder_at is not None
                    else None
                ),
                "trader": (
                    max(0.0, (now - latest_trader_at).total_seconds())
                    if latest_trader_at is not None
                    else None
                ),
            },
        },
        "dex_requests_recent_minute": int(recent_requests or 0),
        "configured_dex_request_ceiling_per_minute": settings.dex_screener_requests_per_minute,
        "average_recent_batch_occupancy_pct": occupancy,
        "http_429s_recent_hour": int(http_429s or 0),
        "http_5xx_recent_hour": int(http_5xx or 0),
        "scheduler_lateness_recent_hour_ms": {
            "mean": round(float(lateness_mean), 3) if lateness_mean is not None else None,
            "p95": round(float(lateness_p95), 3) if lateness_p95 is not None else None,
        },
        "scheduler_lateness_recent_hour_by_lifecycle_state": recent_lateness_by_state,
        "currently_overdue_schedules_by_lifecycle_state": overdue_by_state,
        "scheduler_lateness_recent_hour_by_coverage_class": (recent_lateness_by_coverage),
        "currently_overdue_schedules_by_coverage_class": overdue_by_coverage,
        "coverage_scheduler": {
            "coverage_counts": coverage_counts,
            "retired_population": coverage_counts.get(CoverageClass.RETIRED_CONTROL.value, 0),
            "legacy_unmapped_population": coverage_counts.get("LEGACY_UNMAPPED", 0),
            "resurrection_scan_budget_per_minute": (
                settings.scheduler_control_scan_tokens_per_minute
            ),
            "resurrection_scans_recent_minute": int(recent_control_scans or 0),
            "coverage_demand_requested_per_minute": (
                capacity.requested_token_observations_per_minute
            ),
            "coverage_demand_effective_per_minute": (
                capacity.effective_token_observations_per_minute
            ),
            "coverage_capacity_utilization_pct": capacity.capacity_utilization_pct,
            "coverage_target_interval_seconds": {
                tier.value: capacity.target_interval_seconds[tier] for tier in CapacityTier
            },
            "coverage_effective_interval_seconds": {
                tier.value: capacity.effective_interval_seconds[tier] for tier in CapacityTier
            },
            "overdue_by_coverage_class": overdue_by_coverage,
        },
        "scheduler_capacity": {
            **capacity.snapshot,
            "configured_request_ceiling_per_minute": (settings.dex_screener_requests_per_minute),
            "current_requests_per_minute": int(recent_requests or 0),
            "safety_headroom_ratio": settings.scheduler_capacity_headroom_ratio,
            "reserved_requests_per_minute": (settings.scheduler_reserved_requests_per_minute),
            "target_poll_interval_seconds": _state_intervals(capacity.target_interval_seconds),
            "effective_poll_interval_seconds": _state_intervals(
                capacity.effective_interval_seconds
            ),
        },
        "database_size_bytes": int(db_size or 0),
        "host_filesystem": host_filesystem,
        "storage": storage_status,
        "backup": {
            "verified_artifact_count": len(backup_rows),
            "independent_backup_present": any(row.independent_copy for row in backup_rows),
            "latest_verified_at": (backup_rows[0].verified_at.isoformat() if backup_rows else None),
        },
        "archive": {
            "scope_count": len(archive_rows),
            "latest_archive": (
                {
                    "id": str(archive_rows[0].id),
                    "state": archive_rows[0].state,
                    "start_at": archive_rows[0].start_at.isoformat(),
                    "end_at": archive_rows[0].end_at.isoformat(),
                    "verified_at": (
                        archive_rows[0].verified_at.isoformat()
                        if archive_rows[0].verified_at
                        else None
                    ),
                }
                if archive_rows
                else None
            ),
            "archive_lag_seconds": (
                max(
                    0.0,
                    (
                        now - max(row.end_at for row in archive_rows if row.verified_at)
                    ).total_seconds(),
                )
                if any(row.verified_at for row in archive_rows)
                else None
            ),
            "verified_cold_coverage": [
                {"start_at": row.start_at.isoformat(), "end_at": row.end_at.isoformat()}
                for row in archive_rows
                if row.verified_at is not None
            ],
            "retention_eligible_range_count": sum(
                row.state == "retention_eligible" for row in archive_rows
            ),
            "archive_storage_bytes": sum(
                row.parquet_bytes or 0 for row in archive_rows if row.verified_at is not None
            ),
            "last_failure": next(
                (row.failure_detail for row in archive_rows if row.state == "failed"), None
            ),
            "deletion_available": False,
        },
    }


def _host_filesystem_status(path: Path) -> dict[str, object]:
    """Expose host staging headroom without making it an authoritative DB fact."""
    try:
        usage = shutil.disk_usage(path)
    except OSError as error:
        return {
            "path": str(path),
            "free_bytes": None,
            "free_pct": None,
            "status": "unavailable",
            "error_type": type(error).__name__,
        }
    return {
        "path": str(path),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
        "free_pct": round(100 * usage.free / usage.total, 3),
        "status": "available",
    }


def _state_intervals(intervals: Mapping[CapacityTier, int]) -> dict[str, object]:
    """Retain lifecycle-oriented display while exposing the finite path."""
    return {
        LifecycleState.ACTIVE.value: intervals[CoverageClass.PROTECTED_ACTIVE],
        LifecycleState.RESURRECTED.value: intervals[CoverageClass.PROTECTED_RESURRECTED],
        LifecycleState.NEW.value: {
            "INITIAL": intervals[CoverageClass.INITIAL],
            "EARLY": intervals[CoverageClass.EARLY],
            "MATURE": intervals[CoverageClass.MATURE],
            "COOLED": intervals[CoverageClass.COOLED],
            "LONG_TAIL_DAY": intervals[CoverageClass.LONG_TAIL_DAY],
            "LONG_TAIL_WEEK": intervals[CoverageClass.LONG_TAIL_WEEK],
            "after_7d": "RETIRED_CONTROL",
        },
        LifecycleState.WATCH.value: intervals[CoverageClass.PROTECTED_WATCH],
        LifecycleState.FADING.value: {
            "FADING_TAIL": intervals[CoverageClass.FADING_TAIL],
            "FADING_COOL": intervals[CoverageClass.FADING_COOL],
            "after_6h": "RETIRED_CONTROL",
        },
        LifecycleState.DORMANT.value: "RETIRED_CONTROL",
    }


async def _recent_batch_occupancy(session: AsyncSession, start: datetime) -> float | None:
    result = await session.execute(
        text("""
        WITH batches AS (
          SELECT pb.id, count(pbm.token_id) AS members,
                 NULLIF((pb.configuration_snapshot ->> 'batch_size')::numeric, 0) AS capacity
          FROM poll_batches pb JOIN poll_batch_members pbm
            ON pbm.batch_id = pb.id AND pbm.claimed_at = pb.claimed_at
          WHERE pb.claimed_at >= :start GROUP BY pb.id, pb.configuration_snapshot
        ) SELECT avg(100.0 * members / capacity) FROM batches
    """),
        {"start": start},
    )
    value = result.scalar_one()
    return round(float(value), 3) if value is not None else None


def _storage_status(
    latest: StorageSample | None,
    earlier: StorageSample | None,
    contributors: list[tuple[str, int]],
) -> dict[str, object]:
    """Compute clearly labelled extrapolations from persisted byte samples."""
    if latest is None:
        return {
            "sampled_at": None,
            "database_bytes": None,
            "growth_recent_window_bytes": None,
            "projected_gib_per_day": None,
            "hot_postgres_projection_gib": None,
            "top_growth_contributors": [],
            "projection_label": "extrapolation; insufficient samples",
        }
    growth = 0
    projected_day = None
    elapsed_seconds = None
    if earlier is not None:
        elapsed_seconds = max(1.0, (latest.sampled_at - earlier.sampled_at).total_seconds())
        growth = max(0, latest.database_bytes - earlier.database_bytes)
        projected_day = growth * 86_400 / elapsed_seconds
    gib = 1024**3
    return {
        "sampled_at": latest.sampled_at.isoformat(),
        "database_bytes": latest.database_bytes,
        "growth_recent_window_bytes": growth if earlier is not None else None,
        "growth_recent_window_seconds": elapsed_seconds,
        "projected_gib_per_day": (
            round(projected_day / gib, 6) if projected_day is not None else None
        ),
        "hot_postgres_projection_gib": None
        if projected_day is None
        else {
            str(days): round((latest.database_bytes + projected_day * days) / gib, 6)
            for days in (30, 90, 365)
        },
        "top_growth_contributors": [
            {"relation_family": family, "growth_bytes": max(0, delta)}
            for family, delta in contributors
        ],
        "projection_label": "extrapolation from recent persisted samples; not a forecast",
    }
