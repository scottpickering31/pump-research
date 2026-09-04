"""Production-grade, verified Parquet/Zstandard archival without source deletion."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import duckdb
import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from sqlalchemy import BigInteger, Boolean, DateTime, Integer, Numeric, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.types import Uuid

from pump_research.archive_catalog import (
    ArchiveBusyError,
    canonical_sha256,
    claim_archive_scope,
    mark_archive_exported,
    mark_archive_failed,
    mark_archive_verified,
    record_copy_verification,
)
from pump_research.epochs import current_code_revision, get_epoch_status
from pump_research.persistence.models import (
    ApiRequestLog,
    BackupVerification,
    BoostEvent,
    BoostObservation,
    CandidateEnrichmentTask,
    CandidateEvent,
    CandidatePolicyRecord,
    CandidateTierEvent,
    CollectionEpoch,
    CollectionEpochEvent,
    CollectorRun,
    CollectorRunEvent,
    CoverageDecision,
    CoveragePolicy,
    CreatorHistorySnapshot,
    CreatorRelationshipEvent,
    DeduplicationConflict,
    DiscoveryConnectivityEvent,
    DiscoveryEvent,
    FundingRelationshipEvidence,
    HolderBalanceFact,
    HolderSnapshot,
    LifecycleEvent,
    LifecycleEvidenceEvaluation,
    LifecyclePolicy,
    LiquidityEventEvidence,
    MarketContextSnapshot,
    Observation,
    Pair,
    PairFactEvent,
    PollBatch,
    PollBatchMember,
    PollBatchOutcome,
    PollScheduleDecision,
    SchedulerCapacityDecision,
    SchedulerPolicy,
    SecurityEnrichmentPolicyRecord,
    SecurityFeatureSnapshot,
    SecurityProviderBudgetReservation,
    SecurityProviderRequest,
    StorageRelationSample,
    StorageSample,
    Token,
    TokenMetadataEvent,
    TokenSecuritySnapshot,
    TraderDistributionSnapshot,
    WalletClusterSnapshot,
    WalletRelationshipEdge,
)

ARCHIVE_SCHEMA_VERSION = 2
_DEFAULT_MAX_FILE_ROWS = 1_000_000
_DEFAULT_MINIMUM_FREE_BYTES = 2 * 1024**3


class ArchiveConflictError(RuntimeError):
    """One deterministic identity or output path maps to different content."""


class ArchiveVerificationError(RuntimeError):
    """An archive failed a mandatory integrity/readback check."""


class InsufficientArchiveDiskError(RuntimeError):
    """Local staging would violate the configured free-space safety floor."""


@dataclass(frozen=True, slots=True)
class _ExportSpec:
    name: str
    model: type[Any]
    timestamp_column: str
    query: str
    mode: str = "daily"
    archive_schema_version: int = 1

    @property
    def key_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.model.__table__.primary_key.columns)


_SPECS = (
    _ExportSpec(
        "observations",
        Observation,
        "received_at",
        """SELECT o.* FROM observations o
        JOIN api_request_log ar ON ar.id = o.api_request_log_id
        JOIN collector_runs cr ON cr.id = ar.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND o.received_at >= :start_at AND o.received_at < :end_at
        ORDER BY o.received_at, o.id""",
    ),
    _ExportSpec(
        "api_request_log",
        ApiRequestLog,
        "requested_at",
        """SELECT ar.* FROM api_request_log ar
        JOIN collector_runs cr ON cr.id = ar.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND ar.requested_at >= :start_at AND ar.requested_at < :end_at
        ORDER BY ar.requested_at, ar.id""",
    ),
    _ExportSpec(
        "discovery_events",
        DiscoveryEvent,
        "received_at",
        """SELECT de.* FROM discovery_events de
        JOIN collector_runs cr ON cr.id = de.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND de.received_at >= :start_at AND de.received_at < :end_at
        ORDER BY de.received_at, de.id""",
    ),
    _ExportSpec(
        "discovery_connectivity_events",
        DiscoveryConnectivityEvent,
        "observed_at",
        """SELECT dc.* FROM discovery_connectivity_events dc
        WHERE dc.observed_at >= :start_at AND dc.observed_at < :end_at
        ORDER BY dc.observed_at, dc.id""",
    ),
    _ExportSpec(
        "deduplication_conflicts",
        DeduplicationConflict,
        "occurred_at",
        """SELECT dc.* FROM deduplication_conflicts dc
        WHERE dc.occurred_at >= :start_at AND dc.occurred_at < :end_at
        ORDER BY dc.occurred_at, dc.id""",
    ),
    _ExportSpec(
        "lifecycle_evidence",
        LifecycleEvidenceEvaluation,
        "input_watermark",
        """SELECT le.* FROM lifecycle_evidence_evaluations le
        JOIN api_request_log ar ON ar.id = le.api_request_log_id
        JOIN collector_runs cr ON cr.id = ar.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND le.input_watermark >= :start_at AND le.input_watermark < :end_at
        ORDER BY le.input_watermark, le.id""",
    ),
    _ExportSpec(
        "lifecycle_events",
        LifecycleEvent,
        "decided_at",
        """SELECT le.* FROM lifecycle_events le
        JOIN collector_runs cr ON cr.id = le.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND le.decided_at >= :start_at AND le.decided_at < :end_at
        ORDER BY le.decided_at, le.id""",
    ),
    _ExportSpec(
        "coverage_decisions",
        CoverageDecision,
        "decided_at",
        """SELECT cd.* FROM coverage_decisions cd
        LEFT JOIN collector_runs cr ON cr.id = cd.collector_run_id
        WHERE COALESCE(cd.collection_epoch_id, cr.collection_epoch_id) = :epoch_id
          AND cd.decided_at >= :start_at AND cd.decided_at < :end_at
        ORDER BY cd.decided_at, cd.id""",
    ),
    _ExportSpec(
        "candidate_events",
        CandidateEvent,
        "candidate_at",
        """SELECT ce.* FROM candidate_events ce
        WHERE ce.collection_epoch_id = :epoch_id
          AND ce.candidate_at >= :start_at AND ce.candidate_at < :end_at
        ORDER BY ce.candidate_at, ce.id""",
    ),
    _ExportSpec(
        "candidate_tier_events",
        CandidateTierEvent,
        "decided_at",
        """SELECT ct.* FROM candidate_tier_events ct
        WHERE ct.collection_epoch_id = :epoch_id
          AND ct.decided_at >= :start_at AND ct.decided_at < :end_at
        ORDER BY ct.decided_at, ct.id""",
    ),
    _ExportSpec(
        "candidate_enrichment_tasks",
        CandidateEnrichmentTask,
        "created_at",
        """SELECT cet.* FROM candidate_enrichment_tasks cet
        WHERE cet.collection_epoch_id = :epoch_id
          AND cet.created_at >= :start_at AND cet.created_at < :end_at
        ORDER BY cet.created_at, cet.id""",
    ),
    _ExportSpec(
        "security_provider_budget_reservations",
        SecurityProviderBudgetReservation,
        "reserved_at",
        """SELECT r.* FROM security_provider_budget_reservations r
        JOIN candidate_enrichment_tasks t ON t.id = r.candidate_task_id
        WHERE t.collection_epoch_id = :epoch_id
          AND r.reserved_at >= :start_at AND r.reserved_at < :end_at
        ORDER BY r.reserved_at, r.id""",
    ),
    _ExportSpec(
        "security_provider_requests",
        SecurityProviderRequest,
        "received_at",
        """SELECT r.* FROM security_provider_requests r
        WHERE r.collection_epoch_id = :epoch_id
          AND r.received_at >= :start_at AND r.received_at < :end_at
        ORDER BY r.received_at, r.id""",
    ),
    _ExportSpec(
        "holder_snapshots",
        HolderSnapshot,
        "received_at",
        """SELECT h.* FROM holder_snapshots h WHERE h.collection_epoch_id = :epoch_id
          AND h.received_at >= :start_at AND h.received_at < :end_at
        ORDER BY h.received_at, h.id""",
    ),
    _ExportSpec(
        "holder_balance_facts",
        HolderBalanceFact,
        "persisted_at",
        """SELECT b.* FROM holder_balance_facts b JOIN holder_snapshots h
        ON h.id = b.holder_snapshot_id WHERE h.collection_epoch_id = :epoch_id
          AND h.received_at >= :start_at AND h.received_at < :end_at
        ORDER BY b.persisted_at, b.id""",
    ),
    _ExportSpec(
        "trader_distribution_snapshots",
        TraderDistributionSnapshot,
        "received_at",
        """SELECT d.* FROM trader_distribution_snapshots d
        WHERE d.collection_epoch_id = :epoch_id
          AND d.received_at >= :start_at AND d.received_at < :end_at
        ORDER BY d.received_at, d.id""",
    ),
    _ExportSpec(
        "creator_history_snapshots",
        CreatorHistorySnapshot,
        "received_at",
        """SELECT h.* FROM creator_history_snapshots h
        WHERE h.collection_epoch_id = :epoch_id
          AND h.received_at >= :start_at AND h.received_at < :end_at
        ORDER BY h.received_at, h.id""",
    ),
    _ExportSpec(
        "creator_relationship_events",
        CreatorRelationshipEvent,
        "received_at",
        """SELECT r.* FROM creator_relationship_events r JOIN candidate_events c
        ON c.id = r.candidate_id WHERE c.collection_epoch_id = :epoch_id
          AND r.received_at >= :start_at AND r.received_at < :end_at
        ORDER BY r.received_at, r.id""",
    ),
    _ExportSpec(
        "liquidity_event_evidence",
        LiquidityEventEvidence,
        "received_at",
        """SELECT l.* FROM liquidity_event_evidence l JOIN candidate_events c
        ON c.id = l.candidate_id WHERE c.collection_epoch_id = :epoch_id
          AND l.received_at >= :start_at AND l.received_at < :end_at
        ORDER BY l.received_at, l.id""",
    ),
    _ExportSpec(
        "wallet_relationship_edges",
        WalletRelationshipEdge,
        "evidence_received_at",
        """SELECT w.* FROM wallet_relationship_edges w JOIN candidate_events c
        ON c.id = w.candidate_id WHERE c.collection_epoch_id = :epoch_id
          AND w.evidence_received_at >= :start_at AND w.evidence_received_at < :end_at
        ORDER BY w.evidence_received_at, w.id""",
    ),
    _ExportSpec(
        "funding_relationship_evidence",
        FundingRelationshipEvidence,
        "received_at",
        """SELECT f.* FROM funding_relationship_evidence f JOIN candidate_events c
        ON c.id = f.candidate_id WHERE c.collection_epoch_id = :epoch_id
          AND f.received_at >= :start_at AND f.received_at < :end_at
        ORDER BY f.received_at, f.id""",
    ),
    _ExportSpec(
        "wallet_cluster_snapshots",
        WalletClusterSnapshot,
        "received_at",
        """SELECT w.* FROM wallet_cluster_snapshots w JOIN candidate_events c
        ON c.id = w.candidate_id WHERE c.collection_epoch_id = :epoch_id
          AND w.received_at >= :start_at AND w.received_at < :end_at
        ORDER BY w.received_at, w.id""",
    ),
    _ExportSpec(
        "security_feature_snapshots",
        SecurityFeatureSnapshot,
        "received_at",
        """SELECT s.* FROM security_feature_snapshots s
        WHERE s.collection_epoch_id = :epoch_id
          AND s.received_at >= :start_at AND s.received_at < :end_at
        ORDER BY s.received_at, s.id""",
    ),
    _ExportSpec(
        "security_enrichment_policies",
        SecurityEnrichmentPolicyRecord,
        "persisted_at",
        """SELECT p.* FROM security_enrichment_policies p WHERE EXISTS (
          SELECT 1 FROM security_feature_snapshots s
          WHERE s.policy_sha256 = p.policy_sha256 AND s.collection_epoch_id = :epoch_id
            AND s.received_at >= :start_at AND s.received_at < :end_at)
        ORDER BY p.persisted_at, p.policy_sha256""",
        mode="scope",
    ),
    _ExportSpec(
        "scheduler_capacity_decisions",
        SchedulerCapacityDecision,
        "decided_at",
        """SELECT sc.* FROM scheduler_capacity_decisions sc
        WHERE sc.decided_at >= :start_at AND sc.decided_at < :end_at
          AND (
            EXISTS (
              SELECT 1 FROM poll_batches pb JOIN collector_runs cr ON cr.id = pb.collector_run_id
              WHERE pb.capacity_decision_id = sc.id AND cr.collection_epoch_id = :epoch_id
            ) OR EXISTS (
              SELECT 1 FROM coverage_decisions cd LEFT JOIN collector_runs cr
                ON cr.id = cd.collector_run_id
              WHERE cd.capacity_decision_id = sc.id
                AND COALESCE(cd.collection_epoch_id, cr.collection_epoch_id) = :epoch_id
            ) OR EXISTS (
              SELECT 1 FROM poll_schedule_decisions psd
              WHERE psd.capacity_decision_id = sc.id
                AND psd.collection_epoch_id = :epoch_id
            )
          )
        ORDER BY sc.decided_at, sc.id""",
    ),
    _ExportSpec(
        "poll_schedule_decisions",
        PollScheduleDecision,
        "decided_at",
        """SELECT psd.* FROM poll_schedule_decisions psd
        WHERE psd.collection_epoch_id = :epoch_id
          AND psd.decided_at >= :start_at AND psd.decided_at < :end_at
        ORDER BY psd.decided_at, psd.id""",
    ),
    _ExportSpec(
        "poll_batches",
        PollBatch,
        "claimed_at",
        """SELECT pb.* FROM poll_batches pb
        JOIN collector_runs cr ON cr.id = pb.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND pb.claimed_at >= :start_at AND pb.claimed_at < :end_at
        ORDER BY pb.claimed_at, pb.id""",
    ),
    _ExportSpec(
        "poll_batch_outcomes",
        PollBatchOutcome,
        "completed_at",
        """SELECT po.* FROM poll_batch_outcomes po
        JOIN poll_batches pb ON pb.id = po.batch_id
        JOIN collector_runs cr ON cr.id = pb.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND po.completed_at >= :start_at AND po.completed_at < :end_at
        ORDER BY po.completed_at, po.batch_id""",
    ),
    _ExportSpec(
        "poll_batch_members",
        PollBatchMember,
        "claimed_at",
        """SELECT pm.* FROM poll_batch_members pm
        JOIN poll_batches pb ON pb.id = pm.batch_id
        JOIN collector_runs cr ON cr.id = pb.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND pm.claimed_at >= :start_at AND pm.claimed_at < :end_at
        ORDER BY pm.claimed_at, pm.batch_id, pm.token_id""",
    ),
    _ExportSpec(
        "pair_fact_events",
        PairFactEvent,
        "received_at",
        """SELECT pf.* FROM pair_fact_events pf
        JOIN collector_runs cr ON cr.id = pf.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND pf.received_at >= :start_at AND pf.received_at < :end_at
        ORDER BY pf.received_at, pf.id""",
    ),
    _ExportSpec(
        "boost_observations",
        BoostObservation,
        "received_at",
        """SELECT bo.* FROM boost_observations bo
        JOIN collector_runs cr ON cr.id = bo.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND bo.received_at >= :start_at AND bo.received_at < :end_at
        ORDER BY bo.received_at, bo.id""",
    ),
    _ExportSpec(
        "boost_events",
        BoostEvent,
        "decided_at",
        """SELECT be.* FROM boost_events be
        JOIN boost_observations bo ON bo.id = be.boost_observation_id
        JOIN collector_runs cr ON cr.id = bo.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND be.decided_at >= :start_at AND be.decided_at < :end_at
        ORDER BY be.decided_at, be.id""",
    ),
    _ExportSpec(
        "token_metadata_events",
        TokenMetadataEvent,
        "received_at",
        """SELECT tm.* FROM token_metadata_events tm
        JOIN collector_runs cr ON cr.id = tm.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND tm.received_at >= :start_at AND tm.received_at < :end_at
        ORDER BY tm.received_at, tm.id""",
    ),
    _ExportSpec(
        "token_security_snapshots",
        TokenSecuritySnapshot,
        "received_at",
        """SELECT ts.* FROM token_security_snapshots ts
        JOIN collector_runs cr ON cr.id = ts.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND ts.received_at >= :start_at AND ts.received_at < :end_at
        ORDER BY ts.received_at, ts.id""",
    ),
    _ExportSpec(
        "market_context_snapshots",
        MarketContextSnapshot,
        "received_at",
        """SELECT mc.* FROM market_context_snapshots mc
        WHERE mc.collection_epoch_id = :epoch_id
          AND mc.received_at >= :start_at AND mc.received_at < :end_at
        ORDER BY mc.received_at, mc.id""",
    ),
    _ExportSpec(
        "collection_epoch_events",
        CollectionEpochEvent,
        "occurred_at",
        """SELECT ce.* FROM collection_epoch_events ce
        WHERE ce.collection_epoch_id = :epoch_id
          AND ce.occurred_at <= :end_at
        ORDER BY ce.occurred_at, ce.id""",
        mode="scope",
    ),
    _ExportSpec(
        "collector_run_events",
        CollectorRunEvent,
        "occurred_at",
        """SELECT cre.* FROM collector_run_events cre
        JOIN collector_runs cr ON cr.id = cre.collector_run_id
        WHERE cr.collection_epoch_id = :epoch_id
          AND cre.occurred_at <= :end_at
        ORDER BY cre.occurred_at, cre.id""",
        mode="scope",
    ),
    _ExportSpec(
        "collector_runs",
        CollectorRun,
        "started_at",
        """SELECT cr.* FROM collector_runs cr
        WHERE cr.collection_epoch_id = :epoch_id
          AND cr.started_at < :end_at
        ORDER BY cr.started_at, cr.id""",
        mode="scope",
        archive_schema_version=2,
    ),
    _ExportSpec(
        "collection_epochs",
        CollectionEpoch,
        "created_at",
        """SELECT ce.* FROM collection_epochs ce
        WHERE ce.id = :epoch_id
        ORDER BY ce.created_at, ce.id""",
        mode="scope",
    ),
    _ExportSpec(
        "lifecycle_policies",
        LifecyclePolicy,
        "created_at",
        """SELECT lp.* FROM lifecycle_policies lp
        WHERE EXISTS (
          SELECT 1 FROM lifecycle_evidence_evaluations le
          JOIN api_request_log ar ON ar.id = le.api_request_log_id
          JOIN collector_runs cr ON cr.id = ar.collector_run_id
          WHERE le.policy_sha256 = lp.policy_sha256
            AND cr.collection_epoch_id = :epoch_id
            AND le.input_watermark >= :start_at AND le.input_watermark < :end_at
        )
        ORDER BY lp.created_at, lp.policy_sha256""",
        mode="scope",
    ),
    _ExportSpec(
        "coverage_policies",
        CoveragePolicy,
        "persisted_at",
        """SELECT cp.* FROM coverage_policies cp
        WHERE EXISTS (
          SELECT 1 FROM coverage_decisions cd LEFT JOIN collector_runs cr
            ON cr.id = cd.collector_run_id
          WHERE cd.policy_sha256 = cp.policy_sha256
            AND COALESCE(cd.collection_epoch_id, cr.collection_epoch_id) = :epoch_id
            AND cd.decided_at >= :start_at AND cd.decided_at < :end_at
        )
        ORDER BY cp.persisted_at, cp.policy_sha256""",
        mode="scope",
    ),
    _ExportSpec(
        "candidate_policies",
        CandidatePolicyRecord,
        "persisted_at",
        """SELECT cp.* FROM candidate_policies cp
        WHERE EXISTS (
          SELECT 1 FROM candidate_events ce
          WHERE ce.policy_sha256 = cp.policy_sha256
            AND ce.collection_epoch_id = :epoch_id
            AND ce.candidate_at >= :start_at AND ce.candidate_at < :end_at
        )
        ORDER BY cp.persisted_at, cp.policy_sha256""",
        mode="scope",
    ),
    _ExportSpec(
        "scheduler_policies",
        SchedulerPolicy,
        "persisted_at",
        """SELECT sp.* FROM scheduler_policies sp
        WHERE EXISTS (
          SELECT 1 FROM scheduler_capacity_decisions sc
          WHERE sc.policy_sha256 = sp.policy_sha256
            AND sc.decided_at >= :start_at AND sc.decided_at < :end_at
        )
        ORDER BY sp.persisted_at, sp.policy_sha256""",
        mode="scope",
    ),
    _ExportSpec(
        "storage_samples",
        StorageSample,
        "sampled_at",
        """SELECT ss.* FROM storage_samples ss
        WHERE ss.collection_epoch_id = :epoch_id
          AND ss.sampled_at >= :start_at AND ss.sampled_at < :end_at
        ORDER BY ss.sampled_at, ss.id""",
    ),
    _ExportSpec(
        "storage_relation_samples",
        StorageRelationSample,
        "created_at",
        """SELECT sr.* FROM storage_relation_samples sr
        JOIN storage_samples ss ON ss.id = sr.storage_sample_id
        WHERE ss.collection_epoch_id = :epoch_id
          AND ss.sampled_at >= :start_at AND ss.sampled_at < :end_at
        ORDER BY sr.created_at, sr.id""",
    ),
    _ExportSpec(
        "backup_verifications",
        BackupVerification,
        "verified_at",
        """SELECT bv.* FROM backup_verifications bv
        WHERE bv.collection_epoch_id = :epoch_id
          AND bv.verified_at >= :start_at AND bv.verified_at < :end_at
        ORDER BY bv.verified_at, bv.id""",
    ),
    _ExportSpec(
        "pairs",
        Pair,
        "persisted_at",
        """SELECT DISTINCT p.* FROM pairs p
        WHERE EXISTS (
          SELECT 1 FROM observations o JOIN api_request_log ar ON ar.id = o.api_request_log_id
          JOIN collector_runs cr ON cr.id = ar.collector_run_id
          WHERE o.pair_id = p.id AND cr.collection_epoch_id = :epoch_id
            AND o.received_at >= :start_at AND o.received_at < :end_at
        ) OR EXISTS (
          SELECT 1 FROM pair_fact_events pf JOIN collector_runs cr ON cr.id = pf.collector_run_id
          WHERE pf.pair_id = p.id AND cr.collection_epoch_id = :epoch_id
            AND pf.received_at >= :start_at AND pf.received_at < :end_at
        ) OR EXISTS (
          SELECT 1 FROM boost_observations bo JOIN collector_runs cr
            ON cr.id = bo.collector_run_id
          WHERE bo.pair_id = p.id AND cr.collection_epoch_id = :epoch_id
            AND bo.received_at >= :start_at AND bo.received_at < :end_at
        )
        ORDER BY p.persisted_at, p.id""",
        mode="scope",
    ),
    _ExportSpec(
        "tokens",
        Token,
        "persisted_at",
        """SELECT DISTINCT t.* FROM tokens t
        WHERE EXISTS (
          SELECT 1 FROM discovery_events de JOIN collector_runs cr ON cr.id = de.collector_run_id
          WHERE de.token_id = t.id AND cr.collection_epoch_id = :epoch_id
            AND de.received_at >= :start_at AND de.received_at < :end_at
        ) OR EXISTS (
          SELECT 1 FROM pairs p JOIN observations o ON o.pair_id = p.id
          JOIN api_request_log ar ON ar.id = o.api_request_log_id
          JOIN collector_runs cr ON cr.id = ar.collector_run_id
          WHERE p.token_id = t.id AND cr.collection_epoch_id = :epoch_id
            AND o.received_at >= :start_at AND o.received_at < :end_at
        ) OR EXISTS (
          SELECT 1 FROM lifecycle_events le JOIN collector_runs cr ON cr.id = le.collector_run_id
          WHERE le.token_id = t.id AND cr.collection_epoch_id = :epoch_id
            AND le.decided_at >= :start_at AND le.decided_at < :end_at
        ) OR EXISTS (
          SELECT 1 FROM coverage_decisions cd LEFT JOIN collector_runs cr
            ON cr.id = cd.collector_run_id
          WHERE cd.token_id = t.id
            AND COALESCE(cd.collection_epoch_id, cr.collection_epoch_id) = :epoch_id
            AND cd.decided_at >= :start_at AND cd.decided_at < :end_at
        ) OR EXISTS (
          SELECT 1 FROM boost_observations bo JOIN collector_runs cr
            ON cr.id = bo.collector_run_id
          WHERE bo.token_id = t.id AND cr.collection_epoch_id = :epoch_id
            AND bo.received_at >= :start_at AND bo.received_at < :end_at
        ) OR EXISTS (
          SELECT 1 FROM token_metadata_events tm JOIN collector_runs cr
            ON cr.id = tm.collector_run_id
          WHERE tm.token_id = t.id AND cr.collection_epoch_id = :epoch_id
            AND tm.received_at >= :start_at AND tm.received_at < :end_at
        ) OR EXISTS (
          SELECT 1 FROM token_security_snapshots ts JOIN collector_runs cr
            ON cr.id = ts.collector_run_id
          WHERE ts.token_id = t.id AND cr.collection_epoch_id = :epoch_id
            AND ts.received_at >= :start_at AND ts.received_at < :end_at
        )
        ORDER BY t.persisted_at, t.id""",
        mode="scope",
    ),
)

_SPECS_BY_NAME = {spec.name: spec for spec in _SPECS}


def archive_family_names() -> tuple[str, ...]:
    """Return the stable exportable-family contract for audits and readiness checks."""
    return tuple(spec.name for spec in _SPECS)


async def export_epoch_range(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    epoch_number: int,
    start_at: datetime,
    end_at: datetime,
    output: Path,
    chunk_rows: int,
    now: datetime | None = None,
    max_file_rows: int = _DEFAULT_MAX_FILE_ROWS,
    minimum_free_bytes: int = _DEFAULT_MINIMUM_FREE_BYTES,
    disk_free_override_bytes: int | None = None,
    fail_after_published_files: int | None = None,
) -> Path:
    """Export, independently verify, catalog, and analytically read a closed epoch scope."""
    start = _utc(start_at, "start_at")
    end = _utc(end_at, "end_at")
    current = _utc(now or datetime.now(UTC), "now")
    if start >= end:
        raise ValueError("archive range must have start_at < end_at")
    if end > current:
        raise ValueError("archive range cannot extend into the future")
    if chunk_rows < 1 or max_file_rows < chunk_rows:
        raise ValueError("archive file row target must be at least one export chunk")
    async with session_factory() as session:
        epoch = await get_epoch_status(session, epoch_number)
        source_revision = cast(
            str | None,
            await session.scalar(text("SELECT version_num FROM alembic_version")),
        )
        database_bytes = int(
            cast(int, await session.scalar(text("SELECT pg_database_size(current_database())")))
        )
    if source_revision is None:
        raise ArchiveConflictError("source database has no Alembic revision")
    if epoch.status in {"planned", "running"}:
        raise ValueError("production archive scopes require a closed epoch")
    if epoch.started_at is None or start < epoch.started_at:
        raise ValueError("archive range begins before the epoch start")
    if epoch.ended_at is None or end > epoch.ended_at:
        raise ValueError("archive range ends after the epoch end")

    scope_snapshot: dict[str, object] = {
        "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
        "epoch": epoch_number,
        "epoch_id": str(epoch.id),
        "start_inclusive": start.isoformat(),
        "end_exclusive": end.isoformat(),
        "families": [
            {
                "name": spec.name,
                "source_table": spec.model.__tablename__,
                "family_schema_version": spec.archive_schema_version,
                "mode": spec.mode,
                "key_columns": list(spec.key_columns),
            }
            for spec in _SPECS
        ],
        "partitioning": "schema/family/date/epoch/scope",
        "compression": "zstd",
        "uuid_representation": "canonical string",
        "json_representation": "canonical UTF-8 JSON string",
        "timestamp_semantics": "UTC microseconds; source and received columns remain distinct",
    }
    identity_document = {
        "source_db_schema_revision": source_revision,
        "scope": scope_snapshot,
    }
    identity = canonical_sha256(identity_document)
    claim = await claim_archive_scope(
        session_factory,
        identity_sha256=identity,
        epoch_id=epoch.id,
        start_at=start,
        end_at=end,
        archive_schema_version=ARCHIVE_SCHEMA_VERSION,
        source_db_schema_revision=source_revision,
        source_scope_snapshot=scope_snapshot,
        now=current,
    )
    if claim.reusable_manifest_path is not None:
        manifest_path = Path(claim.reusable_manifest_path)
        verification = await verify_archive(manifest_path)
        if verification["archive_identity_sha256"] != identity:
            raise ArchiveConflictError("catalog manifest identity differs from requested scope")
        if claim.previous_state == "exported":
            existing_manifest = _read_manifest(manifest_path)
            await mark_archive_verified(
                session_factory,
                scope_id=claim.scope_id,
                manifest_sha256=cast(str, verification["manifest_sha256"]),
                verification_detail=verification,
                analytical_reads_passed=bool(verification["duckdb_readback_passed"]),
            )
            await record_copy_verification(
                session_factory,
                scope_id=claim.scope_id,
                copy_role="primary",
                provider_kind="filesystem",
                location=str(_archive_root(manifest_path.resolve(), version=2)),
                manifest_sha256=cast(str, verification["manifest_sha256"]),
                aggregate_file_sha256=cast(str, verification["aggregate_file_sha256"]),
                total_bytes=(
                    _object_int(existing_manifest["parquet_bytes"]) + manifest_path.stat().st_size
                ),
                object_count=len(cast(list[object], existing_manifest["entries"])) + 2,
                independence_asserted=False,
                independence_detail=None,
                verification_method="full local file checksum/content/DuckDB readback",
                detail=verification,
            )
        return manifest_path

    output = output.expanduser().resolve()
    if claim.claim_token is None:
        raise ArchiveConflictError("new archive export has no exclusive claim token")
    # A claim-specific directory prevents an expired/recovered worker from
    # deleting or reusing the current owner's in-progress files. The database
    # lease remains authoritative; this is filesystem defence in depth.
    stage = (
        output
        / ".incomplete"
        / f"scope={identity}"
        / f"claim={claim.claim_token}"
    )
    try:
        _prepare_staging(stage)
        _ensure_disk_capacity(
            output,
            estimated_source_bytes=_estimated_scope_bytes(
                database_bytes=database_bytes,
                epoch_start=epoch.started_at,
                epoch_end=epoch.ended_at,
                start_at=start,
                end_at=end,
            ),
            minimum_free_bytes=minimum_free_bytes,
            disk_free_override_bytes=disk_free_override_bytes,
        )
        export_started = datetime.now(UTC)
        entries: list[dict[str, object]] = []
        for spec in _SPECS:
            ranges = [(start, end)] if spec.mode == "scope" else list(_daily_ranges(start, end))
            for part_start, part_end in ranges:
                entries.extend(
                    await _export_one(
                        session_factory,
                        spec=spec,
                        epoch_id=epoch.id,
                        epoch_number=epoch_number,
                        start_at=part_start,
                        end_at=part_end,
                        stage=stage,
                        identity=identity,
                        chunk_rows=chunk_rows,
                        max_file_rows=max_file_rows,
                    )
                )
        await _verify_source_counts(session_factory, entries=entries, epoch_id=epoch.id)
        _publish_files(
            stage=stage,
            output=output,
            entries=entries,
            fail_after=fail_after_published_files,
        )
        aggregate_digest = _aggregate_file_digest(entries)
        logical_bytes = sum(cast(int, item["uncompressed_logical_bytes"]) for item in entries)
        parquet_bytes = sum(cast(int, item["parquet_bytes"]) for item in entries)
        row_count = sum(cast(int, item["exported_row_count"]) for item in entries)
        manifest_path = _manifest_path(output, epoch_number, identity)
        manifest: dict[str, object] = {
            "archive_schema_version": ARCHIVE_SCHEMA_VERSION,
            "archive_identity_sha256": identity,
            "archive_scope_id": str(claim.scope_id),
            "source_db_schema_revision": source_revision,
            "family_contracts": scope_snapshot["families"],
            "epoch": epoch_number,
            "epoch_id": str(epoch.id),
            "epoch_data_valid": epoch.data_valid,
            "start_at": start.isoformat(),
            "end_at": end.isoformat(),
            "source_watermark": {
                "start_inclusive": start.isoformat(),
                "end_exclusive": end.isoformat(),
            },
            "format": "parquet",
            "compression": {"codec": "zstd", "level": 6},
            "partition_layout": (
                "schema=v2/family=<family>/year=YYYY/month=MM/day=DD/"
                "epoch=N/scope=<identity>/part-NNNNN.parquet"
            ),
            "sort_contract": "family timestamp then primary key",
            "source_row_count": row_count,
            "exported_row_count": row_count,
            "uncompressed_logical_bytes": logical_bytes,
            "parquet_bytes": parquet_bytes,
            "compression_ratio": round(logical_bytes / parquet_bytes, 6) if parquet_bytes else None,
            "aggregate_file_sha256": aggregate_digest,
            "entries": entries,
            "export_started_at": export_started.isoformat(),
            "export_completed_at": datetime.now(UTC).isoformat(),
            "exporter_code_revision": current_code_revision(),
            "configuration": {
                "chunk_rows": chunk_rows,
                "max_file_rows": max_file_rows,
                "minimum_free_bytes": minimum_free_bytes,
            },
            "source_verification": {
                "status": "matched_after_export",
                "method": "per-family PostgreSQL row-count requery",
                "source_scope_fully_covered": True,
            },
            "verification": {
                "status": "verified",
                "methods": [
                    "manifest SHA256 sidecar",
                    "full Parquet readback",
                    "schema/count/null/content/bounds checks",
                    "DuckDB primary-key and referential checks",
                ],
                "verified_at": datetime.now(UTC).isoformat(),
            },
            "deletion_permitted": False,
        }
        _write_verified_manifest(manifest_path, manifest)
        manifest_sha = _sha256_file(manifest_path)
        verification = await verify_archive(manifest_path)
        await mark_archive_exported(
            session_factory,
            claim=claim,
            manifest_path=str(manifest_path),
            manifest_sha256=manifest_sha,
            aggregate_file_sha256=aggregate_digest,
            source_row_count=row_count,
            parquet_bytes=parquet_bytes,
        )
        await mark_archive_verified(
            session_factory,
            scope_id=claim.scope_id,
            manifest_sha256=manifest_sha,
            verification_detail=verification,
            analytical_reads_passed=bool(verification["duckdb_readback_passed"]),
        )
        await record_copy_verification(
            session_factory,
            scope_id=claim.scope_id,
            copy_role="primary",
            provider_kind="filesystem",
            location=str(output),
            manifest_sha256=manifest_sha,
            aggregate_file_sha256=aggregate_digest,
            total_bytes=parquet_bytes + manifest_path.stat().st_size,
            object_count=len(entries) + 2,
            independence_asserted=False,
            independence_detail=None,
            verification_method="full local file checksum/content/DuckDB readback",
            detail=verification,
        )
        return manifest_path
    except ArchiveBusyError:
        raise
    except BaseException as error:
        await mark_archive_failed(
            session_factory, claim=claim, stage="export_or_verify", error=error
        )
        raise
    finally:
        if stage.exists():
            shutil.rmtree(stage)


async def verify_archive(manifest_path: Path) -> dict[str, object]:
    """Read every file and fail closed on manifest, schema, content, or relation drift."""
    manifest_path = manifest_path.expanduser().resolve(strict=True)
    manifest = _read_manifest(manifest_path)
    version = manifest.get("archive_schema_version", manifest.get("schema_version"))
    if version == 1:
        return _verify_legacy_archive(manifest_path, manifest)
    if version != ARCHIVE_SCHEMA_VERSION:
        raise ArchiveVerificationError(f"unsupported archive schema version: {version}")
    sidecar = manifest_path.with_name("manifest.sha256")
    if not sidecar.is_file():
        raise ArchiveVerificationError("manifest checksum sidecar is missing")
    expected_manifest_sha = sidecar.read_text(encoding="ascii").strip().split()[0]
    actual_manifest_sha = _sha256_file(manifest_path)
    if expected_manifest_sha != actual_manifest_sha:
        raise ArchiveVerificationError("manifest checksum mismatch")
    root = _archive_root(manifest_path, version=2)
    entries = cast(list[dict[str, object]], manifest.get("entries"))
    if not isinstance(entries, list):
        raise ArchiveVerificationError("manifest entries must be a list")
    verified_rows = 0
    for entry in entries:
        _verify_entry(root, entry)
        verified_rows += _object_int(entry["exported_row_count"])
    if verified_rows != _object_int(manifest["exported_row_count"]):
        raise ArchiveVerificationError("manifest exported row total differs from files")
    if _object_int(manifest["source_row_count"]) != verified_rows:
        raise ArchiveVerificationError("source and exported row totals differ")
    if _aggregate_file_digest(entries) != manifest["aggregate_file_sha256"]:
        raise ArchiveVerificationError("aggregate file digest mismatch")
    duckdb_detail = _duckdb_integrity(root, entries)
    return {
        "verified": True,
        "manifest_verified": True,
        "manifest": str(manifest_path),
        "manifest_sha256": actual_manifest_sha,
        "archive_identity_sha256": manifest["archive_identity_sha256"],
        "aggregate_file_sha256": manifest["aggregate_file_sha256"],
        "row_count": verified_rows,
        "file_count": len(entries),
        "source_scope_fully_covered": bool(
            cast(dict[str, object], manifest["source_verification"])["source_scope_fully_covered"]
        ),
        "duckdb_readback_passed": True,
        "duckdb": duckdb_detail,
        "verification_method": "full streaming readback plus DuckDB integrity",
    }


def archive_stats(manifest_path: Path) -> dict[str, object]:
    """Summarize measured archive storage and conservative time projections."""
    manifest = _read_manifest(manifest_path)
    rows = _object_int(manifest["exported_row_count"])
    parquet_bytes = _object_int(manifest["parquet_bytes"])
    start = datetime.fromisoformat(cast(str, manifest["start_at"]))
    end = datetime.fromisoformat(cast(str, manifest["end_at"]))
    elapsed_days = (end - start).total_seconds() / 86_400
    bytes_per_day = parquet_bytes / elapsed_days if elapsed_days > 0 else 0
    gib = 1024**3
    return {
        "epoch": manifest["epoch"],
        "archive_identity_sha256": manifest["archive_identity_sha256"],
        "source_rows": manifest["source_row_count"],
        "exported_rows": rows,
        "parquet_bytes": parquet_bytes,
        "bytes_per_row": round(parquet_bytes / rows, 6) if rows else None,
        "compression_ratio": manifest["compression_ratio"],
        "projected_archive_gib_per_day": round(bytes_per_day / gib, 6),
        "projected_archive_gib": {
            str(days): round(bytes_per_day * days / gib, 6) for days in (30, 90, 365)
        },
        "projection_label": "extrapolation from this manifest's measured closed range",
    }


async def _export_one(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    spec: _ExportSpec,
    epoch_id: uuid.UUID,
    epoch_number: int,
    start_at: datetime,
    end_at: datetime,
    stage: Path,
    identity: str,
    chunk_rows: int,
    max_file_rows: int,
) -> list[dict[str, object]]:
    schema = _arrow_schema(spec.model)
    entries: list[dict[str, object]] = []
    writer: pq.ParquetWriter | None = None
    state: dict[str, object] | None = None
    part_number = 0

    def open_part() -> tuple[pq.ParquetWriter, dict[str, object]]:
        nonlocal part_number
        relative = _data_path(
            spec=spec,
            epoch_number=epoch_number,
            start_at=start_at,
            identity=identity,
            part_number=part_number,
        )
        part_number += 1
        target = stage / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        part_state: dict[str, object] = {
            "relative": relative,
            "target": target,
            "rows": 0,
            "logical_bytes": 0,
            "content": hashlib.sha256(),
            "min_time": None,
            "max_time": None,
            "min_id": None,
            "max_id": None,
            "null_counts": defaultdict(int),
        }
        return (
            pq.ParquetWriter(
                target,
                schema,
                compression="zstd",
                compression_level=6,
                use_dictionary=True,
                write_statistics=True,
            ),
            part_state,
        )

    def close_part() -> None:
        nonlocal writer, state
        if writer is None or state is None:
            return
        writer.close()
        entries.append(
            _entry_from_state(
                state=state,
                spec=spec,
                schema=schema,
                start_at=start_at,
                end_at=end_at,
            )
        )
        writer = None
        state = None

    try:
        async with session_factory() as session:
            result = await session.stream(
                text(spec.query),
                {"epoch_id": epoch_id, "start_at": start_at, "end_at": end_at},
            )
            async for partition in result.mappings().partitions(chunk_rows):
                normalized = [_normalize_row(dict(row), spec.model) for row in partition]
                offset = 0
                while offset < len(normalized):
                    if writer is None or state is None:
                        writer, state = open_part()
                    remaining = max_file_rows - _object_int(state["rows"])
                    subset = normalized[offset : offset + remaining]
                    table = pa.Table.from_pylist(subset, schema=schema)
                    writer.write_table(table, row_group_size=chunk_rows)
                    _update_part_state(state, subset, spec, schema)
                    offset += len(subset)
                    if _object_int(state["rows"]) >= max_file_rows:
                        close_part()
        if writer is None and not entries:
            writer, state = open_part()
        close_part()
        return entries
    finally:
        if writer is not None:
            writer.close()


def _entry_from_state(
    *,
    state: dict[str, object],
    spec: _ExportSpec,
    schema: pa.Schema,
    start_at: datetime,
    end_at: datetime,
) -> dict[str, object]:
    target = cast(Path, state["target"])
    relative = cast(Path, state["relative"])
    logical = _object_int(state["logical_bytes"])
    parquet_bytes = target.stat().st_size
    rows = _object_int(state["rows"])
    content = cast(Any, state["content"])
    minimum_time = cast(datetime | None, state["min_time"])
    maximum_time = cast(datetime | None, state["max_time"])
    return {
        "family": spec.name,
        "table": spec.name,
        "source_table": spec.model.__tablename__,
        "family_schema_version": spec.archive_schema_version,
        "start_at": start_at.isoformat(),
        "end_at": end_at.isoformat(),
        "source_scope_method": spec.mode,
        "source_query_sha256": hashlib.sha256(" ".join(spec.query.split()).encode()).hexdigest(),
        "source_row_count": rows,
        "exported_row_count": rows,
        "uncompressed_logical_bytes": logical,
        "parquet_bytes": parquet_bytes,
        "compression_ratio": round(logical / parquet_bytes, 6) if parquet_bytes else None,
        "file": relative.as_posix(),
        "file_sha256": _sha256_file(target),
        "content_sha256": content.hexdigest(),
        "min_timestamp": minimum_time.isoformat() if minimum_time else None,
        "max_timestamp": maximum_time.isoformat() if maximum_time else None,
        "min_id": state["min_id"],
        "max_id": state["max_id"],
        "key_columns": list(spec.key_columns),
        "null_counts": dict(cast(dict[str, int], state["null_counts"])),
        "schema": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in schema
        ],
    }


def _update_part_state(
    state: dict[str, object],
    rows: list[dict[str, object]],
    spec: _ExportSpec,
    schema: pa.Schema,
) -> None:
    content = cast(Any, state["content"])
    null_counts = cast(defaultdict[str, int], state["null_counts"])
    for row in rows:
        encoded = _canonical_row(row, schema.names)
        content.update(encoded)
        state["logical_bytes"] = _object_int(state["logical_bytes"]) + len(encoded)
        observed = row[spec.timestamp_column]
        if isinstance(observed, datetime):
            previous_min = cast(datetime | None, state["min_time"])
            previous_max = cast(datetime | None, state["max_time"])
            state["min_time"] = observed if previous_min is None else min(previous_min, observed)
            state["max_time"] = observed if previous_max is None else max(previous_max, observed)
        identifier = _row_identifier(row, spec.key_columns)
        if identifier is not None:
            previous_min_id = cast(str | None, state["min_id"])
            previous_max_id = cast(str | None, state["max_id"])
            state["min_id"] = (
                identifier if previous_min_id is None else min(previous_min_id, identifier)
            )
            state["max_id"] = (
                identifier if previous_max_id is None else max(previous_max_id, identifier)
            )
        for name in schema.names:
            if row[name] is None:
                null_counts[name] += 1
    state["rows"] = _object_int(state["rows"]) + len(rows)


async def _verify_source_counts(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    entries: list[dict[str, object]],
    epoch_id: uuid.UUID,
) -> None:
    grouped: dict[tuple[str, str, str], int] = defaultdict(int)
    for entry in entries:
        key = (
            cast(str, entry["family"]),
            cast(str, entry["start_at"]),
            cast(str, entry["end_at"]),
        )
        grouped[key] += _object_int(entry["exported_row_count"])
    async with session_factory() as session:
        for (family, start_text, end_text), exported in grouped.items():
            spec = _SPECS_BY_NAME[family]
            source_sql = spec.query.rsplit("ORDER BY", maxsplit=1)[0]
            count = int(
                cast(
                    int,
                    await session.scalar(
                        text(f"SELECT count(*) FROM ({source_sql}) AS archive_source"),
                        {
                            "epoch_id": epoch_id,
                            "start_at": datetime.fromisoformat(start_text),
                            "end_at": datetime.fromisoformat(end_text),
                        },
                    ),
                )
            )
            if count != exported:
                raise ArchiveVerificationError(
                    f"source scope changed during export for {family}: {count} != {exported}"
                )


def _verify_entry(root: Path, entry: dict[str, object]) -> None:
    file_path = root / cast(str, entry["file"])
    if not file_path.is_file():
        raise ArchiveVerificationError(f"archive file is missing: {file_path}")
    if _sha256_file(file_path) != entry["file_sha256"]:
        raise ArchiveVerificationError(f"archive file checksum mismatch: {file_path}")
    parquet = pq.ParquetFile(file_path)
    expected_schema = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in parquet.schema_arrow
    ]
    if expected_schema != entry["schema"]:
        raise ArchiveVerificationError(f"archive schema mismatch: {file_path}")
    if parquet.metadata.num_rows != _object_int(entry["exported_row_count"]):
        raise ArchiveVerificationError(f"archive row-count mismatch: {file_path}")
    spec = _SPECS_BY_NAME.get(cast(str, entry["family"]))
    if spec is None:
        raise ArchiveVerificationError(f"unknown archive family: {entry['family']}")
    content = hashlib.sha256()
    minimum_time: datetime | None = None
    maximum_time: datetime | None = None
    minimum_id: str | None = None
    maximum_id: str | None = None
    null_counts: defaultdict[str, int] = defaultdict(int)
    for batch in parquet.iter_batches(batch_size=25_000):
        for row in batch.to_pylist():
            content.update(_canonical_row(row, batch.schema.names))
            observed = row[spec.timestamp_column]
            if isinstance(observed, datetime):
                minimum_time = observed if minimum_time is None else min(minimum_time, observed)
                maximum_time = observed if maximum_time is None else max(maximum_time, observed)
            identifier = _row_identifier(row, spec.key_columns)
            if identifier is not None:
                minimum_id = identifier if minimum_id is None else min(minimum_id, identifier)
                maximum_id = identifier if maximum_id is None else max(maximum_id, identifier)
            for name in batch.schema.names:
                if row[name] is None:
                    null_counts[name] += 1
    if content.hexdigest() != entry["content_sha256"]:
        raise ArchiveVerificationError(f"archive content checksum mismatch: {file_path}")
    if (minimum_time.isoformat() if minimum_time else None) != entry["min_timestamp"]:
        raise ArchiveVerificationError(f"archive minimum timestamp mismatch: {file_path}")
    if (maximum_time.isoformat() if maximum_time else None) != entry["max_timestamp"]:
        raise ArchiveVerificationError(f"archive maximum timestamp mismatch: {file_path}")
    if minimum_id != entry["min_id"] or maximum_id != entry["max_id"]:
        raise ArchiveVerificationError(f"archive identifier bounds mismatch: {file_path}")
    if dict(null_counts) != entry["null_counts"]:
        raise ArchiveVerificationError(f"archive null-count mismatch: {file_path}")


def _duckdb_integrity(root: Path, entries: list[dict[str, object]]) -> dict[str, object]:
    files_by_family: dict[str, list[str]] = defaultdict(list)
    for entry in entries:
        files_by_family[cast(str, entry["family"])].append(str(root / cast(str, entry["file"])))
    connection = duckdb.connect(database=":memory:")
    duplicate_failures: dict[str, int] = {}
    try:
        for family, files in files_by_family.items():
            connection.from_parquet(files).create_view(family)
            keys = _SPECS_BY_NAME[family].key_columns
            if keys:
                group = ", ".join(f'"{key}"' for key in keys)
                duplicates = _duckdb_int(
                    connection,
                    f"SELECT count(*) FROM (SELECT {group}, count(*) n FROM {family} "
                    f"GROUP BY {group} HAVING count(*) > 1)",
                )
                if duplicates:
                    duplicate_failures[family] = duplicates
        if duplicate_failures:
            raise ArchiveVerificationError(
                f"archive contains duplicate primary identities: {duplicate_failures}"
            )
        relations = {
            "observations_missing_pair": (
                "observations",
                "pairs",
                "SELECT count(*) FROM observations o LEFT JOIN pairs p ON p.id=o.pair_id "
                "WHERE p.id IS NULL",
            ),
            "pairs_missing_token": (
                "pairs",
                "tokens",
                "SELECT count(*) FROM pairs p LEFT JOIN tokens t ON t.id=p.token_id "
                "WHERE t.id IS NULL",
            ),
            "lifecycle_missing_token": (
                "lifecycle_events",
                "tokens",
                "SELECT count(*) FROM lifecycle_events e LEFT JOIN tokens t ON t.id=e.token_id "
                "WHERE t.id IS NULL",
            ),
            "pair_facts_missing_pair": (
                "pair_fact_events",
                "pairs",
                "SELECT count(*) FROM pair_fact_events e LEFT JOIN pairs p ON p.id=e.pair_id "
                "WHERE p.id IS NULL",
            ),
            "boosts_missing_token": (
                "boost_observations",
                "tokens",
                "SELECT count(*) FROM boost_observations b LEFT JOIN tokens t ON t.id=b.token_id "
                "WHERE t.id IS NULL",
            ),
            "metadata_missing_token": (
                "token_metadata_events",
                "tokens",
                "SELECT count(*) FROM token_metadata_events m LEFT JOIN tokens t "
                "ON t.id=m.token_id WHERE t.id IS NULL",
            ),
            "security_missing_token": (
                "token_security_snapshots",
                "tokens",
                "SELECT count(*) FROM token_security_snapshots s LEFT JOIN tokens t "
                "ON t.id=s.token_id WHERE t.id IS NULL",
            ),
        }
        referential: dict[str, int] = {}
        for name, (left, right, query) in relations.items():
            if left in files_by_family and right in files_by_family:
                missing = _duckdb_int(connection, query)
                referential[name] = missing
                if missing:
                    raise ArchiveVerificationError(
                        f"archive referential integrity failed: {name}={missing}"
                    )
        return {
            "family_count": len(files_by_family),
            "primary_key_duplicate_groups": duplicate_failures,
            "referential_missing_rows": referential,
        }
    finally:
        connection.close()


def _write_verified_manifest(path: Path, manifest: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(path, manifest)
    digest = _sha256_file(path)
    sidecar = path.with_name("manifest.sha256")
    _atomic_write_text(sidecar, f"{digest}  manifest.json\n")


def _publish_files(
    *, stage: Path, output: Path, entries: list[dict[str, object]], fail_after: int | None
) -> None:
    for index, entry in enumerate(entries, start=1):
        relative = Path(cast(str, entry["file"]))
        source = stage / relative
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            if _sha256_file(target) != entry["file_sha256"]:
                raise ArchiveConflictError(
                    f"published archive path contains different bytes: {target}"
                )
            source.unlink()
        else:
            os.replace(source, target)
        if fail_after is not None and index >= fail_after:
            raise RuntimeError("injected exporter interruption after file publication")


def _prepare_staging(stage: Path) -> None:
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True, exist_ok=False)


def _ensure_disk_capacity(
    output: Path,
    *,
    estimated_source_bytes: int,
    minimum_free_bytes: int,
    disk_free_override_bytes: int | None,
) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    free = (
        disk_free_override_bytes
        if disk_free_override_bytes is not None
        else shutil.disk_usage(output.parent).free
    )
    required_staging = max(256 * 1024**2, int(estimated_source_bytes * 0.55))
    required = minimum_free_bytes + required_staging
    if free < required:
        raise InsufficientArchiveDiskError(
            f"archive preflight requires {required} free bytes including safety floor; "
            f"only {free} are available"
        )


def _estimated_scope_bytes(
    *,
    database_bytes: int,
    epoch_start: datetime,
    epoch_end: datetime,
    start_at: datetime,
    end_at: datetime,
) -> int:
    epoch_seconds = max(1.0, (epoch_end - epoch_start).total_seconds())
    scope_seconds = max(1.0, (end_at - start_at).total_seconds())
    fraction = min(1.0, scope_seconds / epoch_seconds)
    return max(256 * 1024**2, int(database_bytes * fraction))


def _data_path(
    *,
    spec: _ExportSpec,
    epoch_number: int,
    start_at: datetime,
    identity: str,
    part_number: int,
) -> Path:
    return Path(
        f"schema=v2/family={spec.name}/year={start_at.year:04d}/"
        f"month={start_at.month:02d}/day={start_at.day:02d}/epoch={epoch_number}/"
        f"scope={identity}/part-{part_number:05d}.parquet"
    )


def _manifest_path(output: Path, epoch_number: int, identity: str) -> Path:
    return (
        output
        / "schema=v2"
        / "manifests"
        / f"epoch={epoch_number}"
        / f"scope={identity}"
        / "manifest.json"
    )


def _archive_root(manifest_path: Path, *, version: int) -> Path:
    return manifest_path.parents[4] if version == 2 else manifest_path.parents[2]


def _read_manifest(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ArchiveVerificationError("archive manifest must be a JSON object")
    return cast(dict[str, object], value)


def _aggregate_file_digest(entries: list[dict[str, object]]) -> str:
    return canonical_sha256(
        sorted((cast(str, entry["file"]), cast(str, entry["file_sha256"])) for entry in entries)
    )


def _arrow_schema(model: type[Any]) -> pa.Schema:
    return pa.schema(
        [
            pa.field(column.name, _arrow_type(column.type), nullable=column.nullable)
            for column in model.__table__.columns
        ]
    )


def _arrow_type(column_type: Any) -> pa.DataType:
    if isinstance(column_type, Uuid):
        return pa.string()
    if isinstance(column_type, DateTime):
        return pa.timestamp("us", tz="UTC")
    if isinstance(column_type, JSONB):
        return pa.string()
    if isinstance(column_type, Numeric):
        precision = column_type.precision or 38
        if precision <= 38:
            return pa.decimal128(precision, column_type.scale or 0)
        if precision <= 76:
            return pa.decimal256(precision, column_type.scale or 0)
        # Arrow decimal256 stops at 76 digits. Canonical decimal text is the
        # only lossless representation for wider PostgreSQL NUMERIC values.
        return pa.string()
    if isinstance(column_type, BigInteger):
        return pa.int64()
    if isinstance(column_type, Integer):
        return pa.int32()
    if isinstance(column_type, Boolean):
        return pa.bool_()
    if isinstance(column_type, String):
        return pa.string()
    raise TypeError(f"unsupported archive column type: {column_type!r}")


def _normalize_row(row: dict[str, object], model: type[Any]) -> dict[str, object]:
    result: dict[str, object] = {}
    for column in model.__table__.columns:
        value = row[column.name]
        if value is None:
            result[column.name] = None
        elif isinstance(column.type, Uuid):
            result[column.name] = str(value)
        elif isinstance(column.type, DateTime):
            if not isinstance(value, datetime):
                raise TypeError(f"{column.name} must be a datetime")
            result[column.name] = _utc(value, column.name)
        elif isinstance(column.type, JSONB):
            result[column.name] = json.dumps(
                value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, default=str
            )
        elif isinstance(column.type, Numeric):
            decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
            result[column.name] = (
                format(decimal_value, "f") if (column.type.precision or 38) > 76 else decimal_value
            )
        else:
            result[column.name] = value
    return result


def _canonical_row(row: dict[str, object], columns: list[str]) -> bytes:
    values: list[object] = []
    for column in columns:
        value = row[column]
        if isinstance(value, datetime):
            value = _utc(value, column).isoformat()
        elif isinstance(value, Decimal):
            value = format(value, "f")
        values.append(value)
    return (
        json.dumps(values, separators=(",", ":"), ensure_ascii=True, default=str) + "\n"
    ).encode()


def _row_identifier(row: dict[str, object], keys: tuple[str, ...]) -> str | None:
    if not keys:
        return None
    return "|".join(str(row[key]) for key in keys)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        for block in iter(lambda: file_handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _atomic_write_json(path: Path, value: dict[str, object]) -> None:
    _atomic_write_text(
        path,
        json.dumps(value, indent=2, sort_keys=True, default=str) + "\n",
    )


def _atomic_write_text(path: Path, value: str) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{uuid.uuid4().hex}")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _daily_ranges(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    result: list[tuple[datetime, datetime]] = []
    cursor = start
    while cursor < end:
        boundary = datetime.combine(cursor.date(), time.min, tzinfo=UTC) + timedelta(days=1)
        next_cursor = min(boundary, end)
        result.append((cursor, next_cursor))
        cursor = next_cursor
    return result


def _verify_legacy_archive(manifest_path: Path, manifest: dict[str, object]) -> dict[str, object]:
    """Retain read compatibility for verified schema-v1 archives."""
    root = _archive_root(manifest_path, version=1)
    verified_rows = 0
    for entry_value in cast(list[object], manifest["entries"]):
        entry = cast(dict[str, object], entry_value)
        file_path = root / cast(str, entry["file"])
        if not file_path.is_file() or _sha256_file(file_path) != entry["file_sha256"]:
            raise ArchiveVerificationError(f"legacy archive checksum failure: {file_path}")
        parquet = pq.ParquetFile(file_path)
        if parquet.metadata.num_rows != _object_int(entry["exported_row_count"]):
            raise ArchiveVerificationError(f"legacy archive row-count failure: {file_path}")
        verified_rows += parquet.metadata.num_rows
    if verified_rows != _object_int(manifest["exported_row_count"]):
        raise ArchiveVerificationError("legacy manifest row total mismatch")
    return {
        "verified": True,
        "manifest_verified": False,
        "manifest": str(manifest_path),
        "manifest_sha256": _sha256_file(manifest_path),
        "archive_identity_sha256": manifest["archive_identity_sha256"],
        "row_count": verified_rows,
        "file_count": len(cast(list[object], manifest["entries"])),
        "source_scope_fully_covered": False,
        "duckdb_readback_passed": False,
        "legacy_schema": True,
    }


def _object_int(value: object) -> int:
    if not isinstance(value, int):
        raise ArchiveVerificationError(
            f"expected integer archive value, got {type(value).__name__}"
        )
    return value


def _duckdb_int(connection: duckdb.DuckDBPyConnection, query: str) -> int:
    row = connection.execute(query).fetchone()
    if row is None or not isinstance(row[0], int):
        raise ArchiveVerificationError("DuckDB integrity query did not return an integer")
    return row[0]
