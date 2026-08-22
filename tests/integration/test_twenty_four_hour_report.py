from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.persistence.models import (
    ApiRequestLog,
    DeduplicationConflict,
    DiscoveryEvent,
    LifecycleEvent,
    Observation,
    Pair,
    PollBatch,
    PollBatchMember,
    PollBatchOutcome,
    PollScheduleDecision,
    Token,
)
from pump_research.reporting.twenty_four_hour import generate_report, write_report_files


@pytest.mark.integration
async def test_24_hour_report_uses_durable_facts_and_writes_both_formats(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    report_end = datetime(2026, 8, 14, 12, tzinfo=UTC)
    hour = report_end - timedelta(hours=2)
    token_id, pair_id, request_id = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()
    first_batch_id, second_batch_id = uuid.uuid4(), uuid.uuid4()
    snapshot = {
        "component": "adaptive_scheduler",
        "schema_version": 1,
        "batch_size": 30,
        "interval_seconds": {"NEW": 5, "RESURRECTED": 5},
    }

    async with session_factory() as session, session.begin():
        session.add(
            Token(
                id=token_id,
                chain="solana",
                address="report-token",
                first_discovered_at=hour,
            )
        )
        await session.flush()
        session.add(Pair(id=pair_id, token_id=token_id, chain="solana", address="report-pair"))
        await session.flush()
        session.add_all(
            [
                DiscoveryEvent(
                    token_id=token_id,
                    idempotency_key="report-discovery",
                    provider="test",
                    event_type="token_seen",
                    received_at=hour,
                    source_payload={},
                    source_payload_sha256="a" * 64,
                ),
                ApiRequestLog(
                    id=request_id,
                    idempotency_key="report-request",
                    provider="test",
                    endpoint="/tokens",
                    requested_at=hour,
                    received_at=hour + timedelta(milliseconds=125),
                    outcome="succeeded",
                    http_status_code=429,
                    request_payload={},
                    response_payload={},
                    response_payload_sha256="b" * 64,
                ),
                Observation(
                    id=uuid.uuid4(),
                    received_at=hour + timedelta(milliseconds=125),
                    pair_id=pair_id,
                    api_request_log_id=request_id,
                    price_usd=Decimal("1.0"),
                    liquidity_usd=None,
                    volume_m5_usd=Decimal("2.0"),
                    volume_h1_usd=None,
                ),
                LifecycleEvent(
                    token_id=token_id,
                    idempotency_key="report-pending",
                    previous_state=None,
                    new_state="PENDING_DEX",
                    decided_at=hour - timedelta(minutes=10),
                    input_watermark=hour - timedelta(minutes=10),
                    reason_code="discovered",
                    configuration_sha256="c" * 64,
                    configuration_snapshot={},
                ),
                LifecycleEvent(
                    token_id=token_id,
                    idempotency_key="report-new",
                    previous_state="PENDING_DEX",
                    new_state="NEW",
                    decided_at=hour + timedelta(minutes=1),
                    input_watermark=hour + timedelta(minutes=1),
                    reason_code="dex_pair_present",
                    configuration_sha256="c" * 64,
                    configuration_snapshot={},
                ),
                LifecycleEvent(
                    token_id=token_id,
                    idempotency_key="report-resurrection",
                    previous_state="DORMANT",
                    new_state="RESURRECTED",
                    decided_at=hour + timedelta(minutes=2),
                    input_watermark=hour + timedelta(minutes=2),
                    reason_code="activity_returned",
                    configuration_sha256="c" * 64,
                    configuration_snapshot={},
                ),
                DeduplicationConflict(
                    record_type="discovery_event",
                    idempotency_key="report-discovery",
                    occurred_at=hour,
                ),
                PollBatch(
                    id=first_batch_id,
                    provider="test",
                    chain="solana",
                    claimed_at=hour,
                    lease_expires_at=hour + timedelta(minutes=2),
                    reserved_request_capacity=1,
                    configuration_sha256="d" * 64,
                    configuration_snapshot=snapshot,
                ),
                PollBatch(
                    id=second_batch_id,
                    provider="test",
                    chain="solana",
                    claimed_at=hour + timedelta(hours=1),
                    lease_expires_at=hour + timedelta(hours=1, minutes=2),
                    reserved_request_capacity=1,
                    configuration_sha256="d" * 64,
                    configuration_snapshot=snapshot,
                ),
                PollBatchMember(
                    batch_id=first_batch_id,
                    token_id=token_id,
                    claimed_at=hour,
                    due_at=hour,
                    lifecycle_state="NEW",
                    priority=0,
                    claim_lateness_ms=0,
                ),
                PollBatchMember(
                    batch_id=second_batch_id,
                    token_id=token_id,
                    claimed_at=hour + timedelta(hours=1),
                    due_at=hour + timedelta(hours=1),
                    lifecycle_state="RESURRECTED",
                    priority=1,
                    claim_lateness_ms=0,
                ),
                PollBatchOutcome(
                    batch_id=first_batch_id,
                    api_request_log_id=request_id,
                    outcome="succeeded",
                    completed_at=hour,
                    member_count=1,
                    observation_lateness_min_ms=0,
                    observation_lateness_max_ms=0,
                    observation_lateness_mean_ms=Decimal("0"),
                    configuration_sha256="d" * 64,
                    configuration_snapshot=snapshot,
                ),
                PollScheduleDecision(
                    token_id=token_id,
                    idempotency_key="report-schedule",
                    new_state="NEW",
                    new_due_at=hour,
                    decided_at=hour,
                    reason_code="initial",
                    configuration_sha256="d" * 64,
                    configuration_snapshot=snapshot,
                ),
            ]
        )

    report = await generate_report(session_factory, end_at=report_end)
    report_hour = next(
        entry for entry in report["hourly"] if entry["hour_start"] == "2026-08-14T10:00:00Z"
    )
    assert report_hour["tokens_discovered"] == 1
    assert report_hour["tokens_reaching_dex"] == 1
    assert report_hour["pending_tokens"] == 0
    assert report_hour["observations"] == 1
    assert report_hour["requests"] == 1
    assert report_hour["http_429s"] == 1
    assert report_hour["api_latency_mean_ms"] == 125.0
    assert report_hour["api_latency_p50_ms"] == 125.0
    assert report_hour["api_latency_p95_ms"] == 125.0
    assert report_hour["batch_occupancy_pct"] == pytest.approx(3.333)
    assert report_hour["expected_polling_cadence_seconds"] == 5.0
    assert report_hour["null_rates"]["liquidity_usd"] == 1.0
    assert report_hour["duplicate_rate"] == pytest.approx(0.25)
    assert report["largest_poll_gaps"][0]["gap_seconds"] == 3600.0

    markdown_path, json_path = write_report_files(report, tmp_path)
    assert markdown_path.name == "24h_report.md"
    assert json_path.name == "24h_report.json"
    assert "Hourly metrics" in markdown_path.read_text(encoding="utf-8")
    assert json.loads(json_path.read_text(encoding="utf-8"))["schema_version"] == 1
