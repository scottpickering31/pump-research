from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research.archival import archive_stats, export_epoch_range, verify_archive
from pump_research.archive_analytics import run_archive_analytics
from pump_research.backup import backup_status, verify_backup
from pump_research.collection.runtime import CollectorRuntime
from pump_research.config import Settings
from pump_research.database_safety import assert_destructive_test_database
from pump_research.epochs import (
    CollectionEpochError,
    close_epoch,
    create_epoch,
    get_epoch_status,
    start_epoch,
)
from pump_research.logging import get_logger
from pump_research.monitoring.status import read_collector_status
from pump_research.monitoring.storage import record_storage_sample
from pump_research.persistence.models import (
    ApiRequestLog,
    CollectionEpoch,
    DiscoveryEvent,
    LifecycleEvent,
    Observation,
    Pair,
    PollSchedule,
    PollScheduleDecision,
    StorageRelationSample,
    Token,
)
from pump_research.persistence.repositories import CollectorRunRepository, TokenRepository
from pump_research.reporting.twenty_four_hour import generate_report
from pump_research.scheduling.policy import LifecycleState
from pump_research.scheduling.scheduler import AdaptiveScheduler

NOW = datetime(2026, 8, 15, 20, tzinfo=UTC)


def _settings() -> Settings:
    return Settings(
        environment="test",
        database_url=(
            "postgresql+asyncpg://pump_research:pump_research@localhost:5433/"
            "pump_research_capacity_test"
        ),
    )


@pytest.mark.integration
async def test_actual_connected_test_database_is_guarded(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    bind = session_factory.kw["bind"]
    async with bind.connect() as connection:
        result = await assert_destructive_test_database(
            connection,
            environment="test",
            explicit_test_database_url=True,
            operation="guard integration proof",
        )
        actual = await connection.scalar(text("SELECT current_database()"))
    assert result.destructive_test_operations_permitted
    assert actual == make_url(os.environ["PUMP_RESEARCH_TEST_DATABASE_URL"]).database


@pytest.mark.integration
async def test_epoch_transitions_are_audited_and_only_one_can_run(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = _settings()
    async with session_factory() as session, session.begin():
        first = await create_epoch(
            session, settings, epoch_number=1, purpose="24h research validation", now=NOW
        )
        await create_epoch(session, settings, epoch_number=2, purpose="future research", now=NOW)
        await start_epoch(session, epoch_number=1, now=NOW + timedelta(seconds=1))
        with pytest.raises(CollectionEpochError, match="already running"):
            await start_epoch(session, epoch_number=2, now=NOW + timedelta(seconds=2))
    async with session_factory() as session, session.begin():
        with pytest.raises(DBAPIError, match="immutable"):
            await session.execute(
                update(CollectionEpoch)
                .where(CollectionEpoch.id == first.id)
                .values(purpose="retrospective mutation")
            )


@pytest.mark.integration
async def test_failed_run_is_visible_and_invalid_epoch_is_filtered_by_default(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = _settings()
    reason = (
        "scheduler capacity-decision unique-key failure caused an unrecoverable "
        "observation continuity gap"
    )
    async with session_factory() as session, session.begin():
        epoch = await create_epoch(
            session, settings, epoch_number=1, purpose="24h research validation", now=NOW
        )
        await start_epoch(session, epoch_number=1, now=NOW)
        run = await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="test",
            configuration_sha256="a" * 64,
            configuration_snapshot={},
            collection_epoch_id=epoch.id,
        )
        await CollectorRunRepository().finish(
            session,
            run_id=run.id,
            finished_at=NOW + timedelta(hours=6),
            status="failed",
            failure_detail={"reason": "scheduler_capacity_decision_unique_key_failure"},
        )

    interrupted = await read_collector_status(session_factory, settings)
    assert interrupted["operational_state"] == "FAILED"
    assert interrupted["actively_collecting"] is False
    assert interrupted["continuity_warning"] is not None
    assert interrupted["collection_epoch"]["status"] == "running"

    async with session_factory() as session, session.begin():
        invalid = await close_epoch(
            session,
            epoch_number=1,
            status="invalid",
            reason=reason,
            now=NOW + timedelta(hours=7),
        )
    assert invalid.status == "invalid"
    assert invalid.data_valid is False
    assert invalid.invalid_reason == reason
    async with session_factory() as session:
        rebuilt = await get_epoch_status(session, 1)
    assert rebuilt.data_valid is False
    assert rebuilt.invalid_reason == reason

    with pytest.raises(ValueError, match="excluded from research reports by default"):
        await generate_report(
            session_factory,
            epoch_number=1,
            end_at=NOW + timedelta(hours=7),
        )
    engineering_report = await generate_report(
        session_factory,
        epoch_number=1,
        end_at=NOW + timedelta(hours=7),
        include_invalid=True,
    )
    assert engineering_report["collection_epoch"]["data_valid"] is False


@pytest.mark.integration
async def test_running_epoch_with_stale_collector_heartbeat_is_not_reported_healthy(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = _settings()
    async with session_factory() as session, session.begin():
        epoch = await create_epoch(
            session, settings, epoch_number=1, purpose="stale heartbeat test", now=NOW
        )
        await start_epoch(session, epoch_number=1, now=NOW)
        await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="test",
            configuration_sha256="a" * 64,
            configuration_snapshot={},
            collection_epoch_id=epoch.id,
        )

    status = await read_collector_status(session_factory, settings)

    assert status["collector_run"]["status"] == "running"
    assert status["operational_state"] == "STALE"
    assert status["actively_collecting"] is False
    assert status["continuity_warning"] is not None


@pytest.mark.integration
async def test_new_epoch_rebases_overdue_projection_once_without_state_reset(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = _settings()
    scheduler = AdaptiveScheduler(session_factory, settings)
    token_repository = TokenRepository()
    async with session_factory() as session, session.begin():
        first = await create_epoch(
            session, settings, epoch_number=1, purpose="failed validation", now=NOW
        )
        await start_epoch(session, epoch_number=1, now=NOW)
        failed_run = await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="test",
            configuration_sha256="a" * 64,
            configuration_snapshot={},
            collection_epoch_id=first.id,
        )
        token = await token_repository.get_or_create(
            session,
            chain="solana",
            address="epoch-rebase-active",
            first_discovered_at=NOW,
        )
        await scheduler.set_lifecycle_state_in_session(
            session,
            token_id=token.id,
            state=LifecycleState.ACTIVE,
            decided_at=NOW,
            admitted_at=NOW,
            reason_code="epoch1_test_state",
        )
        schedule = await session.get(PollSchedule, token.id)
        assert schedule is not None
        schedule.next_due_at = NOW - timedelta(hours=5)
        await CollectorRunRepository().finish(
            session,
            run_id=failed_run.id,
            finished_at=NOW + timedelta(hours=6),
            status="failed",
            failure_detail={"reason": "test_gap"},
        )
        await close_epoch(
            session,
            epoch_number=1,
            status="invalid",
            reason="test continuity gap",
            now=NOW + timedelta(hours=6),
        )
        second = await create_epoch(
            session,
            settings,
            epoch_number=2,
            purpose="clean 24h validation",
            now=NOW + timedelta(hours=7),
        )

    runtime = CollectorRuntime(
        session_factory,
        settings,
        logger=get_logger(test="epoch-rebase"),
        epoch_number=2,
        epoch_initializer=scheduler,
    )
    startup = await runtime.start()
    assert startup.collection_epoch_id == second.id
    async with session_factory() as session:
        rebased = await session.get(PollSchedule, token.id)
        rebase_decisions = list(
            (
                await session.execute(
                    select(PollScheduleDecision).where(
                        PollScheduleDecision.collection_epoch_id == second.id,
                        PollScheduleDecision.reason_code == "epoch_start_rebase",
                    )
                )
            ).scalars()
        )
    assert rebased is not None
    assert rebased.lifecycle_state == LifecycleState.ACTIVE.value
    assert rebased.next_due_at is not None
    assert rebased.next_due_at > NOW
    assert rebased.lease_id is None
    assert len(rebase_decisions) == 1
    first_rebased_due = rebased.next_due_at

    restarted_runtime = CollectorRuntime(
        session_factory,
        settings,
        logger=get_logger(test="epoch-restart"),
        epoch_number=2,
        epoch_initializer=scheduler,
    )
    await restarted_runtime.start()
    async with session_factory() as session:
        restarted_schedule = await session.get(PollSchedule, token.id)
        rebase_count = await session.scalar(
            select(func.count())
            .select_from(PollScheduleDecision)
            .where(
                PollScheduleDecision.collection_epoch_id == second.id,
                PollScheduleDecision.reason_code == "epoch_start_rebase",
            )
        )
    assert restarted_schedule is not None
    assert restarted_schedule.next_due_at == first_rebased_due
    assert rebase_count == 1


@pytest.mark.integration
async def test_storage_archive_analytics_and_backup_are_readable(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    settings = _settings()
    async with session_factory() as session, session.begin():
        epoch = await create_epoch(
            session, settings, epoch_number=1, purpose="24h research validation", now=NOW
        )
        await start_epoch(session, epoch_number=1, now=NOW)
        run = await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="test",
            configuration_sha256="a" * 64,
            configuration_snapshot={"test": True},
            collection_epoch_id=epoch.id,
        )
        token = Token(chain="solana", address="epoch1-token", first_discovered_at=NOW)
        session.add(token)
        await session.flush()
        pair = Pair(
            token_id=token.id,
            chain="solana",
            address="epoch1-pair",
            dex_identifier="pumpswap",
            first_discovered_at=NOW,
        )
        session.add(pair)
        request = ApiRequestLog(
            collector_run_id=run.id,
            idempotency_key="epoch1-request",
            provider="dex_screener",
            endpoint="test",
            requested_at=NOW + timedelta(minutes=1),
            received_at=NOW + timedelta(minutes=1, seconds=1),
            outcome="succeeded",
            http_status_code=200,
            request_payload={},
            response_payload={},
            response_payload_sha256="b" * 64,
        )
        session.add(request)
        session.add(
            DiscoveryEvent(
                collector_run_id=run.id,
                token_id=token.id,
                idempotency_key="epoch1-discovery",
                provider="test",
                provider_event_id="1",
                event_type="token_created",
                source_event_at=NOW,
                received_at=NOW,
                source_payload={"mint": token.address},
                source_payload_sha256="c" * 64,
            )
        )
        await session.flush()
        session.add(
            Observation(
                id=uuid.uuid4(),
                received_at=NOW + timedelta(minutes=1, seconds=1),
                pair_id=pair.id,
                api_request_log_id=request.id,
                price_usd=Decimal("0.10"),
                liquidity_usd=Decimal("1000"),
                volume_m5_usd=Decimal("100"),
                buys_m5=10,
                sells_m5=5,
            )
        )
        session.add(
            LifecycleEvent(
                collector_run_id=run.id,
                token_id=token.id,
                idempotency_key="epoch1-lifecycle",
                previous_state="PENDING_DEX",
                new_state="NEW",
                decided_at=NOW + timedelta(minutes=1, seconds=1),
                input_watermark=NOW + timedelta(minutes=1, seconds=1),
                reason_code="dex_pair_present",
                reason_detail={},
                configuration_sha256="d" * 64,
                configuration_snapshot={},
            )
        )
    await record_storage_sample(
        session_factory, collector_run_id=run.id, sampled_at=NOW + timedelta(minutes=2)
    )
    async with session_factory() as session, session.begin():
        await CollectorRunRepository().finish(
            session,
            run_id=run.id,
            finished_at=NOW + timedelta(minutes=5),
            status="cancelled",
        )
        await close_epoch(
            session,
            epoch_number=1,
            status="completed",
            reason="synthetic integration range complete",
            now=NOW + timedelta(minutes=5),
        )
    report = await generate_report(
        session_factory,
        epoch_number=1,
        end_at=NOW + timedelta(minutes=5),
    )
    assert report["collection_epoch"]["epoch_number"] == 1
    assert report["validation"]["dataset"]["observation_rows"] == 1
    manifest = await export_epoch_range(
        session_factory,
        epoch_number=1,
        start_at=NOW,
        end_at=NOW + timedelta(minutes=5),
        output=tmp_path / "archive",
        chunk_rows=10,
        now=NOW + timedelta(minutes=10),
    )
    repeated_manifest = await export_epoch_range(
        session_factory,
        epoch_number=1,
        start_at=NOW,
        end_at=NOW + timedelta(minutes=5),
        output=tmp_path / "archive",
        chunk_rows=10,
        now=NOW + timedelta(minutes=10),
    )
    assert repeated_manifest == manifest
    verification = await verify_archive(manifest)
    stats = archive_stats(manifest)
    analytics = run_archive_analytics(manifest)
    assert verification["verified"] is True
    assert isinstance(stats["exported_rows"], int)
    assert stats["exported_rows"] >= 4
    assert analytics["observation_count"] == 1
    assert analytics["analytical_reads_passed"] is True

    dump = tmp_path / "epoch1-metadata.sql"
    dump.write_text(
        "-- PostgreSQL database dump\nSELECT 1;\n-- PostgreSQL database dump complete\n",
        encoding="utf-8",
    )
    backup = await verify_backup(
        session_factory,
        epoch_number=1,
        path=dump,
        independent_copy=True,
        project_root=Path.cwd(),
    )
    status = await backup_status(session_factory, epoch_number=1)
    assert backup["verified"] is True
    assert status["independent_backup_present"] is True
    async with session_factory() as session:
        telemetry_rows = await session.scalar(select(StorageRelationSample).limit(1))
    assert telemetry_rows is not None
