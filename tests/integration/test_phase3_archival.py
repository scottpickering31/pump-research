from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from pump_research import archival as archival_module
from pump_research.archival import (
    ArchiveVerificationError,
    InsufficientArchiveDiskError,
    export_epoch_range,
    verify_archive,
)
from pump_research.archive_analytics import ColdArchiveQuery, run_archive_analytics
from pump_research.archive_catalog import (
    ArchiveBusyError,
    ArchiveCatalogError,
    archive_status,
    evaluate_retention_eligibility,
)
from pump_research.archive_storage import (
    FilesystemObjectStore,
    copy_archive,
    verify_filesystem_copy,
)
from pump_research.config import Settings
from pump_research.epochs import close_epoch, create_epoch, start_epoch
from pump_research.persistence.models import (
    ApiRequestLog,
    ArchiveCopyVerification,
    ArchiveScope,
    CollectorRun,
    DiscoveryEvent,
    LifecycleEvent,
    Observation,
    Pair,
    Token,
)
from pump_research.persistence.repositories import CollectorRunRepository

NOW = datetime(2026, 8, 15, 12, tzinfo=UTC)


def _settings() -> Settings:
    return Settings(
        database_url="postgresql+asyncpg://unused:unused@localhost/unused",
        environment="test",
    )


async def _closed_epoch(
    session_factory: async_sessionmaker[AsyncSession],
    *,
    epoch_number: int = 2,
    close_status: str = "completed",
) -> tuple[uuid.UUID, uuid.UUID, str]:
    async with session_factory() as session, session.begin():
        epoch = await create_epoch(
            session,
            _settings(),
            epoch_number=epoch_number,
            purpose="Phase 3 isolated archive test",
            now=NOW,
        )
        await start_epoch(session, epoch_number=epoch_number, now=NOW)
        run = await CollectorRunRepository().start(
            session,
            started_at=NOW,
            collector_version="phase3-test",
            configuration_sha256="a" * 64,
            configuration_snapshot={"phase": 3},
            collection_epoch_id=epoch.id,
        )
        await CollectorRunRepository().mark_collection_started(
            session, run_id=run.id, collection_started_at=NOW
        )
        token = Token(
            chain="solana",
            address=f"phase3-token-{epoch_number}",
            first_discovered_at=NOW,
        )
        session.add(token)
        await session.flush()
        pair = Pair(
            token_id=token.id,
            chain="solana",
            address=f"phase3-pair-{epoch_number}",
            dex_identifier="pumpswap",
            first_discovered_at=NOW,
        )
        session.add(pair)
        request = ApiRequestLog(
            collector_run_id=run.id,
            idempotency_key=f"phase3-request-{epoch_number}",
            provider="dex_screener",
            endpoint="tokens/v1",
            requested_at=NOW + timedelta(minutes=1),
            received_at=NOW + timedelta(minutes=1, seconds=1),
            outcome="succeeded",
            http_status_code=200,
            request_payload={"tokens": [token.address]},
            response_payload={"pairs": []},
            response_payload_sha256="b" * 64,
        )
        session.add(request)
        session.add(
            DiscoveryEvent(
                collector_run_id=run.id,
                token_id=token.id,
                idempotency_key=f"phase3-discovery-{epoch_number}",
                provider="pumpportal",
                provider_event_id=str(epoch_number),
                event_type="token_created",
                source_event_at=NOW - timedelta(minutes=1),
                received_at=NOW,
                source_payload={"mint": token.address, "optional": None},
                source_payload_sha256="c" * 64,
            )
        )
        await session.flush()
        session.add(
            Observation(
                received_at=NOW + timedelta(minutes=1, seconds=1),
                pair_id=pair.id,
                api_request_log_id=request.id,
                source_observed_at=NOW - timedelta(minutes=2),
                price_usd=Decimal("0.000000000000000123"),
                liquidity_usd=Decimal("1000.123456789012345678"),
                market_cap_usd=Decimal("2500.000000000000000001"),
                volume_m5_usd=Decimal("25.5"),
                buys_m5=3,
                sells_m5=0,
            )
        )
        session.add(
            LifecycleEvent(
                collector_run_id=run.id,
                token_id=token.id,
                idempotency_key=f"phase3-lifecycle-{epoch_number}",
                previous_state="PENDING_DEX",
                new_state="NEW",
                decided_at=NOW + timedelta(minutes=1),
                input_watermark=NOW + timedelta(minutes=1),
                reason_code="dex_pair_present",
                reason_detail={"source": None},
                configuration_sha256="d" * 64,
                configuration_snapshot={"threshold": "exact"},
            )
        )
    async with session_factory() as session, session.begin():
        await CollectorRunRepository().finish(
            session,
            run_id=run.id,
            finished_at=NOW + timedelta(minutes=5),
            status="stopped",
        )
        await close_epoch(
            session,
            epoch_number=epoch_number,
            status=close_status,
            reason="closed immutable Phase 3 test range",
            now=NOW + timedelta(minutes=5),
        )
    return epoch.id, token.id, token.address


async def _export(
    session_factory: async_sessionmaker[AsyncSession],
    root: Path,
    epoch_number: int = 2,
    **kwargs: Any,
) -> Path:
    return await export_epoch_range(
        session_factory,
        epoch_number=epoch_number,
        start_at=NOW,
        end_at=NOW + timedelta(minutes=5),
        output=root,
        chunk_rows=2,
        max_file_rows=4,
        minimum_free_bytes=256 * 1024**2,
        now=NOW + timedelta(days=1),
        **kwargs,
    )


@pytest.mark.integration
async def test_phase3_archive_contract_precision_nulls_and_duckdb(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    await _closed_epoch(session_factory)
    manifest_path = await _export(session_factory, tmp_path / "primary")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    verification = await verify_archive(manifest_path)
    analytics = run_archive_analytics(manifest_path)
    assert manifest["archive_schema_version"] == 2
    assert manifest["source_db_schema_revision"]
    assert manifest["verification"]["status"] == "verified"
    assert manifest["deletion_permitted"] is False
    assert verification["manifest_verified"] is True
    assert verification["source_scope_fully_covered"] is True
    assert verification["duckdb_readback_passed"] is True
    assert analytics["analytical_reads_passed"] is True
    families = {entry["family"] for entry in manifest["entries"]}
    assert {
        "collection_epochs",
        "collector_runs",
        "scheduler_capacity_decisions",
        "poll_schedule_decisions",
        "pair_fact_events",
        "boost_observations",
        "token_security_snapshots",
    } <= families
    with ColdArchiveQuery([manifest_path]) as archive:
        rows = archive.observations_for_token("phase3-token-2")
        assert rows[0][4] == Decimal("0.000000000000000123")
        assert (
            archive.observations_in_range(
                NOW.isoformat(), (NOW + timedelta(minutes=5)).isoformat()
            )
            == 1
        )


@pytest.mark.integration
async def test_interrupted_export_retries_without_conflict(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    await _closed_epoch(session_factory)
    root = tmp_path / "retry"
    with pytest.raises(RuntimeError, match="injected exporter interruption"):
        await _export(session_factory, root, fail_after_published_files=2)
    manifest = await _export(session_factory, root)
    assert (await verify_archive(manifest))["verified"] is True
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(ArchiveScope)) == 1


@pytest.mark.integration
async def test_concurrent_workers_cannot_publish_conflicting_scope(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    await _closed_epoch(session_factory)
    results = await asyncio.gather(
        _export(session_factory, tmp_path / "concurrent"),
        _export(session_factory, tmp_path / "concurrent"),
        return_exceptions=True,
    )
    failures = [result for result in results if isinstance(result, BaseException)]
    assert all(isinstance(error, ArchiveBusyError) for error in failures), [
        f"{type(error).__name__}: {error}" for error in failures
    ]
    successes = [result for result in results if isinstance(result, Path)]
    if not successes:
        successes.append(await _export(session_factory, tmp_path / "concurrent"))
    manifest = await _export(session_factory, tmp_path / "concurrent")
    assert all(path == manifest for path in successes)
    assert (await verify_archive(manifest))["verified"] is True
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(ArchiveScope)) == 1


@pytest.mark.integration
async def test_corrupt_file_manifest_schema_and_row_count_fail_closed(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    await _closed_epoch(session_factory)
    manifest_path = await _export(session_factory, tmp_path / "corrupt")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    root = manifest_path.parents[4]
    data_path = root / manifest["entries"][0]["file"]
    original = data_path.read_bytes()
    data_path.write_bytes(original + b"corrupt")
    with pytest.raises(ArchiveVerificationError, match="checksum mismatch"):
        await verify_archive(manifest_path)
    data_path.write_bytes(original)

    manifest["archive_schema_version"] = 999
    _rewrite_manifest(manifest_path, manifest)
    with pytest.raises(ArchiveVerificationError, match="unsupported archive schema"):
        await verify_archive(manifest_path)
    manifest["archive_schema_version"] = 2
    manifest["entries"][0]["exported_row_count"] += 1
    _rewrite_manifest(manifest_path, manifest)
    with pytest.raises(ArchiveVerificationError, match="row-count mismatch"):
        await verify_archive(manifest_path)


@pytest.mark.integration
async def test_disk_preflight_fails_before_archive_files(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    await _closed_epoch(session_factory)
    with pytest.raises(InsufficientArchiveDiskError):
        await _export(
            session_factory,
            tmp_path / "no-space",
            disk_free_override_bytes=1,
        )
    assert not list((tmp_path / "no-space").rglob("*.parquet"))


@pytest.mark.integration
async def test_mid_export_crash_cleans_staging_and_restart_recovers(
    session_factory: async_sessionmaker[AsyncSession],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _closed_epoch(session_factory)
    original = archival_module._export_one
    calls = 0

    async def crash_on_second_family(*args: object, **kwargs: object) -> object:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("injected crash in Parquet production")
        return await original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(archival_module, "_export_one", crash_on_second_family)
    root = tmp_path / "mid-file"
    with pytest.raises(RuntimeError, match="injected crash"):
        await _export(session_factory, root)
    assert not list(root.rglob("*.parquet"))
    monkeypatch.setattr(archival_module, "_export_one", original)
    assert (await verify_archive(await _export(session_factory, root)))["verified"] is True


@pytest.mark.integration
async def test_source_scope_change_during_export_fails_closed_then_retries(
    session_factory: async_sessionmaker[AsyncSession],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, token_id, _ = await _closed_epoch(session_factory)
    original = archival_module._verify_source_counts
    changed = False

    async def change_source_then_verify(*args: object, **kwargs: object) -> None:
        nonlocal changed
        if not changed:
            changed = True
            async with session_factory() as session, session.begin():
                run_id = await session.scalar(
                    select(CollectorRun.id).order_by(CollectorRun.started_at.desc()).limit(1)
                )
                session.add(
                    DiscoveryEvent(
                        collector_run_id=run_id,
                        token_id=token_id,
                        idempotency_key="phase3-source-changed-during-export",
                        provider="pumpportal",
                        provider_event_id="late-durable-event",
                        event_type="token_updated",
                        source_event_at=NOW + timedelta(minutes=2),
                        received_at=NOW + timedelta(minutes=2),
                        source_payload={"mint": "phase3-token-2", "changed": True},
                        source_payload_sha256="e" * 64,
                    )
                )
        await original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(
        archival_module, "_verify_source_counts", change_source_then_verify
    )
    root = tmp_path / "source-change"
    with pytest.raises(ArchiveVerificationError, match="source scope changed"):
        await _export(session_factory, root)
    monkeypatch.setattr(archival_module, "_verify_source_counts", original)
    assert (await verify_archive(await _export(session_factory, root)))["verified"] is True


@pytest.mark.integration
async def test_copy_retry_independence_and_retention_eligibility(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    await _closed_epoch(session_factory)
    manifest = await _export(session_factory, tmp_path / "primary")
    secondary = FilesystemObjectStore(tmp_path / "secondary-device-asserted")
    with pytest.raises(RuntimeError, match="injected archive-copy interruption"):
        await copy_archive(
            session_factory,
            manifest_path=manifest,
            store=secondary,
            copy_role="secondary",
            independence_asserted=True,
            independence_detail="test asserts separate failure domain",
            fail_after_objects=2,
        )
    with pytest.raises(ArchiveCatalogError, match="independence assertion"):
        await copy_archive(
            session_factory,
            manifest_path=manifest,
            store=secondary,
            copy_role="secondary",
            independence_asserted=False,
            independence_detail=None,
        )
    copied = await copy_archive(
        session_factory,
        manifest_path=manifest,
        store=secondary,
        copy_role="secondary",
        independence_asserted=True,
        independence_detail="operator asserts different physical device/provider",
    )
    assert copied["verified"] is True
    verified_again = await verify_filesystem_copy(
        session_factory,
        source_manifest=manifest,
        destination=tmp_path / "secondary-device-asserted",
        copy_role="secondary",
        independence_asserted=True,
        independence_detail="operator reasserts different failure domain",
    )
    assert verified_again["verified"] is True
    scope_id = uuid.UUID(json.loads(manifest.read_text(encoding="utf-8"))["archive_scope_id"])
    eligibility = await evaluate_retention_eligibility(
        session_factory,
        scope_id=scope_id,
        minimum_hot_retention_days=14,
        now=NOW + timedelta(days=20),
    )
    assert eligibility["eligible"] is True
    assert eligibility["deletion_performed"] is False
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Observation)) == 1
        assert await session.scalar(select(func.count()).select_from(ArchiveCopyVerification)) == 2
    status = await archive_status(session_factory, epoch_number=2)
    assert status["retention_eligible_scope_count"] == 1
    assert status["deletion_available"] is False


@pytest.mark.integration
async def test_invalid_epoch_archive_can_never_be_retention_eligible(
    session_factory: async_sessionmaker[AsyncSession], tmp_path: Path
) -> None:
    await _closed_epoch(session_factory, epoch_number=4, close_status="invalid")
    manifest = await _export(
        session_factory, tmp_path / "invalid-primary", epoch_number=4
    )
    await copy_archive(
        session_factory,
        manifest_path=manifest,
        store=FilesystemObjectStore(tmp_path / "invalid-secondary"),
        copy_role="secondary",
        independence_asserted=True,
        independence_detail="test separate failure domain",
    )
    scope_id = uuid.UUID(json.loads(manifest.read_text(encoding="utf-8"))["archive_scope_id"])
    result = await evaluate_retention_eligibility(
        session_factory,
        scope_id=scope_id,
        minimum_hot_retention_days=1,
        now=NOW + timedelta(days=20),
    )
    assert result["eligible"] is False
    missing = result["missing"]
    assert isinstance(missing, list)
    assert "epoch_data_valid" in missing


def _rewrite_manifest(path: Path, manifest: dict[str, object]) -> None:
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    path.with_name("manifest.sha256").write_text(
        f"{digest}  manifest.json\n", encoding="ascii"
    )
