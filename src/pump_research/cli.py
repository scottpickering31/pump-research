"""Command-line entry points for the application foundation."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import uuid
from collections.abc import Sequence
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from pump_research.archival import archive_stats, export_epoch_range, verify_archive
from pump_research.archive_analytics import run_archive_analytics
from pump_research.archive_benchmark import benchmark_observation_window
from pump_research.archive_catalog import archive_status, evaluate_retention_eligibility
from pump_research.archive_storage import (
    FilesystemObjectStore,
    copy_archive,
    verify_filesystem_copy,
)
from pump_research.backup import backup_status, verify_backup
from pump_research.candidates.policy import CandidatePolicy
from pump_research.candidates.service import CandidateOrchestrationService
from pump_research.collection.boosts import BoostCollectionWorkflow
from pump_research.collection.dex_availability import DexAvailabilityWorkflow
from pump_research.collection.discovery import DiscoveryCoordinator
from pump_research.collection.market_context import MarketContextWorkflow
from pump_research.collection.polling import ScheduledObservationWorkflow
from pump_research.collection.recovery import reconcile_stale_collector_run
from pump_research.collection.runtime import CollectorRuntime
from pump_research.collection.security import TokenSecurityWorkflow
from pump_research.collection.worker import CollectorWorker
from pump_research.config import get_settings
from pump_research.database import check_database_health, create_database_engine
from pump_research.database_safety import inspect_engine_database_safety
from pump_research.discovery.pumpportal import PumpPortalDiscoverySource
from pump_research.epochs import close_epoch, create_epoch, get_epoch_status, list_epochs
from pump_research.lifecycle.classifier import LifecycleClassifier
from pump_research.logging import configure_logging, get_logger
from pump_research.market_data.dexscreener import DexScreenerClient
from pump_research.market_data.solana_rpc import SolanaRpcClient
from pump_research.monitoring.status import read_collector_status
from pump_research.persistence.models import (
    CandidateEvent,
    CandidateTierEvent,
    CollectionEpoch,
    Token,
)
from pump_research.reporting.twenty_four_hour import generate_report, write_report_files
from pump_research.research.asof import get_token_state_as_of
from pump_research.research.dataset import (
    DatasetBuildSpec,
    build_dataset,
    inspect_dataset,
    research_code_revision,
    verify_dataset,
)
from pump_research.research.features import build_market_features
from pump_research.research.sources import (
    DuckDBArchiveResearchSource,
    HotColdResearchSource,
    PostgresResearchSource,
    ResearchSource,
)
from pump_research.scheduling.scheduler import AdaptiveScheduler
from pump_research.security_enrichment.policy import SecurityEnrichmentPolicy
from pump_research.security_enrichment.provider import StandardSolanaHolderProvider
from pump_research.security_enrichment.service import SecurityEnrichmentWorker


def build_parser() -> argparse.ArgumentParser:
    """Build the small, explicit Phase 1 command-line interface."""
    parser = argparse.ArgumentParser(prog="pump-research")
    commands = parser.add_subparsers(dest="command", required=True)
    for database_name in ("database", "db"):
        database = commands.add_parser(database_name, help="Database maintenance commands")
        database_commands = database.add_subparsers(dest="database_command", required=True)
        database_commands.add_parser("health", help="Check PostgreSQL connectivity")
        database_commands.add_parser(
            "safety-check", help="Inspect connected-database destructive-test safeguards"
        )
    collector = commands.add_parser("collector", help="Collector process commands")
    collector_commands = collector.add_subparsers(dest="collector_command", required=True)
    collector_run = collector_commands.add_parser(
        "run",
        help="Reconstruct durable operational state and run until stopped",
    )
    collector_run.add_argument(
        "--epoch", type=int, required=True, help="Explicit collection epoch number"
    )
    collector_commands.add_parser("status", help="Show durable collector and pipeline status")
    collector_reconcile = collector_commands.add_parser(
        "reconcile-stale",
        help="Audit and stop a stale-running collector row while holding the process lock",
    )
    collector_reconcile.add_argument("--epoch", type=int, required=True)
    collector_reconcile.add_argument("--reason", required=True)
    epoch = commands.add_parser("epoch", help="Auditable collection epoch commands")
    epoch_commands = epoch.add_subparsers(dest="epoch_command", required=True)
    epoch_commands.add_parser("list", help="List declared collection epochs")
    epoch_create = epoch_commands.add_parser("create", help="Declare a planned epoch")
    epoch_create.add_argument("--number", type=int, required=True)
    epoch_create.add_argument("--purpose", required=True)
    epoch_create.add_argument("--name")
    epoch_status = epoch_commands.add_parser("status", help="Show an epoch or the latest")
    epoch_status.add_argument("--number", type=int)
    epoch_close = epoch_commands.add_parser("close", help="Explicitly end a running epoch")
    epoch_close.add_argument("--number", type=int, required=True)
    epoch_close.add_argument("--status", choices=("completed", "aborted", "invalid"), required=True)
    epoch_close.add_argument("--reason", required=True)
    archive = commands.add_parser("archive", help="Non-destructive Parquet archive commands")
    archive_commands = archive.add_subparsers(dest="archive_command", required=True)
    archive_export = archive_commands.add_parser("export", help="Export a closed epoch range")
    archive_export.add_argument("--epoch", type=int, required=True)
    archive_export.add_argument("--from", dest="from_at", required=True)
    archive_export.add_argument("--to", dest="to_at", required=True)
    archive_export.add_argument("--output", type=Path, required=True)
    archive_verify = archive_commands.add_parser("verify", help="Verify a manifest and files")
    archive_verify.add_argument("manifest", type=Path)
    archive_stats_parser = archive_commands.add_parser("stats", help="Measure archive storage")
    archive_stats_parser.add_argument("manifest", type=Path)
    archive_analyze = archive_commands.add_parser(
        "analyze", help="Run direct DuckDB analytical usability queries"
    )
    archive_analyze.add_argument("manifest", type=Path)
    archive_status_parser = archive_commands.add_parser(
        "status", help="Show durable archive scopes and verification state"
    )
    archive_status_parser.add_argument("--epoch", type=int)
    archive_copy = archive_commands.add_parser(
        "copy", help="Copy and read-verify a production archive on filesystem storage"
    )
    archive_copy.add_argument("manifest", type=Path)
    archive_copy.add_argument("--output", type=Path, required=True)
    archive_copy.add_argument("--role", choices=("primary", "secondary"), required=True)
    archive_copy.add_argument("--independent-copy", action="store_true")
    archive_copy.add_argument("--independence-detail")
    archive_verify_copy = archive_commands.add_parser(
        "verify-copy", help="Re-read and verify an existing filesystem archive copy"
    )
    archive_verify_copy.add_argument("manifest", type=Path)
    archive_verify_copy.add_argument("--output", type=Path, required=True)
    archive_verify_copy.add_argument("--role", choices=("primary", "secondary"), required=True)
    archive_verify_copy.add_argument("--independent-copy", action="store_true")
    archive_verify_copy.add_argument("--independence-detail")
    archive_benchmark = archive_commands.add_parser(
        "benchmark", help="Read-only core-observation Parquet benchmark"
    )
    archive_benchmark.add_argument("--epoch", type=int, required=True)
    archive_benchmark.add_argument("--from", dest="from_at", required=True)
    archive_benchmark.add_argument("--to", dest="to_at", required=True)
    archive_benchmark.add_argument("--output", type=Path, required=True)
    retention = commands.add_parser(
        "retention", help="Non-destructive archive retention eligibility"
    )
    retention_commands = retention.add_subparsers(dest="retention_command", required=True)
    retention_status = retention_commands.add_parser(
        "status", help="Evaluate one archive scope without deleting data"
    )
    retention_status.add_argument("--scope-id", required=True)
    retention_status.add_argument("--minimum-hot-days", type=int)
    backup = commands.add_parser("backup", help="Verified backup evidence commands")
    backup_commands = backup.add_subparsers(dest="backup_command", required=True)
    backup_status_parser = backup_commands.add_parser("status", help="Show verified backups")
    backup_status_parser.add_argument("--epoch", type=int)
    backup_verify = backup_commands.add_parser("verify", help="Read and verify a backup")
    backup_verify.add_argument("path", type=Path)
    backup_verify.add_argument("--epoch", type=int, required=True)
    backup_verify.add_argument(
        "--independent-copy",
        action="store_true",
        help="Assert that the artifact is stored outside the project/DB path",
    )
    report = commands.add_parser("report", help="Data-quality and collection reports")
    report_commands = report.add_subparsers(dest="report_command", required=True)
    twenty_four_hour = report_commands.add_parser(
        "24h", help="Generate the trailing 24-hour report"
    )
    twenty_four_hour.add_argument(
        "--output-directory",
        default="reports",
        help="Directory for 24h_report.md and 24h_report.json (default: reports)",
    )
    twenty_four_hour.add_argument(
        "--end-at",
        help="UTC ISO-8601 hour boundary, for a reproducible historical report",
    )
    twenty_four_hour.add_argument(
        "--epoch", type=int, required=True, help="Primary collection epoch filter"
    )
    twenty_four_hour.add_argument(
        "--include-invalid",
        action="store_true",
        help="Explicitly include an invalid epoch for engineering analysis",
    )
    twenty_four_hour.add_argument(
        "--archive-manifest",
        type=Path,
        help="Verified same-epoch manifest whose measured storage is included",
    )
    research = commands.add_parser("research", help="Strict as-of research dataset commands")
    research_commands = research.add_subparsers(dest="research_command", required=True)
    research_build = research_commands.add_parser(
        "build", help="Build one immutable deterministic research dataset"
    )
    research_build.add_argument("--epoch", type=int, required=True)
    research_build.add_argument("--from", dest="from_at", required=True)
    research_build.add_argument("--to", dest="to_at", required=True)
    research_build.add_argument("--output", type=Path, required=True)
    research_build.add_argument("--token", action="append", default=[])
    research_build.add_argument("--archive-manifest", type=Path, action="append", default=[])
    research_build.add_argument(
        "--hot-from", help="Explicit UTC cutoff when combining archive with hot PostgreSQL"
    )
    research_build.add_argument("--minimum-free-bytes", type=int, default=1_073_741_824)
    for command_name in ("as-of", "token-history", "candidate-history"):
        command = research_commands.add_parser(
            command_name,
            help="Reconstruct known state" if command_name == "as-of" else "Inspect bounded facts",
        )
        command.add_argument("--epoch", type=int, required=True)
        command.add_argument("--token", required=True)
        command.add_argument("--at", required=True)
        command.add_argument("--archive-manifest", type=Path, action="append", default=[])
        command.add_argument(
            "--hot-from", help="Explicit UTC cutoff when combining archive with hot PostgreSQL"
        )
    research_inspect = research_commands.add_parser(
        "inspect", help="Inspect and verify a generated dataset"
    )
    research_inspect.add_argument("manifest", type=Path)
    research_verify = research_commands.add_parser(
        "verify", help="Independently verify a generated dataset"
    )
    research_verify.add_argument("manifest", type=Path)
    return parser


async def run_database_health_check() -> int:
    """Run the database health check and log a structured result."""
    settings = get_settings()
    configure_logging(settings)
    logger = get_logger(command="database.health", environment=settings.environment)
    engine = create_database_engine(settings)
    exit_code = 0
    health = None
    try:
        health = await check_database_health(engine)
    except Exception:
        logger.exception("database_health_check_failed")
        exit_code = 1
    finally:
        try:
            await engine.dispose()
        except Exception:
            logger.exception("database_engine_disposal_failed")
            exit_code = 1

    if health is not None and exit_code == 0:
        logger.info(
            "database_health_check_succeeded",
            checked_at=health.checked_at.isoformat(),
            server_version=health.server_version,
        )
    return exit_code


async def run_database_safety_check() -> int:
    """Print the connected database identity and destructive-test decision."""
    settings = get_settings()
    configure_logging(settings)
    engine = create_database_engine(settings)
    try:
        result = await inspect_engine_database_safety(
            engine,
            environment=settings.environment,
            explicit_test_database_url=bool(os.environ.get("PUMP_RESEARCH_TEST_DATABASE_URL")),
        )
        print(
            json.dumps(
                {
                    "connected_database": result.database,
                    "host": result.host,
                    "port": result.port,
                    "environment": result.environment,
                    "destructive_test_operations_permitted": (
                        result.destructive_test_operations_permitted
                    ),
                    "reason": result.reason,
                },
                indent=2,
                sort_keys=True,
            )
        )
    finally:
        await engine.dispose()
    return 0


async def run_collector(*, epoch_number: int) -> int:
    """Run the full durable discovery-to-observation collector pipeline."""
    settings = get_settings()
    configure_logging(settings)
    logger = get_logger(command="collector.run", environment=settings.environment)
    engine = create_database_engine(settings)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with (
            DexScreenerClient(settings, logger=logger) as dex_client,
            PumpPortalDiscoverySource(settings, logger=logger) as discovery_source,
            SolanaRpcClient(settings) as solana_rpc,
        ):
            scheduler = AdaptiveScheduler(session_factory, settings)
            candidate_orchestrator = CandidateOrchestrationService(
                session_factory,
                CandidatePolicy.from_settings(settings),
                task_lease=timedelta(seconds=settings.candidate_task_lease_seconds),
                task_max_attempts=settings.candidate_task_max_attempts,
            )
            selective_security = SecurityEnrichmentWorker(
                session_factory,
                candidate_orchestrator,
                StandardSolanaHolderProvider(solana_rpc),
                SecurityEnrichmentPolicy.from_settings(settings),
            )
            availability = DexAvailabilityWorkflow(
                session_factory,
                dex_client,
                settings,
                logger=logger,
                scheduler=scheduler,
            )
            lifecycle = LifecycleClassifier(
                session_factory,
                settings,
                scheduler=scheduler,
            )
            polling = ScheduledObservationWorkflow(
                session_factory,
                dex_client,
                scheduler,
                lifecycle,
                logger=logger,
                candidate_orchestrator=candidate_orchestrator,
            )
            worker = CollectorWorker(
                session_factory,
                settings,
                discovery=DiscoveryCoordinator(session_factory, discovery_source, availability),
                availability=availability,
                scheduler=scheduler,
                polling=polling,
                logger=logger,
                boosts=BoostCollectionWorkflow(
                    session_factory,
                    dex_client,
                    wakeup_handler=candidate_orchestrator,
                ),
                security=TokenSecurityWorkflow(session_factory, solana_rpc, settings),
                market_context=MarketContextWorkflow(session_factory, settings),
                selective_security=selective_security,
            )
            runtime = CollectorRuntime(
                session_factory,
                settings,
                logger=logger,
                epoch_number=epoch_number,
                worker=worker,
                epoch_initializer=scheduler,
            )
            await runtime.run_until_stopped()
    except Exception:
        logger.exception("collector_failed")
        return 1
    finally:
        try:
            await engine.dispose()
        except Exception:
            logger.exception("database_engine_disposal_failed")
            return 1
    return 0


async def run_collector_status() -> int:
    """Print the durable collector status as structured JSON."""
    settings = get_settings()
    configure_logging(settings)
    engine = create_database_engine(settings)
    try:
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        print(
            json.dumps(
                await read_collector_status(session_factory, settings), indent=2, sort_keys=True
            )
        )
    except Exception:
        get_logger(command="collector.status", environment=settings.environment).exception(
            "collector_status_failed"
        )
        return 1
    finally:
        await engine.dispose()
    return 0


async def run_collector_reconcile(*, epoch_number: int, reason: str) -> int:
    """Reconcile one stale run without starting collection or closing its epoch."""
    settings = get_settings()
    configure_logging(settings)
    engine = create_database_engine(settings)
    try:
        result = await reconcile_stale_collector_run(
            async_sessionmaker(engine, expire_on_commit=False),
            settings,
            epoch_number=epoch_number,
            reason=reason,
        )
        print(json.dumps(result.as_dict(), indent=2, sort_keys=True))
    finally:
        await engine.dispose()
    return 0


async def run_epoch_command(
    *,
    command: str,
    number: int | None = None,
    purpose: str | None = None,
    name: str | None = None,
    status: str | None = None,
    reason: str | None = None,
) -> int:
    """Execute one explicit epoch declaration/query/transition."""
    settings = get_settings()
    configure_logging(settings)
    engine = create_database_engine(settings)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session, session.begin():
            if command == "create":
                assert number is not None and purpose is not None
                result: object = (
                    await create_epoch(
                        session,
                        settings,
                        epoch_number=number,
                        purpose=purpose,
                        name=name,
                    )
                ).as_dict()
            elif command == "list":
                result = [item.as_dict() for item in await list_epochs(session)]
            elif command == "status":
                result = (await get_epoch_status(session, number)).as_dict()
            elif command == "close":
                assert number is not None and status is not None and reason is not None
                result = (
                    await close_epoch(
                        session,
                        epoch_number=number,
                        status=status,
                        reason=reason,
                    )
                ).as_dict()
            else:
                raise ValueError(f"unsupported epoch command: {command}")
        print(json.dumps(result, indent=2, sort_keys=True))
    finally:
        await engine.dispose()
    return 0


async def run_archive_command(
    *,
    command: str,
    manifest: Path | None = None,
    epoch_number: int | None = None,
    from_at: str | None = None,
    to_at: str | None = None,
    output: Path | None = None,
    copy_role: str | None = None,
    independent_copy: bool = False,
    independence_detail: str | None = None,
) -> int:
    """Export, verify, measure, or query immutable Parquet artifacts."""
    settings = get_settings()
    configure_logging(settings)
    if command == "verify":
        assert manifest is not None
        result: object = await verify_archive(manifest)
    elif command == "stats":
        assert manifest is not None
        result = archive_stats(manifest)
    elif command == "analyze":
        assert manifest is not None
        result = run_archive_analytics(manifest)
    elif command == "status":
        engine = create_database_engine(settings)
        try:
            result = await archive_status(
                async_sessionmaker(engine, expire_on_commit=False),
                epoch_number=epoch_number,
            )
        finally:
            await engine.dispose()
    elif command in {"copy", "verify-copy"}:
        assert manifest is not None and output is not None and copy_role is not None
        engine = create_database_engine(settings)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            if command == "copy":
                result = await copy_archive(
                    session_factory,
                    manifest_path=manifest,
                    store=FilesystemObjectStore(output),
                    copy_role=copy_role,
                    independence_asserted=independent_copy,
                    independence_detail=independence_detail,
                )
            else:
                result = await verify_filesystem_copy(
                    session_factory,
                    source_manifest=manifest,
                    destination=output,
                    copy_role=copy_role,
                    independence_asserted=independent_copy,
                    independence_detail=independence_detail,
                )
        finally:
            await engine.dispose()
    elif command == "benchmark":
        assert epoch_number is not None and from_at and to_at and output is not None
        engine = create_database_engine(settings)
        try:
            result = {
                "benchmark_manifest": str(
                    await benchmark_observation_window(
                        async_sessionmaker(engine, expire_on_commit=False),
                        epoch_number=epoch_number,
                        start_at=_parse_utc(from_at),
                        end_at=_parse_utc(to_at),
                        output=output,
                        chunk_rows=settings.archive_export_chunk_rows,
                        minimum_free_bytes=settings.archive_minimum_free_bytes,
                    )
                )
            }
        finally:
            await engine.dispose()
    elif command == "export":
        assert epoch_number is not None and from_at and to_at and output is not None
        engine = create_database_engine(settings)
        try:
            result = {
                "manifest": str(
                    await export_epoch_range(
                        async_sessionmaker(engine, expire_on_commit=False),
                        epoch_number=epoch_number,
                        start_at=_parse_utc(from_at),
                        end_at=_parse_utc(to_at),
                        output=output,
                        chunk_rows=settings.archive_export_chunk_rows,
                        max_file_rows=settings.archive_max_file_rows,
                        minimum_free_bytes=settings.archive_minimum_free_bytes,
                    )
                )
            }
        finally:
            await engine.dispose()
    else:
        raise ValueError(f"unsupported archive command: {command}")
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


async def run_retention_command(*, scope_id: str, minimum_hot_days: int | None) -> int:
    """Calculate retention metadata while exposing no deletion operation."""
    settings = get_settings()
    configure_logging(settings)
    engine = create_database_engine(settings)
    try:
        result = await evaluate_retention_eligibility(
            async_sessionmaker(engine, expire_on_commit=False),
            scope_id=uuid.UUID(scope_id),
            minimum_hot_retention_days=(
                minimum_hot_days
                if minimum_hot_days is not None
                else settings.archive_minimum_hot_retention_days
            ),
        )
    finally:
        await engine.dispose()
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


async def run_backup_command(
    *, command: str, epoch_number: int | None, path: Path | None, independent_copy: bool
) -> int:
    """Report or validate backup artifacts without creating false-positive status."""
    settings = get_settings()
    configure_logging(settings)
    engine = create_database_engine(settings)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        if command == "status":
            result = await backup_status(session_factory, epoch_number=epoch_number)
        elif command == "verify":
            assert epoch_number is not None and path is not None
            result = await verify_backup(
                session_factory,
                epoch_number=epoch_number,
                path=path,
                independent_copy=independent_copy,
                project_root=Path.cwd(),
            )
        else:
            raise ValueError(f"unsupported backup command: {command}")
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    finally:
        await engine.dispose()
    return 0


async def run_twenty_four_hour_report(
    *,
    output_directory: Path,
    end_at: str | None,
    epoch_number: int,
    archive_manifest: Path | None,
    include_invalid: bool,
) -> int:
    """Generate both stable report artifacts from PostgreSQL facts."""
    settings = get_settings()
    configure_logging(settings)
    logger = get_logger(command="report.24h", environment=settings.environment)
    engine = create_database_engine(settings)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        parsed_end_at = datetime.fromisoformat(end_at.replace("Z", "+00:00")) if end_at else None
        report = await generate_report(
            session_factory,
            end_at=parsed_end_at,
            epoch_number=epoch_number,
            include_invalid=include_invalid,
        )
        if archive_manifest is not None:
            report["validation"]["storage"]["parquet_archive"] = archive_stats(archive_manifest)
            report["validation"]["archive_analytics"] = run_archive_analytics(archive_manifest)
        markdown_path, json_path = write_report_files(report, output_directory)
        logger.info(
            "twenty_four_hour_report_generated",
            markdown_path=str(markdown_path),
            json_path=str(json_path),
        )
    except Exception:
        logger.exception("twenty_four_hour_report_failed")
        return 1
    finally:
        await engine.dispose()
    return 0


async def run_research_command(
    *,
    command: str,
    epoch_number: int | None = None,
    token_address: str | None = None,
    at: str | None = None,
    from_at: str | None = None,
    to_at: str | None = None,
    output: Path | None = None,
    tokens: Sequence[str] = (),
    archive_manifests: Sequence[Path] = (),
    hot_from: str | None = None,
    manifest: Path | None = None,
    minimum_free_bytes: int = 1_073_741_824,
) -> int:
    """Build or inspect strict research artifacts without mutating source facts."""
    if command in {"inspect", "verify"}:
        assert manifest is not None
        result = inspect_dataset(manifest) if command == "inspect" else verify_dataset(manifest)
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0
    assert epoch_number is not None
    settings = get_settings()
    configure_logging(settings)
    if command == "candidate-history":
        if archive_manifests:
            raise ValueError(
                "Phase 5 candidate history is hot-only until a future verified archive includes it"
            )
        assert token_address is not None and at is not None
        timestamp = _parse_utc(at)
        candidate_engine = create_database_engine(settings)
        try:
            async with async_sessionmaker(candidate_engine, expire_on_commit=False)() as session:
                token = await session.scalar(
                    select(Token).where(Token.chain == "solana", Token.address == token_address)
                )
                epoch = await session.scalar(
                    select(CollectionEpoch).where(CollectionEpoch.epoch_number == epoch_number)
                )
                if token is None or epoch is None:
                    raise ValueError("token or epoch does not exist")
                candidates = list(
                    (
                        await session.execute(
                            select(CandidateEvent)
                            .where(
                                CandidateEvent.token_id == token.id,
                                CandidateEvent.collection_epoch_id == epoch.id,
                                CandidateEvent.candidate_at <= timestamp,
                                CandidateEvent.input_watermark <= timestamp,
                            )
                            .order_by(CandidateEvent.candidate_at, CandidateEvent.id)
                        )
                    ).scalars()
                )
                tiers = list(
                    (
                        await session.execute(
                            select(CandidateTierEvent)
                            .where(
                                CandidateTierEvent.token_id == token.id,
                                CandidateTierEvent.collection_epoch_id == epoch.id,
                                CandidateTierEvent.decided_at <= timestamp,
                                CandidateTierEvent.input_watermark <= timestamp,
                            )
                            .order_by(CandidateTierEvent.decided_at, CandidateTierEvent.id)
                        )
                    ).scalars()
                )
            result = {
                "epoch": epoch_number,
                "token": token_address,
                "as_of": timestamp,
                "current_tier": tiers[-1].new_tier if tiers else "TIER_0_UNIVERSAL",
                "candidates": [
                    {
                        "id": str(item.id),
                        "candidate_at": item.candidate_at,
                        "input_watermark": item.input_watermark,
                        "trigger_type": item.trigger_type,
                        "evidence_sha256": item.evidence_sha256,
                        "source_fact_ids": item.source_fact_ids,
                    }
                    for item in candidates
                ],
                "tier_events": [
                    {
                        "id": str(item.id),
                        "decided_at": item.decided_at,
                        "input_watermark": item.input_watermark,
                        "previous_tier": item.previous_tier,
                        "new_tier": item.new_tier,
                        "reason_code": item.reason_code,
                    }
                    for item in tiers
                ],
            }
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
            return 0
        finally:
            await candidate_engine.dispose()
    engine = None
    source: ResearchSource
    if archive_manifests:
        cold_source = DuckDBArchiveResearchSource(tuple(archive_manifests))
        if hot_from is not None:
            engine = create_database_engine(settings)
            source = HotColdResearchSource(
                cold_source,
                PostgresResearchSource(async_sessionmaker(engine, expire_on_commit=False)),
                hot_from=_parse_utc(hot_from),
            )
        else:
            source = cold_source
    else:
        if hot_from is not None:
            raise ValueError("--hot-from requires at least one --archive-manifest")
        engine = create_database_engine(settings)
        source = PostgresResearchSource(async_sessionmaker(engine, expire_on_commit=False))
    try:
        if command == "build":
            assert from_at and to_at and output is not None
            start = _parse_utc(from_at)
            end = _parse_utc(to_at)
            histories = await source.load_histories(
                epoch_number=epoch_number,
                token_addresses=tuple(tokens) or None,
                start_at=start,
                end_at=end,
            )
            result = {
                "manifest": str(
                    await build_dataset(
                        histories,
                        spec=DatasetBuildSpec(
                            epoch_numbers=(epoch_number,),
                            scope_start_at=start,
                            scope_end_at=end,
                            token_addresses=tuple(tokens),
                            code_revision=research_code_revision(),
                            minimum_free_bytes=minimum_free_bytes,
                        ),
                        output=output,
                    )
                )
            }
        elif command in {"as-of", "token-history"}:
            assert token_address is not None and at is not None
            timestamp = _parse_utc(at)
            histories = await source.load_histories(
                epoch_number=epoch_number,
                token_addresses=(token_address,),
                end_at=timestamp + timedelta(microseconds=1),
            )
            if len(histories) != 1:
                raise ValueError("token is absent or excluded from the requested valid epoch")
            history = histories[0]
            state = get_token_state_as_of(history, timestamp)
            if command == "as-of":
                features = build_market_features(state)
                result = {
                    "state": asdict(state),
                    "features": features.values,
                    "feature_input_observation_ids": features.input_observation_ids,
                    "availability_watermark": features.availability_watermark,
                }
            else:
                result = {
                    "epoch": epoch_number,
                    "token": token_address,
                    "as_of": timestamp,
                    "counts": {
                        "discoveries": len(history.discoveries),
                        "observations": len(state.observation_history),
                        "lifecycle_events": sum(
                            item.decided_at <= timestamp for item in history.lifecycle
                        ),
                        "pair_facts": sum(
                            item.received_at <= timestamp for item in history.pair_facts
                        ),
                        "boosts": sum(item.received_at <= timestamp for item in history.boosts),
                        "metadata_events": sum(
                            item.received_at <= timestamp for item in history.metadata
                        ),
                        "security_snapshots": sum(
                            item.received_at <= timestamp for item in history.security
                        ),
                    },
                    "availability_watermark": state.availability_watermark,
                }
        else:
            raise ValueError(f"unsupported research command: {command}")
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    finally:
        if engine is not None:
            await engine.dispose()
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch supported commands and return a shell exit code."""
    parser = build_parser()
    arguments = parser.parse_args(argv)

    try:
        if arguments.command in {"database", "db"} and arguments.database_command == "health":
            return asyncio.run(run_database_health_check())
        if arguments.command in {"database", "db"} and arguments.database_command == "safety-check":
            return asyncio.run(run_database_safety_check())
        if arguments.command == "collector" and arguments.collector_command == "run":
            return asyncio.run(run_collector(epoch_number=arguments.epoch))
        if arguments.command == "collector" and arguments.collector_command == "status":
            return asyncio.run(run_collector_status())
        if arguments.command == "collector" and arguments.collector_command == "reconcile-stale":
            return asyncio.run(
                run_collector_reconcile(epoch_number=arguments.epoch, reason=arguments.reason)
            )
        if arguments.command == "epoch":
            return asyncio.run(
                run_epoch_command(
                    command=arguments.epoch_command,
                    number=getattr(arguments, "number", None),
                    purpose=getattr(arguments, "purpose", None),
                    name=getattr(arguments, "name", None),
                    status=getattr(arguments, "status", None),
                    reason=getattr(arguments, "reason", None),
                )
            )
        if arguments.command == "archive":
            return asyncio.run(
                run_archive_command(
                    command=arguments.archive_command,
                    manifest=getattr(arguments, "manifest", None),
                    epoch_number=getattr(arguments, "epoch", None),
                    from_at=getattr(arguments, "from_at", None),
                    to_at=getattr(arguments, "to_at", None),
                    output=getattr(arguments, "output", None),
                    copy_role=getattr(arguments, "role", None),
                    independent_copy=bool(getattr(arguments, "independent_copy", False)),
                    independence_detail=getattr(arguments, "independence_detail", None),
                )
            )
        if arguments.command == "retention":
            return asyncio.run(
                run_retention_command(
                    scope_id=arguments.scope_id,
                    minimum_hot_days=arguments.minimum_hot_days,
                )
            )
        if arguments.command == "backup":
            return asyncio.run(
                run_backup_command(
                    command=arguments.backup_command,
                    epoch_number=getattr(arguments, "epoch", None),
                    path=getattr(arguments, "path", None),
                    independent_copy=bool(getattr(arguments, "independent_copy", False)),
                )
            )
        if arguments.command == "report" and arguments.report_command == "24h":
            return asyncio.run(
                run_twenty_four_hour_report(
                    output_directory=Path(arguments.output_directory),
                    end_at=arguments.end_at,
                    epoch_number=arguments.epoch,
                    include_invalid=arguments.include_invalid,
                    archive_manifest=arguments.archive_manifest,
                )
            )
        if arguments.command == "research":
            return asyncio.run(
                run_research_command(
                    command=arguments.research_command,
                    epoch_number=getattr(arguments, "epoch", None),
                    token_address=getattr(arguments, "token", None)
                    if isinstance(getattr(arguments, "token", None), str)
                    else None,
                    tokens=getattr(arguments, "token", ())
                    if isinstance(getattr(arguments, "token", ()), list)
                    else (),
                    at=getattr(arguments, "at", None),
                    from_at=getattr(arguments, "from_at", None),
                    to_at=getattr(arguments, "to_at", None),
                    output=getattr(arguments, "output", None),
                    archive_manifests=getattr(arguments, "archive_manifest", ()),
                    hot_from=getattr(arguments, "hot_from", None),
                    manifest=getattr(arguments, "manifest", None),
                    minimum_free_bytes=getattr(arguments, "minimum_free_bytes", 1_073_741_824),
                )
            )
    except ValidationError as error:
        parser.error(str(error))

    parser.error("unsupported command")


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("timestamp must include a UTC offset")
    return parsed
