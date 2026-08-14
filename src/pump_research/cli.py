"""Command-line entry points for the application foundation."""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import Sequence

from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker

from pump_research.collection.runtime import CollectorRuntime
from pump_research.config import get_settings
from pump_research.database import check_database_health, create_database_engine
from pump_research.logging import configure_logging, get_logger


def build_parser() -> argparse.ArgumentParser:
    """Build the small, explicit Phase 1 command-line interface."""
    parser = argparse.ArgumentParser(prog="pump-research")
    commands = parser.add_subparsers(dest="command", required=True)
    database = commands.add_parser("database", help="Database maintenance commands")
    database_commands = database.add_subparsers(dest="database_command", required=True)
    database_commands.add_parser("health", help="Check PostgreSQL connectivity")
    collector = commands.add_parser("collector", help="Collector process commands")
    collector_commands = collector.add_subparsers(dest="collector_command", required=True)
    collector_commands.add_parser(
        "run",
        help="Reconstruct durable operational state and run until stopped",
    )
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


async def run_collector() -> int:
    """Run the signal-aware collector control process."""
    settings = get_settings()
    configure_logging(settings)
    logger = get_logger(command="collector.run", environment=settings.environment)
    engine = create_database_engine(settings)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    runtime = CollectorRuntime(session_factory, settings, logger=logger)
    try:
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


def main(argv: Sequence[str] | None = None) -> int:
    """Dispatch supported commands and return a shell exit code."""
    parser = build_parser()
    arguments = parser.parse_args(argv)

    try:
        if arguments.command == "database" and arguments.database_command == "health":
            return asyncio.run(run_database_health_check())
        if arguments.command == "collector" and arguments.collector_command == "run":
            return asyncio.run(run_collector())
    except ValidationError as error:
        parser.error(str(error))

    parser.error("unsupported command")
