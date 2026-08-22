"""Alembic migration environment using the application's async database URL."""

from __future__ import annotations

import asyncio
import os
from logging.config import fileConfig
from typing import Any

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context
from pump_research.config import Settings
from pump_research.database_safety import evaluate_database_safety
from pump_research.persistence.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata
_MANAGED_PARTITION_PREFIXES = (
    "lifecycle_evidence_evaluations_",
    "observations_",
    "poll_batch_members_",
)


def include_object(
    object_: Any,
    name: str | None,
    type_: str,
    reflected: bool,
    compare_to: Any,
) -> bool:
    """Ignore migration-managed child partitions during autogenerate comparison."""
    if not reflected or compare_to is not None:
        return True
    if type_ == "table" and name is not None:
        return not name.startswith(_MANAGED_PARTITION_PREFIXES)
    if type_ == "index":
        table_name = getattr(getattr(object_, "table", None), "name", "")
        return not table_name.startswith(_MANAGED_PARTITION_PREFIXES)
    if type_ == "foreign_key_constraint":
        table_name = getattr(getattr(object_, "table", None), "name", "")
        remote_table_names = {
            element.target_fullname.split(".")[0]
            for element in getattr(object_, "elements", ())
        }
        return not (
            table_name.startswith(_MANAGED_PARTITION_PREFIXES)
            or any(
                remote_name.startswith(_MANAGED_PARTITION_PREFIXES)
                for remote_name in remote_table_names
            )
        )
    return True


def do_run_migrations(connection: Connection) -> None:
    """Run migrations within a synchronous connection bridge."""
    if os.environ.get("PUMP_RESEARCH_MIGRATION_DESTRUCTIVE_TEST") == "1":
        database, host, port = connection.exec_driver_sql(
            "SELECT current_database(), inet_server_addr()::text, inet_server_port()"
        ).one()
        safety = evaluate_database_safety(
            database=str(database),
            host=str(host) if host is not None else None,
            port=int(port) if port is not None else None,
            environment=os.environ.get("PUMP_RESEARCH_ENVIRONMENT", ""),
            explicit_test_database_url=bool(
                os.environ.get("PUMP_RESEARCH_TEST_DATABASE_URL")
            ),
        )
        if not safety.destructive_test_operations_permitted:
            raise RuntimeError(
                "CRITICAL DATABASE SAFETY ABORT: refusing test migration operation on "
                f"{safety.database!r}: {safety.reason}"
            )
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
        include_object=include_object,
    )
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    """Create the async engine required by asyncpg and run migrations."""
    configuration: dict[str, Any] = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = Settings().database_url
    connectable = async_engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    # The connected-database safety query in ``do_run_migrations`` autobegins a
    # SQLAlchemy transaction. Own that transaction here so a successful run is
    # committed instead of being rolled back when the async connection closes.
    async with connectable.begin() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations against the configured PostgreSQL database."""
    asyncio.run(run_async_migrations())


run_migrations_online()
