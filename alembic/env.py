"""Alembic migration environment using the application's async database URL."""

from __future__ import annotations

import asyncio
from logging.config import fileConfig
from typing import Any

from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config

from alembic import context
from pump_research.config import Settings
from pump_research.persistence.models import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata
_MANAGED_PARTITION_PREFIXES = ("observations_", "poll_batch_members_")


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
    return True


def do_run_migrations(connection: Connection) -> None:
    """Run migrations within a synchronous connection bridge."""
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

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations_online() -> None:
    """Run migrations against the configured PostgreSQL database."""
    asyncio.run(run_async_migrations())


run_migrations_online()
