"""Database engine construction and connectivity checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from pump_research.config import Settings


@dataclass(frozen=True, slots=True)
class DatabaseHealth:
    """The successful result of a lightweight PostgreSQL connectivity check."""

    checked_at: datetime
    server_version: str


def create_database_engine(settings: Settings) -> AsyncEngine:
    """Create an async PostgreSQL engine without opening a connection."""
    return create_async_engine(
        settings.database_url,
        connect_args={
            "timeout": settings.database_connect_timeout_seconds,
            "server_settings": {"timezone": "UTC"},
        },
        pool_pre_ping=True,
    )


async def check_database_health(engine: AsyncEngine) -> DatabaseHealth:
    """Verify a connection and return PostgreSQL's reported server version."""
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
        result = await connection.execute(text("SHOW server_version"))
        server_version = cast(str, result.scalar_one())

    return DatabaseHealth(checked_at=datetime.now(UTC), server_version=server_version)
