"""Defense-in-depth checks for destructive test database operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


class DestructiveDatabaseSafetyError(RuntimeError):
    """Raised before destructive test SQL can reach a non-test database."""


@dataclass(frozen=True, slots=True)
class DatabaseSafetyResult:
    """Connected-database identity and the resulting destructive-test decision."""

    database: str
    host: str | None
    port: int | None
    environment: str
    destructive_test_operations_permitted: bool
    reason: str


class _ConnectionLike(Protocol):
    async def execute(self, statement: object) -> Any: ...


def is_approved_test_database_name(database_name: str) -> bool:
    """Accept explicit test markers without accepting a mere ``test`` substring."""
    normalized = database_name.strip().lower()
    return bool(
        normalized == "pump_research_test"
        or normalized.startswith("test_")
        or normalized.endswith("_test")
        or "_test_" in normalized
    )


def evaluate_database_safety(
    *,
    database: str,
    host: str | None,
    port: int | None,
    environment: str,
    explicit_test_database_url: bool,
) -> DatabaseSafetyResult:
    """Evaluate the policy using the identity reported by PostgreSQL itself."""
    reasons: list[str] = []
    if not explicit_test_database_url:
        reasons.append("PUMP_RESEARCH_TEST_DATABASE_URL is not explicitly present")
    if environment.strip().lower() != "test":
        reasons.append("PUMP_RESEARCH_ENVIRONMENT is not explicitly set to test")
    if not is_approved_test_database_name(database):
        reasons.append(
            "connected PostgreSQL database name lacks an approved test marker "
            "(exact pump_research_test, test_ prefix, _test suffix, or _test_ segment)"
        )
    permitted = not reasons
    return DatabaseSafetyResult(
        database=database,
        host=host,
        port=port,
        environment=environment,
        destructive_test_operations_permitted=permitted,
        reason="all destructive-test guards passed" if permitted else "; ".join(reasons),
    )


async def inspect_connected_database_safety(
    connection: AsyncConnection | _ConnectionLike,
    *,
    environment: str,
    explicit_test_database_url: bool,
) -> DatabaseSafetyResult:
    """Read the actual connected database identity, never the URL's claimed name."""
    result = cast(
        Any,
        await connection.execute(
            text("SELECT current_database(), inet_server_addr()::text, inet_server_port()")
        ),
    )
    database, host, port = cast(tuple[str, str | None, int | None], result.one())
    return evaluate_database_safety(
        database=database,
        host=host,
        port=port,
        environment=environment,
        explicit_test_database_url=explicit_test_database_url,
    )


async def assert_destructive_test_database(
    connection: AsyncConnection | _ConnectionLike,
    *,
    environment: str,
    explicit_test_database_url: bool,
    operation: str,
) -> DatabaseSafetyResult:
    """Abort immediately before destructive SQL unless every guard passes."""
    result = await inspect_connected_database_safety(
        connection,
        environment=environment,
        explicit_test_database_url=explicit_test_database_url,
    )
    if not result.destructive_test_operations_permitted:
        raise DestructiveDatabaseSafetyError(
            "CRITICAL DATABASE SAFETY ABORT: refusing "
            f"{operation} on connected database {result.database!r}: {result.reason}"
        )
    return result


async def inspect_engine_database_safety(
    engine: AsyncEngine,
    *,
    environment: str,
    explicit_test_database_url: bool,
) -> DatabaseSafetyResult:
    """Convenience wrapper for read-only CLI and pre-migration checks."""
    async with engine.connect() as connection:
        return await inspect_connected_database_safety(
            connection,
            environment=environment,
            explicit_test_database_url=explicit_test_database_url,
        )
