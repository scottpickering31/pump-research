from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from pump_research.database_safety import (
    DestructiveDatabaseSafetyError,
    assert_destructive_test_database,
    evaluate_database_safety,
    is_approved_test_database_name,
)


@pytest.mark.parametrize(
    "name",
    ["pump_research_test", "pump_research_capacity_test", "test_session42", "pump_test_42"],
)
def test_approved_database_names_are_structurally_marked(name: str) -> None:
    assert is_approved_test_database_name(name)


@pytest.mark.parametrize("name", ["pump_research", "contest", "latest", "test"])
def test_live_or_ambiguous_database_names_are_refused(name: str) -> None:
    assert not is_approved_test_database_name(name)


def test_explicit_test_environment_and_url_are_both_required() -> None:
    result = evaluate_database_safety(
        database="pump_research_test",
        host="127.0.0.1",
        port=5433,
        environment="development",
        explicit_test_database_url=True,
    )
    assert not result.destructive_test_operations_permitted
    assert "environment" in result.reason.lower()


@dataclass
class _Rows:
    values: tuple[str, str | None, int | None]

    def one(self) -> tuple[str, str | None, int | None]:
        return self.values


class _ConnectedDatabase:
    def __init__(self, actual_database: str) -> None:
        self.actual_database = actual_database
        self.statements: list[Any] = []

    async def execute(self, statement: object) -> _Rows:
        self.statements.append(statement)
        return _Rows((self.actual_database, "127.0.0.1", 5433))


@pytest.mark.asyncio
async def test_url_alias_cannot_bypass_connected_database_check() -> None:
    connection = _ConnectedDatabase("pump_research")
    with pytest.raises(DestructiveDatabaseSafetyError, match="CRITICAL DATABASE SAFETY ABORT"):
        await assert_destructive_test_database(
            connection,
            environment="test",
            explicit_test_database_url=True,
            operation="TRUNCATE",
        )
    assert len(connection.statements) == 1


@pytest.mark.asyncio
async def test_destructive_operation_is_only_reached_after_guard_validation() -> None:
    connection = _ConnectedDatabase("pump_research_test")
    calls: list[str] = []
    await assert_destructive_test_database(
        connection,
        environment="test",
        explicit_test_database_url=True,
        operation="TRUNCATE",
    )
    calls.append("destructive_sql")
    assert calls == ["destructive_sql"]
    assert len(connection.statements) == 1
