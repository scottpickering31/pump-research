from __future__ import annotations

import pytest

from pump_research.discovery.payload_safety import postgres_json_string_failure


@pytest.mark.parametrize(
    ("payload", "reason"),
    [
        ({"normal": [None, False, 1.5, "雪🌍", "\\u0000"]}, None),
        ({"name": "\x00"}, "postgresql_nul_string"),
        ({"nested": [{"\x00": "value"}]}, "postgresql_nul_string"),
        ({"nested": [{"key": "\ud800"}]}, "postgresql_unpaired_surrogate"),
        ({"\udfff": "value"}, "postgresql_unpaired_surrogate"),
        ({"surrogate": "\udfff", "nul": "\x00"}, "postgresql_nul_string"),
        ({"nul": "\x00", "surrogate": "\udfff"}, "postgresql_nul_string"),
    ],
)
def test_full_json_tree_is_checked_without_sanitizing(payload: object, reason: str | None) -> None:
    assert postgres_json_string_failure(payload) == reason


def test_iterative_check_handles_deeply_nested_extras() -> None:
    payload: object = "\x00"
    for _ in range(2_000):
        payload = [payload]
    assert postgres_json_string_failure(payload) == "postgresql_nul_string"
