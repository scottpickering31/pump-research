from __future__ import annotations

import pytest

from pump_research.persistence.normalized_text import bounded_normalized_text


@pytest.mark.parametrize("value", [None, "", " PUMP \n", "é🚀", "x" * 128])
def test_bounded_normalized_text_preserves_ordinary_values(value: str | None) -> None:
    assert bounded_normalized_text(value, 128) == value


def test_bounded_normalized_text_uses_code_points_without_unicode_normalization() -> None:
    value = "e\u0301🚀" * 100
    assert bounded_normalized_text(value, 128) == value[:128]
    assert len(value[:128].encode()) > 128
    assert bounded_normalized_text(bounded_normalized_text(value, 128), 128) == value[:128]
    assert value == "e\u0301🚀" * 100


@pytest.mark.parametrize("limit", [0, -1])
def test_bounded_normalized_text_rejects_invalid_limit(limit: int) -> None:
    with pytest.raises(ValueError, match="max_length must be positive"):
        bounded_normalized_text("symbol", limit)
