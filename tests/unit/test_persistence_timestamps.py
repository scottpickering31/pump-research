from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from pump_research.persistence.repositories import _normalize_utc


def test_normalize_utc_rejects_naive_datetimes() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        _normalize_utc(datetime(2026, 1, 1), "received_at")


def test_normalize_utc_converts_offsets_to_utc() -> None:
    source = datetime(2026, 1, 1, 12, tzinfo=timezone(timedelta(hours=2)))

    assert _normalize_utc(source, "received_at") == datetime(2026, 1, 1, 10, tzinfo=UTC)
