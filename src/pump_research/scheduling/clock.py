"""Clock boundary used to make scheduler timing deterministic in tests."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol


class Clock(Protocol):
    """Source of UTC wall-clock time for persisted scheduler timestamps."""

    def now(self) -> datetime:
        """Return the current timezone-aware UTC time."""


class SystemClock:
    """Production UTC wall clock."""

    def now(self) -> datetime:
        return datetime.now(UTC)
