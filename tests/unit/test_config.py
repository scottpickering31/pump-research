from __future__ import annotations

import pytest
from pydantic import ValidationError

from pump_research.config import Settings


def test_settings_accept_asyncpg_postgres_url() -> None:
    settings = Settings(
        database_url="postgresql+asyncpg://researcher:password@localhost:5432/pump_research"
    )

    assert settings.database_connect_timeout_seconds == 5.0
    assert settings.log_level == "INFO"
    assert settings.scheduler_new_initial_interval_seconds == 15
    assert settings.scheduler_new_initial_duration_seconds == 120
    assert settings.scheduler_new_interval_seconds == 30
    assert settings.scheduler_fading_interval_seconds == 120
    assert settings.scheduler_capacity_headroom_ratio == 0.20


def test_settings_reject_non_asyncpg_url() -> None:
    with pytest.raises(ValidationError, match="postgresql\\+asyncpg"):
        Settings(database_url="postgresql://researcher:password@localhost:5432/pump_research")
