"""Runtime configuration loaded from environment variables."""

from __future__ import annotations

import logging
from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url


class Settings(BaseSettings):
    """Application settings for the currently supported local environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="PUMP_RESEARCH_",
        extra="ignore",
    )

    environment: str = "development"
    database_url: str = Field(min_length=1)
    database_connect_timeout_seconds: float = Field(default=5.0, gt=0)
    log_level: str = "INFO"
    log_json: bool = False

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: str) -> str:
        """Require the async PostgreSQL driver used by this application."""
        try:
            url = make_url(value)
        except Exception as error:
            msg = "PUMP_RESEARCH_DATABASE_URL must be a valid SQLAlchemy URL"
            raise ValueError(msg) from error

        if url.drivername != "postgresql+asyncpg":
            msg = "PUMP_RESEARCH_DATABASE_URL must use the postgresql+asyncpg driver"
            raise ValueError(msg)
        return value

    @field_validator("log_level")
    @classmethod
    def normalize_log_level(cls, value: str) -> str:
        """Normalize log-level spelling without accepting an empty value."""
        normalized = value.upper()
        if normalized not in logging.getLevelNamesMapping():
            msg = "PUMP_RESEARCH_LOG_LEVEL must be a standard Python log level"
            raise ValueError(msg)
        return normalized


@lru_cache
def get_settings() -> Settings:
    """Return the cached process configuration."""
    return Settings()
