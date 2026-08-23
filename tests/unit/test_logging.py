from __future__ import annotations

import logging

import pytest

from pump_research.config import Settings
from pump_research.logging import BrokenPipeSafeWriter, configure_logging


class ClosedPipe:
    def write(self, value: str) -> int:
        del value
        raise BrokenPipeError(32, "Broken pipe")

    def flush(self) -> None:
        raise BrokenPipeError(32, "Broken pipe")


def test_closed_log_pipeline_cannot_raise_into_collector_work() -> None:
    writer = BrokenPipeSafeWriter(ClosedPipe())  # type: ignore[arg-type]
    assert writer.write("shutdown log") == len("shutdown log")
    writer.flush()


def test_s3_sdk_debug_logging_is_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "pump_research.logging.structlog.configure", lambda **kwargs: None
    )
    configure_logging(
        Settings(
            database_url="postgresql+asyncpg://unused:unused@localhost/unused",
            log_level="DEBUG",
        )
    )
    for logger_name in ("boto3", "botocore", "s3transfer", "urllib3"):
        assert logging.getLogger(logger_name).level == logging.WARNING
