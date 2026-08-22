"""Structured logging configuration."""

from __future__ import annotations

import logging
import sys
from io import TextIOBase
from typing import Any, TextIO, cast

import structlog

from pump_research.config import Settings


class BrokenPipeSafeWriter(TextIOBase):
    """Make pipeline closure unable to interrupt collector state finalization."""

    def __init__(self, stream: TextIO) -> None:
        self._stream = stream

    def write(self, value: str) -> int:
        try:
            return self._stream.write(value)
        except BrokenPipeError:
            return len(value)

    def flush(self) -> None:
        try:
            self._stream.flush()
        except BrokenPipeError:
            return


def configure_logging(settings: Settings) -> None:
    """Configure structured logs for a command-line process."""
    log_level = logging.getLevelNamesMapping()[settings.log_level]
    logging.basicConfig(format="%(message)s", level=log_level, stream=sys.stdout)

    renderer: structlog.types.Processor
    if settings.log_json:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(
            file=cast(TextIO, BrokenPipeSafeWriter(sys.stdout))
        ),
        cache_logger_on_first_use=True,
    )


def get_logger(**initial_values: Any) -> structlog.stdlib.BoundLogger:
    """Return a structured logger bound to stable command context."""
    logger = structlog.get_logger("pump_research").bind(**initial_values)
    return cast(structlog.stdlib.BoundLogger, logger)
