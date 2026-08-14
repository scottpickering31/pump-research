"""Structured logging configuration."""

from __future__ import annotations

import logging
import sys
from typing import Any, cast

import structlog

from pump_research.config import Settings


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
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(**initial_values: Any) -> structlog.stdlib.BoundLogger:
    """Return a structured logger bound to stable command context."""
    logger = structlog.get_logger("pump_research").bind(**initial_values)
    return cast(structlog.stdlib.BoundLogger, logger)
