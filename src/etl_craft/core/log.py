"""Logging output for the ``etl_craft`` logger tree.

Modules log through ``logging.getLogger(__name__)``. Importing etl-craft configures nothing; the
command line calls ``configure`` once to choose the level and the format, text or JSON lines.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from enum import StrEnum
from typing import IO, Any

from etl_craft.core.errors import UsageError

ROOT_LOGGER = "etl_craft"
LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
TEXT_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

# Every attribute a plain LogRecord has; anything else on a record came from ``extra=``.
_RECORD_ATTRIBUTES = frozenset(vars(logging.makeLogRecord({}))) | {"message", "asctime"}


class LogFormat(StrEnum):
    """How each log record is written."""

    TEXT = "text"
    JSON = "json"


class JsonFormatter(logging.Formatter):
    """Write each record as one JSON object per line.

    The object has ``time`` (UTC, ISO 8601), ``level``, ``logger`` and ``message``, then every
    field passed through ``extra=``, then ``exception`` when the record carries one.
    """

    def format(self, record: logging.LogRecord) -> str:
        """Return the record as a single-line JSON object."""
        entry: dict[str, Any] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in vars(record).items():
            if key not in _RECORD_ATTRIBUTES and key not in entry:
                entry[key] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            entry["stack"] = self.formatStack(record.stack_info)
        return json.dumps(entry, default=str)


class _ConfiguredHandler(logging.StreamHandler[IO[str]]):
    """The handler ``configure`` installs; a later call replaces it rather than adding another."""


def parse_level(level: str | int) -> int:
    """Return the ``logging`` level for a name such as ``info``, or a numeric level unchanged.

    Raises ``UsageError`` for a name outside ``LEVELS``.
    """
    if isinstance(level, int):
        return level
    name = level.upper()
    if name not in LEVELS:
        raise UsageError(f"unknown log level {level!r}; use one of {', '.join(LEVELS)}")
    return logging.getLevelNamesMapping()[name]


def parse_format(fmt: str | LogFormat) -> LogFormat:
    """Return the ``LogFormat`` for ``text`` or ``json``; raises ``UsageError`` otherwise."""
    try:
        return LogFormat(fmt.lower())
    except ValueError:
        choices = ", ".join(member.value for member in LogFormat)
        raise UsageError(f"unknown log format {fmt!r}; use one of {choices}") from None


def configure(
    level: str | int = "INFO",
    fmt: str | LogFormat = LogFormat.TEXT,
    stream: IO[str] | None = None,
) -> logging.Handler:
    """Send ``etl_craft`` records at ``level`` and above to ``stream`` (stderr by default).

    Returns the installed handler. Calling it again replaces the handler it installed before.
    """
    numeric_level = parse_level(level)
    formatter = (
        JsonFormatter() if parse_format(fmt) is LogFormat.JSON else logging.Formatter(TEXT_FORMAT)
    )
    logger = logging.getLogger(ROOT_LOGGER)
    for existing in [h for h in logger.handlers if isinstance(h, _ConfiguredHandler)]:
        logger.removeHandler(existing)
    handler = _ConfiguredHandler(sys.stderr if stream is None else stream)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(numeric_level)
    return handler
