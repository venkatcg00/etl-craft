"""A task's time limit."""

from __future__ import annotations

from collections.abc import Mapping

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import HandlerError


def task_timeout_seconds(params: Mapping[str, str], config: ConnectorConfig) -> int:
    """Return a task's time limit in seconds; 0 means none.

    The task's ``TASK_TIMEOUT_SECONDS`` parameter wins over ``Orchestration.Task_timeout_seconds``.
    Raises ``HandlerError`` for a parameter that is not a whole number of seconds.
    """
    raw = params.get("TASK_TIMEOUT_SECONDS")
    if raw is None:
        return config.limits.task_timeout_seconds
    try:
        value = int(raw)
    except ValueError as error:
        raise HandlerError(
            f"CFG_TASK_PARAMETERS.TASK_TIMEOUT_SECONDS={raw!r} is not a whole number"
        ) from error
    if value < 0:
        raise HandlerError("CFG_TASK_PARAMETERS.TASK_TIMEOUT_SECONDS must not be negative")
    return value
