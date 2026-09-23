"""Resolving a task's operational limits.

[ADDITION, 2026-09-20, E2-17] Its own module because both `handlers.py` and
`runner.py` need it, and `runner` imports `handlers` — so `handlers` cannot
import `runner`. The same reason `execution.py` exists.
"""

from __future__ import annotations

from etl_craft.execution import HandlerError, TaskExecutionContext


def task_timeout_seconds(ctx: TaskExecutionContext) -> int:
    """Resolve this task's wall-clock limit: task parameter, config, then default.

    [ADDITION, 2026-09-20, E2-17] `0` disables it, for a task that genuinely
    runs longer than any sensible global bound.
    """
    raw = ctx.task_params.get("TASK_TIMEOUT_SECONDS")
    if raw is None:
        return ctx.config.limits.task_timeout_seconds
    try:
        value = int(raw)
    except ValueError as exc:
        raise HandlerError(
            f"CFG_TASK_PARAMETERS.TASK_TIMEOUT_SECONDS={raw!r} is not a whole number"
        ) from exc
    if value < 0:
        raise HandlerError("CFG_TASK_PARAMETERS.TASK_TIMEOUT_SECONDS must not be negative")
    return value
