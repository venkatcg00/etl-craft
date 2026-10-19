"""Persist automatic retry admission; delayed attempts use no agent slots."""

from __future__ import annotations

import math
from collections.abc import Mapping, Set
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import Boolean
from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as, current_actor
from etl_craft.core.enums import AttemptStatus, RetryOn, enum_value
from etl_craft.core.errors import MetadataError, StaleTransitionError
from etl_craft.core.time import as_utc
from etl_craft.engine import transitions
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.tasks import fetch_task_parameters


@dataclass(frozen=True)
class RetryPolicy:
    """Additional attempts and capped exponential delay after each failure."""

    retries: int
    delay_seconds: int
    backoff: float
    retry_on: RetryOn = RetryOn.TRANSIENT

    def delay(self, attempt_number: int) -> float:
        """Return the delay after this attempt, capped at one hour."""
        if not self.delay_seconds:
            return 0
        try:
            return min(3600.0, self.delay_seconds * self.backoff ** (attempt_number - 1))
        except OverflowError:
            return 3600.0


def task_retry_policy(params: Mapping[str, str], config: ConnectorConfig) -> RetryPolicy:
    """Read and validate common task retry parameters, with orchestration defaults."""
    values = {}
    for name, default in (
        ("RETRIES", config.dag_defaults.retries or 0),
        ("RETRY_DELAY_SECONDS", 60),
    ):
        raw = params.get(name, str(default))
        try:
            value = int(raw)
            if value < 0:
                raise ValueError(raw)
        except ValueError as error:
            raise MetadataError(
                f"CFG_TASK_PARAMETERS.{name}={raw!r} must be a whole number, 0 or more"
            ) from error
        values[name] = value
    raw = params.get("RETRY_BACKOFF", "2.0")
    try:
        backoff = float(raw)
        if not math.isfinite(backoff) or backoff < 1:
            raise ValueError(raw)
    except ValueError as error:
        raise MetadataError(
            f"CFG_TASK_PARAMETERS.RETRY_BACKOFF={raw!r} must be a finite number, 1 or more"
        ) from error
    retry_on = enum_value(
        RetryOn,
        params.get("RETRY_ON", config.retry_on),
        MetadataError(
            f"CFG_TASK_PARAMETERS.RETRY_ON={params.get('RETRY_ON')!r}; expected "
            f"{', '.join(RetryOn)}; set RETRY_ON to one of these values"
        ),
    )
    return RetryPolicy(values["RETRIES"], values["RETRY_DELAY_SECONDS"], backoff, retry_on)


@acting_as(SYSTEM_ACTOR)
def refresh_retries(
    engine: Engine, config: ConnectorConfig, pipeline_run_id: int, task_ids: Set[int]
) -> tuple[dict[int, datetime], set[int]]:
    """Queue eligible failures once; return delayed attempts and exhausted task IDs."""
    waiting: dict[int, datetime] = {}
    exhausted: set[int] = set()
    with engine.begin() as conn:
        rows = conn.execute(
            statement(conn, "retry_attempts").columns(retryable=Boolean),
            {"pipeline_run_id": pipeline_run_id},
        ).all()
        for row in rows:
            if row.task_id not in task_ids:
                continue
            if row.status == AttemptStatus.QUEUED and row.not_before is not None:
                waiting[row.task_id] = as_utc(row.not_before)
                continue
            if row.status not in {
                AttemptStatus.FAILED,
                AttemptStatus.TIMED_OUT,
                AttemptStatus.LOST,
            }:
                continue
            policy = task_retry_policy(fetch_task_parameters(conn, row.task_id), config)
            if not policy.retries:
                continue
            if not row.retryable or row.attempt_number > policy.retries:
                exhausted.add(row.task_id)
                continue
            due = datetime.now(UTC) + timedelta(seconds=policy.delay(row.attempt_number))
            try:
                queued = transitions.queue_attempt(
                    conn,
                    row.task_run_id,
                    current_actor(),
                    not_before=due,
                    previous_attempt_id=row.attempt_id,
                )
                transitions.set_attempt_retry_policy(conn, queued, policy.retry_on)
            except StaleTransitionError:
                continue
            waiting[row.task_id] = due
    return waiting, exhausted
