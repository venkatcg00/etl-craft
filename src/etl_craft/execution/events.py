"""Durable event waits and atomic completion outside the agent pool."""

from __future__ import annotations

import json
import threading
import time
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConnectorConfig
from etl_craft.core.actor import SYSTEM_ACTOR, acting_as, current_actor
from etl_craft.core.counts import Counts
from etl_craft.core.enums import RunStatus
from etl_craft.core.errors import EtlCraftError, LockTimeoutError
from etl_craft.core.faults import fault_point
from etl_craft.core.time import as_utc
from etl_craft.engine import controls, locks, transitions
from etl_craft.engine.queries import statement
from etl_craft.engine.repository import trackers
from etl_craft.engine.repository.events import consumed_files, record_event
from etl_craft.engine.repository.tasks import fetch_task_parameters
from etl_craft.execution.event_probe import probe_sql
from etl_craft.execution.limits import task_timeout_seconds
from etl_craft.execution.retries import task_retry_policy
from etl_craft.handlers.event_wait import Event, probe, read_event


@dataclass(frozen=True)
class EventCheck:
    """The next durable look, or a completed task outcome."""

    next_check_at: datetime | None = None
    status: str | None = None
    task_run_id: int | None = None
    message: str = ""


_ENDED = frozenset({RunStatus.SUCCESS, RunStatus.SKIPPED, RunStatus.CANCELLED})
"""Task-run statuses after which an event wait has nothing left to check."""


@acting_as(SYSTEM_ACTOR)
def check_event(
    engine: Engine,
    config: ConnectorConfig,
    pipeline_run_id: int,
    task_id: int,
    now: datetime,
    *,
    decisions: tuple[trackers.GateDecision, ...] = (),
    cancel: threading.Event | None = None,
    force_check: bool = False,
) -> EventCheck:
    """Check once; waiting never admits an attempt or holds an agent slot."""
    try:
        with locks.event(task_id).hold(engine, wait_seconds=0.1):
            with engine.begin() as conn:
                fences = controls.blocked(conn, pipeline_run_id)
                if None in fences or task_id in fences:
                    return EventCheck(now + timedelta(seconds=1), message="dispatch is held")
                controls.assert_dispatch(conn, pipeline_run_id, task_id)
            wait = _EventWait(
                engine, config, pipeline_run_id, task_id, decisions, cancel, force_check
            )
            return _check_event(wait, now)
    except LockTimeoutError:
        return EventCheck(now + timedelta(seconds=1))


@dataclass(frozen=True)
class _EventWait:
    """One event-wait task under one run, and what its check may use."""

    engine: Engine
    config: ConnectorConfig
    pipeline_run_id: int
    task_id: int
    decisions: tuple[trackers.GateDecision, ...]
    cancel: threading.Event | None
    force_check: bool


def _check_event(wait: _EventWait, now: datetime) -> EventCheck:
    engine, config, task_id = wait.engine, wait.config, wait.task_id
    params = {"pipeline_run_id": wait.pipeline_run_id, "task_id": task_id}
    with engine.connect() as conn:
        task_run = conn.execute(statement(conn, "task_run"), params).one_or_none()
        if task_run and task_run.status in _ENDED:
            return EventCheck(status=task_run.status, task_run_id=task_run.task_run_id)
        row = conn.execute(statement(conn, "event_wait"), params).one_or_none()
        task_params = fetch_task_parameters(conn, task_id)
        consumed = consumed_files(conn, task_id)
    first = as_utc(row.first_check_at) if row else now
    observations = json.loads(row.observations or "{}") if row else {}
    evidence = None
    message = ""
    try:
        task_timeout_seconds(task_params, config)
        task_retry_policy(task_params, config)
        event = read_event(config, task_params)
        deadline = (
            (as_utc(row.wait_until) if row.wait_until is not None else None)
            if row
            else None
            if event.timeout_seconds is None
            else first + timedelta(seconds=event.timeout_seconds)
        )
        if (
            not wait.force_check
            and row
            and row.next_check_at is not None
            and now < as_utc(row.next_check_at)
        ):
            return EventCheck(as_utc(row.next_check_at))
        # Persist the budget before a query can block or this process can be interrupted.
        with engine.begin() as conn:
            _save_wait(conn, params, (first, deadline, now), observations)
        if deadline is None or now < deadline or row is None:
            evidence, observations, now = _probe(
                wait, event, (now, deadline), observations, consumed
            )
        status, message = _outcome(event, evidence, deadline, now, task_id)
    except (EtlCraftError, OSError, SQLAlchemyError) as error:
        status, message = RunStatus.FAILED, f"EVENT_WAIT task_id={task_id}: {error}"
        deadline = as_utc(row.wait_until) if row and row.wait_until is not None else now
    next_check = None if status is not None else _next_check(event, observations, now, deadline)
    with engine.begin() as conn:
        _save_wait(conn, params, (first, deadline, next_check), observations)
        if status is None:
            return EventCheck(next_check)
        binding = transitions.find_or_create_task_run(conn, task_id, wait.pipeline_run_id)
        if binding.status in _ENDED:
            return EventCheck(status=binding.status, task_run_id=binding.task_run_id)
        _complete(conn, wait, binding.task_run_id, (status, message), evidence)
    return EventCheck(status=status, task_run_id=binding.task_run_id, message=message)


def _save_wait(
    conn: Connection,
    params: dict[str, int],
    times: tuple[datetime, datetime | None, datetime | None],
    observations: dict[str, Any],
) -> None:
    """Record the wait's first check, deadline, next check and file observations."""
    first, deadline, next_check = times
    transitions.lock_active_run(conn, params["pipeline_run_id"])
    conn.execute(
        statement(conn, "save_event_wait"),
        {
            **params,
            "first_check_at": first,
            "wait_until": deadline,
            "next_check_at": next_check,
            "observations": json.dumps(observations),
        },
    )


def _probe(
    wait: _EventWait,
    event: Event,
    clock: tuple[datetime, datetime | None],
    observations: dict[str, Any],
    consumed: set[str],
) -> tuple[dict[str, Any] | None, dict[str, Any], datetime]:
    """Look for the event once; return its evidence, the observations and the time after."""
    now, deadline = clock
    evidence = None
    started = time.monotonic()
    with suppress(LockTimeoutError):
        if event.sql:
            evidence = probe_sql(
                wait.config,
                event,
                30 if deadline is None else max(0.001, (deadline - now).total_seconds()),
                wait.cancel,
            )
        else:
            evidence, observations = probe(
                event, wait.config, wait.engine, now, observations, consumed
            )
    if event.sql:
        now += timedelta(seconds=time.monotonic() - started)
    return evidence, observations, now


def _outcome(
    event: Event,
    evidence: dict[str, Any] | None,
    deadline: datetime | None,
    now: datetime,
    task_id: int,
) -> tuple[str | None, str]:
    """Return the task's status, or ``None`` to keep waiting, and its timeout message."""
    status = (
        RunStatus.SUCCESS
        if evidence
        else event.on_timeout
        if deadline is not None and now >= deadline
        else None
    )
    if status == "FAIL":
        status = RunStatus.FAILED
    elif status == "SKIP":
        status = RunStatus.SKIPPED
    message = ""
    if status and not evidence:
        message = (
            f"EVENT_WAIT task_id={task_id} timed out after "
            f"{(event.timeout_seconds or 0) / 60:g} minutes"
        )
    return status, message


def _next_check(
    event: Event, observations: dict[str, Any], now: datetime, deadline: datetime | None
) -> datetime:
    """Return when to look again: the poll interval, sooner for a file still settling."""
    delay = event.poll_seconds
    if event.file and observations:
        delay = min(
            delay,
            max(
                0.1,
                min(
                    value["since"] + event.settle_seconds - now.timestamp()
                    for value in observations.values()
                ),
            ),
        )
    next_check = now + timedelta(seconds=delay)
    return next_check if deadline is None else min(deadline, next_check)


def _complete(
    conn: Connection,
    wait: _EventWait,
    task_run_id: int,
    outcome: tuple[str, str],
    evidence: dict[str, Any] | None,
) -> None:
    """End the task run: skipped directly, or through one attempt carrying the evidence."""
    status, message = outcome
    if status == RunStatus.SKIPPED:
        trackers.record_decisions(conn, wait.pipeline_run_id, wait.decisions)
        transitions.finish_task_run(conn, task_run_id, status=status, error_message=message)
    else:
        attempt = transitions.queue_attempt(conn, task_run_id, current_actor())
        policy = task_retry_policy(fetch_task_parameters(conn, wait.task_id), wait.config)
        transitions.set_attempt_retry_policy(conn, attempt, policy.retry_on)
        owner = f"event:{attempt}"
        transitions.claim_attempt(
            conn,
            attempt,
            current_actor(),
            owner=owner,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        trackers.record_decisions(conn, wait.pipeline_run_id, wait.decisions, attempt_id=attempt)
        if evidence:
            record_event(conn, wait.task_id, task_run_id, evidence)
            fault_point("event.after_evidence")
        transitions.finish_attempt(
            conn,
            attempt,
            status,
            current_actor(),
            owner=owner,
            counts=Counts(source_count=len(evidence.get("files", [1])) if evidence else None),
            task_log=f"EVENT_EVIDENCE = {json.dumps(evidence, default=str)}" if evidence else None,
            error_message=message or None,
            retryable=False,
        )
