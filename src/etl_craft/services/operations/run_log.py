"""Render a pipeline run's log from the audit tables: one timestamped line per recorded event.

The log is not a second record. Every line comes from a row the run's status, explain and audit
already show, so it covers tasks that ran in several processes or on agents alike.
"""

from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.engine import Connection, Row

from etl_craft.core.enums import AttemptStatus, SlaStatus
from etl_craft.core.enums_logs import RunLogCode
from etl_craft.core.time import as_utc
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import resolve_pipeline_id
from etl_craft.engine.runlog import select_run
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import RunSelection

AuditRow = Row[tuple[object, ...]]


@dataclass(frozen=True, order=True)
class LogLine:
    """One event: ordered by time, then by the order the audit recorded it in."""

    at: datetime
    order: int
    code: RunLogCode
    task: str | None
    actor: str | None
    fields: tuple[tuple[str, object], ...]

    def render(self, run_id: int) -> str:
        """Return the line as ``time CODE run= task= actor= key=value ...``."""
        parts = [
            as_utc(self.at).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            f"{self.code:<24}",
            f"run={run_id}",
            f"task={self.task or '-'}",
            f"actor={self.actor or '-'}",
        ]
        parts += [f"{key}={_value(value)}" for key, value in self.fields if value is not None]
        return " ".join(parts)


def run_log(ctx: OperationContext, request: RunSelection) -> Iterator[bytes]:
    """Read the selected run's audit rows in one snapshot and stream the lines in time order."""
    with read_snapshot(ctx.engine) as conn:
        pipeline_id = resolve_pipeline_id(conn, request.pipeline_code)
        run_id = select_run(conn, pipeline_id, request.selector).pipeline_run_id
        params = {"pipeline_run_id": run_id}
        run = conn.execute(statement(conn, "run_log_run"), params).one()
        attempts = list(conn.execute(statement(conn, "run_log_attempts"), params))
        lines = [
            *_run_lines(run, {row.task_run_id: row.task_status for row in attempts}),
            *_attempt_lines(attempts),
            *_wait_lines(conn, params),
            *_control_lines(conn, params),
            *_intervention_lines(conn, params),
        ]
    for line in sorted(lines):
        yield (line.render(run_id) + "\n").encode()


def _value(value: object) -> str:
    text = as_utc(value).isoformat() if isinstance(value, datetime) else str(value)
    flat = " ".join(text.split())
    return f'"{flat}"' if " " in flat or not flat else flat


def _actor(name: object, kind: object) -> str | None:
    return None if name is None else f"{str(kind or 'user').lower()}:{name}"


def _line(
    at: datetime,
    order: int,
    code: RunLogCode,
    task: str | None,
    actor: str | None,
    **fields: object,
) -> LogLine:
    return LogLine(as_utc(at), order, code, task, actor, tuple(fields.items()))


def _run_lines(run: AuditRow, task_statuses: dict[object, object]) -> list[LogLine]:
    started_by = _actor(run.started_by, run.started_by_kind)
    lines = [
        _line(
            run.start_date,
            0,
            RunLogCode.RUN_STARTED,
            None,
            started_by,
            pipeline=run.pipeline_code,
            trigger=run.trigger_kind,
            run_date=run.run_date,
            run_key=run.run_key,
            backfill="Y" if run.backfill == "Y" else None,
        )
    ]
    if run.sla_breached_at is not None:
        lines.append(_line(run.sla_breached_at, 1, RunLogCode.SLA_BREACHED, None, None))
    if run.end_date is not None:
        if run.sla_status == SlaStatus.MET:
            lines.append(_line(run.end_date, 2, RunLogCode.SLA_MET, None, None))
        elapsed = (as_utc(run.end_date) - as_utc(run.start_date)).total_seconds()
        lines.append(
            _line(
                run.end_date,
                3,
                RunLogCode.RUN_ENDED,
                None,
                _actor(run.ended_by, run.ended_by_kind),
                status=run.status,
                seconds=round(elapsed, 3),
                sla=run.sla_status,
                tasks=_counts(task_statuses.values()),
            )
        )
    return lines


def _counts(statuses: Iterable[object]) -> str | None:
    counted = Counter(str(status) for status in statuses)
    return ",".join(f"{status}:{n}" for status, n in sorted(counted.items())) or None


def _attempt_lines(rows: list[AuditRow]) -> list[LogLine]:
    lines: list[LogLine] = []
    for row in rows:
        task = row.task_code
        if row.attempt_id is None:
            if row.task_end is not None:
                lines.append(
                    _line(
                        row.task_end,
                        row.task_run_id * 100,
                        RunLogCode.TASK_SETTLED,
                        task,
                        None,
                        status=row.task_status,
                    )
                )
            continue
        order = row.attempt_id * 100
        requested = _actor(row.requested_by, row.requested_by_kind)
        events = [
            (
                row.queued_at,
                RunLogCode.TASK_QUEUED,
                requested,
                {"attempt": row.attempt_number, "pool": row.pool},
            ),
            (
                row.not_before if row.attempt_number > 1 else None,
                RunLogCode.TASK_RETRY_DUE,
                None,
                {"attempt": row.attempt_number},
            ),
            (
                row.claimed_at,
                RunLogCode.TASK_ADMITTED,
                None,
                {"attempt": row.attempt_number, "owner": row.owner_id},
            ),
            (
                row.started_at,
                RunLogCode.TASK_STARTED,
                None,
                {"attempt": row.attempt_number, "host": row.host, "pid": row.pid},
            ),
            (
                row.cancel_requested_at,
                RunLogCode.TASK_CANCEL_REQUESTED,
                None,
                {"attempt": row.attempt_number},
            ),
            (
                row.cancel_acknowledged_at,
                RunLogCode.TASK_CANCEL_ACKNOWLEDGED,
                None,
                {"attempt": row.attempt_number},
            ),
            (row.ended_at, _ended(row.status), None, _ending(row)),
        ]
        lines += [
            _line(at, order + index, code, task, actor, **fields)
            for index, (at, code, actor, fields) in enumerate(events)
            if at is not None
        ]
    return lines


def _ended(status: object) -> RunLogCode:
    return RunLogCode.TASK_LOST if status == AttemptStatus.LOST else RunLogCode.TASK_ENDED


def _ending(row: AuditRow) -> dict[str, object]:
    seconds = None
    if row.started_at is not None and row.ended_at is not None:
        seconds = round((as_utc(row.ended_at) - as_utc(row.started_at)).total_seconds(), 3)
    return {
        "attempt": row.attempt_number,
        "status": row.status,
        "exit": row.exit_code,
        "rows": row.rows_written,
        "rejects": row.reject_count or None,
        "seconds": seconds,
        "failure": None if row.error_message is None else row.failure_class,
        "error": row.error_message,
    }


def _wait_lines(conn: Connection, params: dict[str, int]) -> list[LogLine]:
    lines = [
        _line(
            row.first_check_at,
            0,
            RunLogCode.WAIT_OPENED,
            row.task_code,
            None,
            kind=row.wait_kind,
            until=row.wait_until,
            checks=row.looks,
        )
        for row in conn.execute(statement(conn, "run_log_gates"), params)
    ]
    lines += [
        _line(
            row.decided_at,
            row.decision_id,
            RunLogCode.GATE_DECIDED,
            row.task_code,
            None,
            result=row.result,
            selected_run=row.selected_pipeline_run_id,
            reason=row.reason,
        )
        for row in conn.execute(statement(conn, "run_log_decisions"), params)
    ]
    return lines


def _control_lines(conn: Connection, params: dict[str, int]) -> list[LogLine]:
    lines: list[LogLine] = []
    for row in conn.execute(statement(conn, "run_log_controls"), params):
        actor = _actor(row.requested_by, row.requested_by_kind)
        order = row.control_id * 10
        lines.append(
            _line(
                row.requested_at,
                order,
                RunLogCode.CONTROL_REQUESTED,
                row.task_code,
                actor,
                control=row.control_id,
                action=row.action,
                reason=row.reason,
            )
        )
        if row.acknowledged_at is not None:
            lines.append(
                _line(
                    row.acknowledged_at,
                    order + 1,
                    RunLogCode.CONTROL_ACKNOWLEDGED,
                    row.task_code,
                    None,
                    control=row.control_id,
                    action=row.action,
                )
            )
        if row.completed_at is not None:
            lines.append(
                _line(
                    row.completed_at,
                    order + 2,
                    RunLogCode.CONTROL_COMPLETED,
                    row.task_code,
                    None,
                    control=row.control_id,
                    action=row.action,
                    message=row.message,
                )
            )
    return lines


def _intervention_lines(conn: Connection, params: dict[str, int]) -> list[LogLine]:
    return [
        _line(
            row.requested_at,
            row.intervention_id,
            RunLogCode.INTERVENTION,
            row.task_code,
            _actor(row.requested_by, row.requested_by_kind),
            action=row.action,
            **{"from": row.from_status, "to": row.to_status},
            reason=row.reason,
        )
        for row in conn.execute(statement(conn, "run_log_interventions"), params)
    ]
