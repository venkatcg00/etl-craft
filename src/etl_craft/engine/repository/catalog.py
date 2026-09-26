"""What the documentation catalog reads: pipelines, tasks and rules, with their latest runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement
from etl_craft.engine.runlog import as_date


@dataclass(frozen=True)
class LastRun:
    """A pipeline's or task's latest run: its status, when, and a task's counts and error."""

    status: str
    start: datetime | None
    end: datetime | None
    sla_status: str | None = None
    source_count: int | None = None
    target_count: int | None = None
    insert_count: int | None = None
    update_count: int | None = None
    delete_count: int | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class PipelineRow:
    """An active pipeline and its latest run, if it has run."""

    pipeline_id: int
    pipeline_code: str
    pipeline_name: str
    description: str | None
    run_schedule: str | None
    sla_in_hours: float | None
    refresh_type: str
    last_run: LastRun | None
    last_run_id: int | None = None


@dataclass(frozen=True)
class TaskRow:
    """An active task and its latest finished run, if it has one."""

    task_id: int
    pipeline_code: str
    task_code: str
    task_type: str
    handler: str
    run_condition: str | None
    last_run: LastRun | None


@dataclass(frozen=True)
class RuleRow:
    """An active business rule of an active task."""

    business_rule_id: int
    name: str
    rule_type: str
    sql: str
    key_column: str
    target_table: str
    sequence_number: int
    pipeline_code: str
    task_code: str


def fetch_catalog_pipelines(conn: Connection) -> list[PipelineRow]:
    """Return every active pipeline with its latest run."""
    return [
        PipelineRow(
            r.pipeline_id,
            r.pipeline_code,
            r.pipeline_name,
            r.description,
            r.run_schedule,
            float(r.sla_in_hours) if r.sla_in_hours is not None else None,
            r.refresh_type,
            LastRun(r.run_status, r.run_start, r.run_end, sla_status=r.sla_status)
            if r.pipeline_run_id is not None
            else None,
            None if r.pipeline_run_id is None else int(r.pipeline_run_id),
        )
        for r in conn.execute(statement(conn, "catalog_pipelines"))
    ]


def fetch_catalog_tasks(conn: Connection) -> list[TaskRow]:
    """Return every active task of an active pipeline with its latest finished run."""
    return [
        TaskRow(
            r.task_id,
            r.pipeline_code,
            r.task_code,
            r.task_type,
            r.handler,
            r.run_condition,
            LastRun(
                r.run_status,
                r.run_start,
                r.run_end,
                source_count=r.source_count,
                target_count=r.target_count,
                insert_count=r.insert_count,
                update_count=r.update_count,
                delete_count=r.delete_count,
                error_message=r.error_message,
            )
            if r.run_status is not None
            else None,
        )
        for r in conn.execute(statement(conn, "catalog_tasks"))
    ]


def fetch_catalog_rules(conn: Connection) -> list[RuleRow]:
    """Return every active business rule of an active task."""
    return [
        RuleRow(
            r.business_rule_id,
            r.business_rule_name,
            r.business_rule_type,
            r.business_rule_sql,
            r.key_column,
            r.target_table,
            int(r.sequence_number),
            r.pipeline_code,
            r.task_code,
        )
        for r in conn.execute(statement(conn, "catalog_rules"))
    ]


def fetch_documentation_versions(conn: Connection) -> dict[int, int]:
    """Return each documented task's latest documentation version, by task id."""
    rows = conn.execute(statement(conn, "catalog_documentation_versions"))
    return {int(r.task_id): int(r.version) for r in rows}


RUN_HISTORY = 30
"""How many recent runs of each pipeline and task the catalog shows."""


@dataclass(frozen=True)
class RunSummary:
    """One run of a pipeline, for its history."""

    pipeline_run_id: int
    status: str
    run_date: date | None
    backfill: bool
    start: datetime | None
    end: datetime | None
    sla_status: str | None

    @property
    def seconds(self) -> float | None:
        """How long the run took, or ``None`` while it has not ended."""
        return _seconds(self.start, self.end)


@dataclass(frozen=True)
class TaskRunSummary:
    """One run of a task, for its history."""

    pipeline_run_id: int
    status: str
    attempts: int
    start: datetime | None
    end: datetime | None
    source_count: int | None
    target_count: int | None
    insert_count: int | None
    update_count: int | None
    delete_count: int | None
    error_message: str | None

    @property
    def seconds(self) -> float | None:
        """How long the latest attempt took, or ``None`` while it has not ended."""
        return _seconds(self.start, self.end)


@dataclass(frozen=True)
class Consumption:
    """An upstream run a downstream run consumed; the tasks are set for a task dependency."""

    pipeline_code: str
    pipeline_run_id: int
    task_code: str | None
    upstream_pipeline: str
    upstream_run_id: int
    upstream_task: str | None


def fetch_pipeline_runs(conn: Connection, limit: int = RUN_HISTORY) -> dict[str, list[RunSummary]]:
    """Return the latest ``limit`` runs of every active pipeline, by code, newest first."""
    runs: dict[str, list[RunSummary]] = {}
    for r in conn.execute(statement(conn, "catalog_pipeline_runs"), {"limit": limit}):
        runs.setdefault(r.pipeline_code, []).append(
            RunSummary(
                int(r.pipeline_run_id),
                r.status,
                None if r.run_date is None else as_date(r.run_date),
                r.backfill == "Y",
                r.start_date,
                r.end_date,
                r.sla_status,
            )
        )
    return runs


def fetch_task_runs(conn: Connection, limit: int = RUN_HISTORY) -> dict[int, list[TaskRunSummary]]:
    """Return the latest ``limit`` runs of every active task, by task id, newest first."""
    runs: dict[int, list[TaskRunSummary]] = {}
    for r in conn.execute(statement(conn, "catalog_task_runs"), {"limit": limit}):
        runs.setdefault(int(r.task_id), []).append(
            TaskRunSummary(
                int(r.pipeline_run_id),
                r.status,
                int(r.attempts),
                r.start_date,
                r.end_date,
                r.source_count,
                r.target_count,
                r.insert_count,
                r.update_count,
                r.delete_count,
                r.error_message,
            )
        )
    return runs


def fetch_consumption(conn: Connection) -> list[Consumption]:
    """Return every logged consumption by a known downstream run, oldest first."""
    return [
        Consumption(
            r.pipeline_code,
            int(r.pipeline_run_id),
            r.task_code,
            r.upstream_pipeline,
            int(r.upstream_run_id),
            r.upstream_task,
        )
        for r in conn.execute(statement(conn, "catalog_consumption"))
    ]


def _seconds(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    if start.tzinfo is None:
        start = start.replace(tzinfo=UTC)
    if end.tzinfo is None:
        end = end.replace(tzinfo=UTC)
    return max((end - start).total_seconds(), 0.0)
