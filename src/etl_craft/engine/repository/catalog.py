"""What the documentation catalog reads: pipelines, tasks and rules, with their latest runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy.engine import Connection

from etl_craft.engine.queries import statement


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
