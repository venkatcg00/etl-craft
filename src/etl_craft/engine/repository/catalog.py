"""What the documentation catalog reads: pipelines, tasks and rules, with their latest runs."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime

from sqlalchemy import Boolean
from sqlalchemy.engine import Connection

from etl_craft.core.enums import CatalogRunKind
from etl_craft.core.failures import retry_reason
from etl_craft.core.time import elapsed_seconds
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.intervention_rows import Intervention
from etl_craft.engine.repository.normalization import ObjectIdentity
from etl_craft.engine.repository.pipelines import read_tags
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
    reject_count: int | None = None
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
    tags: tuple[str, ...] = ()


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
    has_successful_run: bool = False


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


def fetch_catalog_tables(conn: Connection) -> list[tuple[ObjectIdentity, str | None]]:
    """Read active table identities and optional kind contracts, even without columns."""
    return [
        ((r.catalog_name, r.schema_name, r.table_name), r.object_kind)
        for r in conn.execute(statement(conn, "catalog_tables"))
    ]


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
            read_tags(r.pipeline_parameters, r.pipeline_code),
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
                reject_count=r.reject_count,
                error_message=r.error_message,
            )
            if r.run_status is not None
            else None,
            bool(r.has_successful_run),
        )
        for r in conn.execute(statement(conn, "catalog_tasks"))
    ]


def fetch_catalog_rules(conn: Connection) -> list[RuleRow]:
    """Return every active business rule of an active task."""
    return [RuleRow(**r._mapping) for r in conn.execute(statement(conn, "catalog_rules"))]


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
    started_by: str | None = None
    started_by_kind: str | None = None
    ended_by: str | None = None
    ended_by_kind: str | None = None
    run_kind: CatalogRunKind = CatalogRunKind.OTHER
    bundle_id: str | None = None
    metadata_version: str | None = None
    newer_metadata_available: bool = False

    @property
    def seconds(self) -> float | None:
        """How long the run took, or ``None`` while it has not ended."""
        return elapsed_seconds(self.start, self.end)


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
    reject_count: int | None
    error_message: str | None
    watermark_from: str | None = None
    watermark_to: str | None = None
    run_kind: CatalogRunKind = CatalogRunKind.OTHER
    failure_reasons: tuple[str, ...] = ()
    item_progress: str | None = None

    @property
    def seconds(self) -> float | None:
        """How long the latest attempt took, or ``None`` while it has not ended."""
        return elapsed_seconds(self.start, self.end)


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
    versions = {
        row.pipeline_code: row.metadata_version
        for row in conn.execute(statement(conn, "catalog_metadata_versions"))
    }
    runs: dict[str, list[RunSummary]] = {}
    query = statement(conn, "catalog_pipeline_runs").columns(backfill=Boolean)
    for row in conn.execute(query, {"limit": limit}):
        values = dict(row._mapping)
        code = values.pop("pipeline_code")
        values["newer_metadata_available"] = (
            row.metadata_version is not None and row.metadata_version != versions.get(code)
        )
        values["run_date"] = None if row.run_date is None else as_date(row.run_date)
        values["run_kind"] = CatalogRunKind(row.run_kind)
        runs.setdefault(code, []).append(RunSummary(**values))
    return runs


def fetch_task_runs(conn: Connection, limit: int = RUN_HISTORY) -> dict[int, list[TaskRunSummary]]:
    """Return the latest ``limit`` runs of every active task, by task id, newest first."""
    runs: dict[int, list[TaskRunSummary]] = {}
    rows = conn.execute(statement(conn, "catalog_task_runs"), {"limit": limit}).all()
    failures: dict[int, list[str]] = {}
    if rows:
        query = statement(conn, "catalog_attempt_failures").columns(retryable=Boolean)
        task_runs = json.dumps([r.task_run_id for r in rows])
        for attempt in conn.execute(query, {"task_run_ids": task_runs}):
            reason = retry_reason(
                attempt.failure_class,
                attempt.failure_source,
                attempt.failure_code,
                attempt.retry_on,
            )
            failures.setdefault(attempt.task_run_id, []).append(
                f"Attempt {attempt.attempt_number}: {reason}; retry eligible={attempt.retryable}"
            )
    from etl_craft.engine.items import summaries

    item_progress = summaries(conn, [r.task_run_id for r in rows if r.has_items])
    for row in rows:
        values = dict(row._mapping)
        task_id = values.pop("task_id")
        task_run_id = values.pop("task_run_id")
        if values.pop("has_items", False):
            values["item_progress"] = str(item_progress[task_run_id])
        values["failure_reasons"] = tuple(failures.get(task_run_id, []))
        values["run_kind"] = CatalogRunKind(row.run_kind)
        runs.setdefault(task_id, []).append(TaskRunSummary(**values))
    return runs


def fetch_consumption(conn: Connection, limit: int = RUN_HISTORY) -> list[Consumption]:
    """Read consumption touching a shown run in either direction, oldest first."""
    return [
        Consumption(**r._mapping)
        for r in conn.execute(statement(conn, "catalog_consumption"), {"limit": limit})
    ]


def fetch_catalog_interventions(
    conn: Connection, limit: int = RUN_HISTORY
) -> dict[int, list[Intervention]]:
    """Read interventions of shown runs together, grouped by run identity."""
    changes: dict[int, list[Intervention]] = {}
    for row in conn.execute(statement(conn, "catalog_interventions"), {"limit": limit}):
        changes.setdefault(row.pipeline_run_id, []).append(Intervention(**row._mapping))
    return changes
