"""Immutable operation documents with canonical execution identities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, ClassVar

from etl_craft.core.enums import AttemptStatus, FailureClass, RunStatus, SlaStatus, TriggerKind
from etl_craft.engine.repository.intervention_rows import Intervention
from etl_craft.engine.repository.pauses import Pause


@dataclass(frozen=True)
class RunView:
    """A stored pipeline run, selected by its exact identity."""

    SCHEMA: ClassVar[str] = "etl-craft/run/1"
    pipeline_id: int
    pipeline_code: str
    pipeline_run_id: int
    run_key: str
    trigger_kind: TriggerKind
    run_date: date | None
    status: RunStatus
    start_date: datetime | None
    end_date: datetime | None
    sla_status: SlaStatus | None
    backfill: bool
    started_by: str | None
    started_by_kind: str | None
    ended_by: str | None
    ended_by_kind: str | None
    selected_task_ids: tuple[int, ...] | None = None
    ignore_dependencies: bool = False
    exclusive_run: bool = True
    triggered_by_task_run_id: int | None = None
    parent_run_id: int | None = None
    child_run_ids: tuple[int, ...] = ()
    bundle_id: str | None = None
    metadata_version: str | None = None
    newer_metadata_available: bool = False
    item_progress: str | None = None


@dataclass(frozen=True)
class AttemptView:
    """An attempt's stored outcome and ownership, beneath its task run."""

    SCHEMA: ClassVar[str] = "etl-craft/attempt/1"
    pipeline_id: int
    pipeline_run_id: int
    task_id: int
    task_run_id: int
    attempt_id: int
    attempt_number: int
    status: AttemptStatus
    owner_id: str | None
    lease_expires_at: datetime | None
    heartbeat_at: datetime | None
    queued_at: datetime | None
    claimed_at: datetime | None
    started_at: datetime | None
    ended_at: datetime | None
    host: str | None
    pid: int | None
    process_start: str | None
    exit_code: int | None
    source_count: int | None
    target_count: int | None
    insert_count: int | None
    update_count: int | None
    delete_count: int | None
    rows_written: int | None
    reject_count: int | None
    watermark_from: str | None
    watermark_to: str | None
    error_message: str | None
    log_path: str | None
    requested_by: str | None
    requested_by_kind: str | None
    not_before: datetime | None
    retryable: bool
    failure_class: FailureClass = FailureClass.UNKNOWN
    failure_source: str | None = None
    failure_code: str | None = None
    retry_on: str | None = None
    retry_reason: str | None = None
    item_progress: str | None = None


@dataclass(frozen=True)
class TaskRunView:
    """A stored task summary and its ordered attempts; a skipped task may have none."""

    SCHEMA: ClassVar[str] = "etl-craft/task-run/1"
    pipeline_id: int
    pipeline_run_id: int
    task_id: int
    task_code: str
    task_run_id: int
    status: RunStatus
    start_date: datetime | None
    end_date: datetime | None
    attempt_count: int
    source_count: int | None
    target_count: int | None
    insert_count: int | None
    update_count: int | None
    delete_count: int | None
    rows_written: int | None
    reject_count: int | None
    watermark_from: str | None
    watermark_to: str | None
    error_message: str | None
    attempts: tuple[AttemptView, ...]
    child_run_ids: tuple[int, ...] = ()
    item_progress: str | None = None


@dataclass(frozen=True)
class PipelineView:
    """An active pipeline definition and its open pause."""

    SCHEMA: ClassVar[str] = "etl-craft/pipeline/1"
    pipeline_id: int
    pipeline_code: str
    pipeline_name: str
    refresh_type: str
    run_schedule: str | None
    sla_in_hours: float | None
    paused: Pause | None
    tags: tuple[str, ...] = ()


@dataclass(frozen=True)
class OperationResult:
    """The call's outcome, separate from its stored pipeline and task states.

    A successful mark of FAILED has status SUCCESS here and FAILED in its run or task.
    A call that starts nothing has no invented run or task identity.
    """

    SCHEMA: ClassVar[str] = "etl-craft/operation/1"
    status: RunStatus
    message: str
    pipeline_id: int
    pipeline_code: str
    run: RunView | None = None
    task: TaskRunView | None = None
    pipeline: PipelineView | None = None
    waiting: bool = False


@dataclass(frozen=True)
class BackfillView:
    """Every date's outcome and the outcome that stopped a backfill, if any."""

    SCHEMA: ClassVar[str] = "etl-craft/backfill/1"
    pipeline_id: int
    pipeline_code: str
    first: date
    last: date
    status: RunStatus
    message: str
    runs: tuple[OperationResult, ...]
    stopped: OperationResult | None


@dataclass(frozen=True)
class PipelineListView:
    """Active pipelines in code order."""

    SCHEMA: ClassVar[str] = "etl-craft/pipeline-list/1"
    pipelines: tuple[PipelineView, ...]


@dataclass(frozen=True)
class GraphView:
    """A pipeline's static waves and dependency edges."""

    SCHEMA: ClassVar[str] = "etl-craft/graph/1"
    pipeline_id: int
    pipeline_code: str
    waves: tuple[tuple[str, ...], ...]
    conditional: tuple[str, ...]
    depends_on: Mapping[str, tuple[tuple[str, str], ...]]
    pipeline_dependencies: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class StepView:
    """A configured task and its identity under the selected run, if it ran."""

    SCHEMA: ClassVar[str] = "etl-craft/step/1"
    pipeline_id: int
    pipeline_run_id: int
    task_id: int
    task_code: str
    task_run_id: int | None
    task_type: str
    handler: str
    run_condition: str | None
    run_condition_count: int | None
    parameters: Mapping[str, str]
    status: RunStatus | None


@dataclass(frozen=True)
class StepsView:
    """Configured tasks under one explicitly selected pipeline run."""

    SCHEMA: ClassVar[str] = "etl-craft/steps/1"
    pipeline_id: int
    pipeline_run_id: int
    steps: tuple[StepView, ...]


@dataclass(frozen=True)
class HistoryView:
    """Bounded run or task history and the interventions on the runs shown."""

    SCHEMA: ClassVar[str] = "etl-craft/history/1"
    pipeline_id: int | None
    pipeline_code: str | None
    task_code: str | None
    entries: tuple[RunView | TaskRunView, ...]
    changes: tuple[Intervention, ...]
    before: int | None = None


@dataclass(frozen=True)
class ReconciliationView:
    """Exact attempt and run identities fenced or released by this call."""

    SCHEMA: ClassVar[str] = "etl-craft/reconciliation/1"
    lost_attempt_ids: tuple[int, ...]
    released_pipeline_run_ids: tuple[int, ...]
    message: str


@dataclass(frozen=True)
class ActionView:
    """A complete command request, independent of its flow's later outcome."""

    SCHEMA: ClassVar[str] = "etl-craft/action/1"
    action_id: int
    pipeline_id: int | None
    task_id: int | None
    at: datetime
    actor: str
    kind: str
    command: str
    outcome: str
    arguments: Mapping[str, Any]


@dataclass(frozen=True)
class MetadataChangeView:
    """A metadata edit, including the captured identities of deleted objects."""

    SCHEMA: ClassVar[str] = "etl-craft/metadata-change/1"
    change_id: int
    at: datetime
    actor: str
    kind: str
    table_name: str
    row_key: Any
    operation: str
    before_json: Mapping[str, Any] | None
    after_json: Mapping[str, Any] | None
    migration: str | None


@dataclass(frozen=True)
class AuditView:
    """Command requests and metadata changes in their audit order."""

    SCHEMA: ClassVar[str] = "etl-craft/audit/1"
    actions: tuple[ActionView, ...]
    changes: tuple[MetadataChangeView, ...]


@dataclass(frozen=True)
class Health:
    """The process answers its health probe."""

    SCHEMA: ClassVar[str] = "etl-craft/health/1"
    status: str = "ok"
