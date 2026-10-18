"""Routes that select a pipeline's run as the CLI does: by run id, run key or the active run."""

from collections.abc import Callable
from datetime import date
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Query
from pydantic import BaseModel, ConfigDict, Field

from etl_craft.core.enums import ControlAction
from etl_craft.engine.runlog import RunSelector
from etl_craft.services.operations import (
    OperationContext,
    inspect,
    runs,
    sessions,
    tasks,
    to_json,
)
from etl_craft.services.operations.controls import ControlRequest, get_control, request_control
from etl_craft.services.operations.models import HistoryQuery
from etl_craft.services.operations.requests import RunRequest
from etl_craft.services.operations.status import explain_task, pipeline_status

Authorize = Callable[[str], Callable[..., OperationContext]]


class Selected(BaseModel):
    """A required reason and the run it applies to; without a selector, the active run."""

    reason: str = Field(min_length=1, max_length=4096, pattern=r"\S")
    run_id: int | None = Field(default=None, ge=1)
    run_key: str | None = None

    @property
    def selector(self) -> RunSelector:
        """The run this request selects."""
        return RunSelector(self.run_id, self.run_key)


class ScopedControl(Selected):
    """One exact run/task action with a durable request identity."""

    model_config = ConfigDict(extra="forbid")
    action: ControlAction
    task_code: str | None = None
    request_key: str | None = Field(default=None, min_length=1, max_length=128)


class PipelineMark(Selected):
    """A run, task or new stand-in run's requested status, or a task's watermark reset."""

    task_code: str | None = None
    status: str | None = None
    watermark: str | None = None
    rows: int | None = Field(default=None, ge=0)
    stale: bool = False
    new_run: bool = False


class TaskRerun(Selected):
    """Whether to rerun the tasks downstream of the task as well."""

    with_downstream: bool = False


class TaskRun(BaseModel):
    """An explicit independent selection or one task under a selected pipeline run."""

    model_config = ConfigDict(extra="forbid")
    standalone: bool = False
    run_id: int | None = Field(default=None, ge=1)
    run_key: str | None = None
    with_downstream: bool = False
    ignore_dependencies: bool = False
    run_date: date | None = None
    reason: str | None = Field(default=None, min_length=1, max_length=4096, pattern=r"\S")


def add_selected_routes(app: FastAPI, authorize: Authorize) -> None:
    """Add pipeline-scoped reads and controls, and the caller's own identity."""

    @app.get("/api/v1/whoami")
    def whoami(caller: Annotated[OperationContext, Depends(authorize("whoami"))]) -> dict[str, Any]:
        return to_json(sessions.whoami(caller))

    @app.get("/api/v1/pipelines/{code}/status")
    def status(
        code: str,
        caller: Annotated[OperationContext, Depends(authorize("status"))],
        run_id: Annotated[int | None, Query(ge=1)] = None,
        run_key: str | None = None,
    ) -> dict[str, Any]:
        return to_json(pipeline_status(caller, code, selector=RunSelector(run_id, run_key)))

    @app.get("/api/v1/pipelines/{code}/tasks/{task}/explain")
    def explain(
        code: str,
        task: str,
        caller: Annotated[OperationContext, Depends(authorize("explain"))],
        run_id: Annotated[int | None, Query(ge=1)] = None,
        run_key: str | None = None,
    ) -> dict[str, Any]:
        return to_json(explain_task(caller, code, task, selector=RunSelector(run_id, run_key)))

    @app.get("/api/v1/pipelines/{code}/runs")
    def history(
        code: str,
        caller: Annotated[OperationContext, Depends(authorize("history"))],
        query: Annotated[HistoryQuery, Depends(_history_query)],
    ) -> dict[str, Any]:
        return to_json(inspect.run_history(caller, code, query))

    _add_selected_controls(app, authorize)
    _add_scoped_controls(app, authorize)


def _history_query(
    task_code: str | None = None,
    all_runs: Annotated[bool, Query(alias="all")] = True,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    before: Annotated[int | None, Query(ge=1)] = None,
    run_id: Annotated[int | None, Query(ge=1)] = None,
    run_key: str | None = None,
) -> HistoryQuery:
    """Read the history options from the query string; a page of all runs unless told otherwise."""
    return HistoryQuery(task_code, limit, all_runs, before, RunSelector(run_id, run_key))


def _add_selected_controls(app: FastAPI, authorize: Authorize) -> None:
    """Add cancel, mark and rerun of the selected run."""

    @app.post("/api/v1/pipelines/{code}/tasks/{task}/run")
    def run_task(
        code: str,
        task: str,
        body: TaskRun,
        caller: Annotated[OperationContext, Depends(authorize("run"))],
    ) -> dict[str, Any]:
        return to_json(
            runs.execute_run(
                caller,
                RunRequest(
                    code,
                    task_code=task,
                    selector=RunSelector(body.run_id, body.run_key),
                    standalone=body.standalone,
                    with_downstream=body.with_downstream,
                    ignore_dependencies=body.ignore_dependencies,
                    run_date=body.run_date,
                    reason=body.reason,
                ),
            )
        )

    @app.post("/api/v1/pipelines/{code}/cancel")
    def cancel(
        code: str, body: Selected, caller: Annotated[OperationContext, Depends(authorize("cancel"))]
    ) -> dict[str, Any]:
        return to_json(runs.cancel_run(caller, code, body.reason, selector=body.selector))

    @app.post("/api/v1/pipelines/{code}/mark")
    def mark(
        code: str,
        body: PipelineMark,
        caller: Annotated[OperationContext, Depends(authorize("mark"))],
    ) -> dict[str, Any]:
        return to_json(
            runs.mark(
                caller,
                code,
                body.status,
                body.reason,
                task_code=body.task_code,
                rows=body.rows,
                watermark=body.watermark,
                stale=body.stale,
                new_run=body.new_run,
                selector=body.selector,
            )
        )

    @app.post("/api/v1/pipelines/{code}/tasks/{task}/rerun")
    def rerun(
        code: str,
        task: str,
        body: TaskRerun,
        caller: Annotated[OperationContext, Depends(authorize("run"))],
    ) -> dict[str, Any]:
        return to_json(
            tasks.rerun_task(
                caller,
                code,
                task,
                body.reason,
                with_downstream=body.with_downstream,
                selector=body.selector,
            )
        )


def _add_scoped_controls(app: FastAPI, authorize: Authorize) -> None:
    @app.post("/api/v1/pipelines/{code}/controls")
    def control(
        code: str,
        body: ScopedControl,
        caller: Annotated[OperationContext, Depends(authorize("cancel"))],
    ) -> dict[str, Any]:
        return to_json(
            request_control(
                caller,
                ControlRequest(
                    code,
                    body.action,
                    body.reason,
                    body.selector,
                    body.task_code,
                    body.request_key,
                ),
            )
        )

    @app.get("/api/v1/control-requests/{control_id}")
    def control_status(
        control_id: int,
        caller: Annotated[OperationContext, Depends(authorize("status"))],
    ) -> dict[str, Any]:
        return to_json(get_control(caller, control_id))
