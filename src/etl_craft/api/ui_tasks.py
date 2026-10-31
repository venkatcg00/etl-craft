"""The task page (statistics across runs and its stored offset) and the Uploads page.

Both render existing read actions; the offset reset and the upload post the same `/api/v1`
requests as `etl-craft mark --offset|--watermark` and `etl-craft upload`.
"""

from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import Response

from etl_craft.api.ui_monitor import pipeline_url, run_url
from etl_craft.api.ui_pages import page_values
from etl_craft.api.ui_templates import render
from etl_craft.core.enums import ActionName, ApiRole, RunStatus
from etl_craft.core.time import as_utc
from etl_craft.services.operations.actions import has_role, invoke
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import HistoryView, PipelineView, TaskRunView
from etl_craft.services.operations.offsets import TaskInput, TaskOffset
from etl_craft.services.operations.requests import HistoryInput, NoInput, PipelineInput
from etl_craft.services.operations.uploads import UploadHistory, UploadPipelines, UploadRequest

TASK_RUNS = 50
"""Task runs read for the statistics, newest first."""


def add_task_pages(app: FastAPI, ctx: OperationContext) -> None:
    """Mount the task page and the Uploads page."""

    @app.get(
        "/ui/projects/{project}/pipelines/{pipeline_code}/tasks/{task_code}",
        include_in_schema=False,
    )
    def task(request: Request, project: str, pipeline_code: str, task_code: str) -> Response:
        caller, values = page_values(ctx, request)
        return render(
            request,
            "monitor/task.html",
            values | _task_values(caller, project, pipeline_code, task_code),
        )

    @app.get("/ui/projects/{project}/uploads", include_in_schema=False)
    def uploads(request: Request, project: str) -> Response:
        caller, values = page_values(ctx, request)
        return render(
            request,
            "uploads.html",
            values | _upload_values(caller, project, request.query_params.get("pipeline")),
        )


def _task_values(
    caller: OperationContext, project: str, code: str, task_code: str
) -> dict[str, object]:
    """One page of the task's runs with their counts, and what it keeps between runs."""
    definition = invoke(caller, ActionName.PIPELINE, PipelineInput(pipeline_code=code))
    history = invoke(
        caller,
        ActionName.HISTORY,
        HistoryInput(pipeline_code=code, task_code=task_code, all=True, limit=TASK_RUNS),
    )
    offset = invoke(caller, ActionName.OFFSET, TaskInput(pipeline_code=code, task_code=task_code))
    assert isinstance(definition, PipelineView) and isinstance(history, HistoryView)
    assert isinstance(offset, TaskOffset)
    runs = [entry for entry in history.entries if isinstance(entry, TaskRunView)]
    ended = [r for r in runs if r.start_date and r.end_date]
    seconds = {
        r.task_run_id: (as_utc(r.end_date) - as_utc(r.start_date)).total_seconds() for r in ended
    }
    return {
        "title": f"{task_code} · {code} · {project} · etl-craft",
        "view": "pipelines",
        "pipeline": definition,
        "task_code": task_code,
        "runs": runs,
        "seconds": seconds,
        "average": sum(seconds.values()) / len(seconds) if seconds else None,
        "succeeded": sum(r.status == RunStatus.SUCCESS for r in runs),
        "offset": offset,
        "may_reset": has_role(caller.role, ApiRole.OPERATOR),
        "marks_api": f"/api/v1/projects/{quote(project, safe='')}/marks",
        "pipeline_url": pipeline_url(project, code),
        "run_url": lambda run_id: run_url(project, code, run_id),
        "standing_pause": next(
            (p for p in definition.task_pauses if p.task_code == task_code), None
        ),
    }


def _upload_values(
    caller: OperationContext, project: str, selected: str | None
) -> dict[str, object]:
    """Read the pipelines the caller may upload to and the chosen one's recent files."""
    eligible = invoke(caller, ActionName.UPLOAD_PIPELINES, NoInput())
    assert isinstance(eligible, UploadPipelines)
    codes = [str(p["pipeline_code"]) for p in eligible.pipelines]
    chosen = selected if selected in codes else (codes[0] if codes else None)
    history = (
        invoke(caller, ActionName.UPLOADS, UploadRequest(pipeline_code=chosen))
        if chosen is not None
        else UploadHistory(())
    )
    assert isinstance(history, UploadHistory)
    return {
        "title": f"Uploads · {project} · etl-craft",
        "view": "uploads",
        "eligible": eligible.pipelines,
        "chosen": chosen,
        "files": [{**row, "uploaded_at": as_utc(row["uploaded_at"])} for row in history.uploads],
        "uploads_api": f"/api/v1/projects/{quote(project, safe='')}/uploads",
        "pipeline_url": lambda code: pipeline_url(project, code),
        "run_url": lambda code, run_id: run_url(project, code, run_id),
    }
