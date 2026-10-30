"""Workflow Monitor pages: the pipeline list, one pipeline's runs and tasks, and one run's graph.

Each view renders S5.AA read actions with the filters as their parameters; controls post to the
same `/api/v1` routes as the CLI, and the page only shows what the capabilities read allows.
"""

from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import Response

from etl_craft.api.ui_graph import run_graph
from etl_craft.api.ui_pages import page_values
from etl_craft.api.ui_templates import render
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import ResourceNotFoundError, UsageError
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.capabilities import CapabilitiesView
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import (
    GraphView,
    HistoryView,
    PipelineListView,
    PipelineView,
    RunView,
    StepsView,
    TaskRunView,
)
from etl_craft.services.operations.requests import (
    CapabilitiesInput,
    HistoryInput,
    PipelineInput,
    RunId,
    RunSelection,
    TagFilter,
    TaskSelection,
)
from etl_craft.services.operations.sessions import Identity

PIPELINE_TABS = frozenset({"runs", "tasks"})


def pipeline_url(project: str, pipeline_code: str) -> str:
    """Return the pipeline page's path, every part encoded."""
    return f"/ui/projects/{quote(project, safe='')}/pipelines/{quote(pipeline_code, safe='')}"


def run_url(project: str, pipeline_code: str, run_id: int) -> str:
    """Return the page of one exact run."""
    return f"{pipeline_url(project, pipeline_code)}/runs/{run_id}"


def controls_url(project: str) -> str:
    """Return the project's control route, the same one the CLI calls against a server."""
    return f"/api/v1/projects/{quote(project, safe='')}/controls"


def add_monitor_pages(app: FastAPI, ctx: OperationContext) -> None:
    """Mount the pipeline list, pipeline and run pages."""

    @app.get("/ui/projects/{project}/pipelines", include_in_schema=False)
    def pipelines(request: Request, project: str) -> Response:
        caller, values = page_values(ctx, request)
        return render(
            request, "monitor/pipelines.html", values | _list_values(caller, request, project)
        )

    @app.get("/ui/projects/{project}/pipelines/{pipeline_code}", include_in_schema=False)
    def pipeline(request: Request, project: str, pipeline_code: str) -> Response:
        caller, values = page_values(ctx, request)
        identity = values["identity"]
        assert isinstance(identity, Identity)
        return render(
            request,
            "monitor/pipeline.html",
            values | _pipeline_values(caller, request, identity, project, pipeline_code),
        )

    @app.get(
        "/ui/projects/{project}/pipelines/{pipeline_code}/runs/{run_id}", include_in_schema=False
    )
    def run(request: Request, project: str, pipeline_code: str, run_id: int) -> Response:
        caller, values = page_values(ctx, request)
        task_code = request.query_params.get("task") or None
        return render(
            request,
            "monitor/run.html",
            values
            | _run_values(caller, project, pipeline_code, run_id, task_code)
            | {"title": f"Run {run_id} · {pipeline_code} · etl-craft", "view": "pipelines"},
        )


def _list_values(caller: OperationContext, request: Request, project: str) -> dict[str, object]:
    """Read the tagged pipelines and, for the navigator, every visible pipeline's tags."""
    tags = tuple(request.query_params.getlist("tag"))
    listing = invoke(caller, ActionName.PIPELINES, TagFilter(tag=tags))
    everything = listing if not tags else invoke(caller, ActionName.PIPELINES, TagFilter())
    assert isinstance(listing, PipelineListView) and isinstance(everything, PipelineListView)
    return {
        "title": f"Pipelines · {project} · etl-craft",
        "view": "pipelines",
        "pipelines": listing.pipelines,
        "navigator": sorted({tag for p in everything.pipelines for tag in p.tags}),
        "selected_tags": tags,
        "pipeline_url": lambda code: pipeline_url(project, code),
    }


def _pipeline_values(
    caller: OperationContext, request: Request, identity: Identity, project: str, code: str
) -> dict[str, object]:
    """Read the definition, its task graph and one page of its runs, newest first."""
    tab = request.query_params.get("tab", "runs")
    if tab not in PIPELINE_TABS:
        raise UsageError(f"tab={tab!r}; expected runs or tasks; open the Runs tab")
    definition = invoke(caller, ActionName.PIPELINE, PipelineInput(pipeline_code=code))
    graph = invoke(caller, ActionName.GRAPH, PipelineInput(pipeline_code=code))
    history = invoke(
        caller,
        ActionName.HISTORY,
        HistoryInput(
            pipeline_code=code,
            all=True,
            limit=identity.settings.page_size,
            before=_positive(request, "before"),
        ),
    )
    assert isinstance(definition, PipelineView) and isinstance(graph, GraphView)
    assert isinstance(history, HistoryView)
    runs = tuple(entry for entry in history.entries if isinstance(entry, RunView))
    return {
        "title": f"{code} · {project} · etl-craft",
        "view": "pipelines",
        "tab": tab,
        "pipeline": definition,
        "graph": graph,
        "runs": runs,
        "longest": max((_seconds(run) for run in runs), default=0) or 1,
        "seconds": _seconds,
        "before": history.before,
        "base_url": pipeline_url(project, code),
        "controls_url": controls_url(project),
        "run_url": lambda run_id: run_url(project, code, run_id),
    }


def _run_values(
    caller: OperationContext, project: str, pipeline_code: str, run_id: int, task_code: str | None
) -> dict[str, object]:
    """Read the run, its tasks for the graph, and the selected scope's capabilities and panel."""
    run = invoke(caller, ActionName.RUN_DETAIL, RunId(run_id=run_id))
    assert isinstance(run, RunView)
    if run.pipeline_code != pipeline_code:
        raise ResourceNotFoundError("Not found or no access")
    selection = RunSelection(pipeline_code=pipeline_code, run_id=run_id)
    steps = invoke(caller, ActionName.STEPS, selection)
    graph = invoke(caller, ActionName.GRAPH, PipelineInput(pipeline_code=pipeline_code))
    assert isinstance(steps, StepsView) and isinstance(graph, GraphView)
    if task_code is not None and task_code not in {step.task_code for step in steps.steps}:
        raise ResourceNotFoundError("Not found or no access")
    capabilities = invoke(
        caller,
        ActionName.CAPABILITIES,
        CapabilitiesInput(pipeline_code=pipeline_code, run_id=run_id, task_code=task_code),
    )
    assert isinstance(capabilities, CapabilitiesView)
    page = run_url(project, pipeline_code, run_id)
    layout = run_graph(
        graph,
        {step.task_code: step.status for step in steps.steps},
        task_code,
        lambda code: f"{page}?task={quote(code, safe='')}",
    )
    return {
        "run": run,
        "steps": steps.steps,
        "layout": layout,
        "task_code": task_code,
        "capabilities": capabilities,
        "panel": None if task_code is None else _task_panel(caller, selection, task_code),
        "run_page": page,
        "pipeline_page": pipeline_url(project, pipeline_code),
        "controls_url": controls_url(project),
    }


def _task_panel(
    caller: OperationContext, selection: RunSelection, task_code: str
) -> dict[str, object]:
    """Read the selected task run's attempts and the engine's explanation of its state."""
    history = invoke(
        caller,
        ActionName.HISTORY,
        HistoryInput(
            pipeline_code=selection.pipeline_code, run_id=selection.run_id, task_code=task_code
        ),
    )
    explanation = invoke(
        caller,
        ActionName.EXPLAIN,
        TaskSelection(
            pipeline_code=selection.pipeline_code, run_id=selection.run_id, task_code=task_code
        ),
    )
    assert isinstance(history, HistoryView)
    task_runs = [entry for entry in history.entries if isinstance(entry, TaskRunView)]
    return {"task_run": task_runs[0] if task_runs else None, "explanation": explanation}


def _positive(request: Request, name: str) -> int | None:
    """Read an optional positive whole-number query parameter, refusing anything else."""
    value = request.query_params.get(name)
    if value is None:
        return None
    if not value.isdigit() or int(value) < 1:
        raise UsageError(f"{name}={value!r}; expected a positive run id; open the first page")
    return int(value)


def _seconds(run: RunView) -> int:
    """Return a run's recorded duration; zero until it has both a start and an end."""
    if run.start_date is None or run.end_date is None:
        return 0
    return max(0, int((run.end_date - run.start_date).total_seconds()))
