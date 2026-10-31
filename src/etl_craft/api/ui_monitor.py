"""Workflow Monitor pages: pipelines, one pipeline's runs and tasks, one run's graph, and search.

Each view renders S5.AA read actions with the filters as their parameters; controls post to the
same `/api/v1` routes as the CLI, and the page only shows what the capabilities read allows.
"""

from collections.abc import Iterator
from datetime import UTC, datetime
from urllib.parse import quote, urlencode

from fastapi import FastAPI, Request
from fastapi.responses import Response

from etl_craft.api.ui_graph import run_graph, run_timeline
from etl_craft.api.ui_pages import page_values
from etl_craft.api.ui_templates import render
from etl_craft.core.enums import ActionName, ApiRole, FailureClass
from etl_craft.core.enums_ui import SearchKind
from etl_craft.core.errors import ResourceNotFoundError, UsageError
from etl_craft.engine.logs import LIVE_ATTEMPTS
from etl_craft.services.operations.actions import has_role, invoke
from etl_craft.services.operations.capabilities import CapabilitiesView
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.items import ItemInput, ItemView
from etl_craft.services.operations.models import (
    AttemptView,
    GraphView,
    HistoryView,
    PipelineListView,
    PipelineView,
    RunView,
    StepsView,
    TaskRunView,
)
from etl_craft.services.operations.requests import (
    AttemptId,
    CapabilitiesInput,
    HistoryInput,
    PipelineInput,
    RunId,
    RunSelection,
    TagFilter,
    TaskSelection,
)
from etl_craft.services.operations.search import SearchHit, SearchInput, SearchView
from etl_craft.services.operations.sessions import Identity

PIPELINE_TABS = frozenset({"runs", "matrix", "tasks"})
RUN_VIEWS = frozenset({"graph", "timeline", "log"})


def pipeline_url(project: str, pipeline_code: str) -> str:
    """Return the pipeline page's path, every part encoded."""
    return f"/ui/projects/{quote(project, safe='')}/pipelines/{quote(pipeline_code, safe='')}"


def run_url(project: str, pipeline_code: str, run_id: int) -> str:
    """Return the page of one exact run."""
    return f"{pipeline_url(project, pipeline_code)}/runs/{run_id}"


def task_url(project: str, pipeline_code: str, task_code: str) -> str:
    """Return the page of one task across its runs."""
    return f"{pipeline_url(project, pipeline_code)}/tasks/{quote(task_code, safe='')}"


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
        return render(
            request,
            "monitor/run.html",
            values
            | _run_values(caller, request, project, pipeline_code, run_id)
            | {"title": f"Run {run_id} · {pipeline_code} · etl-craft", "view": "pipelines"},
        )

    @app.get(
        "/ui/projects/{project}/pipelines/{pipeline_code}/runs/{run_id}/attempts/{attempt_id}",
        include_in_schema=False,
    )
    def attempt(
        request: Request, project: str, pipeline_code: str, run_id: int, attempt_id: int
    ) -> Response:
        caller, values = page_values(ctx, request)
        return render(
            request,
            "monitor/attempt.html",
            values | _attempt_values(caller, project, pipeline_code, run_id, attempt_id),
        )

    @app.get(
        "/ui/projects/{project}/pipelines/{pipeline_code}/runs/{run_id}/tasks/{task_code}/items",
        include_in_schema=False,
    )
    def items(
        request: Request, project: str, pipeline_code: str, run_id: int, task_code: str
    ) -> Response:
        caller, values = page_values(ctx, request)
        return render(
            request,
            "monitor/items.html",
            values | _items_values(caller, project, pipeline_code, run_id, task_code),
        )

    @app.get("/ui/projects/{project}/search", include_in_schema=False)
    def search(request: Request, project: str) -> Response:
        caller, values = page_values(ctx, request)
        text = request.query_params.get("q", "")
        found = invoke(caller, ActionName.SEARCH, SearchInput(q=text)) if text.strip() else None
        assert found is None or isinstance(found, SearchView)
        return render(
            request,
            "monitor/search.html",
            values
            | {
                "title": f"Search · {project} · etl-craft",
                "view": "search",
                "search_text": text,
                "found": found,
                "hit_url": lambda hit: hit_url(project, hit),
            },
        )


def hit_url(project: str, hit: SearchHit) -> str:
    """Return the page that shows a search hit."""
    if hit.kind == SearchKind.TABLE:
        return f"/ui/projects/{quote(project, safe='')}/catalog?page={quote(str(hit.catalog_path))}"
    code = str(hit.pipeline_code)
    if hit.kind == SearchKind.RUN and hit.run_id is not None:
        return run_url(project, code, hit.run_id)
    if hit.kind == SearchKind.TASK:
        return task_url(project, code, hit.label)
    return pipeline_url(project, code)


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
        "controls_url": controls_url(project),
        "refresh_url": f"/api/v1/projects/{quote(project, safe='')}/pipelines/refresh",
        "can_develop": has_role(caller.role, ApiRole.DEVELOPER),
        "pipeline_url": lambda code: pipeline_url(project, code),
    }


def _pipeline_values(
    caller: OperationContext, request: Request, identity: Identity, project: str, code: str
) -> dict[str, object]:
    """Read the definition, its task graph and one page of its runs, newest first."""
    tab = request.query_params.get("tab", "runs")
    if tab not in PIPELINE_TABS:
        raise UsageError(f"tab={tab!r}; expected runs, matrix or tasks; open the Runs tab")
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
            pool=(filters := _run_filters(request))[0],
            agent=filters[1],
            failure_class=filters[2],
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
        "matrix": _matrix(caller, code, graph, runs) if tab == "matrix" else {},
        "base_url": pipeline_url(project, code),
        "controls_url": controls_url(project),
        "runs_api": f"/api/v1/projects/{quote(project, safe='')}/runs",
        "project_api": f"/api/v1/projects/{quote(project, safe='')}",
        "run_url": lambda run_id: run_url(project, code, run_id),
        "task_url": lambda task_code: task_url(project, code, task_code),
        "filters": dict(zip(("pool", "agent", "failure_class"), filters, strict=True)),
        "filter_query": urlencode(
            {
                k: v
                for k, v in zip(("pool", "agent", "failure_class"), filters, strict=True)
                if v is not None
            }
        ),
        "failure_classes": tuple(FailureClass),
    }


def _run_filters(request: Request) -> tuple[str | None, int | None, FailureClass | None]:
    """Read the Runs tab's pool, agent and failure-class filters; empty fields do not filter."""
    value = request.query_params.get("failure_class") or None
    if value is not None and value not in tuple(FailureClass):
        raise UsageError(
            f"failure_class={value!r}; expected {', '.join(FailureClass)}; choose one or none"
        )
    return (
        request.query_params.get("pool") or None,
        _positive(request, "agent"),
        None if value is None else FailureClass(value),
    )


def _matrix(
    caller: OperationContext, code: str, graph: GraphView, runs: tuple[RunView, ...]
) -> dict[str, dict[int, TaskRunView]]:
    """Read each task's runs for the listed pipeline runs: one bounded history per task."""
    if not runs:
        return {}
    newest = runs[0].pipeline_run_id
    cells: dict[str, dict[int, TaskRunView]] = {}
    for task_code in (task for wave in graph.waves for task in wave):
        history = invoke(
            caller,
            ActionName.HISTORY,
            HistoryInput(
                pipeline_code=code,
                task_code=task_code,
                all=True,
                limit=len(runs),
                before=newest + 1,
            ),
        )
        assert isinstance(history, HistoryView)
        cells[task_code] = {
            entry.pipeline_run_id: entry
            for entry in history.entries
            if isinstance(entry, TaskRunView)
        }
    return cells


def _run_values(
    caller: OperationContext, request: Request, project: str, pipeline_code: str, run_id: int
) -> dict[str, object]:
    """Read the run, its tasks for the graph, and the selected scope's capabilities and panel."""
    task_code = request.query_params.get("task") or None
    shown = request.query_params.get("view", "graph")
    if shown not in RUN_VIEWS:
        raise UsageError(f"view={shown!r}; expected graph, timeline or log; open the graph")
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
    view_part = "" if shown == "graph" else f"view={shown}&"

    def select(code: str) -> str:
        return f"{page}?{view_part}task={quote(code, safe='')}"

    layout = run_graph(
        graph, {step.task_code: step.status for step in steps.steps}, task_code, select
    )
    timeline = (
        run_timeline(
            [
                task_run
                for step in steps.steps
                if step.task_run_id is not None
                for task_run in _task_runs(caller, selection, step.task_code)
            ],
            datetime.now(UTC),
            task_code,
            select,
        )
        if shown == "timeline"
        else None
    )
    return {
        "run": run,
        "steps": steps.steps,
        "shown": shown,
        "select": select,
        "layout": layout,
        "timeline": timeline,
        "run_log": _run_log(caller, selection) if shown == "log" else None,
        "run_query": urlencode({"pipeline_code": pipeline_code, "run_id": run_id}),
        "project_api": f"/api/v1/projects/{quote(project, safe='')}",
        "task_code": task_code,
        "capabilities": capabilities,
        "panel": None if task_code is None else _task_panel(caller, selection, task_code),
        "run_page": page,
        "pipeline_page": pipeline_url(project, pipeline_code),
        "controls_url": controls_url(project),
    }


def _attempt_values(
    caller: OperationContext, project: str, pipeline_code: str, run_id: int, attempt_id: int
) -> dict[str, object]:
    """Read one attempt of the named run; any other pairing is not found, as for runs."""
    run = invoke(caller, ActionName.RUN_DETAIL, RunId(run_id=run_id))
    found = invoke(caller, ActionName.ATTEMPT, AttemptId(attempt_id=attempt_id))
    assert isinstance(run, RunView) and isinstance(found, AttemptView)
    if run.pipeline_code != pipeline_code or found.pipeline_run_id != run_id:
        raise ResourceNotFoundError("Not found or no access")
    steps = invoke(
        caller, ActionName.STEPS, RunSelection(pipeline_code=pipeline_code, run_id=run_id)
    )
    assert isinstance(steps, StepsView)
    task_code = next(s.task_code for s in steps.steps if s.task_run_id == found.task_run_id)
    api = f"/api/v1/projects/{quote(project, safe='')}/attempts/{attempt_id}"
    return {
        "title": f"Attempt {found.attempt_number} · {task_code} · Run {run_id} · etl-craft",
        "view": "pipelines",
        "attempt": found,
        "run": run,
        "task_code": task_code,
        "live": found.status in LIVE_ATTEMPTS,
        "attempt_api": api,
        "log_api": f"{api}/log",
        "run_page": run_url(project, pipeline_code, run_id),
        "pipeline_page": pipeline_url(project, pipeline_code),
    }


def _run_log(caller: OperationContext, selection: RunSelection) -> str:
    """Read the run's log, rendered from the audit tables, for the page."""
    chunks = invoke(caller, ActionName.RUN_LOG, selection)
    assert isinstance(chunks, Iterator)
    return b"".join(chunks).decode()


def _items_values(
    caller: OperationContext, project: str, pipeline_code: str, run_id: int, task_code: str
) -> dict[str, object]:
    """Read one task run's items under the named run; any other pairing is not found."""
    run = invoke(caller, ActionName.RUN_DETAIL, RunId(run_id=run_id))
    assert isinstance(run, RunView)
    if run.pipeline_code != pipeline_code:
        raise ResourceNotFoundError("Not found or no access")
    selected = ItemInput(pipeline_code=pipeline_code, run_id=run_id, task_code=task_code)
    found = invoke(caller, ActionName.ITEMS, selected)
    assert isinstance(found, ItemView)
    base = f"/api/v1/projects/{quote(project, safe='')}"
    return {
        "title": f"Items · {task_code} · Run {run_id} · etl-craft",
        "view": "pipelines",
        "found": found,
        "run": run,
        "task_code": task_code,
        "run_page": run_url(project, pipeline_code, run_id),
        "pipeline_page": pipeline_url(project, pipeline_code),
        "controls": f"{base}/pipelines/{quote(pipeline_code, safe='')}/runs/{run_id}"
        f"/tasks/{quote(task_code, safe='')}/items/controls",
        "controls_url": controls_url(project),
    }


def _task_panel(
    caller: OperationContext, selection: RunSelection, task_code: str
) -> dict[str, object]:
    """Read the selected task run's attempts and the engine's explanation of its state."""
    task_runs = _task_runs(caller, selection, task_code)
    explanation = invoke(
        caller,
        ActionName.EXPLAIN,
        TaskSelection(
            pipeline_code=selection.pipeline_code, run_id=selection.run_id, task_code=task_code
        ),
    )
    return {"task_run": task_runs[0] if task_runs else None, "explanation": explanation}


def _task_runs(
    caller: OperationContext, selection: RunSelection, task_code: str
) -> list[TaskRunView]:
    """Read one task's run (with its attempts) under the selected run."""
    history = invoke(
        caller,
        ActionName.HISTORY,
        HistoryInput(
            pipeline_code=selection.pipeline_code, run_id=selection.run_id, task_code=task_code
        ),
    )
    assert isinstance(history, HistoryView)
    return [entry for entry in history.entries if isinstance(entry, TaskRunView)]


def _positive(request: Request, name: str) -> int | None:
    """Read an optional positive whole-number query parameter, refusing anything else."""
    value = request.query_params.get(name)
    if not value:
        return None
    if not value.isdigit() or int(value) < 1:
        raise UsageError(f"{name}={value!r}; expected a positive run id; open the first page")
    return int(value)


def _seconds(run: RunView) -> int:
    """Return a run's recorded duration; zero until it has both a start and an end."""
    if run.start_date is None or run.end_date is None:
        return 0
    return max(0, int((run.end_date - run.start_date).total_seconds()))
