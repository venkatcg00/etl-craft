"""Operations pages: system health, agents, scheduled runs and project maintenance.

Each page renders existing read actions; every change posts the same `/api/v1` request the CLI
sends, so the action table checks the role and records the request.
"""

from datetime import UTC, datetime
from urllib.parse import quote

from fastapi import FastAPI, Request
from fastapi.responses import Response

from etl_craft.api.ui_pages import page_values
from etl_craft.api.ui_templates import render
from etl_craft.core.enums import ActionName, ApiRole, DeploymentProfile, SqlAction
from etl_craft.core.errors import PermissionDeniedError
from etl_craft.services.operations.about import About
from etl_craft.services.operations.actions import has_role, invoke
from etl_craft.services.operations.agents import AgentsView
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.models import PipelineListView
from etl_craft.services.operations.requests import NoInput, TagFilter


def add_operations_pages(app: FastAPI, ctx: OperationContext) -> None:
    """Mount system health, agents, scheduled runs and maintenance."""

    @app.get("/ui/system", include_in_schema=False)
    def system(request: Request) -> Response:
        caller, values = page_values(ctx, request)
        return render(request, "operations/system.html", values | _system_values(caller))

    @app.get("/ui/agents", include_in_schema=False)
    def agents(request: Request) -> Response:
        caller, values = page_values(ctx, request)
        return render(
            request,
            "operations/agents.html",
            values
            | {
                "title": "Agents · etl-craft",
                "view": "agents",
                "agents": _agents(caller),
                "may_command": has_role(caller.role, ApiRole.OPERATOR),
            },
        )

    @app.get("/ui/projects/{project}/scheduled", include_in_schema=False)
    def scheduled(request: Request, project: str) -> Response:
        caller, values = page_values(ctx, request)
        return render(
            request, "operations/scheduled.html", values | _scheduled_values(caller, project)
        )

    @app.get("/ui/projects/{project}/maintenance", include_in_schema=False)
    def maintenance(request: Request, project: str) -> Response:
        caller, values = page_values(ctx, request)
        if caller.role != ApiRole.ADMIN:
            raise PermissionDeniedError(
                "the maintenance page needs the admin role; ask an administrator"
            )
        return render(
            request,
            "operations/maintenance.html",
            values
            | {
                "title": f"Maintenance · {project} · etl-craft",
                "view": "maintenance",
                "api": f"/api/v1/projects/{quote(project, safe='')}",
                "upgradable": [
                    a for a in SqlAction if a not in {SqlAction.DROP_TABLE, SqlAction.CREATE_VIEW}
                ],
                "cloning": ctx.config.cloning.enabled,
            },
        )


def _agents(caller: OperationContext) -> AgentsView | None:
    """Read agents only where the deployment runs them."""
    if caller.config.deployment != DeploymentProfile.CONTROLLER_AGENTS:
        return None
    found = invoke(caller, ActionName.AGENTS, NoInput())
    assert isinstance(found, AgentsView)
    return found


def _system_values(caller: OperationContext) -> dict[str, object]:
    """Live controllers with their heartbeat age, the Engine DB versions and agents."""
    about = invoke(caller, ActionName.ABOUT, NoInput())
    assert isinstance(about, About)
    now = datetime.now(UTC)
    return {
        "title": "System health · etl-craft",
        "view": "system",
        "about": about,
        "agents": _agents(caller),
        "ages": {
            c.controller_id: int((now - c.heartbeat_at).total_seconds()) for c in about.controllers
        },
        "mixed": sorted({c.version for c in about.controllers} - {about.package_version}),
    }


def _scheduled_values(caller: OperationContext, project: str) -> dict[str, object]:
    """Scheduled pipelines by next run, and those that will not run with the reason."""
    listed = invoke(caller, ActionName.PIPELINES, TagFilter())
    assert isinstance(listed, PipelineListView)
    timed = [p for p in listed.pipelines if p.run_schedule]
    upcoming = [(p.next_run, p.pipeline_code, p) for p in timed if p.next_run is not None]
    return {
        "title": f"Scheduled runs · {project} · etl-craft",
        "view": "scheduled",
        "upcoming": [p for *_, p in sorted(upcoming, key=lambda row: row[:2])],
        "not_running": [p for p in timed if p.next_run is None],
        "pipelines_url": f"/ui/projects/{quote(project, safe='')}/pipelines",
    }
