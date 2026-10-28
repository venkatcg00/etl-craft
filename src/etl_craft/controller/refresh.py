"""Refresh directly edited PostgreSQL metadata on the existing maintenance worker."""

import threading
from dataclasses import replace

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.text import sha256_hex
from etl_craft.dialects.engine import for_engine
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import selected_project
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke, project_settings
from etl_craft.services.operations.pipeline_refresh import RefreshInput, initialize_missing


def refresh_due(ctx: OperationContext, stop: threading.Event) -> None:
    """Compare committed metadata hashes after SQL history exists; never poll project files."""
    if not for_engine(ctx.engine).supports_api_replicas or stop.is_set():
        return
    with ctx.engine.connect() as conn:
        projects = conn.execute(statement(conn, "refresh_direct_projects")).all()
    for project in projects:
        if stop.is_set():
            return
        scoped = replace(
            ctx,
            actor=SYSTEM_ACTOR,
            role=ApiRole.ADMIN,
            project=project.project_code,
            project_id=int(project.project_id),
        )
        with acting_as(SYSTEM_ACTOR), selected_project(project.project_id):
            selected = project_settings(scoped, ActionName.CLONE, RefreshInput())
            if _changed(selected):
                invoke(selected, ActionName.PIPELINE_REFRESH, RefreshInput())


def _changed(ctx: OperationContext) -> bool:
    """Detect late commits even when their history IDs or dates are older."""
    with ctx.engine.connect() as conn:
        pipelines = conn.execute(
            statement(conn, "refresh_pipelines"),
            {
                "project_id": ctx.project_id,
                "pipeline_code": None,
                "missing_only": False,
            },
        ).all()
        for pipeline in pipelines:
            snapshot = conn.execute(
                statement(conn, "refresh_current"),
                {
                    "pipeline_id": pipeline.pipeline_id,
                },
            ).scalar_one()
            previous = conn.execute(
                statement(conn, "refresh_latest_version"),
                {
                    "pipeline_id": pipeline.pipeline_id,
                },
            ).scalar_one_or_none()
            if previous != sha256_hex(snapshot.encode()):
                return True
    return False


def initialize(ctx: OperationContext) -> None:
    """Seed every configured or published project before schedule admission starts."""
    with ctx.engine.connect() as conn:
        projects = conn.execute(
            statement(conn, "clone_projects"),
            {
                "bootstrap_project": ctx.config.project,
            },
        ).all()
    for project in projects:
        scoped = replace(
            ctx,
            actor=SYSTEM_ACTOR,
            role=ApiRole.ADMIN,
            project=project.project_code,
            project_id=int(project.project_id),
        )
        with acting_as(SYSTEM_ACTOR), selected_project(project.project_id):
            if project.project_code != ctx.config.project:
                scoped = project_settings(scoped, ActionName.CLONE, RefreshInput())
            initialize_missing(scoped)
