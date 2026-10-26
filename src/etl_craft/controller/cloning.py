"""Coalesced per-project clone deadlines on the existing maintenance worker."""

import threading
from dataclasses import replace
from datetime import datetime, timedelta

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import ActionName
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import selected_project
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke, project_settings
from etl_craft.services.operations.requests import NoInput


def clone_due(
    ctx: OperationContext, now: datetime, stop: threading.Event, deadlines: dict[int, datetime]
) -> None:
    """Run each due project's latest approved connection, never replaying missed intervals."""
    with ctx.engine.connect() as conn:
        projects = conn.execute(
            statement(conn, "clone_projects"), {"bootstrap_project": ctx.config.project}
        ).all()
    for project in projects:
        if stop.is_set():
            return
        project_ctx = replace(
            ctx,
            actor=SYSTEM_ACTOR,
            project=project.project_code,
            project_id=project.project_id,
            config=replace(ctx.config, timezone=project.timezone),
        )
        with acting_as(SYSTEM_ACTOR), selected_project(project.project_id):
            selected = project_settings(project_ctx, ActionName.CLONE, NoInput())
            if not selected.config.cloning.enabled:
                continue
            if (deadline := deadlines.get(project.project_id)) is not None and now < deadline:
                continue
            deadlines[project.project_id] = now + timedelta(
                minutes=selected.config.cloning.every_minutes
            )
            invoke(selected, ActionName.CLONE, NoInput())
