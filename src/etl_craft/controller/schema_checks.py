"""Scheduled warehouse schema checks on the existing maintenance worker.

A project's ``SCHEMA_CHECK_SCHEDULE`` makes the controller regenerate its catalog with the
warehouse comparison at each tick, in the project's timezone. Missed ticks are not replayed:
after a restart the next check is the next tick from then.
"""

import logging
import threading
from dataclasses import replace
from datetime import datetime

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.cron import next_after
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import EtlCraftError
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import selected_project
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke, project_settings
from etl_craft.services.operations.documents import DocsInput

logger = logging.getLogger(__name__)


def schema_checks_due(
    ctx: OperationContext, now: datetime, stop: threading.Event, due: dict[int, datetime]
) -> None:
    """Run each scheduled project's check once its tick has passed, then wait for the next."""
    with ctx.engine.connect() as conn:
        projects = conn.execute(statement(conn, "schema_check_projects")).all()
    scheduled = {project.project_id for project in projects}
    for project_id in set(due) - scheduled:
        del due[project_id]
    for project in projects:
        if stop.is_set():
            return
        expr = str(project.schema_check_schedule)
        if project.project_id not in due:
            due[project.project_id] = next_after(expr, now, project.timezone)
            continue
        if now < due[project.project_id]:
            continue
        due[project.project_id] = next_after(expr, now, project.timezone)
        project_ctx = replace(
            ctx,
            actor=SYSTEM_ACTOR,
            project=project.project_code,
            project_id=project.project_id,
            config=replace(ctx.config, timezone=project.timezone),
        )
        with acting_as(SYSTEM_ACTOR), selected_project(project.project_id):
            selected = project_settings(project_ctx, ActionName.GENERATE_DOCS, DocsInput())
            try:
                invoke(selected, ActionName.GENERATE_DOCS, DocsInput(with_warehouse=True))
            except EtlCraftError as error:
                # Maintenance boundary: the action's audit row records the failure; the next
                # tick tries again and earlier output stays until a check succeeds.
                logger.error("%s: scheduled schema check failed: %s", project.project_code, error)
