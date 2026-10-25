"""Daily project purges off the controller's lease-renewal loop."""

import contextvars
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from etl_craft.core.actor import SYSTEM_ACTOR, acting_as
from etl_craft.core.enums import ActionName
from etl_craft.engine.queries import statement
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.retention import PurgeInput

logger = logging.getLogger(__name__)


class RetentionSchedule:
    """A single worker with durable daily completion cursors and bounded retry polling."""

    def __init__(self) -> None:
        """Allocate no worker until a configured daily time is reached."""
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="etl-craft-retention")
        self.work: Future[None] | None = None
        self.next_check: datetime | None = None

    def refresh(self, ctx: OperationContext, now: datetime, stop: threading.Event) -> None:
        """Retry failed days, without replaying every missed day or blocking supervision."""
        if ctx.config.retention.daily_at is None or stop.is_set():
            return
        if self.work is not None:
            if not self.work.done():
                return
            error = self.work.exception()
            if error is not None:
                # Background process boundary: invoke records the refusal/failure as well.
                logger.error("daily retention failed: %s", error, exc_info=error)
            self.work = None
        if self.next_check is not None and now < self.next_check:
            return
        self.next_check = now + timedelta(minutes=1)
        self.work = self.executor.submit(contextvars.copy_context().run, purge_due, ctx, now, stop)

    def close(self) -> None:
        """Finish admitted maintenance before releasing controller leadership."""
        self.executor.shutdown(wait=True)


def purge_due(ctx: OperationContext, now: datetime, stop: threading.Event) -> None:
    """Use each project's zone, retaining only the latest completed day across restarts."""
    daily = ctx.config.retention.daily_at
    if daily is None:
        return
    with ctx.engine.connect() as conn:
        projects = conn.execute(statement(conn, "retention_projects")).all()
    for project in projects:
        if stop.is_set():
            return
        local = now.astimezone(ZoneInfo(project.timezone))
        if local.strftime("%H:%M") < daily:
            continue
        with ctx.engine.connect() as conn:
            previous = conn.execute(
                statement(conn, "retention_tick"),
                {
                    "project_id": project.project_id,
                },
            ).scalar_one_or_none()
        if previous is not None and str(previous) >= local.date().isoformat():
            continue
        config = replace(ctx.config, project=project.project_code, timezone=project.timezone)
        with acting_as(SYSTEM_ACTOR):
            invoke(
                replace(
                    ctx,
                    config=config,
                    actor=SYSTEM_ACTOR,
                    project=project.project_code,
                    project_id=project.project_id,
                ),
                ActionName.PURGE,
                PurgeInput(),
            )
