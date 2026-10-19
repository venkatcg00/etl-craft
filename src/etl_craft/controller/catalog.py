"""One background catalog build per due tick, with durable request history."""

from __future__ import annotations

import contextvars
import logging
import threading
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime

from etl_craft.core.cron import next_after
from etl_craft.core.time import as_utc
from etl_craft.engine.queries import statement
from etl_craft.services.operations import OperationContext, documents

logger = logging.getLogger(__name__)


class CatalogSchedule:
    """Keep builds off the lease-renewal loop; join before releasing leadership."""

    def __init__(self) -> None:
        """Allocate no thread until a catalog build is due."""
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="etl-craft-catalog")
        self.build: Future[documents.DocsBuild] | None = None
        self.next_due: datetime | None = None

    def refresh(self, ctx: OperationContext, now: datetime, stop: threading.Event | None) -> None:
        """Build once at the next tick; never replay ticks missed while stopped or building."""
        schedule = ctx.config.docs_site.schedule
        if schedule is None or (stop is not None and stop.is_set()):
            return
        if self.build is not None:
            if not self.build.done():
                if self.next_due is not None and self.next_due <= now:
                    self.next_due = next_after(schedule, now, ctx.config.timezone)
                return
            self.build = None
        if self.next_due is not None and now < self.next_due:
            return
        with ctx.engine.connect() as conn:
            recorded = conn.execute(statement(conn, "catalog_latest_request")).scalar_one_or_none()
        latest = None if recorded is None else as_utc(recorded)
        if self.next_due is None:
            self.next_due = next_after(schedule, latest or now, ctx.config.timezone)
            if self.next_due < now:
                self.next_due = next_after(schedule, now, ctx.config.timezone)
        if now < self.next_due:
            return
        if latest is None or latest < self.next_due:
            context = contextvars.copy_context()
            self.build = self.executor.submit(
                context.run,
                documents.generate_docs,
                ctx,
                with_warehouse=False,
                output=None,
                strict=False,
            )
            self.build.add_done_callback(lambda build: _report(ctx, build))
        self.next_due = next_after(schedule, now, ctx.config.timezone)

    def close(self) -> None:
        """Let an admitted build finish while this controller still holds leadership."""
        self.executor.shutdown(wait=True)


def _report(ctx: OperationContext, build: Future[documents.DocsBuild]) -> None:
    """Log a recorded failure without raising it into the supervision loop."""
    error = build.exception()
    if error is not None:
        logger.error(
            "catalog folder=%s: build failed: %s",
            documents.docs_folder(ctx.config),
            error,
            exc_info=(type(error), error, error.__traceback__),
        )
