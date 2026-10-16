"""Marks a running run ``BREACHED`` the moment its SLA passes, from a background thread."""

from __future__ import annotations

import contextvars
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import TracebackType

from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.time import as_utc
from etl_craft.engine import runlog, transitions

logger = logging.getLogger(__name__)

RETRY_SECONDS = 15.0
"""How long the watcher waits before writing a breach again after the Engine DB refused it."""


@dataclass(frozen=True)
class SlaLapse:
    """A run that has passed its pipeline's SLA."""

    pipeline_code: str
    pipeline_run_id: int
    sla_hours: float
    elapsed_hours: float


class SlaWatch:
    """Marks the run breached at its deadline and reports the lapse once.

    A failed breach write is retried every ``RETRY_SECONDS`` until it succeeds or the run ends,
    so an Engine DB outage at the deadline neither loses the breach nor the alert.
    """

    def __init__(
        self,
        engine: Engine,
        pipeline_code: str,
        pipeline_run_id: int,
        sla_hours: float | None,
        on_lapse: Callable[[SlaLapse], None],
    ) -> None:
        """Watch ``pipeline_run_id``; ``sla_hours`` of ``None`` watches nothing."""
        self.engine = engine
        self.pipeline_code = pipeline_code
        self.pipeline_run_id = pipeline_run_id
        self.sla_hours = sla_hours
        self.on_lapse = on_lapse
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None

    def __enter__(self) -> SlaWatch:
        """Start the watcher thread when the pipeline has an SLA."""
        hours = self.sla_hours
        if hours is None:
            return self
        with self.engine.connect() as conn:
            start = as_utc(runlog.fetch_run_sla(conn, self.pipeline_run_id).start_date)
        self.thread = threading.Thread(
            target=contextvars.copy_context().run,
            args=(self._watch, start, hours),
            name="etl-craft-sla-watch",
            daemon=True,
        )
        self.thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Stop watching once the run's supervision ends."""
        self.stop.set()
        if self.thread is not None:
            self.thread.join()

    def _watch(self, start: datetime, hours: float) -> None:
        wait = (start + timedelta(hours=hours) - datetime.now(UTC)).total_seconds()
        if self.stop.wait(timeout=max(wait, 0.0)):
            return
        marked = self._mark()
        if not marked:
            return
        elapsed = runlog.elapsed_hours(start, datetime.now(UTC))
        logger.warning(
            "%s: pipeline_run_id=%d is still running past its SLA of %g h",
            self.pipeline_code,
            self.pipeline_run_id,
            hours,
        )
        self.on_lapse(SlaLapse(self.pipeline_code, self.pipeline_run_id, hours, elapsed))

    def _mark(self) -> bool:
        """Write the breach, retrying a refused write until it lands or the run ends."""
        while True:
            try:
                with self.engine.begin() as conn:
                    return transitions.mark_sla_breached(conn, self.pipeline_run_id)
            except SQLAlchemyError as error:
                logger.error(
                    "%s: pipeline_run_id=%d: could not mark the SLA breached (%s); retrying in %gs",
                    self.pipeline_code,
                    self.pipeline_run_id,
                    error,
                    RETRY_SECONDS,
                )
            if self.stop.wait(timeout=RETRY_SECONDS):
                return False
