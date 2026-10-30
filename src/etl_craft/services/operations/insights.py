"""The Pipeline Insights read: visible pipelines, a bounded UTC day window, every built-in panel."""

from dataclasses import dataclass, replace
from datetime import date, timedelta

from etl_craft.core.errors import UsageError
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import read_tags
from etl_craft.services.insights import DayInsight as DayInsight
from etl_craft.services.insights import Insights as Insights
from etl_craft.services.insights import gap_counts, measure, window
from etl_craft.services.operations.catalog import GapsInput, get_gaps
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.permissions import pipeline_access
from etl_craft.services.operations.requests import TagFilter

MAX_DAYS = 92


@dataclass(frozen=True, kw_only=True)
class InsightsInput(TagFilter):
    """UTC days ``start`` to ``end`` inclusive (default the last 7), and optional pipelines."""

    start: date | None = None
    end: date | None = None
    pipelines: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Refuse a reversed or unbounded window before any database work."""
        super().__post_init__()
        object.__setattr__(self, "pipelines", tuple(self.pipelines))
        start, end = self.days()
        if end < start:
            raise UsageError(f"start={start} is after end={end}; give start on or before end")
        if (end - start).days + 1 > MAX_DAYS:
            raise UsageError(
                f"{start} to {end} is {(end - start).days + 1} days; expected at most {MAX_DAYS}; "
                "choose a shorter window"
            )

    def days(self) -> tuple[date, date]:
        """Return the first and last UTC day of the window."""
        start, end = window(self.start, self.end)
        return start.date(), (end - timedelta(days=1)).date()


def insights(ctx: OperationContext, request: InsightsInput) -> Insights:
    """Measure the visible pipelines that match every tag and, when named, the given codes."""
    window_start, window_end = window(*request.days())
    with read_snapshot(ctx.engine) as conn:
        access = pipeline_access(conn, ctx)
        selected = {
            row.pipeline_id: row.pipeline_code
            for row in conn.execute(statement(conn, "active_pipelines"))
            if access.permits(row.pipeline_code)
            and set(request.tag) <= set(read_tags(row.pipeline_parameters, row.pipeline_code))
            and (not request.pipelines or row.pipeline_code in request.pipelines)
        }
        unknown = sorted(set(request.pipelines) - set(selected.values()))
        if unknown:
            raise UsageError(
                f"pipelines={unknown}: not an active pipeline you can view with these tags; "
                "choose pipelines from `etl-craft pipelines`"
            )
        measured = measure(conn, selected, window_start, window_end)
    snapshot = get_gaps(ctx, GapsInput(tag=request.tag))
    codes = set(selected.values())
    checks = (
        (record.attribute, record.state)
        for record in snapshot.records
        if not request.pipelines or any(task.partition(".")[0] in codes for task in record.tasks)
    )
    return replace(measured, gaps=gap_counts(checks))
