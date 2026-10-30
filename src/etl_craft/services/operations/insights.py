"""The Pipeline Insights read: visible pipelines, a bounded UTC day window, every built-in panel."""

import json
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta

from etl_craft.core.enums import GapAttribute, GapState, YesNo
from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError, UsageError
from etl_craft.core.time import as_utc
from etl_craft.engine.connection import read_snapshot
from etl_craft.engine.queries import statement
from etl_craft.engine.repository.pipelines import read_tags
from etl_craft.services.insights import DayInsight as DayInsight
from etl_craft.services.insights import Insights as Insights
from etl_craft.services.insights import gap_counts, measure, window
from etl_craft.services.operations.catalog import GapsInput, get_gaps
from etl_craft.services.operations.catalog_pages import CatalogInput, read_catalog
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
        settings = conn.execute(statement(conn, "project_schema_checks")).one()
    codes = set(selected.values())

    def chosen(tasks: Iterable[str]) -> bool:
        return not request.pipelines or any(task.partition(".")[0] in codes for task in tasks)

    stored = _stored_comparison(ctx)
    if stored is not None:
        compared_at, records = stored
        checks = [(attribute, state) for attribute, state, tasks in records if chosen(tasks)]
    else:
        compared_at = None
        snapshot = get_gaps(ctx, GapsInput(tag=request.tag))
        checks = [(r.attribute, r.state) for r in snapshot.records if chosen(r.tasks)]
    return replace(
        measured,
        gaps=gap_counts(checks),
        gaps_compared_at=compared_at,
        schema_check_schedule=settings.schema_check_schedule,
        schema_check_on_demand=settings.schema_check_on_demand == YesNo.YES,
    )


Check = tuple[GapAttribute, GapState, tuple[str, ...]]


def _stored_comparison(ctx: OperationContext) -> tuple[datetime, list[Check]] | None:
    """Return the newest catalog's warehouse comparison, or None when there is none to show.

    The catalog is read with its own checks (whole-project view, path rules); a catalog built
    without the warehouse comparison has no ``compared_at`` and the live metadata check is used.
    """
    try:
        page = read_catalog(ctx, CatalogInput(path="gaps.json"))
    except (PermissionDeniedError, ResourceNotFoundError):
        return None
    snapshot = json.loads(b"".join(page))
    if snapshot.get("compared_at") is None:
        return None
    records = [
        (GapAttribute(r["attribute"]), GapState(r["state"]), tuple(r["tasks"]))
        for r in snapshot["records"]
    ]
    return as_utc(datetime.fromisoformat(snapshot["compared_at"])), records
