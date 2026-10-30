"""The Pipeline Insights page: the `insights` read with its filters as query parameters."""

from dataclasses import dataclass
from datetime import date

from fastapi import FastAPI, Request
from fastapi.responses import Response

from etl_craft.api.ui_monitor import pipeline_url, run_url
from etl_craft.api.ui_pages import page_values
from etl_craft.api.ui_templates import render
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import UsageError
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.insights import DayInsight, Insights, InsightsInput
from etl_craft.services.operations.models import PipelineListView
from etl_craft.services.operations.requests import TagFilter

CHART_HEIGHT = 120
BAR_WIDTH = 18
BAR_GAP = 6


@dataclass(frozen=True)
class DayBar:
    """One day's runs as stacked bars: succeeded, failed and the rest."""

    day: DayInsight
    x: int
    succeeded: int
    failed: int
    other: int


def add_insights_page(app: FastAPI, ctx: OperationContext) -> None:
    """Mount the Insights page."""

    @app.get("/ui/projects/{project}/insights", include_in_schema=False)
    def insights(request: Request, project: str) -> Response:
        caller, values = page_values(ctx, request)
        query = InsightsInput(
            start=_day(request, "from"),
            end=_day(request, "to"),
            pipelines=tuple(request.query_params.getlist("pipeline")),
            tag=tuple(request.query_params.getlist("tag")),
        )
        found = invoke(caller, ActionName.INSIGHTS, query)
        listed = invoke(caller, ActionName.PIPELINES, TagFilter())
        assert isinstance(found, Insights) and isinstance(listed, PipelineListView)
        start, end = query.days()
        return render(
            request,
            "insights.html",
            values
            | {
                "title": f"Insights · {project} · etl-craft",
                "view": "insights",
                "found": found,
                "start": start,
                "end": end,
                "choices": sorted(p.pipeline_code for p in listed.pipelines),
                "chosen": query.pipelines,
                "bars": _bars(found.days),
                "chart_height": CHART_HEIGHT,
                "chart_width": max(1, len(found.days)) * (BAR_WIDTH + BAR_GAP),
                "bar_width": BAR_WIDTH,
                "pipeline_url": lambda code: pipeline_url(project, code),
                "run_url": lambda code, run_id: run_url(project, code, run_id),
            },
        )


def _day(request: Request, name: str) -> date | None:
    value = request.query_params.get(name)
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise UsageError(f"{name}={value!r}; expected a date as YYYY-MM-DD") from error


def _bars(days: tuple[DayInsight, ...]) -> tuple[DayBar, ...]:
    """Scale each day's stacked counts to the chart height of the busiest day."""
    busiest = max((day.runs for day in days), default=0) or 1

    def height(count: int) -> int:
        return round(count / busiest * CHART_HEIGHT)

    return tuple(
        DayBar(
            day,
            index * (BAR_WIDTH + BAR_GAP),
            height(day.succeeded),
            height(day.failed),
            height(day.runs - day.succeeded - day.failed),
        )
        for index, day in enumerate(days)
    )
