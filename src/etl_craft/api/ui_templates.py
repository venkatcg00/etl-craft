"""Autoescaped browser documents and the catalog's shared static theme control."""

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

from fastapi import Request
from fastapi.responses import HTMLResponse
from jinja2 import ChoiceLoader, Environment, PackageLoader, PrefixLoader, select_autoescape

from etl_craft.core.enums_ui import TimeDisplay
from etl_craft.core.time import as_utc
from etl_craft.services.operations.personal import Settings

ENVIRONMENT = Environment(
    loader=ChoiceLoader(
        [
            PackageLoader("etl_craft.api", "templates"),
            PrefixLoader({"shared": PackageLoader("etl_craft.core", "static")}),
        ]
    ),
    autoescape=select_autoescape(("html", "xml")),
)


def zoned(value: datetime | None, zone: str = "UTC") -> str:
    """Every displayed absolute instant names its timezone."""
    if value is None:
        return "Not recorded"
    return as_utc(value).astimezone(ZoneInfo(zone)).strftime("%Y-%m-%d %H:%M:%S") + " " + zone


def time_text(value: datetime | None, settings: Settings) -> str:
    """Relative labels retain the user's timezone; the title gives the exact instant."""
    if value is None or settings.time_display == TimeDisplay.ABSOLUTE:
        return zoned(value, settings.timezone)
    seconds = max(0, int((datetime.now(UTC) - as_utc(value)).total_seconds()))
    return (
        f"{seconds // 60} min ago · {settings.timezone}"
        if seconds < 3600
        else f"{seconds // 3600} h ago · {settings.timezone}"
    )


def combine(values: dict[str, object], **more: object) -> dict[str, object]:
    """Return ``values`` with ``more`` added, for building request bodies in templates."""
    return {**values, **more}


ENVIRONMENT.filters.update(zoned=zoned, time_text=time_text, combine=combine)


def render(
    request: Request, template: str, values: dict[str, object], status: int = 200
) -> HTMLResponse:
    """Render only presentation values supplied by the page's read actions."""
    defaults = {
        "request": request,
        "project": None,
        "identity": None,
        "title": "etl-craft",
        "csrf": "",
        "view": "",
        "return_to": "/ui",
        "projects": (),
        "settings": Settings(),
    }
    return HTMLResponse(
        ENVIRONMENT.get_template(template).render(defaults | values), status_code=status
    )
