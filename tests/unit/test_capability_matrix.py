"""Every action a person can request over HTTP has a place in the browser UI, or a stated reason.

S7.A.5: the test is generated from the action table, so a new action cannot ship without a UI
place (its page reads it by name, or a page or script posts to its route) or an entry below.
"""

import re
from pathlib import Path

import pytest

import etl_craft.api
from etl_craft.core.enums import ActionName, ApiRole, Placement
from etl_craft.services.operations.actions import ACTIONS

pytestmark = pytest.mark.unit

API = Path(etl_craft.api.__file__).parent
SOURCES = "".join(
    path.read_text(encoding="utf-8")
    for pattern in ("ui_*.py", "login.py", "templates/**/*.html", "static/*.js")
    for path in sorted(API.glob(pattern))
)

COMPOSED = {
    ActionName.DATA_PROVENANCE: '"{{ values_api }}/provenance"',
    ActionName.DATA_UNLOCK: '"{{ values_api }}/unlock"',
    ActionName.CONTROL_BULK: "${dialog.dataset.url}/bulk",
}
"""Routes a page builds from a prefix, with the exact text that builds them."""

COVERED = {
    ActionName.STATUS: "the run page shows each run's state through the run and history reads",
    ActionName.CATALOG_GAPS: "Insights shows the stored schema checks",
    ActionName.CONTROL_REQUEST: "each action's receipt shows the request's recorded status",
}
"""Reads whose information a page shows through another read."""

CONSOLE_ONLY = {
    ActionName.BUNDLE_PUBLISH: "publishes a bundle built from the project directory on disk",
    ActionName.RENAME: "renames change config files in the project repository",
    ActionName.HEALTH: "an unauthenticated liveness probe; System health is the page for people",
    ActionName.PIPELINE_REFRESH_HISTORY: "Refresh shows its findings; older ones are a CLI read",
    ActionName.TRIGGER_PIPELINE: "for external orchestrators; people use Run now",
}
"""Actions deliberately left to the CLI and the console, each with its reason."""


def placed(name: ActionName) -> bool:
    action = ACTIONS[name]
    assert action.path is not None
    route = action.path.split("/api/v1", 1)[1].split("{")[0].rstrip("/")
    return f"ActionName.{name.name}" in SOURCES or bool(
        route and re.search(re.escape(route) + r"[\"'?/]", SOURCES)
    )


def people_actions() -> list[ActionName]:
    return [
        name
        for name, action in ACTIONS.items()
        if action.path is not None
        and action.placement != Placement.CONTROLLER_HOST
        and action.role != ApiRole.AGENT
    ]


@pytest.mark.parametrize("name", people_actions(), ids=str)
def test_every_action_has_a_ui_place_or_a_stated_reason(name):
    if name in COMPOSED:
        assert COMPOSED[name] in SOURCES, f"{name}: the page no longer builds {COMPOSED[name]}"
        return
    if name in COVERED or name in CONSOLE_ONLY:
        assert not placed(name), f"{name} now has a UI place; remove it from the exceptions"
        return
    assert placed(name), (
        f"{name} ({ACTIONS[name].path}) has no browser UI place; add it to a page, or list it "
        "in CONSOLE_ONLY with the reason it stays in the CLI"
    )


def test_every_exception_names_an_action_people_can_request():
    allowed = set(people_actions())
    assert set(COMPOSED) | set(COVERED) | set(CONSOLE_ONLY) <= allowed
