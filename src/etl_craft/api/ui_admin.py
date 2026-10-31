"""Administration pages: accounts, groups, tokens, access, projects, pools and announcements.

Each tab renders an existing admin read; every change posts the same `/api/v1` request the
CLI sends, so the action table checks the role and records the request.
"""

from collections.abc import Callable

from fastapi import FastAPI, Request
from fastapi.responses import Response

from etl_craft.api.ui_pages import page_values
from etl_craft.api.ui_templates import render
from etl_craft.core.enums import AccountKind, ActionName, ApiRole
from etl_craft.core.enums_access import EnvironmentType, Governance, Permission
from etl_craft.core.enums_ui import AnnouncementLevel
from etl_craft.core.errors import PermissionDeniedError, UsageError
from etl_craft.services.operations.access_models import AccessFilter
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.admin_metadata import PoolIdentity, ProjectIdentity
from etl_craft.services.operations.announcements import AnnouncementFilter
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.requests import NoInput

LISTING = "the administration page"
TABS: dict[str, tuple[ActionName, Callable[[str | None], object]]] = {
    "accounts": (ActionName.ACCOUNT_LIST, lambda _: NoInput()),
    "groups": (ActionName.GROUP_LIST, lambda _: NoInput()),
    "tokens": (ActionName.TOKEN_LIST, lambda _: NoInput()),
    "access": (ActionName.ACCESS_LIST, lambda project: AccessFilter(project=project)),
    "projects": (ActionName.PROJECT_LIST, lambda _: ProjectIdentity(reason=LISTING)),
    "pools": (ActionName.POOL_LIST, lambda _: PoolIdentity(reason=LISTING)),
    "announcements": (ActionName.ANNOUNCE_LIST, lambda _: AnnouncementFilter(all=True)),
}


def add_admin_pages(app: FastAPI, ctx: OperationContext) -> None:
    """Mount the administration page; only administrators see it."""

    @app.get("/ui/admin", include_in_schema=False)
    def admin(request: Request) -> Response:
        caller, values = page_values(ctx, request)
        if caller.role != ApiRole.ADMIN:
            raise PermissionDeniedError(
                "the administration page needs the admin role; ask an administrator"
            )
        tab = request.query_params.get("tab", "accounts")
        if tab not in TABS:
            raise UsageError(f"tab={tab!r}; expected one of {', '.join(TABS)}; open Accounts")
        action, request_for = TABS[tab]
        project = request.query_params.get("project") or None
        found = (
            None
            if tab == "access" and project is None
            else invoke(caller, action, request_for(project))
        )
        return render(
            request,
            "admin.html",
            values
            | {
                "title": f"Administration · {tab.capitalize()} · etl-craft",
                "view": "admin",
                "tab": tab,
                "tabs": tuple(TABS),
                "found": found,
                "roles": [r for r in ApiRole if r != ApiRole.AGENT],
                "kinds": tuple(AccountKind),
                "permissions": tuple(Permission),
                "environments": tuple(EnvironmentType),
                "governance": tuple(Governance),
                "levels": tuple(AnnouncementLevel),
            },
        )
