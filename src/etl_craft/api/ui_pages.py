"""Browser views render the same actor-scoped reads as the CLI and API."""

from dataclasses import replace
from pathlib import Path
from urllib.parse import quote

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse, Response, StreamingResponse
from starlette.background import BackgroundTask
from starlette.staticfiles import StaticFiles

from etl_craft.api import browser_sessions as browser
from etl_craft.api.ui_templates import render
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName
from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError
from etl_craft.services.operations.actions import invoke
from etl_craft.services.operations.catalog_pages import CatalogInput, CatalogPage
from etl_craft.services.operations.context import OperationContext
from etl_craft.services.operations.project_health import HealthInput, ProjectHealth
from etl_craft.services.operations.requests import NoInput
from etl_craft.services.operations.sessions import Identity


def page_values(
    ctx: OperationContext, request: Request
) -> tuple[OperationContext, dict[str, object]]:
    """Resolve identity and project health without page-owned authorization or counts."""
    caller = browser.cookie_context(ctx, request)
    selected = request.path_params.get("project") or request.query_params.get("project") or None
    caller = replace(caller, project=str(selected or ""), project_id=None)
    identity = invoke(caller, ActionName.WHOAMI, NoInput())
    assert isinstance(identity, Identity)
    if identity.must_change_password and request.url.path != "/ui/profile/password":
        raise HTTPException(
            307,
            headers={
                "Location": "/ui/profile/password?return_to="
                + quote(str(request.url.path), safe="")
            },
        )
    health = (
        None
        if identity.must_change_password
        else invoke(
            caller,
            ActionName.PROJECT_HEALTH,
            HealthInput(
                project=selected,
                after=request.query_params.get("after"),
                limit=identity.settings.page_size,
            ),
        )
    )
    assert health is None or isinstance(health, ProjectHealth)
    project = health.projects[0] if selected and health and health.projects else None
    return caller, {
        "identity": identity,
        "settings": identity.settings,
        "project": project,
        "projects": () if health is None else health.projects,
        "health": health,
        "csrf": browser.csrf_value(request),
        "return_to": browser.return_path(
            request.url.path + ("?" + request.url.query if request.url.query else "")
        ),
        "cli_login": "etl-craft --api " + str(request.base_url).rstrip("/") + " login",
    }


def add_pages(app: FastAPI, ctx: OperationContext) -> None:
    """Mount shipped assets and only the pages implemented in this slice."""
    app.mount(
        "/static/shared",
        StaticFiles(directory=Path(__file__).parents[1] / "core/static"),
        name="shared",
    )
    app.mount(
        "/static/ui", StaticFiles(directory=Path(__file__).parent / "static"), name="ui-static"
    )

    _profile_pages(app, ctx)
    _catalog_pages(app, ctx)

    @app.get("/", include_in_schema=False)
    def root() -> Response:
        return RedirectResponse("/ui", 307)

    @app.get("/signin", include_in_schema=False)
    def signin(request: Request) -> Response:
        destination = browser.return_path(request.query_params.get("return_to", "/ui"))
        methods = invoke(
            replace(ctx, actor=SYSTEM_ACTOR, role=None), ActionName.LOGIN_METHODS, NoInput()
        )
        csrf = browser.csrf_value(request)
        response = render(
            request,
            "signin.html",
            {
                "title": "Sign in · etl-craft",
                "view": "signin",
                "methods": methods,
                "csrf": csrf,
                "return_to": destination,
                "contact": ctx.config.auth.admin_contact,
                "notice": ctx.config.auth.signin_notice,
            },
        )
        browser.challenge(response, csrf, ctx.config.auth.session_hours)
        return response

    @app.get("/ui", include_in_schema=False)
    @app.get("/ui/projects/{project}", include_in_schema=False)
    def home(request: Request) -> Response:
        _, values = page_values(ctx, request)
        identity = values["identity"]
        assert isinstance(identity, Identity)
        if (
            request.url.path == "/ui"
            and not request.query_params
            and identity.settings.default_project
        ):
            return RedirectResponse(
                "/ui/projects/" + quote(identity.settings.default_project, safe=""), 307
            )
        return render(request, "home.html", values | {"title": "Home · etl-craft", "view": "home"})


def _profile_pages(app: FastAPI, ctx: OperationContext) -> None:
    """Render personal settings, password change and public versions."""

    @app.get("/ui/profile/settings", include_in_schema=False)
    def settings(request: Request) -> Response:
        _, values = page_values(ctx, request)
        return render(
            request,
            "settings.html",
            values | {"title": "Personal settings · etl-craft", "view": "settings"},
        )

    @app.get("/ui/profile/password", include_in_schema=False)
    def password(request: Request) -> Response:
        _, values = page_values(ctx, request)
        identity = values["identity"]
        assert isinstance(identity, Identity)
        if not identity.local_password:
            raise ResourceNotFoundError("Not found or no access")
        return render(
            request,
            "password.html",
            values
            | {
                "title": "Change password · etl-craft",
                "view": "password",
                "return_to": browser.return_path(request.query_params.get("return_to", "/ui")),
            },
        )

    @app.get("/ui/about", include_in_schema=False)
    def about(request: Request) -> Response:
        caller, values = page_values(ctx, request)
        return render(
            request,
            "about.html",
            values
            | {
                "about": invoke(caller, ActionName.ABOUT, NoInput()),
                "title": "About · etl-craft",
                "view": "about",
            },
        )


def _catalog_pages(app: FastAPI, ctx: OperationContext) -> None:
    """Wrap the existing catalog without changing its generated bytes."""

    @app.get("/ui/projects/{project}/catalog", include_in_schema=False)
    def catalog(request: Request) -> Response:
        caller, values = page_values(ctx, request)
        page = invoke(caller, ActionName.CATALOG_PAGE, CatalogInput())
        assert isinstance(page, CatalogPage)
        page.close()
        return render(
            request,
            "catalog.html",
            values
            | {"title": request.path_params["project"] + " catalog · etl-craft", "view": "catalog"},
        )

    @app.get("/ui/projects/{project}/catalog/files/{path:path}", include_in_schema=False)
    def catalog_file(request: Request, path: str) -> Response:
        caller = replace(
            browser.cookie_context(ctx, request),
            project=request.path_params["project"],
            project_id=None,
        )
        raw = request.scope.get("raw_path", b"")
        if b"%" in raw:
            raise PermissionDeniedError(
                "catalog path contains encoded characters; expected a literal relative "
                "path; open the catalog index"
            )
        page = invoke(caller, ActionName.CATALOG_PAGE, CatalogInput(path=path))
        assert isinstance(page, CatalogPage)
        return StreamingResponse(
            page,
            media_type=page.content_type,
            headers={"X-Content-Type-Options": "nosniff"},
            background=BackgroundTask(page.close),
        )
