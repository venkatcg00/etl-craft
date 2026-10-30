"""Cookie authority, page states and account security through generated action routes."""

import re
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft import __version__
from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from fixtures.actions import act

SESSION = "__Host-etl-craft-session"
CSRF = "__Host-etl-craft-csrf"
PASSWORD = "correct horse battery"


@pytest.fixture
def browser_scene(engine_db):
    from etl_craft.engine.migrations import mark_packaged_migrations_applied

    with engine_db.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
    ctx = OperationContext(
        engine_db.engine, engine_db.config, Actor("owner", ActorKind.USER), role=ApiRole.ADMIN
    )
    act(ctx, ActionName.ACCOUNT_ADD, name="owner", role=ApiRole.ADMIN, password=PASSWORD)
    act(ctx, ActionName.ACCOUNT_ADD, name="alice", role=ApiRole.VIEWER, password=PASSWORD)
    return ctx, TestClient(
        create_app(ctx), base_url="https://testserver", raise_server_exceptions=False
    )


def login(client, name="alice", password=PASSWORD, destination="/ui"):
    page = client.get("/signin", params={"return_to": destination})
    assert page.status_code == 200, page.text
    csrf = re.search(r'name="csrf-token" content="([^"]+)"', page.text).group(1)
    return client.post(
        "/api/v1/login/password",
        headers={"HX-Request": "true", "X-CSRF-Token": csrf, "Origin": "https://testserver"},
        json={"name": name, "password": password, "_return_to": destination},
    )


def change(client, path, fields):
    return client.post(path, headers={"X-CSRF-Token": client.cookies[CSRF]}, json=fields)


def test_browser_cookie_return_to_csrf_and_shared_tab_revocation(browser_scene):
    _, client = browser_scene
    before = client.get("/ui/about", follow_redirects=False)
    assert before.status_code == 303 and "return_to=%2Fui%2Fabout" in before.headers["location"]
    response = login(client, destination="/ui/about")
    assert response.status_code == 200, response.text
    assert response.headers["HX-Redirect"] == "/ui/about"
    assert "access_token" not in response.text
    cookie = next(
        value
        for value in response.headers.get_list("set-cookie")
        if value.startswith(SESSION + "=")
    )
    assert all(flag in cookie.lower() for flag in ("httponly", "secure", "samesite=lax", "path=/"))
    assert "domain=" not in cookie.lower()
    tab = TestClient(client.app, base_url="https://testserver")
    tab.cookies.update(client.cookies)
    assert tab.get("/ui/about").status_code == 200
    assert client.post("/api/v1/account/settings", json={"theme": "dark"}).status_code == 403
    assert (
        client.post("/api/v1/account/settings", json={"theme": "dark", "_csrf": "é"}).status_code
        == 403
    )
    denied = client.post(
        "/api/v1/account/settings",
        headers={"X-CSRF-Token": client.cookies[CSRF], "Origin": "https://evil.example"},
        json={"theme": "dark"},
    )
    assert denied.status_code == 403
    assert (
        change(
            client,
            "/api/v1/account/settings",
            {
                "theme": "dark",
                "timezone": "Asia/Kolkata",
                "page_size": 40,
                "time_display": "absolute",
            },
        ).status_code
        == 200
    )
    settings = tab.get("/api/v1/whoami").json()["settings"]
    assert settings["theme"] == "dark" and settings["page_size"] == 40
    assert 'data-theme="dark"' in tab.get("/ui/profile/settings").text
    assert change(client, "/api/v1/login/revoke", {}).status_code in (200, 303)
    assert tab.get("/api/v1/whoami").status_code == 401


@pytest.mark.parametrize(
    "destination",
    [
        "https://evil.example",
        "//evil.example",
        "/%2fevil.example",
        "/%255cevil",
        "/\\evil",
        "/%0d%0aLocation:evil",
    ],
)
def test_browser_return_to_refuses_external_or_encoded_authority(browser_scene, destination):
    _, client = browser_scene
    response = client.get("/signin", params={"return_to": destination})
    assert response.status_code == 400
    assert "same-site" in response.text


def test_lockout_generic_message_unlock_temporary_password_and_settings(browser_scene):
    ctx, client = browser_scene
    generic = login(client, password="wrong").text
    assert login(client, name="missing", password="wrong").text == generic
    for _ in range(ctx.config.auth.lockout_after - 1):
        assert login(client, password="wrong").status_code == 400
    assert "Account locked" not in login(client, password="wrong").text
    assert "Account locked, contact your administrator" in login(client).text
    act(ctx, ActionName.ACCOUNT_UNLOCK, name="alice")
    assert login(client).status_code == 200
    with ctx.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT FAILED_SIGNINS AS failed_signins, LOCKED_DATE AS locked_date FROM "
                "CFG_ACCOUNTS WHERE ACCOUNT_NAME='alice'"
            )
        ).one()
        assert row.failed_signins == 0 and row.locked_date is None
    act(ctx, ActionName.ACCOUNT_RESET_PASSWORD, name="alice", password="temporary password")
    response = login(client, password="temporary password", destination="/ui/about")
    assert "/ui/profile/password" in response.headers["HX-Redirect"]
    assert client.get("/api/v1/about").status_code == 403
    from etl_craft.core.errors import PermissionDeniedError
    from etl_craft.services.operations.identity import authenticated_context
    from etl_craft.services.operations.tokens import authenticate

    identity = authenticate(ctx, client.cookies[SESSION])
    with pytest.raises(PermissionDeniedError, match="temporary password"):
        act(authenticated_context(ctx, identity), ActionName.MIGRATE)
    assert client.get("/ui/profile/password").status_code == 200
    response = change(
        client,
        "/api/v1/account/password",
        {"current_password": "temporary password", "password": "replacement password"},
    )
    assert response.status_code == 200
    assert client.get("/api/v1/whoami").status_code == 401
    assert login(client, password="replacement password").status_code == 200
    assert client.get("/ui/about").status_code == 200


def test_about_page_states_headers_and_session_expiry(browser_scene, monkeypatch):
    ctx, client = browser_scene
    assert login(client).status_code == 200
    now = datetime.now(UTC)
    with ctx.engine.begin() as conn:
        for version, heartbeat, stopped in (
            ("live-controller", now, None),
            ("expired-controller", now - timedelta(minutes=2), None),
            ("stopped-controller", now, now),
        ):
            conn.execute(
                text(
                    "INSERT INTO AUD_CONTROLLERS (HOST,PID,VERSION,STARTED_AT,HEARTBEAT_AT,"
                    "STOPPED_AT,STARTED_BY,STARTED_BY_KIND) "
                    "VALUES ('private-host',1,:version,:now,:heartbeat,:stopped,'owner','USER')"
                ),
                {"version": version, "now": now, "heartbeat": heartbeat, "stopped": stopped},
            )
    about = client.get("/api/v1/about").json()
    assert [row["version"] for row in about["controllers"]] == ["live-controller"]
    assert "private-host" not in str(about)
    assert about["package_version"] == about["server_version"] == __version__
    assert about["schema_version"].startswith("0075")
    assert about["license"] == "Apache-2.0"
    page = client.get("/ui/about")
    assert __version__ in page.text and 'id="main"' in page.text
    assert "frame-ancestors 'self'" in page.headers["content-security-policy"]
    assert page.headers["x-frame-options"] == "SAMEORIGIN"
    assert page.headers["x-content-type-options"] == "nosniff"
    assert "Running runs" in client.get("/ui").text
    assert client.get("/ui/missing").status_code == 404
    from etl_craft.api import ui_pages

    original = ui_pages.invoke

    def failing(ctx, name, request):
        if name == ActionName.ABOUT:
            raise OSError("secret database path")
        return original(ctx, name, request)

    monkeypatch.setattr(ui_pages, "invoke", failing)
    failure = client.get("/ui/about")
    assert failure.status_code == 500
    assert "frame-ancestors" in failure.headers["content-security-policy"]
    assert failure.headers["x-request-id"] in failure.text
    assert "secret database path" not in failure.text
    assert "Copy details" in failure.text and "Request id:" in failure.text
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_API_TOKENS SET EXPIRES_AT=:expired"),
            {"expired": datetime.now(UTC) - timedelta(seconds=1)},
        )
    assert client.get("/ui/about", follow_redirects=False).status_code == 303


def test_signin_notice_is_escaped_and_empty_state_has_a_next_step(browser_scene):
    from dataclasses import replace

    ctx, _ = browser_scene
    ctx = replace(
        ctx,
        config=replace(
            ctx.config,
            auth=replace(ctx.config.auth, signin_notice='<script>alert("notice")</script>'),
        ),
    )
    client = TestClient(create_app(ctx), base_url="https://testserver")
    page = client.get("/signin")
    assert "&lt;script&gt;" in page.text and '<script>alert("notice")</script>' not in page.text
    assert login(client).status_code == 200
    with ctx.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PIPELINE_GRANTS SET ACTIVE_FLAG='N'"))
    empty = client.get("/ui")
    assert empty.status_code == 200
    assert "No visible projects" in empty.text and "Ask your administrator" in empty.text


def test_browser_sso_uses_same_cookie_and_has_no_password_page(browser_scene, monkeypatch):
    from dataclasses import replace

    from etl_craft.services import oidc

    ctx, _ = browser_scene
    act(ctx, ActionName.ACCOUNT_ADD, name="sso", role=ApiRole.VIEWER, oidc_subject="subject")
    ctx = replace(
        ctx,
        config=replace(
            ctx.config,
            auth=replace(
                ctx.config.auth, oidc_issuer="https://issuer.example", oidc_client_id="console"
            ),
        ),
    )
    monkeypatch.setattr(
        oidc,
        "start_device",
        lambda auth: oidc.DeviceGrant(
            "ABC", "https://issuer.example/approve", "device", "nonce", 1, 60
        ),
    )
    monkeypatch.setattr(
        oidc, "poll_device", lambda auth, device, nonce: oidc.IdentityClaims("subject")
    )
    client = TestClient(create_app(ctx), base_url="https://testserver")
    client.get("/signin")
    h = {"HX-Request": "true", "X-CSRF-Token": client.cookies[CSRF]}
    started = client.post("/api/v1/login/sso", headers=h, json={"_return_to": "/ui/about"})
    assert started.status_code == 200 and "ABC" in started.text
    response = client.post(
        "/api/v1/login/sso/poll",
        headers=h,
        json={"device_code": "device", "nonce": "nonce", "_return_to": "/ui/about"},
    )
    assert response.status_code == 200 and response.headers["HX-Redirect"] == "/ui/about"
    assert SESSION in client.cookies and "access_token" not in response.text
    assert client.get("/ui/profile/password").status_code == 404
    assert "Change password" not in client.get("/ui/about").text


def test_expired_form_returns_to_its_page_and_keeps_encoded_query(browser_scene):
    ctx, client = browser_scene
    destination = "/ui/profile/settings?note=a%26b"
    assert login(client, destination=destination).headers["HX-Redirect"] == destination
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_API_TOKENS SET EXPIRES_AT=:expired"),
            {"expired": datetime.now(UTC) - timedelta(seconds=1)},
        )
    response = client.post(
        "/api/v1/account/settings",
        headers={
            "HX-Request": "true",
            "HX-Current-URL": "https://testserver" + destination,
            "X-CSRF-Token": client.cookies[CSRF],
        },
        json={"theme": "dark"},
    )
    assert response.status_code == 200
    assert (
        response.headers["HX-Redirect"]
        == "/signin?return_to=%2Fui%2Fprofile%2Fsettings%3Fnote%3Da%2526b"
    )


def test_full_settings_replacement_repairs_invalid_preferences(browser_scene):
    ctx, client = browser_scene
    assert login(client).status_code == 200
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_ACCOUNTS SET UI_SETTINGS=:settings WHERE ACCOUNT_NAME='alice'"),
            {"settings": '{"timezone":"Invalid/Place"}'},
        )
    partial = change(client, "/api/v1/account/settings", {"theme": "light"})
    assert partial.status_code == 404 and "replace all five" in partial.text
    result = change(
        client,
        "/api/v1/account/settings",
        {
            "theme": "system",
            "timezone": "UTC",
            "default_project": "",
            "page_size": 25,
            "time_display": "relative",
        },
    )
    assert result.status_code == 200
    assert client.get("/api/v1/whoami").json()["settings"]["timezone"] == "UTC"


def test_production_badge_is_explicit_on_every_selected_page(browser_scene):
    ctx, client = browser_scene
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_PROJECTS SET ENVIRONMENT_TYPE='PROD' WHERE PROJECT_CODE='default'")
        )
    assert login(client).status_code == 200
    for path in (
        "/ui/projects/default",
        "/ui/about?project=default",
        "/ui/profile/settings?project=default",
    ):
        page = client.get(path)
        assert page.status_code == 200
        assert "environment production" in page.text and "PRODUCTION" in page.text
