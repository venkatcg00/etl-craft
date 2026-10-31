"""The administration page lists every admin read and posts the same requests the CLI sends."""

import re

import pytest
from fastapi.testclient import TestClient

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from fixtures.actions import act

PASSWORD = "correct horse battery"
TABS = ("accounts", "groups", "tokens", "access", "projects", "pools", "announcements")


@pytest.fixture
def site(metadata_project):
    from etl_craft.engine.migrations import mark_packaged_migrations_applied

    p = metadata_project
    with p.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
    ctx = OperationContext(p.engine, p.config, Actor("owner", ActorKind.USER), role=ApiRole.ADMIN)
    for name, role in (("owner", ApiRole.ADMIN), ("alice", ApiRole.VIEWER)):
        act(ctx, ActionName.ACCOUNT_ADD, name=name, role=role, password=PASSWORD)
    client = TestClient(
        create_app(ctx), base_url="https://testserver", raise_server_exceptions=False
    )
    return ctx, client


def sign_in(client, name, password=PASSWORD):
    page = client.get("/signin")
    csrf = re.search(r'name="csrf-token" content="([^"]+)"', page.text).group(1)
    return client.post(
        "/api/v1/login/password",
        headers={"HX-Request": "true", "X-CSRF-Token": csrf, "Origin": "https://testserver"},
        json={"name": name, "password": password, "_return_to": "/ui"},
    )


def csrf_of(page):
    return re.search(r'name="csrf-token" content="([^"]+)"', page.text).group(1)


@pytest.mark.parametrize("tab", TABS)
def test_every_tab_renders_its_admin_read(site, tab):
    _, client = site
    sign_in(client, "owner")
    page = client.get(f"/ui/admin?tab={tab}&project=default")
    assert page.status_code == 200, page.text
    assert f'href="/ui/admin?tab={tab}" aria-current="page"' in page.text
    assert 'href="/ui/admin"' in page.text


def test_viewers_get_no_link_and_are_refused(site):
    _, client = site
    sign_in(client, "alice")
    assert 'href="/ui/admin"' not in client.get("/ui").text
    refused = client.get("/ui/admin")
    assert refused.status_code == 403
    assert "needs the admin role" in refused.text


def test_unknown_tab_names_the_choices(site):
    _, client = site
    sign_in(client, "owner")
    page = client.get("/ui/admin?tab=secrets")
    assert page.status_code == 400
    assert "expected one of accounts, groups" in page.text


def test_a_locked_account_shows_its_state_and_an_unlock_that_works(site):
    ctx, client = site
    for _ in range(ctx.config.auth.lockout_after):
        sign_in(client, "alice", "wrong password")
    sign_in(client, "owner")
    page = client.get("/ui/admin?tab=accounts")
    row = re.search(r'<th scope="row">alice</th>.*?</tr>', page.text, re.S).group(0)
    assert "Locked" in row and 'data-post="/api/v1/accounts/unlock"' in row
    headers = {"X-CSRF-Token": csrf_of(page), "Origin": "https://testserver"}
    unlocked = client.post("/api/v1/accounts/unlock", json={"name": "alice"}, headers=headers)
    assert unlocked.status_code == 200, unlocked.text
    alice = next(a for a in unlocked.json()["accounts"] if a["name"] == "alice")
    assert (alice["failed_signins"], alice["locked_at"]) == (0, None)


def test_a_created_token_is_returned_once_as_json(site):
    _, client = site
    sign_in(client, "owner")
    page = client.get("/ui/admin?tab=tokens")
    headers = {
        "X-CSRF-Token": csrf_of(page),
        "Origin": "https://testserver",
        "Accept": "application/json",
    }
    body = {"name": "loader", "role": "operator", "expires": "30d"}
    created = client.post("/api/v1/tokens", json=body, headers=headers)
    assert created.status_code == 200, created.text
    assert created.json()["token"]
    listed = client.get("/ui/admin?tab=tokens").text
    assert "loader" in listed and created.json()["token"] not in listed


def test_a_posted_announcement_shows_on_every_page_until_ended(site):
    ctx, client = site
    act(ctx, ActionName.ANNOUNCE_ADD, message="Upgrade at 18:00 UTC", reason="upgrade")
    sign_in(client, "alice")
    assert "Upgrade at 18:00 UTC" in client.get("/ui").text
    ended = act(ctx, ActionName.ANNOUNCE_LIST).announcements[0]
    act(ctx, ActionName.ANNOUNCE_END, announcement_id=ended.announcement_id, reason="done")
    assert "Upgrade at 18:00 UTC" not in client.get("/ui").text
