"""Operations pages: system health, agents, scheduled runs and maintenance over existing reads."""

import re

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.engine.repository import controllers
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.metadata import add_pipeline

PASSWORD = "correct horse battery"
OWNER = Actor("owner", ActorKind.USER)


@pytest.fixture
def site(metadata_project):
    from etl_craft.engine.migrations import mark_packaged_migrations_applied

    p = metadata_project
    with p.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
    ctx = OperationContext(p.engine, p.config, OWNER, role=ApiRole.ADMIN)
    for name, role in (("owner", ApiRole.ADMIN), ("alice", ApiRole.VIEWER)):
        act(ctx, ActionName.ACCOUNT_ADD, name=name, role=role, password=PASSWORD)
    client = TestClient(
        create_app(ctx), base_url="https://testserver", raise_server_exceptions=False
    )
    return ctx, client


def sign_in(client, name):
    page = client.get("/signin")
    csrf = re.search(r'name="csrf-token" content="([^"]+)"', page.text).group(1)
    client.post(
        "/api/v1/login/password",
        headers={"HX-Request": "true", "X-CSRF-Token": csrf, "Origin": "https://testserver"},
        json={"name": name, "password": PASSWORD, "_return_to": "/ui"},
    )


def test_system_health_says_when_no_controller_is_running_and_when_one_is(site):
    ctx, client = site
    sign_in(client, "alice")
    page = client.get("/ui/system")
    assert page.status_code == 200, page.text
    assert "No controller has sent a heartbeat in the last minute" in page.text
    assert "Not used: this deployment runs tasks on the controller" in page.text
    with acting_as(OWNER):
        controller = controllers.start(ctx.engine)
    page = client.get("/ui/system").text
    assert "1 with a heartbeat in the last minute" in page
    assert f'<th scope="row">{controller}</th>' in page


def test_about_json_gives_each_live_controller_its_heartbeat(site):
    ctx, _ = site
    with acting_as(OWNER):
        controllers.start(ctx.engine)
    about = act(ctx, ActionName.ABOUT)
    assert about.controllers[0].heartbeat_at.tzinfo is not None


def test_agents_page_explains_a_deployment_without_agents(site):
    _, client = site
    sign_in(client, "alice")
    page = client.get("/ui/agents")
    assert page.status_code == 200
    assert "Not used: this deployment runs tasks on the controller" in page.text


def test_scheduled_runs_list_the_next_run_first_and_explain_paused_schedules(site):
    ctx, client = site
    with acting_as(OWNER), ctx.engine.begin() as conn:
        for code in ("HOURLY", "DAILY", "HELD"):
            add_pipeline(conn, code)
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET RUN_SCHEDULE=:cron, SCHEDULE_TIMEZONE='UTC' "
                "WHERE PIPELINE_CODE=:code"
            ),
            [
                {"code": "HOURLY", "cron": "0 * * * *"},
                {"code": "DAILY", "cron": "0 0 * * *"},
                {"code": "HELD", "cron": "0 0 * * *"},
            ],
        )
    act(
        ctx,
        ActionName.CONTROL,
        pipeline_code="HELD",
        action="PAUSE",
        reason="supplier outage",
        request_key="held-pause",
    )
    sign_in(client, "alice")
    page = client.get("/ui/projects/default/scheduled").text
    upcoming = page.split("What runs next")[1].split("Scheduled but not running")[0]
    assert upcoming.index(">HOURLY<") < upcoming.index(">DAILY<")
    assert ">HELD<" not in upcoming
    assert "supplier outage" in page.split("Scheduled but not running")[1]


def test_maintenance_is_admin_only_and_previews_through_the_api(site):
    _, client = site
    sign_in(client, "alice")
    assert client.get("/ui/projects/default/maintenance").status_code == 403
    assert "/maintenance" not in client.get("/ui/projects/default/pipelines").text
    client.cookies.clear()
    sign_in(client, "owner")
    page = client.get("/ui/projects/default/maintenance")
    assert page.status_code == 200, page.text
    assert 'data-url="/api/v1/projects/default/purge"' in page.text
    assert "Cloning is off" in page.text
    csrf = re.search(r'name="csrf-token" content="([^"]+)"', page.text).group(1)
    preview = client.post(
        "/api/v1/projects/default/purge",
        json={"dry_run": True},
        headers={
            "X-CSRF-Token": csrf,
            "Origin": "https://testserver",
            "Accept": "application/json",
        },
    )
    assert preview.status_code == 200, preview.text
    assert preview.json()["dry_run"] is True
