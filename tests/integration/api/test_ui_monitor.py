"""Workflow Monitor pages render the same reads as the API and act only through its routes."""

import json
import re

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.metadata import add_dependency, add_pipeline, add_task, finish_run, start_run

CSRF = "__Host-etl-craft-csrf"
PASSWORD = "correct horse battery"
OWNER = Actor("owner", ActorKind.USER)
BASE = "/ui/projects/default/pipelines"


@pytest.fixture
def monitor(engine_db):
    from etl_craft.engine.migrations import mark_packaged_migrations_applied

    with engine_db.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
    ctx = OperationContext(engine_db.engine, engine_db.config, OWNER, role=ApiRole.ADMIN)
    act(ctx, ActionName.ACCOUNT_ADD, name="owner", role=ApiRole.ADMIN, password=PASSWORD)
    act(ctx, ActionName.ACCOUNT_ADD, name="alice", role=ApiRole.VIEWER, password=PASSWORD)
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        for code, tags in (("SALES", ["team:sales"]), ("OPS", ["team:ops"])):
            pipeline = add_pipeline(conn, code)
            conn.execute(
                text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS=:p WHERE PIPELINE_ID=:id"),
                {"p": json.dumps({"TAGS": tags}), "id": pipeline},
            )
            extract = add_task(conn, pipeline, "EXTRACT")
            load = add_task(conn, pipeline, "LOAD")
            add_dependency(conn, pipeline, load, extract)
            finish_run(conn, start_run(conn, pipeline))
        active = start_run(conn, pipeline)
    client = TestClient(
        create_app(ctx), base_url="https://testserver", raise_server_exceptions=False
    )
    return ctx, client, active


def login(client, name="owner"):
    page = client.get("/signin")
    csrf = re.search(r'name="csrf-token" content="([^"]+)"', page.text).group(1)
    return client.post(
        "/api/v1/login/password",
        headers={"HX-Request": "true", "X-CSRF-Token": csrf, "Origin": "https://testserver"},
        json={"name": name, "password": PASSWORD, "_return_to": "/ui"},
    )


def api(client, path, **params):
    return client.get(f"/api/v1/projects/default{path}", params=params).json()


def test_pipeline_list_shows_the_pipelines_read_and_filters_by_its_tag_parameter(monitor):
    _, client, _ = monitor
    login(client)
    page = client.get(BASE)
    assert page.status_code == 200
    listed = [p["pipeline_code"] for p in api(client, "/pipelines")["pipelines"]]
    assert re.findall(r'<th scope="row"><a href="[^"]+">([^<]+)</a>', page.text) == listed
    tagged = client.get(BASE, params={"tag": "team:ops"})
    shown = re.findall(r'<th scope="row"><a href="[^"]+">([^<]+)</a>', tagged.text)
    assert (
        shown
        == [p["pipeline_code"] for p in api(client, "/pipelines", tag="team:ops")["pipelines"]]
        == ["OPS"]
    )
    assert 'href="?tag=team%3Asales"' in tagged.text and "Tagged team:ops" in tagged.text


def test_pipeline_page_lists_the_history_read_and_its_tasks(monitor):
    _, client, _ = monitor
    login(client)
    page = client.get(f"{BASE}/OPS")
    history = api(client, "/history", pipeline_code="OPS", all="true", limit=25)
    run_ids = [str(entry["pipeline_run_id"]) for entry in history["entries"]]
    assert re.findall(r'<th scope="row"><a href="[^"]+/runs/(\d+)">', page.text) == run_ids
    tasks = client.get(f"{BASE}/OPS", params={"tab": "tasks"})
    assert "EXTRACT" in tasks.text and "LOAD" in tasks.text and "(SUCCESS)" in tasks.text
    bad = client.get(f"{BASE}/OPS", params={"tab": "edit"})
    assert bad.status_code == 400 and "expected runs or tasks" in bad.text


def test_run_page_selects_a_task_and_shows_its_scope_capabilities(monitor):
    _, client, active = monitor
    login(client)
    run = client.get(f"{BASE}/OPS/runs/{active}")
    assert run.status_code == 200
    assert f"Selected: the whole run {active}" in run.text
    assert f'href="{BASE}/OPS/runs/{active}?task=LOAD"' in run.text
    task = client.get(f"{BASE}/OPS/runs/{active}", params={"task": "LOAD"})
    expected = api(
        client, "/capabilities", pipeline_code="OPS", run_id=str(active), task_code="LOAD"
    )
    shown = re.findall(r'data-control="([A-Z_]+)"', task.text)
    assert shown == [c["action"] for c in expected["actions"]]
    for capability in expected["actions"]:
        button = re.search(rf'<button[^>]*data-control="{capability["action"]}"[^>]*>', task.text)
        assert ("disabled" in button.group(0)) != capability["available"]
    assert "task <strong>LOAD</strong>" in task.text and 'aria-current="true"' in task.text


def test_unknown_or_foreign_objects_look_the_same_as_no_access(monitor):
    _, client, active = monitor
    login(client)
    for path in (
        f"{BASE}/MISSING",
        f"{BASE}/SALES/runs/{active}",
        f"{BASE}/OPS/runs/{active}?task=MISSING",
    ):
        page = client.get(path)
        assert page.status_code == 404, path
        assert "Not found or no access" in page.text


def test_a_hold_from_the_run_page_posts_the_api_control_and_a_viewer_is_refused(monitor):
    ctx, client, active = monitor
    login(client, "alice")
    viewer = client.get(f"{BASE}/OPS/runs/{active}", params={"task": "LOAD"})
    assert re.search(r'<button[^>]*data-control="HOLD"[^>]*disabled', viewer.text)
    request = {"pipeline_code": "OPS", "action": "HOLD", "run_id": active, "task_code": "LOAD"}
    refused = client.post(
        "/api/v1/projects/default/controls",
        headers={"X-CSRF-Token": client.cookies[CSRF], "Accept": "application/json"},
        json=request | {"reason": "viewer try"},
    )
    assert refused.status_code == 403
    client.cookies.clear()
    login(client)
    page = client.get(f"{BASE}/OPS/runs/{active}", params={"task": "LOAD"})
    assert 'data-url="/api/v1/projects/default/controls"' in page.text
    held = client.post(
        "/api/v1/projects/default/controls",
        headers={"X-CSRF-Token": client.cookies[CSRF], "Accept": "application/json"},
        json=request | {"reason": "check the load", "request_key": "ui-hold-1"},
    )
    assert held.status_code == 200, held.text
    assert held.json()["action"] == "HOLD" and held.json()["task_code"] == "LOAD"
    with ctx.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ACTION AS action, REASON AS reason FROM AUD_CONTROL_REQUESTS "
                "WHERE PIPELINE_RUN_ID=:run"
            ),
            {"run": active},
        ).all()
    assert [(row.action, row.reason) for row in rows] == [("HOLD", "check the load")]
