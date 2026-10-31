"""Held and paused work, and run filters by pool, agent and failure class."""

import re
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole, AttemptStatus, ControlAction, FailureClass
from etl_craft.core.errors import UsageError
from etl_craft.engine import transitions
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_task, finish_run, start_run

PASSWORD = "correct horse battery"
OWNER = Actor("owner", ActorKind.USER)


def ended_run(conn, pipeline, task, status, *, retryable=True):
    run = start_run(conn, pipeline)
    summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
    attempt = transitions.queue_attempt(conn, summary, OWNER)
    lease = datetime.now(UTC) + timedelta(minutes=5)
    transitions.claim_attempt(conn, attempt, OWNER, owner="w1", lease_expires_at=lease)
    transitions.start_attempt(conn, attempt, OWNER, owner="w1", host="h", pid=4242)
    transitions.finish_attempt(
        conn, attempt, status, OWNER, owner="w1", error_message="x", retryable=retryable
    )
    finish_run(conn, run, "SUCCESS" if status == AttemptStatus.SUCCESS else "FAILED")
    return run


@pytest.fixture
def site(metadata_project):
    from etl_craft.engine.migrations import mark_packaged_migrations_applied

    p = metadata_project
    with p.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
    ctx = OperationContext(p.engine, p.config, OWNER, role=ApiRole.ADMIN)
    for name, role in (("owner", ApiRole.ADMIN), ("alice", ApiRole.VIEWER)):
        act(ctx, ActionName.ACCOUNT_ADD, name=name, role=role, password=PASSWORD)
    with acting_as(OWNER), p.engine.begin() as conn:
        pipeline = add_pipeline(conn, "SALES")
        load = add_task(conn, pipeline, "LOAD")
        add_task(conn, pipeline, "PUBLISH")
        runs = {
            "ok": ended_run(conn, pipeline, load, AttemptStatus.SUCCESS),
            "permanent": ended_run(conn, pipeline, load, AttemptStatus.FAILED, retryable=False),
            "unknown": ended_run(conn, pipeline, load, AttemptStatus.FAILED),
        }
        runs["active"] = start_run(conn, pipeline)
    client = TestClient(
        create_app(ctx), base_url="https://testserver", raise_server_exceptions=False
    )
    return ctx, client, runs


def sign_in(client, name):
    page = client.get("/signin")
    csrf = re.search(r'name="csrf-token" content="([^"]+)"', page.text).group(1)
    client.post(
        "/api/v1/login/password",
        headers={"HX-Request": "true", "X-CSRF-Token": csrf, "Origin": "https://testserver"},
        json={"name": name, "password": PASSWORD, "_return_to": "/ui"},
    )


def control(ctx, **fields):
    return act(ctx, ActionName.CONTROL, pipeline_code="SALES", reason="test", **fields)


def test_holds_lists_only_holds_still_in_force(site):
    ctx, _, runs = site
    control(ctx, action=ControlAction.HOLD, run_id=runs["active"], task_code="LOAD")
    control(ctx, action=ControlAction.HOLD, run_id=runs["active"], task_code="PUBLISH")
    control(ctx, action=ControlAction.RELEASE, run_id=runs["active"], task_code="PUBLISH")
    holds = act(ctx, ActionName.HOLDS).holds
    assert [(h.pipeline_run_id, h.task_code, h.requested_by) for h in holds] == [
        (runs["active"], "LOAD", "owner")
    ]


def test_the_held_page_lists_holds_pauses_and_their_way_back(site):
    ctx, client, runs = site
    control(ctx, action=ControlAction.HOLD, run_id=runs["active"], task_code="LOAD")
    control(ctx, action=ControlAction.PAUSE, task_code="PUBLISH")
    sign_in(client, "owner")
    page = client.get("/ui/projects/default/held")
    assert page.status_code == 200, page.text
    held = page.text.split("Held runs and tasks")[1].split("Paused pipelines")[0]
    assert f">{runs['active']}<" in held and '"action": "RELEASE"' in held
    tasks = page.text.split("Paused tasks")[1]
    assert ">PUBLISH<" in tasks and '"action": "RESUME"' in tasks
    client.cookies.clear()
    sign_in(client, "alice")
    viewer = client.get("/ui/projects/default/held").text
    assert ">LOAD<" in viewer and '"action": "RELEASE"' not in viewer


@pytest.mark.parametrize(
    ("filters", "expected"),
    [
        ({"failure_class": FailureClass.PERMANENT}, {"permanent"}),
        ({"failure_class": FailureClass.UNKNOWN}, {"unknown"}),
        ({"pool": "default"}, {"ok", "permanent", "unknown"}),
        ({"pool": "nowhere"}, set()),
    ],
)
def test_history_filters_runs_by_their_attempts(site, filters, expected):
    ctx, _, runs = site
    found = act(ctx, ActionName.HISTORY, pipeline_code="SALES", all=True, **filters)
    names = {
        name for name, run in runs.items() if run in {e.pipeline_run_id for e in found.entries}
    }
    assert names == expected


def test_run_filters_need_a_list_of_pipeline_runs(site):
    ctx, _, _ = site
    with pytest.raises(UsageError, match="give `all` and omit `task_code`"):
        act(ctx, ActionName.HISTORY, pipeline_code="SALES", pool="default")


def test_the_runs_tab_filters_and_keeps_its_filters(site):
    _, client, runs = site
    sign_in(client, "alice")
    page = client.get("/ui/projects/default/pipelines/SALES?failure_class=PERMANENT").text
    assert f">{runs['permanent']}</a>" in page and f">{runs['ok']}</a>" not in page
    assert '<option value="PERMANENT" selected>' in page
    empty = client.get("/ui/projects/default/pipelines/SALES?pool=nowhere").text
    assert "No runs match these filters." in empty
    bad = client.get("/ui/projects/default/pipelines/SALES?failure_class=LOUD")
    assert bad.status_code == 400 and "expected TRANSIENT, PERMANENT, UNKNOWN" in bad.text
