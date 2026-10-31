"""The task page shows a task's runs and stored offset; the Uploads page lists upload targets."""

import re
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.counts import Counts
from etl_craft.core.enums import ActionName, ApiRole, AttemptStatus, PositionKind
from etl_craft.core.enums_access import Permission
from etl_craft.engine import transitions
from etl_craft.engine.repository.offsets import StoredOffset
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_task, finish_run, start_run

PASSWORD = "correct horse battery"
OWNER = Actor("owner", ActorKind.USER)
TASK = "/ui/projects/default/pipelines/SALES/tasks"


def measured_run(conn, pipeline, task, *, inserted, offset=None):
    run = start_run(conn, pipeline)
    summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
    attempt = transitions.queue_attempt(conn, summary, OWNER)
    lease = datetime.now(UTC) + timedelta(minutes=5)
    transitions.claim_attempt(conn, attempt, OWNER, owner="w1", lease_expires_at=lease)
    transitions.start_attempt(conn, attempt, OWNER, owner="w1", host="h", pid=4242)
    transitions.finish_attempt(
        conn,
        attempt,
        AttemptStatus.SUCCESS,
        OWNER,
        owner="w1",
        counts=Counts(source_count=inserted, insert_count=inserted),
        offset=offset,
    )
    finish_run(conn, run)


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
        ingest = add_task(
            conn,
            pipeline,
            "INGEST",
            "PYTHON",
            SCRIPT_FILE="python_scripts/ingest.py",
            SCRIPT_KIND="INGESTION",
        )
        add_task(conn, pipeline, "NOTIFY", "EMAIL_ALERT")
        measured_run(conn, pipeline, ingest, inserted=120, offset=StoredOffset("NUMBER", "120"))
        measured_run(conn, pipeline, ingest, inserted=80, offset=StoredOffset("NUMBER", "200"))
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


def test_offset_read_gives_the_stored_position_or_says_there_is_none(site):
    ctx, _ = site
    stored = act(ctx, ActionName.OFFSET, pipeline_code="SALES", task_code="INGEST")
    assert (stored.kind, stored.offset_type, stored.offset_value) == (
        PositionKind.OFFSET,
        "NUMBER",
        "200",
    )
    none = act(ctx, ActionName.OFFSET, pipeline_code="SALES", task_code="NOTIFY")
    assert (none.kind, none.offset_value) == (None, None)


def test_the_task_page_shows_each_runs_counts_and_the_offset_with_its_reset(site):
    _, client = site
    sign_in(client, "owner")
    page = client.get(f"{TASK}/INGEST")
    assert page.status_code == 200, page.text
    assert "Last 2 runs" in page.text and "2 of 2" in page.text
    assert "<td>120</td>" in page.text and "<td>80</td>" in page.text
    assert "<code>200</code>" in page.text
    assert 'name="offset"' in page.text and "/api/v1/projects/default/marks" in page.text


def test_viewers_see_the_offset_without_a_reset_and_other_tasks_have_no_panel(site):
    _, client = site
    sign_in(client, "alice")
    page = client.get(f"{TASK}/INGEST").text
    assert "<code>200</code>" in page and 'name="offset"' not in page
    notify = client.get(f"{TASK}/NOTIFY").text
    assert "This task has not run yet." in notify and "Watermark" not in notify
    assert ">Offset<" not in notify


def test_the_tasks_tab_links_each_task_to_its_page(site):
    _, client = site
    sign_in(client, "alice")
    page = client.get("/ui/projects/default/pipelines/SALES?tab=tasks").text
    assert f'href="{TASK}/INGEST"' in page


def test_uploads_lists_only_pipelines_the_caller_may_upload_to(site):
    ctx, client = site
    sign_in(client, "alice")
    assert (
        "No pipeline takes uploads from you here" in client.get("/ui/projects/default/uploads").text
    )
    with ctx.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PIPELINES SET STEWARDABLE_FLAG='Y'"))
    act(
        ctx,
        ActionName.ACCESS_GRANT,
        project="default",
        account="alice",
        pipeline="SALES",
        permission=Permission.CAN_UPLOAD,
        reason="steward",
    )
    page = client.get("/ui/projects/default/uploads").text
    assert '<option value="SALES" selected>' in page
    assert "No files uploaded to SALES yet." in page
    assert 'data-url="/api/v1/projects/default/uploads"' in page
