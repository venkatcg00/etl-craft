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
    assert bad.status_code == 400 and "expected runs, matrix or tasks" in bad.text


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


def ran_extract(ctx, active, *, finish=True):
    from datetime import UTC, datetime, timedelta

    from etl_craft.core.enums import AttemptStatus
    from etl_craft.engine import transitions

    with acting_as(OWNER), ctx.engine.begin() as conn:
        task_id = conn.execute(
            text(
                "SELECT t.TASK_ID FROM CFG_TASKS t JOIN CFG_PIPELINES p "
                "ON p.PIPELINE_ID=t.PIPELINE_ID WHERE p.PIPELINE_CODE='OPS' "
                "AND t.TASK_CODE='EXTRACT'"
            )
        ).scalar_one()
        task_run = transitions.find_or_create_task_run(conn, task_id, active).task_run_id
        attempt = transitions.queue_attempt(conn, task_run, OWNER)
        lease = datetime.now(UTC) + timedelta(minutes=1)
        transitions.claim_attempt(conn, attempt, OWNER, owner="w1", lease_expires_at=lease)
        transitions.start_attempt(conn, attempt, OWNER, owner="w1", host="worker-1")
        if finish:
            transitions.finish_attempt(conn, attempt, AttemptStatus.SUCCESS, OWNER, owner="w1")
    return attempt


def attempt_log(ctx, active, attempt, text_value):
    """Place the attempt's local log where the engine records it and point LOG_PATH at it."""
    path = ctx.config.log_dir / "project-1" / "OPS" / f"run-{active}" / "EXTRACT.attempt-1.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text_value)
    with ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
            {"path": str(path.absolute()), "id": attempt},
        )
    return path


def test_matrix_cells_match_each_tasks_history_and_link_to_the_task(monitor):
    ctx, client, active = monitor
    ran_extract(ctx, active)
    login(client)
    page = client.get(f"{BASE}/OPS", params={"tab": "matrix"})
    assert page.status_code == 200
    cells = re.findall(r'href="[^"]+/runs/(\d+)\?task=([A-Z]+)"', page.text)
    for task in ("EXTRACT", "LOAD"):
        history = api(client, "/history", pipeline_code="OPS", task_code=task, all="true")
        assert sorted(int(r) for r, t in cells if t == task) == sorted(
            e["pipeline_run_id"] for e in history["entries"]
        )


def test_timeline_draws_each_attempt_and_keeps_the_view_when_selecting(monitor):
    ctx, client, active = monitor
    ran_extract(ctx, active)
    login(client)
    page = client.get(f"{BASE}/OPS/runs/{active}", params={"view": "timeline"})
    assert page.status_code == 200
    assert re.findall(r'class="segment ([A-Za-z_-]+)"', page.text) == ["queued", "SUCCESS"]
    assert f'href="{BASE}/OPS/runs/{active}?view=timeline&amp;task=EXTRACT"' in page.text
    bad = client.get(f"{BASE}/OPS/runs/{active}", params={"view": "gantt"})
    assert bad.status_code == 400 and "expected graph, timeline or log" in bad.text


def test_list_shows_next_run_recent_runs_and_sla_from_the_read(monitor):
    ctx, client, _ = monitor
    with ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_PIPELINES SET RUN_SCHEDULE='0 6 * * *', SCHEDULE_TIMEZONE='UTC' "
                "WHERE PIPELINE_CODE='SALES'"
            )
        )
    login(client)
    page = client.get(BASE)
    listed = {p["pipeline_code"]: p for p in api(client, "/pipelines")["pipelines"]}
    assert listed["SALES"]["next_run"] is not None
    assert listed["SALES"]["next_run"][:10] in page.text
    for code, pipeline in listed.items():
        for run in pipeline["recent_runs"]:
            assert f'/{code}/runs/{run["pipeline_run_id"]}" title="Run' in page.text
    assert 'data-bulk="PAUSE" disabled' in page.text and 'data-csv="pipeline-table"' in page.text


def test_search_page_lists_the_search_reads_hits_with_links_to_their_pages(monitor):
    _, client, active = monitor
    login(client)
    page = client.get("/ui/projects/default/search", params={"q": "ex"})
    assert page.status_code == 200
    hits = api(client, "/search", q="ex")["hits"]
    links = re.findall(r'<td><a href="([^"]+)">([^<]+)</a>', page.text)
    assert [label for _, label in links] == [hit["label"] for hit in hits]
    assert set(links) == {
        ("/ui/projects/default/pipelines/SALES?tab=tasks", "EXTRACT"),
        ("/ui/projects/default/pipelines/OPS?tab=tasks", "EXTRACT"),
    }
    run = client.get("/ui/projects/default/search", params={"q": str(active)})
    assert f'href="{BASE}/OPS/runs/{active}"' in run.text
    empty = client.get("/ui/projects/default/search")
    assert empty.status_code == 200 and 'id="recent-list"' in empty.text
    assert 'id="global-search"' in empty.text
    too_long = client.get("/ui/projects/default/search", params={"q": "x" * 201})
    assert too_long.status_code == 400 and "expected 1 to 200" in too_long.text


def test_catalog_wrapper_opens_only_a_valid_catalog_page(monitor):
    _, client, _ = monitor
    login(client)
    refused = client.get("/ui/projects/default/catalog", params={"page": "../secret.html"})
    assert refused.status_code == 400 and "relative generated" in refused.text


def test_attempt_page_shows_the_attempt_read_and_links_its_log(monitor):
    ctx, client, active = monitor
    attempt = ran_extract(ctx, active)
    attempt_log(ctx, active, attempt, "extract started\nextract done\n")
    login(client)
    run_page = client.get(f"{BASE}/OPS/runs/{active}", params={"task": "EXTRACT"})
    assert f'href="{BASE}/OPS/runs/{active}/attempts/{attempt}"' in run_page.text
    page = client.get(f"{BASE}/OPS/runs/{active}/attempts/{attempt}")
    assert page.status_code == 200
    document = api(client, f"/attempts/{attempt}")
    assert document["host"] == "worker-1" and ">worker-1<" in page.text
    log_api = f"/api/v1/projects/default/attempts/{attempt}/log"
    assert f'href="{log_api}" download="attempt-{attempt}.log"' in page.text
    assert client.get(log_api).text == "extract started\nextract done\n"
    assert client.get(log_api, params={"offset": 16}).text == "extract done\n"


def test_attempt_page_refuses_another_runs_or_pipelines_attempt_alike(monitor):
    ctx, client, active = monitor
    attempt = ran_extract(ctx, active)
    login(client)
    for path in (
        f"{BASE}/OPS/runs/{active - 1}/attempts/{attempt}",
        f"{BASE}/SALES/runs/{active}/attempts/{attempt}",
        f"{BASE}/OPS/runs/{active}/attempts/999999",
    ):
        page = client.get(path)
        assert page.status_code == 404 and "Not found or no access" in page.text, path


def test_run_log_view_shows_the_run_log_read_and_links_its_download(monitor):
    import html

    ctx, client, active = monitor
    ran_extract(ctx, active)
    login(client)
    page = client.get(f"{BASE}/OPS/runs/{active}", params={"view": "log"})
    assert page.status_code == 200
    expected = client.get(
        "/api/v1/projects/default/run-log", params={"pipeline_code": "OPS", "run_id": active}
    ).text
    assert "TASK_STARTED" in expected
    shown = re.search(r'<pre class="log"[^>]*>(.*?)</pre>', page.text, re.S).group(1)
    assert html.unescape(shown) == expected
    link = f"/api/v1/projects/default/run-log?pipeline_code=OPS&amp;run_id={active}"
    assert f'href="{link}" download="run-{active}.log"' in page.text
    selected = client.get(f"{BASE}/OPS/runs/{active}", params={"view": "log", "task": "EXTRACT"})
    assert f'href="{BASE}/OPS/runs/{active}?view=log&amp;task=LOAD"' in selected.text
