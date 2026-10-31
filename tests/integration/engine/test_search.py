"""One search text finds visible pipelines, tasks, tables and runs, never hidden ones."""

import pytest
from sqlalchemy import text

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.enums_ui import SearchKind
from etl_craft.core.errors import UsageError
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.identity import authenticated_context
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_table, add_task, start_run

OWNER = Actor("owner", ActorKind.USER)


@pytest.fixture
def scene(engine_db):
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        sales = add_pipeline(conn, "SALES_DAILY")
        add_task(conn, sales, "LOAD_SALES")
        hidden = add_pipeline(conn, "HR_SALES")
        add_task(conn, hidden, "LOAD_SALES_HR")
        add_table(conn, "ods.sales_orders")
        run = start_run(conn, sales)
        secret = start_run(conn, hidden)
        conn.execute(
            text("UPDATE AUD_PIPELINES_RUN_LOG SET RUN_KEY='nightly-7' WHERE PIPELINE_RUN_ID=:r"),
            {"r": run},
        )
    owner = OperationContext(engine_db.engine, engine_db.config, OWNER, role=ApiRole.ADMIN)
    token = act(owner, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    return owner, authenticated_context(owner, token.metadata), run, secret


def found(view):
    return {(hit.kind, hit.label, hit.pipeline_code) for hit in view.hits}


def test_finds_every_kind_by_part_of_a_name_and_runs_by_exact_id_or_key(scene):
    owner, _, run, _ = scene
    hits = found(act(owner, ActionName.SEARCH, q="sales"))
    assert {
        (SearchKind.PIPELINE, "SALES_DAILY", "SALES_DAILY"),
        (SearchKind.PIPELINE, "HR_SALES", "HR_SALES"),
        (SearchKind.TASK, "LOAD_SALES", "SALES_DAILY"),
        (SearchKind.TABLE, "ods.sales_orders", None),
    } <= hits
    table = next(h for h in act(owner, ActionName.SEARCH, q="ORDERS").hits)
    assert table.catalog_path == "tables/ods.sales_orders.html"
    by_id = act(owner, ActionName.SEARCH, q=str(run)).hits[0]
    assert (by_id.kind, by_id.run_id, by_id.pipeline_code) == (SearchKind.RUN, run, "SALES_DAILY")
    by_key = act(owner, ActionName.SEARCH, q=" nightly-7 ").hits
    assert [(h.kind, h.run_id) for h in by_key] == [(SearchKind.RUN, run)]
    assert act(owner, ActionName.SEARCH, q="nightly").hits == ()


def test_limit_marks_truncation(scene):
    owner, _, _, _ = scene
    view = act(owner, ActionName.SEARCH, q="sales", limit=2)
    assert len(view.hits) == 2 and view.truncated
    assert not act(owner, ActionName.SEARCH, q="sales").truncated


def test_pipeline_limited_reader_sees_only_granted_pipelines_and_no_tables(scene, engine_db):
    owner, reader, _, secret = scene
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (PIPELINE_CODE,GRANTEE_ROLE,PERMISSION) "
                "VALUES ('HR_SALES','operator','CAN_VIEW')"
            )
        )
    hits = found(act(reader, ActionName.SEARCH, q="sales"))
    assert {code for _, _, code in hits} == {"SALES_DAILY"}
    assert act(reader, ActionName.SEARCH, q=str(secret)).hits == ()
    assert any(h.pipeline_code == "HR_SALES" for h in act(owner, ActionName.SEARCH, q="hr").hits)


@pytest.mark.parametrize(("q", "limit"), [("   ", 20), ("x" * 201, 20), ("sales", 0), ("a", 51)])
def test_refuses_empty_long_or_unbounded_searches(scene, q, limit):
    owner, _, _, _ = scene
    with pytest.raises(UsageError, match="expected 1 to"):
        act(owner, ActionName.SEARCH, q=q, limit=limit)


def test_api_returns_the_search_document(scene):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app

    owner, _, _, _ = scene
    token = act(owner, ActionName.TOKEN_CREATE, name="api", role=ApiRole.ADMIN).token
    client = TestClient(create_app(owner))
    response = client.get(
        "/api/v1/projects/default/search",
        params={"q": "sales"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["schema"] == "etl-craft/search/1"
    assert {(h["kind"], h["label"]) for h in body["hits"]} >= {("TASK", "LOAD_SALES")}


def test_cli_prints_the_same_hits_as_text_and_json(metadata_project, monkeypatch, capsys):
    import json

    from etl_craft.cli import main

    p = metadata_project
    with acting_as(OWNER), p.engine.begin() as conn:
        add_task(conn, add_pipeline(conn, "SALES_DAILY"), "LOAD_SALES")
    ctx = OperationContext(p.engine, p.config, OWNER, role=ApiRole.ADMIN)
    expected = act(ctx, ActionName.SEARCH, q="a").hits
    token = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    monkeypatch.chdir(p.root)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token.token)
    assert main(["search", "a", "--format", "json"]) == 0
    document = json.loads(capsys.readouterr().out)
    assert [h["label"] for h in document["hits"]] == [h.label for h in expected]
    assert main(["search", "a"]) == 0
    lines = capsys.readouterr().out.splitlines()
    assert lines[0].split("\t") == ["KIND", "NAME", "PIPELINE", "RUN_ID", "DETAIL"]
    assert len(lines) - 1 == len(expected) > 0
    assert main(["search", "  "]) != 0
    assert "expected 1 to 200" in capsys.readouterr().err
