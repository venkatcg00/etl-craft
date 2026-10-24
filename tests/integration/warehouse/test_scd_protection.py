"""Human protection shares the merge's transaction, hash and ownership boundary."""

import json
from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.actor import Actor
from etl_craft.core.enums import ActionName, ActorKind, ApiRole
from etl_craft.core.errors import HandlerError, PermissionDeniedError, UsageError
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.sql_warehouse import warehouses


def prepare(w, kind, compare="name|amount"):
    source = "SELECT 1 AS id, CAST('Source' AS VARCHAR(30)) AS name, 10 AS amount"
    w.setup("protected", source, kind)
    params = {
        "SQL_ACTION": kind,
        "TARGET_OBJECT": "protected",
        "SELECT_QUERY": source,
        "MERGE_KEY": "id",
        "MERGE_COMPARE_COLUMNS": compare,
    }
    w.run("load", **params)
    ctx = OperationContext(
        w.engine_db, w.config, Actor("alice", ActorKind.USER), role=ApiRole.ADMIN
    )
    return ctx, params


def current(w):
    with w.warehouse.connect() as conn:
        return [
            dict(r)
            for r in conn.execute(
                text(f"SELECT * FROM {w.name('protected')} WHERE active_flag = 'Y' ORDER BY id")
            ).mappings()
        ]


@warehouses("duckdb", "postgres")
@pytest.mark.parametrize("kind", ["SCD1_MERGE", "SCD2_MERGE"])
def test_value_locks_effective_hashes_provenance_and_unlock(sql_world, kind):
    w = sql_world
    ctx, params = prepare(w, kind)
    assert not act(
        ctx, ActionName.DATA_PROVENANCE, pipeline_code="P", task_code="load", key={"id": 1}
    ).enabled
    edited = act(
        ctx,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 1},
        values={"name": "Manual"},
        reason="fix name",
    )
    state = {r["column_name"]: r for r in edited.records}
    assert state["name"]["locked"] and state["name"]["writer"] == "alice"
    assert state["amount"]["writer"] is None
    before = current(w)
    unchanged = w.run("load", **params)
    assert (unchanged.update_count, unchanged.insert_count) == (0, 0)
    assert current(w) == before
    assert unchanged.variables == {"PROTECTED_ROWS": 1, "PROTECTED_VALUES": 1}
    changed = w.run(
        "load", **{**params, "SELECT_QUERY": params["SELECT_QUERY"].replace("10 AS", "20 AS")}
    )
    assert changed.update_count == 1
    assert [(r["name"], r["amount"]) for r in current(w)] == [("Manual", 20)]
    state = {
        r["column_name"]: r
        for r in act(
            ctx, ActionName.DATA_PROVENANCE, pipeline_code="P", task_code="load", key={"id": 1}
        ).records
    }
    assert state["name"]["writer"] == "alice" and state["amount"]["writer"] == "etl-craft"
    act(
        ctx,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 1},
        values={"name": "Correction"},
        reason="second fix",
    )
    assert current(w)[0]["name"] == "Correction"
    act(
        ctx,
        ActionName.DATA_UNLOCK,
        pipeline_code="P",
        task_code="load",
        key={"id": 1},
        reason="return to source",
        columns=("name",),
    )
    w.run("load", **params)
    assert current(w)[0]["name"] == "Source"
    state = {
        r["column_name"]: r
        for r in act(
            ctx, ActionName.DATA_PROVENANCE, pipeline_code="P", task_code="load", key={"id": 1}
        ).records
    }
    assert not state["name"]["locked"] and state["name"]["writer"] == "etl-craft"
    if kind == "SCD2_MERGE":
        assert count_versions(w, "Y") == 1
        assert (
            w.rows(f"SELECT COUNT(*) FROM {w.name('protected')} WHERE active_flag='N'")[0][0] >= 4
        )


@warehouses("duckdb", "postgres")
@pytest.mark.parametrize("kind", ["SCD1_MERGE", "SCD2_MERGE"])
def test_human_insert_row_lock_delete_revival_and_service_writer(sql_world, kind):
    w = sql_world
    ctx, params = prepare(w, kind)
    act(
        ctx,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 2},
        values={"name": "Human", "amount": 30},
        reason="insert",
    )
    state = act(ctx, ActionName.DATA_PROVENANCE, pipeline_code="P", task_code="load", key={"id": 2})
    assert any(r["column_name"] == "*" and r["locked"] for r in state.records)
    delete = {
        "SQL_ACTION": "DELETE_ROWS",
        "TARGET_OBJECT": "protected",
        "SELECT_QUERY": "SELECT 2 AS id",
        "MERGE_KEY": "id",
    }
    assert w.run("delete", **delete).delete_count == 0
    assert w.run("delete", **{**delete, "HARD_DELETE": "true"}).delete_count == 0
    incoming = {**params, "SELECT_QUERY": params["SELECT_QUERY"].replace("1 AS id", "2 AS id")}
    assert w.run("load", **incoming).update_count == 0
    assert current(w)[1]["name"] == "Human"
    act(
        ctx,
        ActionName.DATA_UNLOCK,
        pipeline_code="P",
        task_code="load",
        key={"id": 2},
        reason="release insert",
    )
    assert w.run("delete", **delete).delete_count == 1
    w.run("load", **incoming)
    assert current(w)[1]["name"] == "Source"
    robot = replace(ctx, actor=Actor("erp", ActorKind.SERVICE))
    act(
        robot,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 2},
        values={"name": "ERP"},
        reason="integration",
    )
    assert not any(
        r["locked"]
        for r in act(
            ctx, ActionName.DATA_PROVENANCE, pipeline_code="P", task_code="load", key={"id": 2}
        ).records
    )
    w.run("load", **incoming)
    assert current(w)[1]["name"] == "Source"
    with pytest.raises(PermissionDeniedError, match="human admin"):
        act(
            robot,
            ActionName.DATA_UNLOCK,
            pipeline_code="P",
            task_code="load",
            key={"id": 2},
            reason="not permitted",
        )


@warehouses("duckdb", "postgres")
def test_roles_validation_and_unobserved_writes_fail_closed(sql_world):
    w = sql_world
    ctx, params = prepare(w, "SCD1_MERGE")
    for role in (ApiRole.VIEWER, ApiRole.OPERATOR, ApiRole.DEVELOPER):
        with pytest.raises(PermissionDeniedError, match="admin"):
            act(
                replace(ctx, role=role),
                ActionName.DATA_EDIT,
                pipeline_code="P",
                task_code="load",
                key={"id": 1},
                values={"name": "X"},
                reason="fix",
            )
    with pytest.raises(UsageError, match="business columns"):
        act(
            ctx,
            ActionName.DATA_EDIT,
            pipeline_code="P",
            task_code="load",
            key={"id": 1},
            values={"updated_by": "forged"},
            reason="fix",
        )
    with pytest.raises(UsageError, match="non-NULL"):
        act(
            ctx,
            ActionName.DATA_EDIT,
            pipeline_code="P",
            task_code="load",
            key={"id": None},
            values={"name": "X"},
            reason="fix",
        )
    assert not act(
        ctx, ActionName.DATA_PROVENANCE, pipeline_code="P", task_code="load", key={"id": 1}
    ).enabled
    act(
        ctx,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 1},
        values={"name": "Human"},
        reason="fix",
    )
    w.execute(f"UPDATE {w.name('protected')} SET amount=99")
    with pytest.raises(HandlerError, match="unobserved warehouse writes"):
        w.run("load", **params)
    with pytest.raises(HandlerError, match="unobserved warehouse writes"):
        act(
            ctx,
            ActionName.DATA_UNLOCK,
            pipeline_code="P",
            task_code="load",
            key={"id": 1},
            reason="fix",
        )
    assert current(w)[0]["name"] == "Human"


@warehouses("duckdb", "postgres")
def test_protected_replacement_and_atomic_evidence_rollback(sql_world, monkeypatch):
    w = sql_world
    ctx, params = prepare(w, "SCD2_MERGE")
    act(
        ctx,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 1},
        values={"name": "Human"},
        reason="fix",
    )
    before = w.rows(f"SELECT * FROM {w.name('protected')} ORDER BY row_id")
    for action in ("CREATE_TABLE", "OVERWRITE_TABLE", "APPEND_TABLE", "DROP_TABLE"):
        with pytest.raises(HandlerError, match="cannot replace protected"):
            w.run(
                "replace_" + action,
                SQL_ACTION=action,
                TARGET_OBJECT="protected",
                **({"SELECT_QUERY": params["SELECT_QUERY"]} if action != "DROP_TABLE" else {}),
            )

    def fail(point):
        if point == "sql.protection.before_evidence":
            raise HandlerError("injected evidence failure")

    monkeypatch.setattr("etl_craft.handlers.sql.protection.fault_point", fail)
    with pytest.raises(HandlerError, match="injected evidence failure"):
        act(
            ctx,
            ActionName.DATA_EDIT,
            pipeline_code="P",
            task_code="load",
            key={"id": 1},
            values={"name": "Lost"},
            reason="fix",
        )
    assert w.rows(f"SELECT * FROM {w.name('protected')} ORDER BY row_id") == before
    assert any(
        r["writer"] == "alice"
        for r in act(
            ctx, ActionName.DATA_PROVENANCE, pipeline_code="P", task_code="load", key={"id": 1}
        ).records
    )


@warehouses("trino_iceberg", "duckdb_iceberg")
def test_nonatomic_warehouses_refuse_protection(sql_world):
    w = sql_world
    ctx, _params = prepare(w, "SCD1_MERGE")
    with pytest.raises(HandlerError, match=r"protected writes unsupported.*atomic"):
        act(
            ctx,
            ActionName.DATA_EDIT,
            pipeline_code="P",
            task_code="load",
            key={"id": 1},
            values={"name": "Human"},
            reason="fix",
        )
    assert current(w)[0]["name"] == "Source"


@warehouses("duckdb", "postgres")
@pytest.mark.parametrize("kind", ["SCD1_MERGE", "SCD2_MERGE"])
@pytest.mark.parametrize("engine_kind", ["sqlite", "postgresql"])
def test_edit_and_unlock_race_with_etl(sql_world, kind, request, engine_kind):
    from etl_craft.handlers import sql
    from fixtures.races import two_at_once

    w = sql_world
    if engine_kind == "postgresql":
        use_postgres_engine(w, request)
    ctx, params = prepare(w, kind)
    context = w.task(
        "load", **{**params, "SELECT_QUERY": params["SELECT_QUERY"].replace("10 AS", "20 AS")}
    )
    two_at_once(
        lambda: act(
            ctx,
            ActionName.DATA_EDIT,
            pipeline_code="P",
            task_code="load",
            key={"id": 1},
            values={"name": "Human"},
            reason="fix",
        ),
        lambda: sql.run(context, w.engine_db),
        at="etl_craft.engine.locks.target",
    )
    assert [(r["name"], r["amount"]) for r in current(w)] == [("Human", 20)]
    two_at_once(
        lambda: act(
            ctx,
            ActionName.DATA_UNLOCK,
            pipeline_code="P",
            task_code="load",
            key={"id": 1},
            reason="release",
        ),
        lambda: sql.run(context, w.engine_db),
        at="etl_craft.engine.locks.target",
    )
    assert not any(
        r["locked"]
        for r in act(
            ctx, ActionName.DATA_PROVENANCE, pipeline_code="P", task_code="load", key={"id": 1}
        ).records
    )
    sql.run(context, w.engine_db)
    assert current(w)[0]["name"] == "Source"


@warehouses("duckdb", "postgres")
def test_service_writes_respect_human_values_and_row_locks(sql_world):
    w = sql_world
    ctx, _params = prepare(w, "SCD2_MERGE")
    act(
        ctx,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 1},
        values={"name": "Human"},
        reason="fix",
    )
    robot = replace(ctx, actor=Actor("erp", ActorKind.SERVICE))
    act(
        robot,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 1},
        values={"name": "ERP", "amount": 42},
        reason="refresh",
    )
    assert [(r["name"], r["amount"]) for r in current(w)] == [("Human", 42)]
    act(
        ctx,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 1},
        values={"name": "Human"},
        reason="hold row",
        row_lock=True,
    )
    before = current(w)
    act(
        robot,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 1},
        values={"amount": 99},
        reason="refresh",
    )
    assert current(w) == before


@warehouses("duckdb", "postgres")
@pytest.mark.parametrize("engine_kind", ["sqlite", "postgresql"])
def test_values_api_uses_human_credentials_and_shared_role_policy(sql_world, request, engine_kind):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app

    w = sql_world
    if engine_kind == "postgresql":
        use_postgres_engine(w, request)
    ctx, params = prepare(w, "SCD2_MERGE")
    act(
        ctx,
        ActionName.ACCOUNT_ADD,
        name="alice",
        role=ApiRole.ADMIN,
        password="correct horse battery",
    )
    token = act(ctx, ActionName.LOGIN, name="alice", password="correct horse battery").access_token
    headers = {"Authorization": "Bearer " + token}
    path = "/api/v1/projects/default/values"
    task = {"pipeline_code": "P", "task_code": "load"}
    with TestClient(create_app(ctx)) as client:
        body = {
            **task,
            "key": {"id": 1},
            "values": {"name": "O'Connor"},
            "reason": "verified spelling",
        }
        assert client.post(path, json=body).status_code == 401
        response = client.post(path, json=body, headers=headers)
        assert response.status_code == 200, response.text
        assert response.json()["schema"] == "etl-craft/scd-values/1"
        assert all(r["happened_at"].endswith("+00:00") for r in response.json()["records"])
        with w.engine_db.connect() as conn:
            audit = (
                conn.execute(
                    text(
                        "SELECT ACTOR AS actor, ACTOR_KIND AS actor_kind, "
                        "PIPELINE_ID AS pipeline_id, "
                        "TASK_ID AS task_id, ARGUMENTS AS arguments FROM AUD_ACTIONS "
                        "WHERE COMMAND = 'data edit' ORDER BY ACTION_ID DESC LIMIT 1"
                    )
                )
                .mappings()
                .one()
            )
        assert audit["actor"] == "alice" and audit["actor_kind"] == "USER"
        assert audit["pipeline_id"] == w.pipeline_id and audit["task_id"] == w.tasks["load"]
        arguments = audit["arguments"]
        if isinstance(arguments, str):
            arguments = json.loads(arguments)
        assert response.json()["request_id"] == arguments["request_id"]
        assert "O'Connor" not in json.dumps(arguments)
        assert current(w)[0]["name"] == "O'Connor"
        assert current(w)[0].get("UPDATED_BY", current(w)[0].get("updated_by")) == "alice"
        assert client.post(path, json={**body, "unknown": True}, headers=headers).status_code == 400
        act(ctx, ActionName.ACCOUNT_SET_ROLE, name="alice", role=ApiRole.OPERATOR)
        assert client.post(path, json=body, headers=headers).status_code == 403
        denied = client.post(
            path + "/unlock", json={**task, "key": {"id": 1}, "reason": "release"}, headers=headers
        )
        assert denied.status_code == 403
        assert (
            client.post(
                path + "/provenance", json={**task, "key": {"id": 1}}, headers=headers
            ).status_code
            == 200
        )
        act(ctx, ActionName.ACCOUNT_SET_ROLE, name="alice", role=ApiRole.ADMIN)
        response = client.post(
            path + "/unlock", json={**task, "key": {"id": 1}, "reason": "release"}, headers=headers
        )
        assert response.status_code == 200, response.text
        w.run("load", **params)
        assert current(w)[0]["name"] == "Source"


@warehouses("duckdb", "postgres")
def test_noncompare_edits_null_values_and_historical_tampering(sql_world):
    w = sql_world
    ctx, params = prepare(w, "SCD2_MERGE", compare="amount")
    act(
        ctx,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 1},
        values={"name": None},
        reason="remove wrong name",
    )
    assert current(w)[0]["name"] is None
    incoming = {**params, "SELECT_QUERY": params["SELECT_QUERY"].replace("10 AS", "20 AS")}
    w.run("load", **incoming)
    assert current(w)[0]["name"] is None and current(w)[0]["amount"] == 20
    w.execute(f"UPDATE {w.name('protected')} SET name='tampered' WHERE active_flag='N'")
    with pytest.raises(HandlerError, match="unobserved warehouse writes"):
        w.run("load", **incoming)


def count_versions(w, active):
    with w.warehouse.connect() as conn:
        return conn.execute(
            text(f"SELECT COUNT(*) AS count FROM {w.name('protected')} WHERE active_flag=:active"),
            {"active": active},
        ).scalar_one()


@warehouses("duckdb", "postgres")
def test_evidence_cannot_be_a_task_target_and_partial_restore_fails(sql_world):
    w = sql_world
    ctx, params = prepare(w, "SCD1_MERGE")
    act(
        ctx,
        ActionName.DATA_EDIT,
        pipeline_code="P",
        task_code="load",
        key={"id": 1},
        values={"name": "Human"},
        reason="fix",
    )
    journal = next(t for t in w.tables() if t.startswith("etl_protection_"))
    with pytest.raises(HandlerError, match="reserved SCD protection evidence"):
        w.run(
            "destroy_evidence",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=journal,
            SELECT_QUERY="SELECT 1 AS id",
        )
    snapshot = next(t for t in w.tables() if t.startswith("etl_fingerprints_"))
    w.execute(f"DROP TABLE {w.name(snapshot)}")
    with pytest.raises(HandlerError, match=r"incomplete protection evidence.*restore"):
        w.run("load", **params)
    assert current(w)[0]["name"] == "Human"


def use_postgres_engine(w, request):
    from fixtures.engine_db import apply_schema
    from fixtures.metadata import add_pipeline, start_run

    db = request.getfixturevalue("postgres_database")
    apply_schema(db.engine)
    w.config = replace(w.config, engine=db.config.engine)
    w.engine_db = db.engine
    with db.engine.begin() as conn:
        w.pipeline_id = add_pipeline(conn, "P")
        w.pipeline_run_id = start_run(conn, w.pipeline_id)


@warehouses("duckdb", "postgres")
def test_manual_edit_refuses_conflicting_merge_lifecycles(sql_world):
    from fixtures.metadata import add_pipeline, add_task

    w = sql_world
    ctx, params = prepare(w, "SCD1_MERGE")
    with w.engine_db.begin() as conn:
        other = add_pipeline(conn, "OTHER")
        add_task(
            conn,
            other,
            "other",
            "SQL",
            **{
                **params,
                "SQL_ACTION": "SCD2_MERGE",
                "TARGET_OBJECT": w.name("protected"),
            },
        )
    with pytest.raises(HandlerError, match="conflicts with an active SCD2_MERGE writer"):
        act(
            ctx,
            ActionName.DATA_EDIT,
            pipeline_code="P",
            task_code="load",
            key={"id": 1},
            values={"name": "Human"},
            reason="fix",
        )
    assert current(w)[0]["name"] == "Source"
