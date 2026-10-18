"""Human protection shares the merge's transaction, hash and ownership boundary."""

import json
from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.actor import Actor
from etl_craft.core.enums import ActorKind, ApiRole
from etl_craft.core.errors import HandlerError, PermissionDeniedError, UsageError
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.values import (
    ValueEdit,
    ValueUnlock,
    provenance,
    unlock_values,
    write_values,
)
from fixtures.sql_warehouse import warehouses


def prepare(w, kind, compare="name|amount"):
    source = "SELECT 1 AS id, CAST('Source' AS VARCHAR(30)) AS name, 10 AS amount"
    w.setup("protected", source, kind)
    params = {
        "SQL_ACTION": kind,
        "TARGET_OBJECT": "protected",
        "SOURCE_SQL": source,
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
    assert not provenance(ctx, "P", "load", {"id": 1}).enabled
    edited = write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": "Manual"}, "fix name"))
    state = {r["column_name"]: r for r in edited.records}
    assert state["name"]["locked"] and state["name"]["writer"] == "alice"
    assert state["amount"]["writer"] is None
    before = current(w)
    unchanged = w.run("load", **params)
    assert (unchanged.update_count, unchanged.insert_count) == (0, 0)
    assert current(w) == before
    assert unchanged.variables == {"PROTECTED_ROWS": 1, "PROTECTED_VALUES": 1}
    changed = w.run(
        "load", **{**params, "SOURCE_SQL": params["SOURCE_SQL"].replace("10 AS", "20 AS")}
    )
    assert changed.update_count == 1
    assert [(r["name"], r["amount"]) for r in current(w)] == [("Manual", 20)]
    state = {r["column_name"]: r for r in provenance(ctx, "P", "load", {"id": 1}).records}
    assert state["name"]["writer"] == "alice" and state["amount"]["writer"] == "etl-craft"
    write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": "Correction"}, "second fix"))
    assert current(w)[0]["name"] == "Correction"
    unlock_values(ctx, "P", "load", ValueUnlock({"id": 1}, "return to source", ("name",)))
    w.run("load", **params)
    assert current(w)[0]["name"] == "Source"
    state = {r["column_name"]: r for r in provenance(ctx, "P", "load", {"id": 1}).records}
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
    write_values(ctx, "P", "load", ValueEdit({"id": 2}, {"name": "Human", "amount": 30}, "insert"))
    state = provenance(ctx, "P", "load", {"id": 2})
    assert any(r["column_name"] == "*" and r["locked"] for r in state.records)
    delete = {
        "SQL_ACTION": "DELETE_ROWS",
        "TARGET_OBJECT": "protected",
        "SOURCE_SQL": "SELECT 2 AS id",
        "MERGE_KEY": "id",
    }
    assert w.run("delete", **delete).delete_count == 0
    assert w.run("delete", **{**delete, "HARD_DELETE": "true"}).delete_count == 0
    incoming = {**params, "SOURCE_SQL": params["SOURCE_SQL"].replace("1 AS id", "2 AS id")}
    assert w.run("load", **incoming).update_count == 0
    assert current(w)[1]["name"] == "Human"
    unlock_values(ctx, "P", "load", ValueUnlock({"id": 2}, "release insert"))
    assert w.run("delete", **delete).delete_count == 1
    w.run("load", **incoming)
    assert current(w)[1]["name"] == "Source"
    robot = replace(ctx, actor=Actor("erp", ActorKind.SERVICE))
    write_values(robot, "P", "load", ValueEdit({"id": 2}, {"name": "ERP"}, "integration"))
    assert not any(r["locked"] for r in provenance(ctx, "P", "load", {"id": 2}).records)
    w.run("load", **incoming)
    assert current(w)[1]["name"] == "Source"
    with pytest.raises(PermissionDeniedError, match="human admin"):
        unlock_values(robot, "P", "load", ValueUnlock({"id": 2}, "not permitted"))


@warehouses("duckdb", "postgres")
def test_roles_validation_and_unobserved_writes_fail_closed(sql_world):
    w = sql_world
    ctx, params = prepare(w, "SCD1_MERGE")
    for role in (ApiRole.VIEWER, ApiRole.OPERATOR, ApiRole.DEVELOPER):
        with pytest.raises(PermissionDeniedError, match="admin"):
            write_values(
                replace(ctx, role=role), "P", "load", ValueEdit({"id": 1}, {"name": "X"}, "fix")
            )
    with pytest.raises(UsageError, match="business columns"):
        write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"updated_by": "forged"}, "fix"))
    with pytest.raises(UsageError, match="non-NULL"):
        write_values(ctx, "P", "load", ValueEdit({"id": None}, {"name": "X"}, "fix"))
    assert not provenance(ctx, "P", "load", {"id": 1}).enabled
    write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": "Human"}, "fix"))
    w.execute(f"UPDATE {w.name('protected')} SET amount=99")
    with pytest.raises(HandlerError, match="unobserved warehouse writes"):
        w.run("load", **params)
    with pytest.raises(HandlerError, match="unobserved warehouse writes"):
        unlock_values(ctx, "P", "load", ValueUnlock({"id": 1}, "fix"))
    assert current(w)[0]["name"] == "Human"


@warehouses("duckdb", "postgres")
def test_protected_replacement_and_atomic_evidence_rollback(sql_world, monkeypatch):
    w = sql_world
    ctx, params = prepare(w, "SCD2_MERGE")
    write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": "Human"}, "fix"))
    before = w.rows(f"SELECT * FROM {w.name('protected')} ORDER BY row_id")
    for action in ("CREATE_TABLE", "OVERWRITE_TABLE", "APPEND_TABLE", "DROP_TABLE"):
        with pytest.raises(HandlerError, match="cannot replace protected"):
            w.run(
                "replace_" + action,
                SQL_ACTION=action,
                TARGET_OBJECT="protected",
                **({"SOURCE_SQL": params["SOURCE_SQL"]} if action != "DROP_TABLE" else {}),
            )

    def fail(point):
        if point == "sql.protection.before_evidence":
            raise HandlerError("injected evidence failure")

    monkeypatch.setattr("etl_craft.handlers.sql.protection.fault_point", fail)
    with pytest.raises(HandlerError, match="injected evidence failure"):
        write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": "Lost"}, "fix"))
    assert w.rows(f"SELECT * FROM {w.name('protected')} ORDER BY row_id") == before
    assert any(r["writer"] == "alice" for r in provenance(ctx, "P", "load", {"id": 1}).records)


@warehouses("trino_iceberg", "duckdb_iceberg")
def test_nonatomic_warehouses_refuse_protection(sql_world):
    w = sql_world
    ctx, _params = prepare(w, "SCD1_MERGE")
    with pytest.raises(HandlerError, match=r"protected writes unsupported.*atomic"):
        write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": "Human"}, "fix"))
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
        "load", **{**params, "SOURCE_SQL": params["SOURCE_SQL"].replace("10 AS", "20 AS")}
    )
    two_at_once(
        lambda: write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": "Human"}, "fix")),
        lambda: sql.run(context, w.engine_db),
        at="etl_craft.engine.locks.target",
    )
    assert [(r["name"], r["amount"]) for r in current(w)] == [("Human", 20)]
    two_at_once(
        lambda: unlock_values(ctx, "P", "load", ValueUnlock({"id": 1}, "release")),
        lambda: sql.run(context, w.engine_db),
        at="etl_craft.engine.locks.target",
    )
    assert not any(r["locked"] for r in provenance(ctx, "P", "load", {"id": 1}).records)
    sql.run(context, w.engine_db)
    assert current(w)[0]["name"] == "Source"


@warehouses("duckdb", "postgres")
def test_service_writes_respect_human_values_and_row_locks(sql_world):
    w = sql_world
    ctx, _params = prepare(w, "SCD2_MERGE")
    write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": "Human"}, "fix"))
    robot = replace(ctx, actor=Actor("erp", ActorKind.SERVICE))
    write_values(robot, "P", "load", ValueEdit({"id": 1}, {"name": "ERP", "amount": 42}, "refresh"))
    assert [(r["name"], r["amount"]) for r in current(w)] == [("Human", 42)]
    write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": "Human"}, "hold row", True))
    before = current(w)
    write_values(robot, "P", "load", ValueEdit({"id": 1}, {"amount": 99}, "refresh"))
    assert current(w) == before


@warehouses("duckdb", "postgres")
@pytest.mark.parametrize("engine_kind", ["sqlite", "postgresql"])
def test_values_api_uses_human_credentials_and_shared_role_policy(sql_world, request, engine_kind):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.core.enums import AccountKind, YesNo
    from etl_craft.services.operations import accounts, sessions

    w = sql_world
    if engine_kind == "postgresql":
        use_postgres_engine(w, request)
    ctx, params = prepare(w, "SCD2_MERGE")
    account = accounts.Account(0, "alice", AccountKind.USER, ApiRole.ADMIN, None, None, YesNo.YES)
    accounts.add_account(ctx, account, "correct horse battery")
    token = sessions.sign_in(ctx, "alice", "correct horse battery").access_token
    headers = {"Authorization": "Bearer " + token}
    path = "/api/v1/pipelines/P/tasks/load/values"
    with TestClient(create_app(ctx)) as client:
        body = {"key": {"id": 1}, "values": {"name": "O'Connor"}, "reason": "verified spelling"}
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
        assert client.post(path, json={**body, "unknown": True}, headers=headers).status_code == 422
        accounts.set_role(ctx, "alice", ApiRole.OPERATOR)
        assert client.post(path, json=body, headers=headers).status_code == 403
        denied = client.post(
            path + "/unlock", json={"key": {"id": 1}, "reason": "release"}, headers=headers
        )
        assert denied.status_code == 403
        assert (
            client.post(path + "/provenance", json={"key": {"id": 1}}, headers=headers).status_code
            == 200
        )
        accounts.set_role(ctx, "alice", ApiRole.ADMIN)
        response = client.post(
            path + "/unlock", json={"key": {"id": 1}, "reason": "release"}, headers=headers
        )
        assert response.status_code == 200, response.text
        w.run("load", **params)
        assert current(w)[0]["name"] == "Source"


@warehouses("duckdb", "postgres")
def test_noncompare_edits_null_values_and_historical_tampering(sql_world):
    w = sql_world
    ctx, params = prepare(w, "SCD2_MERGE", compare="amount")
    write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": None}, "remove wrong name"))
    assert current(w)[0]["name"] is None
    incoming = {**params, "SOURCE_SQL": params["SOURCE_SQL"].replace("10 AS", "20 AS")}
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
    write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": "Human"}, "fix"))
    journal = next(t for t in w.tables() if t.startswith("etl_protection_"))
    with pytest.raises(HandlerError, match="reserved SCD protection evidence"):
        w.run(
            "destroy_evidence",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=journal,
            SOURCE_SQL="SELECT 1 AS id",
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
        write_values(ctx, "P", "load", ValueEdit({"id": 1}, {"name": "Human"}, "fix"))
    assert current(w)[0]["name"] == "Source"
