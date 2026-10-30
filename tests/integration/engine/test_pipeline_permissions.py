"""Group policy through action admission, HTTP, CLI and durable configuration on both DBs."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.api.app import create_app
from etl_craft.cli import main
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole, Transport
from etl_craft.core.errors import ExitCode, PermissionDeniedError, ResourceNotFoundError
from etl_craft.engine import transitions
from etl_craft.services.oidc import IdentityClaims
from etl_craft.services.operations import OperationContext, sessions
from etl_craft.services.operations.identity import authenticated_context
from fixtures.actions import act
from fixtures.metadata import add_dependency, add_pipeline, add_task, task_run

OWNER = Actor("owner", ActorKind.USER)


@pytest.fixture
def scene(engine_db):
    from etl_craft.engine.migrations import mark_packaged_migrations_applied

    with engine_db.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
    ctx = OperationContext(engine_db.engine, engine_db.config, OWNER, role=ApiRole.ADMIN)
    credential = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    reader = authenticated_context(ctx, credential.metadata)
    group = act(ctx, ActionName.GROUP_ADD, name="sales", description="Sales operators").groups[0]
    with acting_as(OWNER), ctx.engine.begin() as conn:
        open_id = add_pipeline(conn, "OPEN")
        private_id = add_pipeline(conn, "PRIVATE")
        conn.execute(
            text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS=:tags WHERE PIPELINE_ID=:id"),
            {"tags": json.dumps({"TAGS": ["sales"]}), "id": private_id},
        )
        task = add_task(conn, private_id, "secret")
        run_id = transitions.create_run(conn, private_id, OWNER)
        task_run(conn, task, run_id)
    return ctx, reader, credential.token, group.group_id, open_id, private_id, run_id, task


def grant(ctx, *, tag=False):
    with acting_as(OWNER), ctx.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (GROUP_ID, PIPELINE_CODE, TAG, PERMISSION) "
                "VALUES ((SELECT GROUP_ID FROM CFG_GROUPS WHERE GROUP_NAME='sales'), "
                ":code, :tag, 'CAN_RUN')"
            ),
            {"code": None if tag else "PRIVATE", "tag": "sales" if tag else None},
        )


@pytest.mark.parametrize("tag", [False, True])
def test_direct_and_tag_grants_filter_lists_and_elevate_only_members(scene, tag):
    ctx, reader, token, _, _, _, run_id, _ = scene
    grant(ctx, tag=tag)
    assert {p.pipeline_code for p in act(reader, ActionName.PIPELINES).pipelines} == {"OPEN"}
    with pytest.raises(ResourceNotFoundError, match=r"PRIVATE.*missing or not permitted"):
        act(reader, ActionName.RUN_DETAIL, run_id=run_id)
    assert act(reader, ActionName.HISTORY, all=True).entries == ()
    assert act(reader, ActionName.STATUS).runs == ()
    api = TestClient(create_app(ctx))
    headers = {"Authorization": "Bearer " + token}
    assert api.get(f"/api/v1/projects/default/runs/{run_id}", headers=headers).status_code == 404
    assert [
        p["pipeline_code"]
        for p in api.get("/api/v1/projects/default/pipelines", headers=headers).json()["pipelines"]
    ] == ["OPEN"]
    act(ctx, ActionName.GROUP_ADD_MEMBER, name="sales", account="reader")
    assert act(reader, ActionName.RUN_DETAIL, run_id=run_id).pipeline_run_id == run_id
    assert api.get(f"/api/v1/projects/default/runs/{run_id}", headers=headers).status_code == 200
    # A viewer can operate their own restricted pipeline, not deployment-wide controls.
    act(reader, ActionName.CONTROL, pipeline_code="PRIVATE", action="PAUSE", reason="test")
    with pytest.raises(PermissionDeniedError, match="admin role is required"):
        act(reader, ActionName.GROUP_DISABLE, name="sales")
    act(ctx, ActionName.GROUP_REMOVE_MEMBER, name="sales", account="reader")
    with pytest.raises(ResourceNotFoundError, match=r"PRIVATE.*missing or not permitted"):
        act(reader, ActionName.PIPELINE, pipeline_code="PRIVATE")


def test_disabled_groups_keep_pipelines_restricted_and_changes_are_audited(scene):
    ctx, reader, _, _, _, _, _, _ = scene
    grant(ctx)
    act(ctx, ActionName.GROUP_ADD_MEMBER, name="sales", account="reader")
    act(ctx, ActionName.GROUP_DISABLE, name="sales")
    with pytest.raises(ResourceNotFoundError, match=r"PRIVATE.*missing or not permitted"):
        act(reader, ActionName.PIPELINE, pipeline_code="PRIVATE")
    with ctx.engine.connect() as conn:
        changed = conn.execute(
            text("SELECT ACTOR AS actor FROM AUD_CFG_GROUPS_HISTORY WHERE GROUP_NAME='sales'")
        ).all()
        assert len(changed) == 2 and all(row.actor == "owner" for row in changed)
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS n FROM AUD_CFG_PIPELINE_GRANTS_HISTORY "
                    "WHERE PIPELINE_CODE IS NOT NULL OR TAG IS NOT NULL"
                )
            ).scalar_one()
            == 1
        )
    with (
        pytest.raises(DBAPIError, match="ACTIVE_FLAG=N"),
        acting_as(OWNER),
        ctx.engine.begin() as conn,
    ):
        conn.execute(text("DELETE FROM CFG_GROUP_MEMBERS"))


def test_cli_uses_the_same_grant_and_admin_policy(metadata_project, monkeypatch, capsys):
    project = metadata_project
    ctx = OperationContext(project.engine, project.config, OWNER, role=ApiRole.ADMIN)
    token = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    act(ctx, ActionName.GROUP_ADD, name="sales")
    with acting_as(OWNER), project.engine.begin() as conn:
        add_pipeline(conn, "PRIVATE")
    grant(ctx)
    monkeypatch.chdir(project.root)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token.token)
    assert main(["pipelines", "--format", "json"]) == ExitCode.SUCCESS
    assert json.loads(capsys.readouterr().out)["pipelines"] == []
    assert main(["group", "list"]) == ExitCode.PERMISSION_DENIED
    assert "admin" in capsys.readouterr().err


def test_sso_ids_survive_rename_refresh_and_logout_but_current_restrictions_apply(scene):
    ctx, _, _, group_id, _, _, _, _ = scene
    act(ctx, ActionName.ACCOUNT_ADD, name="sso", role=ApiRole.VIEWER, oidc_subject="subject")
    grant(ctx)
    session = sessions.sign_in_subject(
        replace(ctx, transport=Transport.HTTP), IdentityClaims("subject", ("sales", "unknown"))
    )
    with acting_as(OWNER), ctx.engine.begin() as conn:
        # The grant's group identity survives a rename too.
        conn.execute(
            text("UPDATE CFG_GROUPS SET GROUP_NAME='renamed' WHERE GROUP_ID=:id"), {"id": group_id}
        )
    refresh_end = datetime.now(UTC) + timedelta(minutes=5)
    with acting_as(OWNER), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_API_TOKENS SET EXPIRES_AT=:expires WHERE TOKEN_KIND='REFRESH'"),
            {"expires": refresh_end},
        )
    replacement = next(
        g for g in act(ctx, ActionName.GROUP_ADD, name="sales").groups if g.name == "sales"
    )
    assert replacement.group_id != group_id
    with acting_as(OWNER), ctx.engine.begin() as conn:
        add_pipeline(conn, "OTHER")
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (GROUP_ID, PIPELINE_CODE, PERMISSION) "
                "VALUES (:id, 'OTHER', 'CAN_RUN')"
            ),
            {"id": replacement.group_id},
        )
    refreshed = act(ctx, ActionName.REFRESH, token=session.refresh_token)
    assert refreshed.metadata.expires_at == refresh_end
    caller = authenticated_context(ctx, refreshed.metadata)
    assert act(caller, ActionName.PIPELINE, pipeline_code="PRIVATE").pipeline_code == "PRIVATE"
    with pytest.raises(ResourceNotFoundError, match=r"OTHER.*missing or not permitted"):
        act(caller, ActionName.PIPELINE, pipeline_code="OTHER")
    with ctx.engine.connect() as conn:
        ids = conn.execute(
            text("SELECT SSO_GROUP_IDS AS ids FROM CFG_API_TOKENS WHERE TOKEN_ID=:id"),
            {"id": refreshed.metadata.token_id},
        ).scalar_one()
        assert json.loads(ids) == [group_id]
        audit = conn.execute(
            text("SELECT ARGUMENTS AS arguments FROM AUD_ACTIONS WHERE COMMAND='login'")
        ).all()
        assert any("unknown" in str(row.arguments) for row in audit)
    act(ctx, ActionName.GROUP_DISABLE, name="renamed")
    with pytest.raises(ResourceNotFoundError, match=r"PRIVATE.*missing or not permitted"):
        act(caller, ActionName.PIPELINE, pipeline_code="PRIVATE")


def test_dependency_views_keep_only_the_restricted_pipeline_name(scene):
    ctx, reader, _, _, open_id, private_id, _, upstream = scene
    grant(ctx)
    with acting_as(OWNER), ctx.engine.begin() as conn:
        downstream = add_task(conn, open_id, "down")
        add_dependency(conn, open_id, downstream, upstream, upstream_pipeline=private_id)
        run = transitions.create_run(conn, open_id, OWNER)
    graph = act(reader, ActionName.GRAPH, pipeline_code="OPEN")
    assert graph.depends_on["down"] == (("PRIVATE", "SUCCESS"),)
    explanation = act(
        reader, ActionName.EXPLAIN, pipeline_code="OPEN", task_code="down", run_id=run
    )
    dependency = explanation.dependencies[0]
    assert dependency.upstream == "PRIVATE"
    assert dependency.status is None
    assert dependency.selected_pipeline_run_id is None
    assert dependency.selected_task_run_id is None
    assert "secret" not in dependency.reason


def test_ids_cannot_bypass_restrictions_for_attempts_logs_or_controls(scene):
    ctx, reader, _, _, _, _, run, task = scene
    grant(ctx)
    with acting_as(OWNER), ctx.engine.begin() as conn:
        binding = transitions.find_or_create_task_run(conn, task, run)
        attempt = transitions.queue_attempt(conn, binding.task_run_id, OWNER)
    control = act(
        ctx,
        ActionName.CONTROL,
        pipeline_code="PRIVATE",
        run_id=run,
        action="HOLD",
        reason="test restriction",
    )
    for action, fields in (
        (ActionName.ATTEMPT, {"attempt_id": attempt}),
        (ActionName.LOGS, {"attempt_id": attempt}),
        (ActionName.CONTROL_REQUEST, {"control_id": control.control_id}),
    ):
        with pytest.raises(ResourceNotFoundError, match=r"PRIVATE.*missing or not permitted"):
            act(reader, action, **fields)


def test_run_admission_persists_sso_ids_after_session_revocation(metadata_project):
    project = metadata_project
    project.load_examples()
    ctx = OperationContext(project.engine, project.config, OWNER, role=ApiRole.ADMIN)
    act(ctx, ActionName.ACCOUNT_ADD, name="sso", role=ApiRole.VIEWER, oidc_subject="subject")
    group = act(ctx, ActionName.GROUP_ADD, name="sales").groups[0]
    code = act(ctx, ActionName.PIPELINES).pipelines[0].pipeline_code
    with acting_as(OWNER), ctx.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINE_GRANTS (GROUP_ID, PIPELINE_CODE, PERMISSION) "
                "VALUES (:group, :code, 'CAN_RUN')"
            ),
            {"group": group.group_id, "code": code},
        )
    session = sessions.sign_in_subject(ctx, IdentityClaims("subject", ("sales",)))
    caller = authenticated_context(ctx, session.metadata)
    result = act(caller, ActionName.RUN, pipeline_code=code, init_only=True)
    act(ctx, ActionName.LOGOUT, token=session.access_token)
    with ctx.engine.connect() as conn:
        ids = conn.execute(
            text(
                "SELECT SSO_GROUP_IDS AS ids FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_RUN_ID=:id"
            ),
            {"id": result.run.pipeline_run_id},
        ).scalar_one()
        assert json.loads(ids) == [group.group_id]


def test_unproven_identity_cannot_use_an_existing_group_membership(scene):
    ctx, reader, _, _, _, _, _, _ = scene
    grant(ctx)
    act(ctx, ActionName.GROUP_ADD_MEMBER, name="sales", account="reader")
    with pytest.raises(ResourceNotFoundError, match=r"project.*default.*missing or not permitted"):
        act(replace(reader, role=None), ActionName.PIPELINE, pipeline_code="PRIVATE")


def test_shared_catalog_gap_reasons_do_not_name_restricted_writer_tasks(scene, monkeypatch):
    from types import SimpleNamespace

    from etl_craft.core.enums import Handler, SqlAction
    from etl_craft.services.catalog.collect import Catalog, Column, TableAsset
    from etl_craft.services.operations import to_json

    ctx, reader, _, _, _, _, _, _ = scene
    grant(ctx)
    public = SimpleNamespace(
        row=SimpleNamespace(pipeline_code="OPEN", handler=Handler.PYTHON),
        params={},
        lineage_error=None,
    )
    private = SimpleNamespace(
        row=SimpleNamespace(pipeline_code="PRIVATE", handler=Handler.SQL),
        params={"SQL_ACTION": SqlAction.CREATE_TABLE},
        lineage_error="private SQL details",
    )
    catalog = Catalog(
        datetime.now(UTC),
        {code: SimpleNamespace(row=SimpleNamespace(tags=())) for code in ("OPEN", "PRIVATE")},
        {"OPEN.down": public, "PRIVATE.secret": private},
        {
            "sales.shared": TableAsset(
                "sales.shared",
                columns={"id": Column("id", declared=False)},
                writers=["PRIVATE.secret"],
                readers=["OPEN.down"],
                in_warehouse=True,
                warehouse_checked=True,
            )
        },
        {},
        {},
        [],
        [],
        compared_at=datetime.now(UTC),
    )
    monkeypatch.setattr(
        "etl_craft.services.operations.catalog.collect_catalog", lambda *args, **kwargs: catalog
    )
    result = act(reader, ActionName.CATALOG_GAPS)
    assert result.records
    document = json.dumps(to_json(result))
    assert "PRIVATE.secret" not in document and "private SQL details" not in document
    assert "OPEN.down" in document


@pytest.mark.parametrize("column", [None, "id"])
def test_lineage_paths_hide_restricted_task_labels_and_transformations(scene, monkeypatch, column):
    from etl_craft.services.lineage import Edge, TaskLineage
    from etl_craft.services.operations import to_json

    ctx, reader, _, _, _, _, _, _ = scene
    grant(ctx)
    lineages = [
        TaskLineage(
            "PRIVATE.secret",
            "sales.shared",
            [
                Edge(
                    "sales.shared",
                    "id",
                    "raw.private",
                    "id",
                    "private transformation",
                    "PRIVATE.secret",
                )
            ],
        ),
        TaskLineage(
            "OPEN.down",
            "sales.public",
            [Edge("sales.public", "id", "sales.shared", "id", "id", "OPEN.down")],
        ),
    ]
    monkeypatch.setattr(
        "etl_craft.services.operations.diagnostics.lineage.collect",
        lambda *args, **kwargs: lineages,
    )
    result = act(reader, ActionName.LINEAGE, table="sales.public", column=column)
    document = json.dumps(to_json(result))
    assert "PRIVATE.secret" not in document and "private transformation" not in document
    assert "PRIVATE" in document and "OPEN.down" in document
