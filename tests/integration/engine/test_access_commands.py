"""Permission delegation and administrator metadata through their attributed actions."""

import json
from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.enums_access import Permission
from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError, UsageError
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.permissions import pipeline_access
from fixtures.actions import act
from fixtures.metadata import add_pipeline
from fixtures.races import two_at_once

OWNER = Actor("owner", ActorKind.USER)


@pytest.fixture
def access_scene(engine_db):
    ctx = OperationContext(engine_db.engine, engine_db.config, OWNER, role=ApiRole.ADMIN)
    act(
        ctx, ActionName.ACCOUNT_ADD, name="alice", role=ApiRole.VIEWER, oidc_subject="alice-subject"
    )
    act(ctx, ActionName.GROUP_ADD, name="team")
    act(ctx, ActionName.GROUP_ADD_MEMBER, name="team", account="alice")
    with acting_as(OWNER), ctx.engine.begin() as conn:
        for code in ("P", "OTHER"):
            identity = add_pipeline(conn, code)
            conn.execute(
                text("UPDATE CFG_PIPELINES SET PIPELINE_PARAMETERS=:tags WHERE PIPELINE_ID=:id"),
                {
                    "tags": json.dumps({"TAGS": ["team"] if code == "P" else ["other"]}),
                    "id": identity,
                },
            )
    return ctx, replace(ctx, actor=Actor("alice", ActorKind.USER), role=ApiRole.VIEWER)


def change(ctx, **values):
    return act(ctx, ActionName.ACCESS_GRANT, project="default", reason="test", **values)


@pytest.mark.parametrize("principal", ["account", "group", "role"])
@pytest.mark.parametrize("permission", list(Permission))
@pytest.mark.parametrize("target", ["pipeline", "tag"])
def test_principals_levels_and_securables(access_scene, principal, permission, target):
    ctx, alice = access_scene
    identity = {
        principal: {"account": "alice", "group": "team", "role": "viewer"}[principal],
        target: "P" if target == "pipeline" else "team",
    }
    result = change(ctx, **identity, permission=permission)
    assert result.changes[0].grant.permission == permission
    with ctx.engine.connect() as conn:
        assert pipeline_access(conn, alice).levels["P"] == permission
    assert act(alice, ActionName.ACCESS_LIST, project="default").effective
    selected = act(alice, ActionName.ACCESS_LIST, project="default", tag="team")
    assert {code for code, _, _ in selected.effective} == {"P"}
    with ctx.engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT ACTOR AS actor,PERMISSION AS permission FROM "
                "AUD_CFG_PIPELINE_GRANTS_HISTORY "
                "WHERE PIPELINE_CODE IS NOT NULL OR TAG IS NOT NULL"
            )
        ).one()
        assert row.actor == "owner" and row.permission == permission


def test_delegation_never_escapes_managed_securable(access_scene):
    ctx, alice = access_scene
    change(ctx, account="alice", pipeline="P", permission=Permission.CAN_MANAGE)
    change(alice, group="team", pipeline="P", permission=Permission.CAN_RUN)
    for target in ({"pipeline": "OTHER"}, {"tag": "team"}, {"tag": "other"}):
        with pytest.raises(PermissionDeniedError) as caught:
            change(alice, role="viewer", permission=Permission.CAN_MANAGE, **target)
        for part in ("alice", "effective level", "CAN_MANAGE", "administrator"):
            assert part in str(caught.value)
    with pytest.raises(PermissionDeniedError):
        act(alice, ActionName.PROJECT_ADD, project="escape", reason="test")
    change(ctx, account="alice", tag="team", permission=Permission.CAN_MANAGE)
    change(alice, role="viewer", tag="team", permission=Permission.CAN_VIEW)
    act(
        alice,
        ActionName.ACCESS_REVOKE,
        project="default",
        reason="test",
        group="team",
        pipeline="P",
    )


def test_highest_level_and_immediate_revocation(access_scene):
    ctx, alice = access_scene
    change(ctx, group="team", tag="team", permission=Permission.CAN_VIEW)
    change(ctx, account="alice", pipeline="P", permission=Permission.CAN_MANAGE)
    with ctx.engine.connect() as conn:
        assert pipeline_access(conn, alice).levels["P"] == Permission.CAN_MANAGE
    act(
        ctx,
        ActionName.ACCESS_REVOKE,
        project="default",
        account="alice",
        pipeline="P",
        reason="test",
    )
    with pytest.raises(PermissionDeniedError):
        change(alice, role="viewer", pipeline="P", permission=Permission.CAN_RUN)
    with ctx.engine.connect() as conn:
        assert pipeline_access(conn, alice).levels["P"] == Permission.CAN_VIEW
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM CFG_PIPELINE_GRANTS WHERE (PIPELINE_CODE IS NOT "
                    "NULL OR TAG IS NOT NULL) AND ACTIVE_FLAG='N'"
                )
            ).scalar_one()
            == 1
        )


def test_bulk_validates_every_line_before_writes_and_dry_run(access_scene):
    ctx, alice = access_scene
    header = "PROJECT,GROUP_NAME,ROLE,PIPELINE_CODE,TAG,PERMISSION,ACTION\n"
    valid = "default,team,,P,,CAN_RUN,GRANT\n"
    result = change(ctx, content=header + valid, dry_run=True)
    assert result.changes[0].change == "added"
    assert not any(
        g.pipeline or g.tag for g in act(ctx, ActionName.ACCESS_LIST, project="default").grants
    )
    bad = (
        "default,missing,,P,,CAN_RUN,GRANT\n"
        "default,,invalid,P,,CAN_RUN,GRANT\n"
        "default,team,,unknown,,CAN_RUN,GRANT\n"
        "default,team,,,absent,CAN_RUN,GRANT\n"
        "default,team,,P,,bad,GRANT\n"
        "default,alice,,P,,CAN_RUN,GRANT\n"
        "missing,team,,P,,CAN_RUN,GRANT\n"
    )
    with pytest.raises(UsageError) as caught:
        change(ctx, content=header + valid + bad)
    for line in range(3, 10):
        assert f"line {line}" in str(caught.value)
    assert "access grant --account" in str(caught.value)
    assert not any(
        g.pipeline or g.tag for g in act(ctx, ActionName.ACCESS_LIST, project="default").grants
    )
    with pytest.raises(PermissionDeniedError):
        change(alice, content=header + valid, dry_run=True)
    change(ctx, content=header + valid)
    result = change(ctx, content=header + valid)
    assert result.changes[0].change == "unchanged"
    with pytest.raises(UsageError, match=r"ACCOUNT.*access grant --account"):
        change(ctx, content=header.replace("GROUP_NAME", "ACCOUNT") + valid)


@pytest.mark.parametrize("revoking", [False, True])
def test_competing_granters_publish_one_identity(access_scene, revoking):
    ctx, _ = access_scene
    change(ctx, group="team", pipeline="P", permission=Permission.CAN_VIEW)

    def first():
        return change(ctx, group="team", pipeline="P", permission=Permission.CAN_MANAGE)

    def second():
        if revoking:
            return act(
                ctx,
                ActionName.ACCESS_REVOKE,
                project="default",
                group="team",
                pipeline="P",
                reason="race",
            )
        return change(ctx, group="team", pipeline="P", permission=Permission.CAN_RUN)

    two_at_once(first, second, at="etl_craft.services.operations.access.inputs")
    with ctx.engine.connect() as conn:
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM CFG_PIPELINE_GRANTS WHERE PIPELINE_CODE IS NOT NULL "
                    "OR TAG IS NOT NULL"
                )
            ).scalar_one()
            == 1
        )
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) FROM AUD_CFG_PIPELINE_GRANTS_HISTORY WHERE PIPELINE_CODE "
                    "IS NOT NULL OR TAG IS NOT NULL"
                )
            ).scalar_one()
            == 3
        )


def test_admin_project_pool_validation_and_disable(access_scene):
    ctx, alice = access_scene
    act(ctx, ActionName.POOL_ADD, name="special", reason="test", description="Special")
    result = act(
        ctx, ActionName.PROJECT_ADD, project="sales", reason="test", default_pool="special"
    )
    assert result.projects[0].default_pool == "special"
    with pytest.raises(UsageError, match=r"special.*project sales.*active references"):
        act(ctx, ActionName.POOL_DISABLE, name="special", reason="test")
    act(ctx, ActionName.PROJECT_SET, project="sales", reason="test", name="Sales")
    result = act(ctx, ActionName.PROJECT_LIST, project="sales", reason="test")
    assert result.projects[0].default_pool == "special"
    for values in (
        {},
        {"timezone": "Not/AZone"},
        {"max_running_tasks": 0},
        {"default_pool": "absent"},
        {"name": ""},
    ):
        with pytest.raises(UsageError):
            act(ctx, ActionName.PROJECT_SET, project="sales", reason="test", **values)
    with pytest.raises(UsageError, match="explicitly"):
        act(ctx, ActionName.PROJECT_ADD, reason="test")
    with pytest.raises(PermissionDeniedError):
        act(alice, ActionName.POOL_LIST, reason="test")
    act(ctx, ActionName.PROJECT_DISABLE, project="sales", reason="test")
    act(ctx, ActionName.POOL_DISABLE, name="special", reason="test")
    result = act(ctx, ActionName.PROJECT_ADD, project="finance", reason="test")
    assert result.projects[0].default_pool == "default"


@pytest.mark.parametrize("principal", ["account", "group", "role"])
@pytest.mark.parametrize("level", list(Permission))
@pytest.mark.parametrize("target", ["pipeline", "tag"])
def test_granters_need_manage_on_the_target(access_scene, principal, level, target):
    ctx, alice = access_scene
    identity = {
        principal: {"account": "alice", "group": "team", "role": "viewer"}[principal],
        target: "P" if target == "pipeline" else "team",
    }
    change(ctx, **identity, permission=level)
    kwargs = {
        "account": "alice",
        target: "P" if target == "pipeline" else "team",
        "permission": Permission.CAN_MANAGE,
    }
    if level == Permission.CAN_MANAGE:
        change(alice, **kwargs)
    else:
        with pytest.raises(PermissionDeniedError, match="CAN_MANAGE"):
            change(alice, **kwargs)


def test_pool_disable_names_active_task_references(access_scene):
    from fixtures.metadata import add_task

    ctx, _ = access_scene
    act(ctx, ActionName.POOL_ADD, name="taskpool", reason="test")
    with acting_as(OWNER), ctx.engine.begin() as conn:
        pipeline = conn.execute(
            text("SELECT PIPELINE_ID FROM CFG_PIPELINES WHERE PIPELINE_CODE='P'")
        ).scalar_one()
        add_task(conn, pipeline, "load", "PYTHON", POOL="taskpool")
    with pytest.raises(UsageError) as caught:
        act(ctx, ActionName.POOL_DISABLE, name="taskpool", reason="test")
    for part in ("taskpool", "default/P.load", "active references", "change"):
        assert part in str(caught.value)


def test_cli_mutations_require_explicit_project_and_thin_api(metadata_project, monkeypatch, capsys):
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app
    from etl_craft.cli import main
    from etl_craft.core.errors import ExitCode

    ctx = OperationContext(
        metadata_project.engine, metadata_project.config, OWNER, role=ApiRole.ADMIN
    )
    credential = act(ctx, ActionName.TOKEN_CREATE, name="admin", role=ApiRole.ADMIN)
    viewer = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    monkeypatch.chdir(metadata_project.root)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", credential.token)
    assert main(["project", "add", "--reason", "test"]) == ExitCode.USAGE
    assert "--project" in capsys.readouterr().err
    config_path = metadata_project.root / "craft-connector.yml"
    config_path.write_text(config_path.read_text().replace("Project: default", "Project: sales"))
    assert main(["project", "add", "--project", "sales", "--reason", "test"]) == ExitCode.SUCCESS
    capsys.readouterr()
    assert main(["project", "set", "--project", "sales", "--reason", "test"]) == ExitCode.USAGE
    assert "--default-pool" in capsys.readouterr().err
    api = TestClient(create_app(ctx))
    reader = {"Authorization": "Bearer " + viewer.token}
    admin = {"Authorization": "Bearer " + credential.token}
    for path in ("/api/v1/projects", "/api/v1/pools"):
        expected = 200 if path.endswith("projects") else 403
        assert api.get(path, params={"reason": "test"}, headers=reader).status_code == expected
        assert api.get(path, params={"reason": "test"}, headers=admin).status_code == 200
    with acting_as(OWNER), ctx.engine.begin() as conn:
        add_pipeline(conn, "P")
    payload = {
        "project": "default",
        "account": "reader",
        "pipeline": "P",
        "permission": "CAN_MANAGE",
        "reason": "test",
    }
    assert api.post("/api/v1/access/grants", json=payload, headers=reader).status_code == 403
    assert api.post("/api/v1/access/grants", json=payload, headers=admin).status_code == 200
    assert (
        api.post(
            "/api/v1/access/grants", json={**payload, "permission": "CAN_RUN"}, headers=reader
        ).status_code
        == 200
    )
    assert (
        api.get("/api/v1/access/grants", params={"project": "default"}, headers=reader).status_code
        == 200
    )
    assert api.post("/api/v1/access/revocations", json=payload, headers=reader).status_code == 403


@pytest.mark.parametrize("principal", ["account", "role"])
def test_admitted_children_recheck_account_and_role_grants(access_scene, principal):
    from etl_craft.core.enums import TriggerKind
    from etl_craft.engine import transitions
    from etl_craft.services.operations.child_admission import check_child_start

    ctx, alice = access_scene
    identity = {principal: "alice" if principal == "account" else "viewer"}
    change(ctx, **identity, pipeline="P", permission=Permission.CAN_RUN)
    # A retained restriction remains when the caller's operating grant is revoked.
    change(ctx, group="team", pipeline="P", permission=Permission.CAN_VIEW)
    with acting_as(alice.actor), ctx.engine.begin() as conn:
        pipeline = conn.execute(
            text("SELECT PIPELINE_ID FROM CFG_PIPELINES WHERE PIPELINE_CODE='P'")
        ).scalar_one()
        run = transitions.create_run(
            conn, pipeline, alice.actor, trigger_kind=TriggerKind.PIPELINE_TASK
        )
        assert check_child_start(ctx.engine, ctx.config, conn, run) is None
    act(
        ctx,
        ActionName.ACCESS_REVOKE,
        project="default",
        pipeline="P",
        reason="Withdraw",
        **identity,
    )
    with ctx.engine.connect() as conn:
        refusal = check_child_start(ctx.engine, ctx.config, conn, run)
        assert refusal is not None
        for part in ("alice", "P", "operator", "permission"):
            assert part in refusal


def test_delegation_refusals_are_attributed_and_audited(access_scene):
    ctx, alice = access_scene
    with pytest.raises(PermissionDeniedError):
        change(alice, account="alice", pipeline="P", permission=Permission.CAN_MANAGE)
    with ctx.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ACTOR AS actor,OUTCOME AS outcome,EXIT_CODE AS exit_code "
                "FROM AUD_ACTIONS WHERE COMMAND='access grant'"
            )
        ).all()
        assert len(rows) == 1
        assert rows[0].actor == "alice" and rows[0].outcome == "REFUSED"
        assert rows[0].exit_code == PermissionDeniedError.exit_code


def test_permission_delegation_does_not_cross_projects(access_scene):
    ctx, alice = access_scene
    change(ctx, account="alice", pipeline="P", permission=Permission.CAN_MANAGE)
    act(ctx, ActionName.PROJECT_ADD, project="other", reason="test")
    with acting_as(OWNER), ctx.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PROJECT_ID,PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE) "
                "SELECT PROJECT_ID,'P','Other P','FULL' FROM CFG_PROJECTS WHERE "
                "PROJECT_CODE='other'"
            )
        )
    with pytest.raises(ResourceNotFoundError, match=r"project 'other' is missing or not permitted"):
        act(
            alice,
            ActionName.ACCESS_GRANT,
            project="other",
            account="alice",
            pipeline="P",
            permission=Permission.CAN_MANAGE,
            reason="No cross-project delegation",
        )
    assert act(ctx, ActionName.ACCESS_LIST, project="other").grants == ()


def test_admin_bulk_projects_come_from_submitted_rows(access_scene):
    ctx, _ = access_scene
    act(ctx, ActionName.PROJECT_ADD, project="other", reason="test")
    with acting_as(OWNER), ctx.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_PIPELINES (PROJECT_ID,PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE) "
                "SELECT PROJECT_ID,'P','Other P','FULL' FROM CFG_PROJECTS WHERE "
                "PROJECT_CODE='other'"
            )
        )
    content = (
        "PROJECT,GROUP_NAME,ROLE,PIPELINE_CODE,TAG,PERMISSION,ACTION\n"
        "default,team,,P,,CAN_VIEW,GRANT\nother,,viewer,P,,CAN_RUN,GRANT\n"
    )
    done = act(ctx, ActionName.ACCESS_GRANT, content=content, reason="Bulk onboarding")
    assert {d.grant.project for d in done.changes} == {"default", "other"}
    for project_code in ("default", "other"):
        assert (
            sum(
                g.pipeline == "P"
                for g in act(ctx, ActionName.ACCESS_LIST, project=project_code).grants
            )
            == 1
        )


def test_cli_bulk_reads_content_without_requiring_project_flag(
    metadata_project, monkeypatch, capsys
):
    from etl_craft.cli import main
    from etl_craft.core.errors import ExitCode

    ctx = OperationContext(
        metadata_project.engine, metadata_project.config, OWNER, role=ApiRole.ADMIN
    )
    credential = act(ctx, ActionName.TOKEN_CREATE, name="bulk-admin", role=ApiRole.ADMIN)
    act(ctx, ActionName.GROUP_ADD, name="team")
    with acting_as(OWNER), ctx.engine.begin() as conn:
        add_pipeline(conn, "P")
    path = metadata_project.root / "access.csv"
    path.write_text(
        "PROJECT,GROUP_NAME,ROLE,PIPELINE_CODE,TAG,PERMISSION,ACTION\n"
        "default,team,,P,,CAN_RUN,GRANT\n"
    )
    monkeypatch.chdir(metadata_project.root)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", credential.token)
    assert (
        main(["access", "grant", "--file", str(path), "--dry-run", "--reason", "Review"])
        == ExitCode.SUCCESS
    )
    assert "added" in capsys.readouterr().out
    assert not any(
        g.pipeline or g.tag for g in act(ctx, ActionName.ACCESS_LIST, project="default").grants
    )
    assert main(["access", "grant", "--file", str(path), "--reason", "Apply"]) == ExitCode.SUCCESS
    capsys.readouterr()
    assert (
        sum(g.pipeline == "P" for g in act(ctx, ActionName.ACCESS_LIST, project="default").grants)
        == 1
    )
