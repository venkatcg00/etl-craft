"""One role policy across real CLI, HTTP and Python service boundaries."""

import os
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

from etl_craft.api.app import create_app
from etl_craft.cli import main
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.core.enums import ActionName, ApiRole, Audit, ControlAction
from etl_craft.services.operations import OperationContext, accounts, tokens
from etl_craft.services.operations.actions import ACTIONS, admitted
from etl_craft.services.operations.config_files import ConfigDirectory
from etl_craft.services.operations.documents import DocsInput, PublishInput
from etl_craft.services.operations.maintenance import RehashInput
from etl_craft.services.operations.requests import (
    LineageRefreshInput,
    NoInput,
    RunRequest,
    TagFilter,
)
from fixtures.actions import act


@pytest.fixture
def project(cli_project, monkeypatch, cli_logging):
    p = cli_project
    monkeypatch.setenv("ETL_CRAFT_CONFIG", str(p.config.config_path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(p.config.project_dir / "person"))
    bootstrap = OperationContext(p.engine, p.config, SYSTEM_ACTOR)
    identity = tokens.authenticate(bootstrap, os.environ["ETL_CRAFT_TOKEN"])
    ctx = replace(bootstrap, actor=identity.actor, role=identity.role)
    credentials = {
        role: act(
            ctx,
            ActionName.TOKEN_CREATE,
            name="policy-" + role,
            role=role,
            pool="default" if role == ApiRole.AGENT else None,
        )
        for role in ApiRole
    }
    return p, ctx, credentials


def actions(engine):
    with engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT ACTOR AS actor, COMMAND AS command, OUTCOME AS outcome, "
                "EXIT_CODE AS exit_code, ARGUMENTS AS arguments "
                "FROM AUD_ACTIONS ORDER BY ACTION_ID"
            )
        ).all()


REQUESTS = {
    ActionName.PIPELINES: TagFilter(),
    ActionName.RUN: RunRequest(pipeline_code="P"),
    ActionName.CONFIG_APPLY: ConfigDirectory(),
    ActionName.ACCOUNT_LIST: NoInput(),
    ActionName.ACCOUNT_RESET_PASSWORD: accounts.AccountPassword(name="x", password="do-not-record"),
    ActionName.CLONE: NoInput(),
    ActionName.REHASH: RehashInput(target="s.t"),
    ActionName.CONTROLLER: NoInput(),
    ActionName.PUBLISH_DOCS: PublishInput(),
    ActionName.DOCS_VERSION: NoInput(),
    ActionName.GENERATE_DOCS: DocsInput(),
    ActionName.LINEAGE_REFRESH: LineageRefreshInput(),
}


@pytest.mark.parametrize("role", tuple(ApiRole))
@pytest.mark.parametrize(
    "name,required",
    [
        (ActionName.PIPELINES, ApiRole.VIEWER),
        (ActionName.RUN, ApiRole.OPERATOR),
        (ActionName.CONFIG_APPLY, ApiRole.ADMIN),
        (ActionName.ACCOUNT_LIST, ApiRole.ADMIN),
        (ActionName.ACCOUNT_RESET_PASSWORD, ApiRole.ADMIN),
        (ActionName.CLONE, ApiRole.ADMIN),
        (ActionName.REHASH, ApiRole.ADMIN),
        (ActionName.CONTROLLER, ApiRole.ADMIN),
        (ActionName.PUBLISH_DOCS, ApiRole.ADMIN),
        (ActionName.DOCS_VERSION, ApiRole.ADMIN),
        (ActionName.GENERATE_DOCS, ApiRole.ADMIN),
        (ActionName.LINEAGE_REFRESH, ApiRole.DEVELOPER),
    ],
)
def test_role_inheritance_and_refusal_records(project, role, name, required):
    from etl_craft.core.errors import PermissionDeniedError

    p, ctx, creds = project
    caller = replace(ctx, actor=creds[role].metadata.actor, role=role)
    before = len(actions(p.engine))
    allowed_roles = {
        ApiRole.VIEWER: {ApiRole.VIEWER, ApiRole.OPERATOR, ApiRole.DEVELOPER, ApiRole.ADMIN},
        ApiRole.OPERATOR: {ApiRole.OPERATOR, ApiRole.DEVELOPER, ApiRole.ADMIN},
        ApiRole.DEVELOPER: {ApiRole.DEVELOPER, ApiRole.ADMIN},
        ApiRole.ADMIN: {ApiRole.ADMIN},
    }
    allowed = role in allowed_roles[required]
    audited = ACTIONS[name].audit == Audit.REQUEST
    if allowed:
        with admitted(caller, name, REQUESTS[name]):
            pass
        assert len(actions(p.engine)) == before + int(audited)
    else:
        with pytest.raises(PermissionDeniedError) as error, admitted(caller, name, REQUESTS[name]):
            pass
        message = str(error.value)
        assert caller.actor.name in message and role in message
        assert (
            "CAN_MANAGE"
            if name in {ActionName.CONFIG_APPLY, ActionName.GENERATE_DOCS} and role != ApiRole.AGENT
            else ACTIONS[name].role
        ) in message
        assert "administrator" in message
        record = actions(p.engine)[-1]
        assert record.actor == caller.actor.name
        assert record.command == name
        assert record.outcome == "REFUSED"
        assert record.exit_code == error.value.exit_code
        assert "do-not-record" not in str(record.arguments)
        assert len(actions(p.engine)) == before + 1


@pytest.mark.parametrize("role", tuple(ApiRole))
def test_cli_and_http_share_the_service_run_policy(project, role, monkeypatch, capsys):
    p, ctx, creds = project
    token = creds[role].token
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token)
    args = ["run", "--pipeline-code", "P", "--init-only", "--run-key", "one"]
    code = main(args)
    output = capsys.readouterr()
    response = TestClient(create_app(ctx)).post(
        "/api/v1/projects/default/runs",
        json={"pipeline_code": "P", "init_only": True},
        headers={"Authorization": "Bearer " + token},
    )
    # A viewer may not run a pipeline, and an agent token cannot invoke a person's action.
    if role in {ApiRole.VIEWER, ApiRole.AGENT}:
        assert code == 26, output
        assert response.status_code == 403, response.text
        assert response.json()["exit_code"] == code
        refused = [a for a in actions(p.engine) if a.outcome == "REFUSED"]
        assert len(refused) == 2
        assert all(a.actor == f"policy-{role}" and a.command == "run" for a in refused)
        with p.engine.connect() as conn:
            assert (
                conn.execute(
                    text("SELECT COUNT(*) AS count FROM AUD_PIPELINES_RUN_LOG")
                ).scalar_one()
                == 0
            )
    else:
        assert code == 0, output
        assert response.status_code == 200, response.text


def test_changing_the_shared_policy_changes_every_transport(project, monkeypatch, capsys):
    from etl_craft.core.errors import PermissionDeniedError

    p, ctx, creds = project
    credential = creds[ApiRole.OPERATOR]
    caller = replace(ctx, actor=credential.metadata.actor, role=ApiRole.OPERATOR)
    control = ACTIONS[ActionName.CONTROL]
    monkeypatch.setitem(ACTIONS, ActionName.CONTROL, replace(control, role=ApiRole.ADMIN))
    with pytest.raises(PermissionDeniedError):
        act(
            caller,
            ActionName.CONTROL,
            pipeline_code="P",
            reason="policy test",
            action=ControlAction.PAUSE,
        )
    monkeypatch.setenv("ETL_CRAFT_TOKEN", credential.token)
    assert main(["pause", "--pipeline-code", "P", "--reason", "policy test"]) == 26
    message = capsys.readouterr().err
    assert "pipeline 'P' needs admin role" in message
    assert "effective role=operator" in message
    response = TestClient(create_app(ctx)).post(
        "/api/v1/projects/default/controls",
        json={"pipeline_code": "P", "action": "PAUSE", "reason": "policy test"},
        headers={"Authorization": "Bearer " + credential.token},
    )
    assert response.status_code == 403, response.text
    refused = [a for a in actions(p.engine) if a.outcome == "REFUSED"]
    assert len(refused) == 3
    assert all(a.actor == credential.metadata.name and a.command == "control" for a in refused)


def test_services_reject_unproven_roles_and_metadata_writes_before_io(project):
    from etl_craft.core.errors import PermissionDeniedError, ResourceNotFoundError

    _, ctx, creds = project
    with pytest.raises(ResourceNotFoundError, match="missing or not permitted"):
        act(replace(ctx, role=None), ActionName.PIPELINES)
    for role in (ApiRole.VIEWER, ApiRole.OPERATOR):
        caller = replace(ctx, role=role, actor=creds[role].metadata.actor)
        with pytest.raises(PermissionDeniedError, match="CAN_MANAGE"):
            act(caller, ActionName.CONFIG_APPLY, config_dir=Path("does-not-exist"))


def test_developer_can_apply_project_migrations_but_not_engine_migrations(
    project, tmp_path, monkeypatch, capsys
):
    p, _, creds = project
    act(
        project[1],
        ActionName.ACCESS_GRANT,
        project="default",
        account="policy-developer",
        permission="CAN_MANAGE",
        reason="Manage project migrations",
    )
    directory = tmp_path / "migrations"
    directory.mkdir()
    (directory / "0001_project.sql").write_text("CREATE TABLE PROJECT_NOTE (ID INTEGER)\n")
    monkeypatch.setenv("ETL_CRAFT_TOKEN", creds[ApiRole.DEVELOPER].token)
    assert main(["migrate", "--migrations-dir", str(directory)]) == 0, capsys.readouterr()
    with p.engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM SCHEMA_MIGRATIONS WHERE SOURCE='ENGINE' "
                "AND VERSION=(SELECT MAX(VERSION) FROM SCHEMA_MIGRATIONS WHERE SOURCE='ENGINE')"
            )
        )
    assert main(["migrate", "--migrations-dir", str(directory)]) == 26
    assert "admin role is required" in capsys.readouterr().err
    assert actions(p.engine)[-1].outcome == "REFUSED"
