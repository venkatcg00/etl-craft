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
from etl_craft.core.enums import ApiRole
from etl_craft.services.operations import OperationContext, inspect, pipelines, tokens


@pytest.fixture
def project(cli_project, monkeypatch, cli_logging):
    p = cli_project
    monkeypatch.setenv("ETL_CRAFT_CONFIG", str(p.config.config_path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(p.config.project_dir / "person"))
    bootstrap = OperationContext(p.engine, p.config, SYSTEM_ACTOR)
    identity = tokens.authenticate(bootstrap, os.environ["ETL_CRAFT_TOKEN"])
    ctx = replace(bootstrap, actor=identity.actor, role=identity.role)
    credentials = {role: tokens.create_token(ctx, "policy-" + role, role) for role in ApiRole}
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


@pytest.mark.parametrize("role", tuple(ApiRole))
@pytest.mark.parametrize(
    "command,required",
    [
        ("list", ApiRole.VIEWER),
        ("run", ApiRole.OPERATOR),
        ("config apply", ApiRole.DEVELOPER),
        ("account list", ApiRole.ADMIN),
        ("clone", ApiRole.ADMIN),
        ("rehash", ApiRole.ADMIN),
        ("server", ApiRole.ADMIN),
        ("publish-docs", ApiRole.ADMIN),
        ("docs-version", ApiRole.ADMIN),
        ("generate-docs", ApiRole.DEVELOPER),
        ("lineage refresh", ApiRole.DEVELOPER),
    ],
)
def test_role_inheritance_and_refusal_records(project, role, command, required):
    from etl_craft.core.errors import PermissionDeniedError
    from etl_craft.services.operations.permissions import authorize

    p, ctx, creds = project
    caller = replace(ctx, actor=creds[role].metadata.actor, role=role)
    before = len(actions(p.engine))
    arguments = {"password": "do-not-record", "pipeline_code": "P"}
    allowed_roles = {
        ApiRole.VIEWER: {ApiRole.VIEWER, ApiRole.OPERATOR, ApiRole.DEVELOPER, ApiRole.ADMIN},
        ApiRole.OPERATOR: {ApiRole.OPERATOR, ApiRole.DEVELOPER, ApiRole.ADMIN},
        ApiRole.DEVELOPER: {ApiRole.DEVELOPER, ApiRole.ADMIN},
        ApiRole.ADMIN: {ApiRole.ADMIN},
    }
    allowed = role in allowed_roles[required]
    if allowed:
        authorize(caller, command, arguments)
        assert len(actions(p.engine)) == before
    else:
        with pytest.raises(PermissionDeniedError) as error:
            authorize(caller, command, arguments)
        message = str(error.value)
        assert caller.actor.name in message and role in message and required in message
        assert "administrator" in message
        record = actions(p.engine)[-1]
        assert record.actor == caller.actor.name
        assert record.command == command
        assert record.outcome == "REFUSED"
        assert record.exit_code == error.value.exit_code
        assert "do-not-record" not in str(record.arguments)
        assert len(actions(p.engine)) == before + 1


@pytest.mark.parametrize("role", tuple(ApiRole))
def test_cli_and_http_share_the_service_run_policy(project, role, monkeypatch, capsys):
    p, ctx, creds = project
    token = creds[role].token
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token)
    args = ["run", "--pipeline_code", "P", "--init-only", "--run-key", "one"]
    code = main(args)
    output = capsys.readouterr()
    response = TestClient(create_app(ctx)).post(
        "/api/v1/pipelines/P/runs", json={}, headers={"Authorization": "Bearer " + token}
    )
    if role == ApiRole.VIEWER:
        assert code == 26, output
        assert response.status_code == 403, response.text
        assert response.json()["exit_code"] == code
        refused = [a for a in actions(p.engine) if a.outcome == "REFUSED"]
        assert len(refused) == 2
        assert all(a.actor == "policy-viewer" and a.command == "run" for a in refused)
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
    from etl_craft.services.operations.permissions import ROLES

    p, ctx, creds = project
    credential = creds[ApiRole.OPERATOR]
    caller = replace(ctx, actor=credential.metadata.actor, role=ApiRole.OPERATOR)
    monkeypatch.setitem(ROLES, "pause", ApiRole.ADMIN)
    with pytest.raises(PermissionDeniedError):
        pipelines.set_pause(caller, "P", "policy test", verb="pause")
    monkeypatch.setenv("ETL_CRAFT_TOKEN", credential.token)
    assert main(["pause", "--pipeline_code", "P", "--reason", "policy test"]) == 26
    assert "admin role is required" in capsys.readouterr().err
    response = TestClient(create_app(ctx)).post(
        "/api/v1/pipelines/P/pause",
        json={"reason": "policy test"},
        headers={"Authorization": "Bearer " + credential.token},
    )
    assert response.status_code == 403, response.text
    refused = [a for a in actions(p.engine) if a.outcome == "REFUSED"]
    assert len(refused) == 3
    assert all(a.actor == credential.metadata.name and a.command == "pause" for a in refused)


def test_services_reject_unproven_roles_and_metadata_writes_before_io(project):
    from etl_craft.core.errors import PermissionDeniedError
    from etl_craft.services.operations.config_files import apply_config

    _, ctx, creds = project
    with pytest.raises(PermissionDeniedError, match="unproven"):
        inspect.list_pipelines(replace(ctx, role=None))
    for role in (ApiRole.VIEWER, ApiRole.OPERATOR):
        caller = replace(ctx, role=role, actor=creds[role].metadata.actor)
        with pytest.raises(PermissionDeniedError, match="developer role is required"):
            apply_config(caller, Path("does-not-exist"))


def test_developer_can_apply_project_migrations_but_not_engine_migrations(
    project, tmp_path, monkeypatch, capsys
):
    p, _, creds = project
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
