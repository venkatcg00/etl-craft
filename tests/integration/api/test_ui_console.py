"""The command console runs CLI lines as the signed-in person and prints what the CLI prints."""

import re

import pytest
from fastapi.testclient import TestClient

from etl_craft.api.app import create_app
from etl_craft.cli.commands import COMMANDS
from etl_craft.cli.console import KEPT_IN_FILES, SIGN_IN, console
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.actions import ACTIONS, has_role
from fixtures.actions import act
from fixtures.metadata import add_pipeline, add_task

PASSWORD = "correct horse battery"
URL = "/ui/projects/default/console"


@pytest.fixture
def site(metadata_project):
    from etl_craft.engine.migrations import mark_packaged_migrations_applied

    p = metadata_project
    with p.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
    ctx = OperationContext(p.engine, p.config, Actor("owner", ActorKind.USER), role=ApiRole.ADMIN)
    for name, role in (("owner", ApiRole.ADMIN), ("alice", ApiRole.VIEWER)):
        act(ctx, ActionName.ACCOUNT_ADD, name=name, role=role, password=PASSWORD)
    with acting_as(Actor("owner", ActorKind.USER)), p.engine.begin() as conn:
        add_task(conn, add_pipeline(conn, "ORDERS"), "LOAD")
    client = TestClient(
        create_app(ctx, console=console()),
        base_url="https://testserver",
        raise_server_exceptions=False,
    )
    return p, ctx, client


def login(client, name):
    page = client.get("/signin")
    csrf = re.search(r'name="csrf-token" content="([^"]+)"', page.text).group(1)
    client.post(
        "/api/v1/login/password",
        headers={"HX-Request": "true", "X-CSRF-Token": csrf, "Origin": "https://testserver"},
        json={"name": name, "password": PASSWORD, "_return_to": "/ui"},
    )
    page = client.get(URL)
    return re.search(r'name="csrf-token" content="([^"]+)"', page.text).group(1), page


def run(client, csrf, line):
    response = client.post(
        URL, json={"line": line}, headers={"X-CSRF-Token": csrf, "Origin": "https://testserver"}
    )
    assert response.status_code == 200, response.text
    return response.json()


def expected_commands(role):
    return {
        command.name
        for command in COMMANDS
        if command.actions
        and all(
            ACTIONS[a].path is not None
            and a not in KEPT_IN_FILES | SIGN_IN
            and ACTIONS[a].role != ApiRole.AGENT
            for a in command.actions
        )
        and any(has_role(role, ACTIONS[a].role) for a in command.actions)
    }


@pytest.mark.parametrize(("name", "role"), [("owner", ApiRole.ADMIN), ("alice", ApiRole.VIEWER)])
def test_lists_exactly_the_commands_the_role_may_run(site, name, role):
    _, _, client = site
    _, page = login(client, name)
    listed = set(re.findall(r"<li><code>([a-z-]+)</code></li>", page.text))
    assert listed == expected_commands(role)
    assert {"controller", "migrate", "config", "login", "ui"}.isdisjoint(listed)


def test_prints_the_same_text_and_json_as_the_cli(site, monkeypatch, capsys):
    from etl_craft.cli import main

    p, ctx, client = site
    csrf, _ = login(client, "owner")
    token = act(ctx, ActionName.TOKEN_CREATE, name="cli", role=ApiRole.ADMIN).token
    monkeypatch.chdir(p.root)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", token)
    for line in ("pipelines", "pipelines --format json", "graph --pipeline-code ORDERS"):
        assert main(line.split()) == 0
        printed = capsys.readouterr().out
        result = run(client, csrf, f"etl-craft {line}")
        assert (result["exit_code"], result["output"], result["error"]) == (0, printed, "")
    assert main(["status", "--pipeline-code", "NOPE"]) != 0
    cli_error = capsys.readouterr().err
    result = run(client, csrf, "status --pipeline-code NOPE")
    assert result["exit_code"] != 0 and result["error"] == cli_error


def test_refuses_hidden_host_metadata_file_and_other_server_commands(site):
    _, _, client = site
    csrf, _ = login(client, "alice")
    hidden = sorted(expected_commands(ApiRole.ADMIN) - expected_commands(ApiRole.VIEWER))[0]
    refusals = {
        hidden: "not a command you can run in the console",
        "pause --pipeline-code ORDERS --reason x": "needs operator role",
        "controller": "not a command you can run in the console",
        "config apply": "not a command you can run in the console",
        "logs --pipeline-code ORDERS --zip /tmp/run.zip": "reads or writes a file on the server",
        "pipelines --api https://elsewhere": "`--api` is not used in the console",
        "pipelines --project other": "this console runs in project 'default'",
        "pipelines --no-such-flag": "unrecognized arguments",
        'pipelines "unclosed': "close the quote",
    }
    for line, message in refusals.items():
        result = run(client, csrf, line)
        assert result["exit_code"] != 0 and message in result["error"], (line, result)
    help_text = run(client, csrf, "help")["output"]
    assert re.search(r"^\s+pipelines\s", help_text, re.M)
    assert not re.search(r"^\s+controller\s", help_text, re.M)


def test_requires_the_page_csrf_proof_and_records_requests_in_audit_mine(site):
    _, _, client = site
    csrf, _ = login(client, "owner")
    refused = client.post(URL, json={"line": "pipelines"}, headers={"Origin": "https://testserver"})
    assert refused.status_code == 403
    result = run(client, csrf, "pause --pipeline-code ORDERS --reason maintenance")
    assert result["exit_code"] == 0, result
    mine = client.get("/api/v1/projects/default/audit", params={"mine": "true"}).json()
    assert mine["actions"][-1]["command"] == "control"
    assert {a["actor"] for a in mine["actions"]} == {"owner"}
    assert mine["changes"] == []


def test_never_prompts_a_scheduled_resume_prints_the_refusal_instead(site, monkeypatch):
    import sys

    from sqlalchemy import text

    p, _, client = site
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    with p.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_PIPELINES SET RUN_SCHEDULE='0 6 * * *'"))
    csrf, _ = login(client, "owner")
    assert run(client, csrf, "pause --pipeline-code ORDERS --reason upkeep")["exit_code"] == 0
    resumed = run(client, csrf, "resume --pipeline-code ORDERS --reason upkeep")
    assert resumed["exit_code"] != 0 and "scheduled resume requires" in resumed["error"]
