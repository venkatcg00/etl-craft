"""A local installation operating a real server over HTTP, within the person's role."""

import json
import socket
import threading
import time
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta

import pytest
import uvicorn
import yaml
from sqlalchemy import text

from etl_craft.api.app import create_app
from etl_craft.cli import main
from etl_craft.cli.commands import login
from etl_craft.core.actor import SYSTEM_ACTOR, Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole, ControlAction, RunStatus
from etl_craft.core.errors import ExitCode
from etl_craft.engine import transitions
from etl_craft.engine.schema import init_db
from etl_craft.services import credentials
from etl_craft.services.operations import OperationContext, accounts, tokens
from etl_craft.services.operations.actions import ACTIONS, invoke
from etl_craft.services.operations.models import BackfillView, OperationResult
from etl_craft.services.operations.requests import ControlRequest
from fixtures.metadata import add_pipeline, add_task, start_run

PASSWORD = "correct horse battery"


@pytest.fixture
def server(empty_engine_db, tmp_path, monkeypatch, cli_logging):
    """A served API over an Engine DB with pipeline P, its task T and one run in progress."""
    db = empty_engine_db
    init_db(db.engine)
    config = replace(db.config, log_dir=tmp_path / "logs")
    monkeypatch.setenv("ETL_CRAFT_CONFIG", str(_write_project(tmp_path, db)))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "person"))
    monkeypatch.delenv("ETL_CRAFT_TOKEN", raising=False)
    monkeypatch.delenv("ETL_CRAFT_CONTROLLER", raising=False)
    system = OperationContext(db.engine, config, SYSTEM_ACTOR)
    admin = _admin(system)
    for name, role in (("alice", ApiRole.OPERATOR), ("vera", ApiRole.VIEWER)):
        add = accounts.AccountAdd(name=name, role=role, password=PASSWORD)
        invoke(admin, ActionName.ACCOUNT_ADD, add)
    service = {
        role: invoke(
            admin, ActionName.TOKEN_CREATE, tokens.TokenCreate(name=f"ci-{role}", role=role)
        ).token
        for role in ApiRole
    }
    with acting_as(SYSTEM_ACTOR), db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(conn, pipeline, "T")
        run = start_run(conn, pipeline)
        summary = transitions.find_or_create_task_run(conn, task, run).task_run_id
        attempt = transitions.queue_attempt(conn, summary, SYSTEM_ACTOR)
    ctx = replace(system, actor=Actor("ci-admin", ActorKind.SERVICE), role=ApiRole.ADMIN)
    with Served(ctx) as url:
        yield {"url": url, "ctx": ctx, "service": service, "run": run, "attempt": attempt}


def _admin(system):
    """Create the first admin locally, and return a context acting as it."""
    owner = accounts.AccountAdd(name="owner", role=ApiRole.ADMIN, password=PASSWORD, recovery=True)
    invoke(replace(system, provisioning=True), ActionName.ACCOUNT_ADD, owner)
    return replace(system, actor=Actor("owner", ActorKind.USER), role=ApiRole.ADMIN)


def _write_project(tmp_path, db):
    profile = db.config.engine
    block = {"Jdbc_url": profile.jdbc_url, "Schema": profile.schema or "main"}
    if profile.auth_mode != "NONE":
        block.update(User=profile.user, Auth_mode=profile.auth_mode, Secret=profile.secret_var)
    raw = {
        "Secrets": {"Source_type": "ENVIRONMENT"},
        "Orchestration": {},
        "Engine": {"dev": block},
    }
    path = tmp_path / "craft-connector.yml"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return path


class Served:
    """Run the app on a free local port for the duration of a ``with`` block."""

    def __init__(self, ctx):
        self.server = uvicorn.Server(
            uvicorn.Config(create_app(ctx), log_level="warning", lifespan="off")
        )
        self.sock = socket.socket()
        self.sock.bind(("127.0.0.1", 0))

    def __enter__(self):
        self.thread = threading.Thread(target=self.server.run, kwargs={"sockets": [self.sock]})
        self.thread.start()
        deadline = time.monotonic() + 10
        while not self.server.started and time.monotonic() < deadline:
            time.sleep(0.01)
        assert self.server.started
        return f"http://127.0.0.1:{self.sock.getsockname()[1]}"

    def __exit__(self, *exc):
        self.server.should_exit = True
        self.thread.join(timeout=10)
        self.sock.close()


def run(capsys, *argv):
    """Run the CLI and return its exit code, standard output and standard error."""
    code = main(list(argv))
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def sign_in(monkeypatch, capsys, url, name="alice", *answers):
    replies = iter([*answers, name])
    monkeypatch.setattr("builtins.input", lambda prompt="": next(replies))
    monkeypatch.setattr(login, "password", lambda confirm=False: PASSWORD)
    monkeypatch.setattr(login, "interactive", lambda: None)
    return run(capsys, "login", "--controller", url)


def tokens_of(ctx, name):
    with ctx.engine.connect() as conn:
        return conn.execute(
            text(
                "SELECT t.TOKEN_ID AS token_id, t.TOKEN_KIND AS kind, "
                "t.REFRESH_TOKEN_ID AS refresh_id, t.REVOKED_AT AS revoked_at "
                "FROM CFG_API_TOKENS t JOIN CFG_ACCOUNTS a ON a.ACCOUNT_ID=t.ACCOUNT_ID "
                "WHERE a.ACCOUNT_NAME=:name ORDER BY t.TOKEN_ID"
            ),
            {"name": name},
        ).all()


def test_password_sign_in_keeps_only_tokens_locally_and_whoami_reads_the_server(
    server, monkeypatch, capsys, tmp_path
):
    url = server["url"]
    code, out, _ = sign_in(monkeypatch, capsys, url)
    assert code == 0 and f"Signed in to {url} as alice (operator)" in out
    stored = json.loads(credentials.credential_path().read_text())
    assert set(stored) == {url}
    assert set(stored[url]) == {"access", "refresh", "access_expires_at"}
    assert "jdbc" not in json.dumps(stored) and PASSWORD not in json.dumps(stored)
    assert [row.kind for row in tokens_of(server["ctx"], "alice")] == ["REFRESH", "ACCESS"]
    code, out, _ = run(capsys, "whoami", "--controller", url, "--format", "json")
    assert code == 0
    assert json.loads(out) == {
        "schema": "etl-craft/identity/1",
        "name": "alice",
        "role": "operator",
    }


def test_an_expired_access_token_is_renewed_and_an_expired_refresh_needs_sign_in(
    server, monkeypatch, capsys
):
    url = server["url"]
    sign_in(monkeypatch, capsys, url)
    stored = credentials.read_server(url)
    past = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
    credentials.store_session(url, {**stored, "access_expires_at": past})
    assert run(capsys, "whoami", "--controller", url)[0] == 0
    renewed = credentials.read_server(url)
    assert renewed["access"] != stored["access"] and renewed["refresh"] == stored["refresh"]
    rows = tokens_of(server["ctx"], "alice")
    refresh_id = next(row.token_id for row in rows if row.kind == "REFRESH")
    assert [row.refresh_id for row in rows if row.kind == "ACCESS"] == [refresh_id, refresh_id]
    with acting_as(SYSTEM_ACTOR), server["ctx"].engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_API_TOKENS SET EXPIRES_AT=:past WHERE TOKEN_ID=:id"),
            {"past": datetime.now(UTC) - timedelta(days=1), "id": refresh_id},
        )
    credentials.store_session(url, {**renewed, "access_expires_at": past})
    code, _, err = run(capsys, "whoami", "--controller", url)
    assert code == ExitCode.USAGE and "refresh token is unknown, expired or revoked" in err


def test_disabling_the_account_and_logging_out_revoke_its_server_session(
    server, monkeypatch, capsys
):
    url, ctx = server["url"], server["ctx"]
    sign_in(monkeypatch, capsys, url)
    assert run(capsys, "logout", "--controller", url)[0] == 0
    assert credentials.read_server(url) is None
    assert all(row.revoked_at is not None for row in tokens_of(ctx, "alice"))
    code, _, err = run(capsys, "whoami", "--controller", url)
    assert code == ExitCode.USAGE and f"etl-craft login --controller {url}" in err
    sign_in(monkeypatch, capsys, url)
    invoke(ctx, ActionName.ACCOUNT_DISABLE, accounts.AccountName(name="alice"))
    code, _, err = run(capsys, "whoami", "--controller", url)
    assert code == ExitCode.USAGE and "refresh token is unknown, expired or revoked" in err


def test_remote_reads_print_exactly_what_the_local_project_prints(server, monkeypatch, capsys):
    url = server["url"]
    with acting_as(SYSTEM_ACTOR), server["ctx"].engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE AUD_PIPELINES_RUN_LOG SET STATUS='SUCCESS', END_DATE=START_DATE "
                "WHERE PIPELINE_RUN_ID=:id"
            ),
            {"id": server["run"]},
        )
        conn.execute(
            text(
                "UPDATE AUD_TASK_RUN_LOG SET STATUS='SUCCESS', END_DATE=START_DATE "
                "WHERE PIPELINE_RUN_ID=:id"
            ),
            {"id": server["run"]},
        )
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["viewer"])
    selected = ("--pipeline-code", "P", "--run-id", str(server["run"]))
    reads = (
        ("pipelines",),
        ("graph", "--pipeline-code", "P"),
        ("steps", *selected),
        ("status", *selected),
        ("capabilities", *selected),
        ("explain", *selected, "--task-code", "T"),
        ("history", "--pipeline-code", "P", "--all"),
        ("history", "--pipeline-code", "P", "--task-code", "T", "--all"),
        ("audit", "--pipeline-code", "P"),
        ("validate",),
        ("lineage",),
    )
    for argv in reads:
        for output in ("text", "json"):
            local = run(capsys, *argv, "--format", output)
            remote = run(capsys, *argv, "--format", output, "--controller", url)
            assert remote == local and local[0] in (0, 1), argv
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["admin"])
    for argv in (("token", "list"), ("account", "list")):
        local = run(capsys, *argv, "--format", "json")
        remote = run(capsys, "--controller", url, *argv, "--format", "json")
        assert remote == local and local[0] == 0, argv


def test_remote_controls_follow_the_role_and_record_refusals(server, monkeypatch, capsys):
    url, ctx = server["url"], server["ctx"]
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["viewer"])
    code, _, err = run(
        capsys, "pause", "--pipeline-code", "P", "--reason", "x", "--controller", url
    )
    assert code == ExitCode.PERMISSION_DENIED
    assert "pipeline 'P' needs operator role" in err
    assert "group grant" in err
    with ctx.engine.connect() as conn:
        refused = conn.execute(
            text("SELECT ACTOR AS actor FROM AUD_ACTIONS WHERE OUTCOME='REFUSED'")
        ).scalars()
        assert "ci-viewer" in list(refused)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["operator"])
    for verb in ("pause", "resume"):
        code, out, err = run(
            capsys, verb, "--pipeline-code", "P", "--reason", "r", "--controller", url
        )
        assert code == 0 and out.startswith("P: "), err
    with ctx.engine.connect() as conn:
        paused_by = conn.execute(text("SELECT PAUSED_BY AS actor FROM AUD_PIPELINE_PAUSES"))
        assert paused_by.scalar_one() == "ci-operator"


def test_remote_controls_reach_the_same_actions_with_the_selected_run(server, monkeypatch, capsys):
    url, run_id = server["url"], server["run"]
    calls = []
    held = ControlRequest(pipeline_code="P", action=ControlAction.HOLD, reason="test")
    control = replace(invoke(server["ctx"], ActionName.CONTROL, held), message="P: done")
    done = OperationResult(RunStatus.SUCCESS, "P: done", 1, "P")
    backfill = BackfillView(
        1, "P", date(2026, 1, 1), date(2026, 1, 2), RunStatus.SUCCESS, "b", (), None
    )

    def record(name):
        def operation(caller, request):
            calls.append((name, caller.actor.name, request))
            if name == ActionName.CONTROL:
                return control
            return backfill if getattr(request, "backfill", None) else done

        return operation

    for name in (ActionName.MARK, ActionName.CONTROL, ActionName.RUN):
        monkeypatch.setitem(ACTIONS, name, replace(ACTIONS[name], call=record(name)))
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["operator"])
    by_key = ("--run-key", "k1")
    for argv in (
        ("mark", "--pipeline-code", "P", "--task-code", "T", "--status", "SUCCESS", *by_key),
        ("abort", "--pipeline-code", "P", "--run-id", str(run_id)),
        ("run", "--pipeline-code", "P", "--run-date", "2026-01-05"),
        ("run", "--pipeline-code", "P", "--task-code", "T", "--rerun", "--with-downstream"),
        ("run", "--pipeline-code", "P", "--backfill", "2026-01-01:2026-01-02"),
    ):
        code, out, err = run(capsys, *argv, "--reason", "why", "--controller", url)
        assert code == 0, (argv, err)
        if argv[0] == "abort":
            assert "control_id=" in out and out.endswith("P: done\n"), out
        else:
            assert out in ("P: done\n", "b\n"), out
    assert [name for name, *_ in calls] == [
        ActionName.MARK,
        ActionName.CONTROL,
        ActionName.RUN,
        ActionName.RUN,
        ActionName.RUN,
    ]
    assert {actor for _, actor, _ in calls} == {"ci-operator"}
    mark, abort, trigger, rerun, filled = (request for *_, request in calls)
    assert mark.task_code == "T" and mark.run_key == "k1"
    assert abort.action == ControlAction.ABORT and abort.run_id == run_id
    assert trigger.run_date == date(2026, 1, 5) and trigger.reason == "why"
    assert rerun.task_code == "T" and rerun.rerun and rerun.with_downstream
    assert filled.backfill == (date(2026, 1, 1), date(2026, 1, 2)) and filled.reason == "why"


def test_host_only_actions_and_options_are_refused_against_a_server(server, monkeypatch, capsys):
    url = server["url"]
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["operator"])
    for argv, expected in (
        (("config", "apply"), "`etl-craft config apply` there"),
        (("migrate",), "`etl-craft migrate` there"),
        (("init-db",), "run it on the controller's host"),
        (("setup",), "run it on the controller's host"),
        (("doctor",), "run it on the controller's host"),
        (("controller",), "start it on the host that serves it"),
        (
            ("run", "--pipeline-code", "P", "--force"),
            "`--force` works only on the controller's host",
        ),
    ):
        code, _, err = run(capsys, "--controller", url, *argv)
        assert code == ExitCode.USAGE and expected in err, (argv, err)


def test_only_https_is_accepted_except_on_this_machine(server, monkeypatch, capsys):
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["viewer"])
    code, _, err = run(capsys, "whoami", "--controller", "http://example.com")
    assert code == ExitCode.USAGE and "plain HTTP is refused except to localhost" in err
    code, _, err = run(capsys, "whoami", "--controller", "https://user:secret@example.com")
    assert code == ExitCode.USAGE and "nothing after it" in err
    monkeypatch.setenv("ETL_CRAFT_CONTROLLER", "https://127.0.0.1:1")
    code, _, err = run(capsys, "whoami")
    assert code == ExitCode.SERVER and "cannot reach the controller" in err


def test_attempt_logs_stream_from_the_server_within_the_role(server, monkeypatch, capsys):
    url, ctx = server["url"], server["ctx"]
    path = ctx.config.log_dir / "P" / f"run-{server['run']}" / "T.attempt-1.log"
    path.parent.mkdir(parents=True)
    path.write_bytes("first\nsecond é\n".encode())
    with acting_as(SYSTEM_ACTOR), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE AUD_TASK_ATTEMPTS SET LOG_PATH=:path WHERE ATTEMPT_ID=:id"),
            {"path": str(path), "id": server["attempt"]},
        )
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["viewer"])
    attempt = str(server["attempt"])
    assert run(capsys, "logs", "--attempt-id", attempt, "--controller", url) == (
        0,
        "first\nsecond é\n",
        "",
    )
    assert run(capsys, "logs", "--attempt-id", attempt, "--offset", "6", "--controller", url)[
        1
    ] == ("second é\n")


def test_sso_sign_in_runs_the_device_flow_on_the_server(
    empty_engine_db, issuer, tmp_path, monkeypatch, capsys, cli_logging
):
    auth, _ = issuer
    db = empty_engine_db
    init_db(db.engine)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "person"))
    monkeypatch.delenv("ETL_CRAFT_TOKEN", raising=False)
    system = OperationContext(db.engine, replace(db.config, auth=auth), SYSTEM_ACTOR)
    account = accounts.AccountAdd(name="sam", role=ApiRole.VIEWER, oidc_subject="known-subject")
    invoke(_admin(system), ActionName.ACCOUNT_ADD, account)
    monkeypatch.setattr(login.interrupts, "sleep", lambda seconds: None)
    with Served(replace(system, actor=Actor("server", ActorKind.SERVICE))) as url:
        code, out, err = sign_in(monkeypatch, capsys, url, "unused", "y")
        assert code == 0, err
        assert "enter ABCD-EFGH" in out and f"Signed in to {url} as sam (viewer)" in out
        assert run(capsys, "whoami", "--controller", url)[1] == "sam viewer\n"


@pytest.mark.parametrize("standalone", [False, True])
@pytest.mark.parametrize("status", [RunStatus.SUCCESS, RunStatus.IN_PROGRESS])
def test_remote_task_run_forwards_the_exact_selection(
    server, monkeypatch, capsys, standalone, status
):
    calls = []

    def execute(caller, request):
        calls.append((caller.actor.name, request))
        return OperationResult(status, "P: task done", 1, "P")

    monkeypatch.setitem(ACTIONS, ActionName.RUN, replace(ACTIONS[ActionName.RUN], call=execute))
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["operator"])
    args = ["run", "--pipeline-code", "P", "--task-code", "T", "--run-key", "manual:test"]
    if standalone:
        args += [
            "--standalone",
            "--with-downstream",
            "--ignore-dependencies",
            "--reason",
            "checked",
        ]
    code, out, err = run(capsys, *args, "--controller", server["url"])
    expected = ExitCode.SUCCESS if status == RunStatus.SUCCESS else ExitCode.INCOMPLETE
    assert code == expected and out == "P: task done\n", err
    actor, request = calls[0]
    assert actor == "ci-operator" and request.pipeline_code == "P"
    assert request.run_key == "manual:test" and request.task_code == "T"
    assert (
        request.standalone == request.with_downstream == request.ignore_dependencies == standalone
    )


def test_remote_scoped_cli_uses_same_request_and_status_document(server, monkeypatch, capsys):
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["operator"])
    url, run_id = server["url"], server["run"]
    code, out, err = run(
        capsys,
        "hold",
        "--pipeline-code",
        "P",
        "--run-id",
        str(run_id),
        "--task-code",
        "T",
        "--reason",
        "maintenance",
        "--format",
        "json",
        "--controller",
        url,
    )
    assert code == 0, err
    held = json.loads(out)
    assert held["status"] == "COMPLETED" and held["action"] == "HOLD"
    code, out, err = run(
        capsys,
        "control-request",
        "--control-id",
        str(held["control_id"]),
        "--format",
        "json",
        "--controller",
        url,
    )
    assert code == 0, err
    assert json.loads(out) == held
    code, out, err = run(
        capsys,
        "stop",
        "--pipeline-code",
        "P",
        "--run-id",
        str(run_id),
        "--task-code",
        "T",
        "--reason",
        "drain",
        "--format",
        "json",
        "--controller",
        url,
    )
    assert code == 0, err
    stopped = json.loads(out)
    assert stopped["action"] == "STOP" and stopped["status"] == "COMPLETED"
    assert stopped["run_status"] == "IN-PROGRESS" and stopped["task_code"] == "T"
    code, out, err = run(
        capsys,
        "release",
        "--pipeline-code",
        "P",
        "--run-id",
        str(run_id),
        "--task-code",
        "T",
        "--reason",
        "ready",
        "--format",
        "json",
        "--controller",
        url,
    )
    assert code == 0, err
    assert json.loads(out)["action"] == "RELEASE"


def test_remote_cli_prepares_recovery_restart_and_selected_bulk(
    server, monkeypatch, capsys, tmp_path
):
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["operator"])
    url, source = server["url"], server["run"]
    code, out, err = run(
        capsys,
        "abort",
        "--pipeline-code",
        "P",
        "--run-id",
        str(source),
        "--reason",
        "maintenance",
        "--format",
        "json",
        "--controller",
        url,
    )
    assert code == 0, err
    assert json.loads(out)["run_status"] == "CANCELLED"
    code, out, err = run(
        capsys,
        "run",
        "--pipeline-code",
        "P",
        "--run-id",
        str(source),
        "--restart",
        "--request-key",
        "remote-restart",
        "--reason",
        "fresh context",
        "--format",
        "json",
        "--controller",
        url,
    )
    assert code == 0, err
    restarted = json.loads(out)
    assert restarted["execution_run_id"] != source
    assert restarted["pipeline_run_id"] == source
    code, out, err = run(
        capsys,
        "capabilities",
        "--pipeline-code",
        "P",
        "--run-id",
        str(source),
        "--format",
        "json",
        "--controller",
        url,
    )
    assert code == 0, err
    assert json.loads(out)["schema"] == "etl-craft/capabilities/1"
    selection = tmp_path / "controls.json"
    selection.write_text(
        json.dumps(
            [
                {
                    "pipeline_code": "P",
                    "run_id": restarted["execution_run_id"],
                    "action": "HOLD",
                    "reason": "pause new work",
                    "request_key": "bulk:hold",
                }
            ]
        )
    )
    code, out, err = run(
        capsys, "run", "--controls-file", str(selection), "--format", "json", "--controller", url
    )
    assert code == 0, err
    assert json.loads(out)["outcomes"][0]["control"]["action"] == "HOLD"
