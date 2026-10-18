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
from etl_craft.core.enums import AccountKind, ApiRole, RunStatus, YesNo
from etl_craft.core.errors import ExitCode
from etl_craft.engine import transitions
from etl_craft.engine.schema import init_db
from etl_craft.services import credentials
from etl_craft.services.operations import OperationContext, accounts, tokens
from etl_craft.services.operations.models import BackfillView, OperationResult
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
    monkeypatch.delenv("ETL_CRAFT_SERVER", raising=False)
    system = OperationContext(db.engine, config, SYSTEM_ACTOR)
    admin = _admin(system)
    for name, role in (("alice", ApiRole.OPERATOR), ("vera", ApiRole.VIEWER)):
        account = accounts.Account(0, name, AccountKind.USER, role, None, None, YesNo.YES)
        accounts.add_account(admin, account, PASSWORD)
    service = {role: tokens.create_token(admin, f"ci-{role}", role).token for role in tokens.ROLES}
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
    owner = accounts.Account(0, "owner", AccountKind.USER, ApiRole.ADMIN, None, None, YesNo.YES)
    accounts.add_account(system, owner, PASSWORD, recovery=True)
    return replace(system, actor=Actor("owner", ActorKind.USER), role=ApiRole.ADMIN)


def _write_project(tmp_path, db):
    profile = db.config.engine
    block = {"jdbc_url": profile.jdbc_url, "schema": profile.schema or "main"}
    if profile.auth_mode != "none":
        block.update(user=profile.user, auth_mode=profile.auth_mode, secret=profile.secret_var)
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
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
    return run(capsys, "login", "--server", url)


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
    code, out, _ = run(capsys, "whoami", "--server", url, "--format", "json")
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
    assert run(capsys, "whoami", "--server", url)[0] == 0
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
    code, _, err = run(capsys, "whoami", "--server", url)
    assert code == ExitCode.USAGE and "refresh token is unknown, expired or revoked" in err


def test_disabling_the_account_and_logging_out_revoke_its_server_session(
    server, monkeypatch, capsys
):
    url, ctx = server["url"], server["ctx"]
    sign_in(monkeypatch, capsys, url)
    assert run(capsys, "logout", "--server", url)[0] == 0
    assert credentials.read_server(url) is None
    assert all(row.revoked_at is not None for row in tokens_of(ctx, "alice"))
    code, _, err = run(capsys, "whoami", "--server", url)
    assert code == ExitCode.USAGE and f"etl-craft login --server {url}" in err
    sign_in(monkeypatch, capsys, url)
    accounts.disable(ctx, "alice")
    code, _, err = run(capsys, "whoami", "--server", url)
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
    selected = ("--pipeline_code", "P", "--run-id", str(server["run"]))
    for argv in (
        ("status", *selected),
        ("explain", *selected, "--task_code", "T"),
        ("history", "--pipeline_code", "P", "--all"),
        ("history", "--pipeline_code", "P", "--task_code", "T", "--all"),
    ):
        for output in ("text", "json"):
            local = run(capsys, *argv, "--format", output)
            remote = run(capsys, *argv, "--format", output, "--server", url)
            assert remote == local and local[0] == 0, argv


def test_remote_controls_follow_the_role_and_record_refusals(server, monkeypatch, capsys):
    url, ctx = server["url"], server["ctx"]
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["viewer"])
    code, _, err = run(capsys, "pause", "--pipeline_code", "P", "--reason", "x", "--server", url)
    assert code == ExitCode.PERMISSION_DENIED and "operator role is required" in err
    with ctx.engine.connect() as conn:
        refused = conn.execute(
            text("SELECT ACTOR AS actor FROM AUD_ACTIONS WHERE OUTCOME='REFUSED'")
        ).scalars()
        assert "ci-viewer" in list(refused)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["operator"])
    for verb in ("pause", "resume"):
        code, out, err = run(capsys, verb, "--pipeline_code", "P", "--reason", "r", "--server", url)
        assert code == 0 and out.startswith("P: "), err
    with ctx.engine.connect() as conn:
        paused_by = conn.execute(text("SELECT PAUSED_BY AS actor FROM AUD_PIPELINE_PAUSES"))
        assert paused_by.scalar_one() == "ci-operator"


def test_remote_controls_reach_the_same_operations_with_the_selected_run(
    server, monkeypatch, capsys
):
    url, run_id = server["url"], server["run"]
    calls = []
    done = OperationResult(RunStatus.SUCCESS, "P: done", 1, "P")

    def record(name, result=done):
        def operation(caller, *args, **kwargs):
            calls.append((name, caller.actor.name, args, kwargs))
            return result

        return operation

    backfill = BackfillView(
        1, "P", date(2026, 1, 1), date(2026, 1, 2), RunStatus.SUCCESS, "b", (), None
    )
    for target, result in (
        ("runs.mark", done),
        ("runs.cancel_run", done),
        ("runs.trigger_run", done),
        ("tasks.rerun_task", done),
        ("backfills.run_backfill", backfill),
    ):
        monkeypatch.setattr(f"etl_craft.services.operations.{target}", record(target, result))
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["operator"])
    by_key = ("--run-key", "k1")
    for argv in (
        ("mark", "--pipeline_code", "P", "--task_code", "T", "--status", "SUCCESS", *by_key),
        ("cancel", "--pipeline_code", "P", "--run-id", str(run_id)),
        ("run", "--pipeline_code", "P", "--run-date", "2026-01-05"),
        ("run", "--pipeline_code", "P", "--task_code", "T", "--rerun", "--with-downstream"),
        ("run", "--pipeline_code", "P", "--backfill", "2026-01-01:2026-01-02"),
    ):
        code, out, err = run(capsys, *argv, "--reason", "why", "--server", url)
        assert code == 0, (argv, err)
        assert out in ("P: done\n", "b\n"), out
    names = [name for name, *_ in calls]
    assert names == [
        "runs.mark",
        "runs.cancel_run",
        "runs.trigger_run",
        "tasks.rerun_task",
        "backfills.run_backfill",
    ]
    assert {actor for _, actor, *_ in calls} == {"ci-operator"}
    mark, cancel, trigger, rerun, filled = (call[2:] for call in calls)
    assert mark[1]["task_code"] == "T" and mark[1]["selector"].run_key == "k1"
    assert cancel[1]["selector"].run_id == run_id
    assert trigger[1]["run_date"] == date(2026, 1, 5) and trigger[1]["reason"] == "why"
    assert rerun[0][1] == "T" and rerun[1]["with_downstream"] is True
    assert filled[0][1:] == (date(2026, 1, 1), date(2026, 1, 2), "why")


def test_metadata_and_host_only_commands_are_refused_against_a_server(server, capsys):
    url = server["url"]
    for argv, expected in (
        (("config", "apply"), "`etl-craft config apply` there"),
        (("migrate",), "`etl-craft migrate` there"),
        (("init-db",), "on the server's host"),
        (("setup",), "on the server's host"),
        (("account", "list"), "`etl-craft account` on the server's host"),
        (("token", "list"), "`etl-craft token` on the server's host"),
        (("validate",), "works on a local project only"),
        (("run", "--pipeline_code", "P", "--force"), "--force: works on the server's host only"),
    ):
        code, _, err = run(capsys, "--server", url, *argv)
        assert code == ExitCode.USAGE and expected in err, argv


def test_only_https_is_accepted_except_on_this_machine(server, monkeypatch, capsys):
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["viewer"])
    code, _, err = run(capsys, "whoami", "--server", "http://example.com")
    assert code == ExitCode.USAGE and "plain HTTP is refused except to localhost" in err
    code, _, err = run(capsys, "whoami", "--server", "https://user:secret@example.com")
    assert code == ExitCode.USAGE and "nothing after it" in err
    monkeypatch.setenv("ETL_CRAFT_SERVER", "https://127.0.0.1:1")
    code, _, err = run(capsys, "whoami")
    assert code == ExitCode.SERVER and "cannot reach the server" in err


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
    assert run(capsys, "logs", "--attempt-id", attempt, "--server", url) == (
        0,
        "first\nsecond é\n",
        "",
    )
    assert run(capsys, "logs", "--attempt-id", attempt, "--offset", "6", "--server", url)[1] == (
        "second é\n"
    )


def test_sso_sign_in_runs_the_device_flow_on_the_server(
    empty_engine_db, issuer, tmp_path, monkeypatch, capsys, cli_logging
):
    auth, _ = issuer
    db = empty_engine_db
    init_db(db.engine)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "person"))
    monkeypatch.delenv("ETL_CRAFT_TOKEN", raising=False)
    system = OperationContext(db.engine, replace(db.config, auth=auth), SYSTEM_ACTOR)
    account = accounts.Account(
        0, "sam", AccountKind.USER, ApiRole.VIEWER, None, "known-subject", YesNo.YES
    )
    accounts.add_account(_admin(system), account, None)
    monkeypatch.setattr(login.interrupts, "sleep", lambda seconds: None)
    with Served(replace(system, actor=Actor("server", ActorKind.SERVICE))) as url:
        code, out, err = sign_in(monkeypatch, capsys, url, "unused", "y")
        assert code == 0, err
        assert "enter ABCD-EFGH" in out and f"Signed in to {url} as sam (viewer)" in out
        assert run(capsys, "whoami", "--server", url)[1] == "sam viewer\n"


@pytest.mark.parametrize("standalone", [False, True])
@pytest.mark.parametrize("status", [RunStatus.SUCCESS, RunStatus.IN_PROGRESS])
def test_remote_task_run_forwards_the_exact_selection(
    server, monkeypatch, capsys, standalone, status
):
    calls = []

    def execute(caller, request):
        calls.append((caller.actor.name, request))
        return OperationResult(status, "P: task done", 1, "P")

    monkeypatch.setattr("etl_craft.services.operations.runs.execute_run", execute)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", server["service"]["operator"])
    args = ["run", "--pipeline_code", "P", "--task_code", "T", "--run-key", "manual:test"]
    if standalone:
        args += [
            "--standalone",
            "--with-downstream",
            "--ignore-dependencies",
            "--reason",
            "checked",
        ]
    code, out, err = run(capsys, *args, "--server", server["url"])
    expected = ExitCode.SUCCESS if status == RunStatus.SUCCESS else ExitCode.INCOMPLETE
    assert code == expected and out == "P: task done\n", err
    actor, request = calls[0]
    assert actor == "ci-operator" and request.pipeline_code == "P"
    assert request.selector.run_key == "manual:test" and request.task_code == "T"
    assert (
        request.standalone == request.with_downstream == request.ignore_dependencies == standalone
    )
