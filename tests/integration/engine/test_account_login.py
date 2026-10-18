"""Registered account lifecycle and credential-proven CLI actions on both Engine DBs."""

import json
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.core.actor import SYSTEM_ACTOR, Actor, ActorKind
from etl_craft.core.enums import AccountKind, ApiRole, YesNo
from etl_craft.core.errors import PermissionDeniedError, UsageError
from etl_craft.services import credentials
from etl_craft.services.operations import OperationContext, accounts, sessions, tokens

PASSWORD = "correct horse battery"


@pytest.fixture
def project(empty_engine_db, tmp_path, monkeypatch, cli_logging):
    from etl_craft.engine.schema import init_db

    db = empty_engine_db
    init_db(db.engine)
    profile = db.config.engine
    block = {"jdbc_url": profile.jdbc_url, "schema": profile.schema or "main"}
    if profile.auth_mode != "none":
        block.update(user=profile.user, auth_mode=profile.auth_mode, secret=profile.secret_var)
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "environment"},
                "Orchestration": {"Mode": "local"},
                "Engine": {"dev": block},
            },
            sort_keys=False,
        )
    )
    monkeypatch.setenv("ETL_CRAFT_CONFIG", str(path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "person"))
    monkeypatch.delenv("ETL_CRAFT_TOKEN", raising=False)
    ctx = OperationContext(db.engine, db.config, SYSTEM_ACTOR)
    admin = accounts.Account(0, "owner", AccountKind.USER, ApiRole.ADMIN, None, None, YesNo.YES)
    accounts.add_account(ctx, admin, PASSWORD, recovery=True)
    return replace(ctx, actor=Actor("owner", ActorKind.USER), role=ApiRole.ADMIN)


def user(ctx, name="alice", subject=None):
    account = accounts.Account(0, name, AccountKind.USER, ApiRole.VIEWER, None, subject, YesNo.YES)
    accounts.add_account(ctx, account, None if subject else PASSWORD)


def test_sessions_expire_revoke_and_follow_current_account_roles(project):
    ctx = project
    user(ctx)
    with pytest.raises(UsageError, match="sign-in refused"):
        sessions.sign_in(ctx, "alice", "wrong")
    created = sessions.sign_in(ctx, "alice", PASSWORD)
    assert tokens.authenticate(ctx, created.token).name == "alice"
    assert created.metadata.expires_at <= datetime.now(UTC) + timedelta(hours=12)
    accounts.set_role(ctx, "alice", ApiRole.OPERATOR)
    assert tokens.authenticate(ctx, created.token).role == ApiRole.OPERATOR
    accounts.reset_password(ctx, "alice", "new password for alice")
    assert tokens.authenticate(ctx, created.token) is None
    with pytest.raises(UsageError):
        sessions.sign_in(ctx, "alice", PASSWORD)
    other = sessions.sign_in(ctx, "alice", "new password for alice")
    sessions.sign_out(ctx, other.token)
    assert tokens.authenticate(ctx, other.token) is None
    last = sessions.sign_in(ctx, "alice", "new password for alice")
    accounts.disable(ctx, "alice")
    assert tokens.authenticate(ctx, last.token) is None
    with pytest.raises(UsageError):
        sessions.sign_in(ctx, "alice", "new password for alice")
    assert any(
        a.name == "alice" and a.active_flag == YesNo.NO
        for a in accounts.list_accounts(ctx).accounts
    )


def test_sso_requires_a_registered_active_user(project):
    ctx = project
    with pytest.raises(UsageError, match="no active registered"):
        sessions.sign_in_subject(ctx, "external-id")
    user(ctx, subject="external-id")
    session = sessions.sign_in_subject(ctx, "external-id")
    assert tokens.authenticate(ctx, session.token).name == "alice"
    accounts.disable(ctx, "alice")
    with pytest.raises(UsageError, match="no active registered"):
        sessions.sign_in_subject(ctx, "external-id")


def test_account_controls_require_a_proven_admin(project):
    ctx = project
    with pytest.raises(PermissionDeniedError, match="admin role is required"):
        accounts.list_accounts(replace(ctx, role=None))
    with pytest.raises(PermissionDeniedError, match="admin role is required"):
        accounts.disable(replace(ctx, role=ApiRole.VIEWER), "owner")
    with pytest.raises(UsageError, match="active admin already exists"):
        accounts.add_account(
            ctx,
            accounts.Account(0, "recovery", AccountKind.USER, ApiRole.ADMIN, None, None, YesNo.YES),
            PASSWORD,
            recovery=True,
        )
    with pytest.raises(UsageError, match="already exists"):
        accounts.add_account(
            ctx,
            accounts.Account(0, "owner", AccountKind.USER, ApiRole.ADMIN, None, None, YesNo.YES),
            PASSWORD,
        )
    with pytest.raises(UsageError, match="not found"):
        accounts.set_role(ctx, "missing", ApiRole.VIEWER)


def test_cli_password_login_whoami_env_precedence_and_logout(project, monkeypatch, capsys):
    ctx = project
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "owner")
    monkeypatch.setattr("getpass.getpass", lambda _: PASSWORD)
    assert main(["login"]) == 0
    assert "Signed in as owner" in capsys.readouterr().out
    key = credentials.database_key(ctx.engine, ctx.config.engine.schema)
    value = credentials.read_session(key)
    assert value and tokens.authenticate(ctx, value)
    assert main(["whoami", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["name"] == "owner"
    monkeypatch.setenv("ETL_CRAFT_TOKEN", "invalid")
    assert main(["whoami"]) == 3
    assert "no signed-in account" in capsys.readouterr().err
    monkeypatch.delenv("ETL_CRAFT_TOKEN")
    assert main(["logout"]) == 0
    assert tokens.authenticate(ctx, value) is None
    assert credentials.read_session(key) is None
    capsys.readouterr()
    assert main(["whoami"]) == 3
    assert "run `etl-craft login`" in capsys.readouterr().err


def test_session_expiry_is_enforced_on_the_next_command(project, capsys):
    ctx = project
    session = sessions.sign_in(ctx, "owner", PASSWORD)
    credentials.store_session(
        credentials.database_key(ctx.engine, ctx.config.engine.schema), session.token
    )
    from etl_craft.core.actor import acting_as

    with acting_as(ctx.actor), ctx.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_API_TOKENS SET EXPIRES_AT=:expired"),
            {"expired": datetime.now(UTC) - timedelta(seconds=1)},
        )
    assert main(["whoami"]) == 3
    assert "no signed-in account" in capsys.readouterr().err
    assert main(["logout"]) == 0


def test_account_cli_and_local_recovery(project, monkeypatch, capsys):
    ctx = project
    service = tokens.create_token(ctx, "automation", ApiRole.ADMIN)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", service.token)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("getpass.getpass", lambda _: PASSWORD)
    assert main(["account", "add", "--name", "alice", "--role", "viewer"]) == 0
    capsys.readouterr()
    assert main(["account", "set-role", "--name", "alice", "--role", "operator"]) == 0
    assert "alice" in capsys.readouterr().out
    assert main(["account", "reset-password", "--name", "alice"]) == 0
    assert main(["account", "disable", "--name", "alice"]) == 0
    capsys.readouterr()
    accounts.disable(ctx, "owner")
    accounts.disable(ctx, "automation")
    monkeypatch.delenv("ETL_CRAFT_TOKEN")
    monkeypatch.setattr(sys.stdin, "isatty", lambda: False)
    assert main(["setup"]) == 2
    assert "interactive terminal required" in capsys.readouterr().err
    assert not accounts.has_admin(ctx.engine)
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "recovery-owner")
    assert main(["setup"]) == 0
    assert "created admin recovery-owner" in capsys.readouterr().out
    assert accounts.has_admin(ctx.engine)
    assert tokens.authenticate(ctx, service.token) is None
    session = sessions.sign_in(ctx, "recovery-owner", PASSWORD)
    assert tokens.authenticate(ctx, session.token).role == ApiRole.ADMIN
    from fastapi.testclient import TestClient

    from etl_craft.api.app import create_app

    client = TestClient(create_app(ctx))
    assert client.post("/api/v1/setup").status_code == 404


def test_password_reset_and_sign_in_cannot_escape_revocation(project):
    from fixtures.races import two_at_once

    ctx = project
    user(ctx)

    def login():
        try:
            return sessions.sign_in(ctx, "alice", PASSWORD).token
        except UsageError:
            return None

    def reset():
        accounts.reset_password(ctx, "alice", "new password for alice")

    token, _ = two_at_once(login, reset, at="etl_craft.engine.locks.for_engine")
    assert token is None or tokens.authenticate(ctx, token) is None
    assert sessions.sign_in(ctx, "alice", "new password for alice").metadata.name == "alice"


def test_two_recoveries_create_only_one_admin(engine_db):
    from fixtures.races import two_at_once

    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR)

    def recover(name):
        admin = accounts.Account(
            0, name, AccountKind.USER, ApiRole.ADMIN, None, "sso-" + name, YesNo.YES
        )
        try:
            accounts.add_account(ctx, admin, recovery=True)
            return True
        except UsageError as error:
            assert "active admin already exists" in str(error)
            return False

    results = two_at_once(
        lambda: recover("one"), lambda: recover("two"), at="etl_craft.engine.locks.for_engine"
    )
    assert sorted(results) == [False, True]


def test_cli_sso_against_the_local_tls_issuer(project, issuer, monkeypatch, capsys):
    from etl_craft.config import resolve_config_path

    ctx = project
    user(ctx, subject="known-subject")
    auth, state = issuer
    path = resolve_config_path()
    raw = yaml.safe_load(path.read_text())
    raw["Auth"] = {"Oidc_issuer": auth.oidc_issuer, "Oidc_client_id": auth.oidc_client_id}
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "y")
    assert main(["login"]) == 0
    assert "Signed in as alice" in capsys.readouterr().out
    assert main(["whoami"]) == 0
    assert capsys.readouterr().out == "alice viewer\n"
    with ctx.engine.connect() as conn:
        before = conn.execute(
            text("SELECT COUNT(*) AS count FROM AUD_ACTIONS WHERE COMMAND='login'")
        ).scalar_one()
    state["wrong_issuer"] = True
    assert main(["login"]) == 3
    assert "discovery issuer differs" in capsys.readouterr().err
    with ctx.engine.connect() as conn:
        after = conn.execute(
            text("SELECT COUNT(*) AS count FROM AUD_ACTIONS WHERE COMMAND='login'")
        ).scalar_one()
    assert after == before + 1


def test_user_sessions_and_service_credentials_keep_their_boundaries(project):
    ctx = project
    user(ctx)
    with pytest.raises(UsageError, match="kind=SERVICE"):
        tokens.create_token(ctx, "alice", ApiRole.VIEWER)
    service = tokens.create_token(ctx, "worker", ApiRole.OPERATOR)
    with pytest.raises(UsageError, match="sign-in refused"):
        sessions.sign_in(ctx, "worker", PASSWORD)
    with pytest.raises(UsageError, match="wrong kind"):
        accounts.reset_password(ctx, "worker", PASSWORD)
    assert tokens.authenticate(ctx, service.token)
    bad = accounts.Account(0, "bad", AccountKind.SERVICE, ApiRole.VIEWER, None, None, YesNo.YES)
    with pytest.raises(UsageError, match="SERVICE expects a token"):
        accounts.add_account(ctx, bad, PASSWORD)
    empty = replace(bad, kind=AccountKind.USER)
    with pytest.raises(UsageError, match="USER requires a password"):
        accounts.add_account(ctx, empty)
    session = sessions.sign_in(ctx, "alice", PASSWORD)
    with ctx.engine.connect() as conn:
        hashes = (
            conn.execute(text("SELECT TOKEN_SHA256 AS digest FROM CFG_API_TOKENS")).scalars().all()
        )
        recorded = conn.execute(
            text(
                "SELECT ACTOR AS actor, ARGUMENTS AS arguments FROM AUD_ACTIONS "
                "WHERE COMMAND='login' ORDER BY ACTION_ID"
            )
        ).all()
    assert session.token not in hashes
    assert all(len(digest) == 64 for digest in hashes)
    assert recorded[-1].actor == "alice"
    assert PASSWORD not in str(recorded) and session.token not in str(recorded)


def test_cli_token_administration_cannot_escalate_a_viewer(project, monkeypatch, capsys):
    ctx = project
    viewer = tokens.create_token(ctx, "reader", ApiRole.VIEWER)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", viewer.token)
    assert main(["token", "create", "--name", "elevated", "--role", "admin"]) == 26
    assert "admin role is required" in capsys.readouterr().err
    assert not any(a.name == "elevated" for a in accounts.list_accounts(ctx).accounts)
    assert main(["token", "list"]) == 26
    assert "admin role is required" in capsys.readouterr().err


def test_remote_requester_reason_is_audited_without_an_intervention(project, monkeypatch, capsys):
    from etl_craft.config import resolve_config_path
    from etl_craft.core.actor import acting_as
    from fixtures.metadata import add_pipeline

    ctx = project
    with acting_as(ctx.actor), ctx.engine.begin() as conn:
        add_pipeline(conn, "P")
    path = resolve_config_path()
    raw = yaml.safe_load(path.read_text())
    raw["Orchestration"]["Mode"] = "remote"
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    service = tokens.create_token(ctx, "airflow", ApiRole.OPERATOR)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", service.token)
    assert (
        main(
            [
                "run",
                "--pipeline_code",
                "P",
                "--init-only",
                "--run-key",
                "orchestrator:one",
                "--reason",
                "Airflow user: alice",
            ]
        )
        == 0
    ), capsys.readouterr()
    with ctx.engine.connect() as conn:
        action = conn.execute(
            text(
                "SELECT ACTOR AS actor, ARGUMENTS AS arguments FROM AUD_ACTIONS "
                "WHERE COMMAND='run' ORDER BY ACTION_ID DESC LIMIT 1"
            )
        ).one()
        assert action.actor == "airflow"
        arguments = (
            json.loads(action.arguments) if isinstance(action.arguments, str) else action.arguments
        )
        assert arguments["reason"] == "Airflow user: alice"
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM AUD_RUN_INTERVENTIONS")).scalar_one()
            == 0
        )
