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
from etl_craft.core.enums import AccountKind, ActionName, ApiRole, YesNo
from etl_craft.core.errors import PermissionDeniedError, UsageError
from etl_craft.services import credentials
from etl_craft.services.oidc import IdentityClaims
from etl_craft.services.operations import OperationContext, accounts, sessions, tokens
from fixtures.actions import act

PASSWORD = "correct horse battery"


@pytest.fixture
def project(empty_engine_db, tmp_path, monkeypatch, cli_logging):
    from etl_craft.engine.schema import init_db

    db = empty_engine_db
    init_db(db.engine)
    profile = db.config.engine
    block = {"Jdbc_url": profile.jdbc_url, "Schema": profile.schema or "main"}
    if profile.auth_mode != "NONE":
        block.update(User=profile.user, Auth_mode=profile.auth_mode, Secret=profile.secret_var)
    path = tmp_path / "craft-connector.yml"
    path.write_text(
        yaml.safe_dump(
            {
                "Secrets": {"Source_type": "ENVIRONMENT"},
                "Orchestration": {"Project": "default"},
                "Engine": {"dev": block},
            },
            sort_keys=False,
        )
    )
    monkeypatch.setenv("ETL_CRAFT_CONFIG", str(path))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "person"))
    monkeypatch.delenv("ETL_CRAFT_TOKEN", raising=False)
    ctx = OperationContext(db.engine, db.config, SYSTEM_ACTOR)
    act(
        replace(ctx, provisioning=True),
        ActionName.ACCOUNT_ADD,
        name="owner",
        role=ApiRole.ADMIN,
        password=PASSWORD,
        recovery=True,
    )
    return replace(ctx, actor=Actor("owner", ActorKind.USER), role=ApiRole.ADMIN)


def user(ctx, name="alice", subject=None):
    act(
        ctx,
        ActionName.ACCOUNT_ADD,
        name=name,
        role=ApiRole.VIEWER,
        oidc_subject=subject,
        password=None if subject else PASSWORD,
    )


def test_sessions_expire_revoke_and_follow_current_account_roles(project):
    ctx = project
    user(ctx)
    with pytest.raises(UsageError, match="sign-in refused"):
        act(ctx, ActionName.LOGIN, name="alice", password="wrong")
    created = act(ctx, ActionName.LOGIN, name="alice", password=PASSWORD)
    assert tokens.authenticate(ctx, created.access_token).name == "alice"
    assert created.metadata.expires_at <= datetime.now(UTC) + timedelta(hours=12)
    act(ctx, ActionName.ACCOUNT_SET_ROLE, name="alice", role=ApiRole.OPERATOR)
    assert tokens.authenticate(ctx, created.access_token).role == ApiRole.OPERATOR
    act(ctx, ActionName.ACCOUNT_RESET_PASSWORD, name="alice", password="new password for alice")
    assert tokens.authenticate(ctx, created.access_token) is None
    with pytest.raises(UsageError):
        act(ctx, ActionName.LOGIN, name="alice", password=PASSWORD)
    other = act(ctx, ActionName.LOGIN, name="alice", password="new password for alice")
    act(ctx, ActionName.LOGOUT, token=other.access_token)
    assert tokens.authenticate(ctx, other.access_token) is None
    last = act(ctx, ActionName.LOGIN, name="alice", password="new password for alice")
    act(ctx, ActionName.ACCOUNT_DISABLE, name="alice")
    assert tokens.authenticate(ctx, last.access_token) is None
    with pytest.raises(UsageError):
        act(ctx, ActionName.LOGIN, name="alice", password="new password for alice")
    assert any(
        a.name == "alice" and a.active_flag == YesNo.NO
        for a in act(ctx, ActionName.ACCOUNT_LIST).accounts
    )


def test_sso_requires_a_registered_active_user(project):
    ctx = project
    with pytest.raises(UsageError, match="no active registered"):
        sessions.sign_in_subject(ctx, IdentityClaims("external-id"))
    user(ctx, subject="external-id")
    session = sessions.sign_in_subject(ctx, IdentityClaims("external-id"))
    assert tokens.authenticate(ctx, session.access_token).name == "alice"
    act(ctx, ActionName.ACCOUNT_DISABLE, name="alice")
    with pytest.raises(UsageError, match="no active registered"):
        sessions.sign_in_subject(ctx, IdentityClaims("external-id"))


def test_account_controls_require_a_proven_admin(project):
    ctx = project
    with pytest.raises(PermissionDeniedError, match="admin role is required"):
        act(replace(ctx, role=None), ActionName.ACCOUNT_LIST)
    with pytest.raises(PermissionDeniedError, match="admin role is required"):
        act(replace(ctx, role=ApiRole.VIEWER), ActionName.ACCOUNT_DISABLE, name="owner")
    with pytest.raises(UsageError, match="active admin already exists"):
        act(
            ctx,
            ActionName.ACCOUNT_ADD,
            name="recovery",
            role=ApiRole.ADMIN,
            password=PASSWORD,
            recovery=True,
        )
    with pytest.raises(UsageError, match="already exists"):
        act(ctx, ActionName.ACCOUNT_ADD, name="owner", role=ApiRole.ADMIN, password=PASSWORD)
    with pytest.raises(UsageError, match="not found"):
        act(ctx, ActionName.ACCOUNT_SET_ROLE, name="missing", role=ApiRole.VIEWER)


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
    session = act(ctx, ActionName.LOGIN, name="owner", password=PASSWORD)
    credentials.store_session(
        credentials.database_key(ctx.engine, ctx.config.engine.schema), session.access_token
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
    service = act(ctx, ActionName.TOKEN_CREATE, name="automation", role=ApiRole.ADMIN)
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
    act(ctx, ActionName.ACCOUNT_DISABLE, name="owner")
    act(ctx, ActionName.ACCOUNT_DISABLE, name="automation")
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
    session = act(ctx, ActionName.LOGIN, name="recovery-owner", password=PASSWORD)
    assert tokens.authenticate(ctx, session.access_token).role == ApiRole.ADMIN
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
            return act(ctx, ActionName.LOGIN, name="alice", password=PASSWORD).access_token
        except UsageError:
            return None

    def reset():
        act(ctx, ActionName.ACCOUNT_RESET_PASSWORD, name="alice", password="new password for alice")

    token, _ = two_at_once(login, reset, at="etl_craft.engine.locks.for_engine")
    assert token is None or tokens.authenticate(ctx, token) is None
    assert (
        act(ctx, ActionName.LOGIN, name="alice", password="new password for alice").metadata.name
        == "alice"
    )


def test_two_recoveries_create_only_one_admin(engine_db):
    from fixtures.races import two_at_once

    ctx = OperationContext(engine_db.engine, engine_db.config, SYSTEM_ACTOR, provisioning=True)

    def recover(name):
        try:
            act(
                ctx,
                ActionName.ACCOUNT_ADD,
                name=name,
                role=ApiRole.ADMIN,
                oidc_subject="sso-" + name,
                recovery=True,
            )
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
        act(ctx, ActionName.TOKEN_CREATE, name="alice", role=ApiRole.VIEWER)
    service = act(ctx, ActionName.TOKEN_CREATE, name="agent", role=ApiRole.OPERATOR)
    with pytest.raises(UsageError, match="sign-in refused"):
        act(ctx, ActionName.LOGIN, name="agent", password=PASSWORD)
    with pytest.raises(UsageError, match="wrong kind"):
        act(ctx, ActionName.ACCOUNT_RESET_PASSWORD, name="agent", password=PASSWORD)
    assert tokens.authenticate(ctx, service.token)
    with pytest.raises(UsageError, match="SERVICE expects a token"):
        accounts.AccountAdd(
            name="bad", kind=AccountKind.SERVICE, role=ApiRole.VIEWER, password=PASSWORD
        )
    with pytest.raises(UsageError, match="USER requires `password`"):
        accounts.AccountAdd(name="bad", role=ApiRole.VIEWER)
    session = act(ctx, ActionName.LOGIN, name="alice", password=PASSWORD)
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
    assert session.access_token not in hashes
    assert all(len(digest) == 64 for digest in hashes)
    assert recorded[-1].actor == "alice"
    assert PASSWORD not in str(recorded) and session.access_token not in str(recorded)


def test_cli_token_administration_cannot_escalate_a_viewer(project, monkeypatch, capsys):
    ctx = project
    viewer = act(ctx, ActionName.TOKEN_CREATE, name="reader", role=ApiRole.VIEWER)
    monkeypatch.setenv("ETL_CRAFT_TOKEN", viewer.token)
    assert main(["token", "create", "--name", "elevated", "--role", "admin"]) == 26
    assert "admin role is required" in capsys.readouterr().err
    assert not any(a.name == "elevated" for a in act(ctx, ActionName.ACCOUNT_LIST).accounts)
    assert main(["token", "list"]) == 26
    assert "admin role is required" in capsys.readouterr().err


def test_a_refused_password_costs_one_check_whether_or_not_the_account_exists(project, monkeypatch):
    ctx = project
    user(ctx, "sso-only", subject="external-id")
    act(ctx, ActionName.TOKEN_CREATE, name="robot", role=ApiRole.VIEWER)
    checks = []
    real = sessions.verify_password
    monkeypatch.setattr(
        sessions,
        "verify_password",
        lambda password, encoded: checks.append(encoded) or real(password, encoded),
    )
    for name in ("nobody", "robot", "sso-only", "owner"):
        with pytest.raises(UsageError, match="sign-in refused"):
            act(ctx, ActionName.LOGIN, name=name, password="not the right password")
    assert len(checks) == 4
    assert checks[:3] == [sessions.DECOY] * 3 and checks[3] != sessions.DECOY
