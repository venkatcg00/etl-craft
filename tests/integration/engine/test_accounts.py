"""Account-owned credentials and lossless upgrades on both Engine DBs."""

from dataclasses import replace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError

from etl_craft.api.app import create_app
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.core.errors import MigrationError, UsageError
from etl_craft.core.text import sha256_hex
from etl_craft.engine import migrations
from etl_craft.services.operations import OperationContext, tokens
from fixtures.actions import act
from fixtures.released_schema import SCHEMAS, install

OWNER = Actor("owner", ActorKind.USER)


def context(db):
    return OperationContext(db.engine, db.config, OWNER, role=ApiRole.ADMIN)


def test_tokens_share_current_account_role_and_disable_together(engine_db):
    ctx = context(engine_db)
    first = act(ctx, ActionName.TOKEN_CREATE, name="ci", role="viewer")
    second = act(ctx, ActionName.TOKEN_CREATE, name="ci", role="viewer")
    client = TestClient(create_app(ctx))
    headers = {"Authorization": "Bearer " + first.token}
    assert client.get("/api/v1/tokens", headers=headers).status_code == 403
    with acting_as(OWNER), ctx.engine.begin() as conn:
        assert conn.execute(text("SELECT COUNT(*) AS count FROM CFG_ACCOUNTS")).scalar_one() == 1
        conn.execute(text("UPDATE CFG_ACCOUNTS SET ROLE='admin' WHERE ACCOUNT_NAME='ci'"))
    assert client.get("/api/v1/tokens", headers=headers).status_code == 200
    assert tokens.authenticate(ctx, second.token).role == "admin"
    assert {t.role for t in act(ctx, ActionName.TOKEN_LIST).tokens} == {"admin"}
    with pytest.raises(UsageError, match=r"account 'ci'.*expected an active account"):
        act(ctx, ActionName.TOKEN_CREATE, name="ci", role="viewer")
    with acting_as(OWNER), ctx.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_ACCOUNTS SET ACTIVE_FLAG='N' WHERE ACCOUNT_NAME='ci'"))
    assert client.get("/api/v1/health", headers=headers).status_code == 401
    assert tokens.authenticate(ctx, second.token) is None
    with pytest.raises(UsageError, match="ACTIVE_FLAG='N'"):
        act(ctx, ActionName.TOKEN_CREATE, name="ci", role="admin")
    assert len(act(ctx, ActionName.TOKEN_LIST).tokens) == 2


def test_account_history_never_contains_password_hashes(engine_db):
    ctx = context(engine_db)
    act(ctx, ActionName.TOKEN_CREATE, name="person", role="developer")
    with acting_as(OWNER), ctx.engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_ACCOUNTS SET ACCOUNT_KIND='USER', PASSWORD_HASH=:hash, "
                "OIDC_SUBJECT='issuer:person' WHERE ACCOUNT_NAME='person'"
            ),
            {"hash": "scrypt:private-password-hash"},
        )
        conn.execute(text("UPDATE CFG_ACCOUNTS SET PASSWORD_HASH='scrypt:rotated'"))
    with ctx.engine.connect() as conn:
        columns = {
            c["name"].upper()
            for c in inspect(conn).get_columns(
                "aud_cfg_accounts_history"
                if conn.dialect.name == "postgresql"
                else "AUD_CFG_ACCOUNTS_HISTORY"
            )
        }
        assert "PASSWORD_HASH" not in columns
        versions = conn.execute(
            text(
                "SELECT CREATED_BY AS creator, UPDATED_BY AS updater, CURRENT_FLAG AS current_flag "
                "FROM AUD_CFG_ACCOUNTS_HISTORY ORDER BY CHANGE_ID"
            )
        ).all()
        assert len(versions) == 3
        assert all(v.creator == v.updater == "owner" for v in versions)
        assert [v.current_flag for v in versions] == ["N", "N", "Y"]
        evidence = str(
            conn.execute(
                text(
                    "SELECT BEFORE_JSON AS before_json, AFTER_JSON AS after_json "
                    "FROM AUD_METADATA_CHANGES WHERE TABLE_NAME='CFG_ACCOUNTS'"
                )
            ).all()
        )
        assert "scrypt:" not in evidence and "password_hash" not in evidence.lower()


@pytest.mark.parametrize(
    "change",
    [
        "ACCOUNT_KIND='SYSTEM'",
        "ROLE='root'",
        "ACTIVE_FLAG='X'",
        "PASSWORD_HASH='secret'",
        "ACCOUNT_NAME=''",
    ],
)
def test_account_constraints_refuse_invalid_values(engine_db, change):
    act(context(engine_db), ActionName.TOKEN_CREATE, name="ci", role="viewer")
    with engine_db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT ACCOUNT_NAME AS name FROM CFG_ACCOUNTS")).scalar_one() == "ci"
        )
    with pytest.raises(DBAPIError), acting_as(OWNER), engine_db.engine.begin() as conn:
        conn.execute(text(f"UPDATE CFG_ACCOUNTS SET {change}"))


def test_account_identity_is_reserved_and_tokens_require_an_account(engine_db):
    act(context(engine_db), ActionName.TOKEN_CREATE, name="ci", role="viewer")
    with acting_as(OWNER), engine_db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_ACCOUNTS SET ACTIVE_FLAG='N', OIDC_SUBJECT='subject'"))
    for values in ("'ci', NULL", "'other', 'subject'"):
        with pytest.raises(DBAPIError), acting_as(OWNER), engine_db.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO CFG_ACCOUNTS "
                    "(ACCOUNT_NAME, OIDC_SUBJECT, ACCOUNT_KIND, ROLE) VALUES ("
                    + values
                    + ", 'SERVICE', 'viewer')"
                )
            )
    with (
        pytest.raises(DBAPIError, match="cannot be deleted"),
        acting_as(OWNER),
        engine_db.engine.begin() as conn,
    ):
        conn.execute(text("DELETE FROM CFG_ACCOUNTS"))
    with pytest.raises(DBAPIError), acting_as(OWNER), engine_db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_API_TOKENS SET ACCOUNT_ID=-1"))
    with pytest.raises(DBAPIError), acting_as(OWNER), engine_db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_API_TOKENS SET ACCOUNT_ID=NULL"))


def before_accounts(db, release, monkeypatch):
    install(db, release)
    original = migrations.migration_streams

    def prefix(*args, **kwargs):
        return [
            replace(s, files=tuple(f for f in s.files if f.version < "0019"))
            for s in original(*args, **kwargs)
        ]

    with monkeypatch.context() as patch:
        patch.setattr(migrations, "migration_streams", prefix)
        migrations.apply_pending_migrations(db.engine)


@pytest.mark.parametrize("release", sorted(p.name for p in SCHEMAS.iterdir() if p.is_dir()))
def test_account_upgrade_preserves_legacy_credentials(
    empty_engine_db, release, monkeypatch, tmp_path
):
    db = empty_engine_db
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    before_accounts(db, release, monkeypatch)
    with acting_as(OWNER), db.engine.begin() as conn:
        for name, role, revoked in (("ci", "admin", None), ("old", "operator", "2026-01-01")):
            conn.execute(
                text(
                    "INSERT INTO CFG_API_TOKENS "
                    "(NAME,TOKEN_SHA256,ROLE,CREATED_BY,CREATED_AT,REVOKED_AT) "
                    "VALUES (:name,:digest,:role,'original','2026-01-01T00:00:00+00:00',:revoked)"
                ),
                {
                    "name": name,
                    "digest": sha256_hex(name.encode()),
                    "role": role,
                    "revoked": revoked,
                },
            )
    with acting_as(OWNER):
        assert "0034_accounts.sql" in migrations.apply_pending_migrations(db.engine)
    assert tokens.authenticate(context(db), "ci").role == "admin"
    assert tokens.authenticate(context(db), "old") is None
    with db.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT a.ACCOUNT_NAME AS name,a.ACCOUNT_KIND AS kind,a.ROLE AS role,"
                "t.CREATED_BY AS creator,t.TOKEN_ID AS token_id FROM CFG_API_TOKENS t "
                "JOIN CFG_ACCOUNTS a ON a.ACCOUNT_ID=t.ACCOUNT_ID ORDER BY t.TOKEN_ID"
            )
        ).all()
        assert [(r.name, r.kind, r.role, r.creator) for r in rows] == [
            ("ci", "SERVICE", "admin", "original"),
            ("old", "SERVICE", "operator", "original"),
        ]
        assert [r.token_id for r in rows] == [1, 2]
        versions = conn.execute(
            text(
                "SELECT ACCOUNT_ID AS account_id, MIGRATION AS migration "
                "FROM AUD_CFG_ACCOUNTS_HISTORY"
            )
        ).all()
        assert len(versions) == 2
        assert all(v.account_id and v.migration == "engine/0034_accounts.sql" for v in versions)
    assert migrations.apply_pending_migrations(db.engine) == []


@pytest.mark.parametrize("second_role", ["viewer", "admin"])
def test_duplicate_legacy_names_refuse_before_any_upgrade_write(
    empty_engine_db, monkeypatch, tmp_path, second_role
):
    db = empty_engine_db
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    before_accounts(db, "0.3.0", monkeypatch)
    with acting_as(OWNER), db.engine.begin() as conn:
        for i, role in enumerate(("viewer", second_role)):
            conn.execute(
                text(
                    "INSERT INTO CFG_API_TOKENS "
                    "(NAME,TOKEN_SHA256,ROLE,CREATED_BY,CREATED_AT,REVOKED_AT) "
                    "VALUES ('ci',:digest,:role,'original','2026-01-01T00:00:00+00:00',:revoked)"
                ),
                {
                    "digest": sha256_hex(str(i).encode()),
                    "role": role,
                    "revoked": None if i == 0 else "2026-01-01",
                },
            )
    with db.engine.connect() as conn:
        before = conn.execute(text("SELECT COUNT(*) AS count FROM SCHEMA_MIGRATIONS")).scalar_one()
    with pytest.raises(MigrationError) as failed:
        migrations.apply_pending_migrations(db.engine)
    message = str(failed.value)
    assert "'ci'" in message and "TOKEN_ID=1" in message and "TOKEN_ID=2" in message
    assert "ROLE=" + second_role in message and "Nothing was changed" in message
    assert "project migration" in message
    with db.engine.connect() as conn:
        assert not inspect(conn).has_table("cfg_accounts")
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM SCHEMA_MIGRATIONS")).scalar_one()
            == before
        )
        assert "role" in {
            c["name"].lower()
            for c in inspect(conn).get_columns(
                "cfg_api_tokens" if conn.dialect.name == "postgresql" else "CFG_API_TOKENS"
            )
        }


def test_account_migration_rolls_back_with_its_ledger(empty_engine_db, monkeypatch, tmp_path):
    db = empty_engine_db
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    before_accounts(db, "0.3.0", monkeypatch)
    original = migrations._record

    def refuse(conn, migration):
        if migration.version == "0034_accounts.sql":
            raise MigrationError("ledger refused")
        original(conn, migration)

    monkeypatch.setattr(migrations, "_record", refuse)
    with pytest.raises(MigrationError, match="ledger refused"):
        migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        inspector = inspect(conn)
        assert not inspector.has_table("cfg_accounts")
        columns = {
            c["name"].lower()
            for c in inspector.get_columns(
                "cfg_api_tokens" if conn.dialect.name == "postgresql" else "CFG_API_TOKENS"
            )
        }
        assert "role" in columns and "account_id" not in columns
        assert (
            conn.execute(
                text(
                    "SELECT COUNT(*) AS count FROM SCHEMA_MIGRATIONS "
                    "WHERE VERSION='0034_accounts.sql'"
                )
            ).scalar_one()
            == 0
        )


@pytest.mark.parametrize("name", ["", "ci\n"])
def test_invalid_legacy_actor_names_refuse_before_upgrade(
    empty_engine_db, monkeypatch, tmp_path, name
):
    db = empty_engine_db
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    before_accounts(db, "0.3.0", monkeypatch)
    with db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_API_TOKENS (NAME,TOKEN_SHA256,ROLE,CREATED_BY,CREATED_AT) "
                "VALUES (:name,:hash,'viewer','original','2026-01-01T00:00:00+00:00')"
            ),
            {"name": name, "hash": "0" * 64},
        )
        before = conn.execute(text("SELECT COUNT(*) AS count FROM SCHEMA_MIGRATIONS")).scalar_one()
    with pytest.raises(MigrationError) as failed:
        migrations.apply_pending_migrations(db.engine)
    message = str(failed.value)
    assert "TOKEN_ID=1" in message and repr(name) in message
    assert "invalid actor name" in message and "Nothing was changed" in message
    assert "project migration" in message
    with db.engine.connect() as conn:
        assert not inspect(conn).has_table("cfg_accounts")
        assert (
            conn.execute(text("SELECT COUNT(*) AS count FROM SCHEMA_MIGRATIONS")).scalar_one()
            == before
        )
