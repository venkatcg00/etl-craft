"""Every released schema preserves service expiry and safely binds old agent tokens."""

import shutil

import pytest
from sqlalchemy import text

from etl_craft.dialects.engine.base import EngineDialect
from etl_craft.engine import migrations
from fixtures.released_schema import SCHEMAS, install


@pytest.mark.parametrize(
    "release", sorted(path.name for path in SCHEMAS.iterdir() if path.is_dir())
)
def test_agent_tokens_upgrade_bind_latest_registration_and_revoke_unused(
    empty_engine_db, release, tmp_path, monkeypatch
):
    db = empty_engine_db
    install(db, release)
    previous = tmp_path / "previous"
    previous.mkdir()
    for file in db.dialect.migrations_dir().glob("*.sql"):
        if file.name < "0065":
            shutil.copyfile(file, previous / file.name)
    with monkeypatch.context() as patch:
        patch.setattr(EngineDialect, "migrations_dir", lambda self: previous)
        migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        conn.execute(text("INSERT INTO CFG_POOLS (POOL_NAME) VALUES ('latest_pool')"))
        account = conn.execute(
            text(
                "INSERT INTO CFG_ACCOUNTS (ACCOUNT_NAME,ACCOUNT_KIND,ROLE) VALUES "
                "('legacy','SERVICE','viewer') RETURNING ACCOUNT_ID AS id"
            )
        ).scalar_one()
        ids = {}
        for name, kind in (("registered", "AGENT"), ("unused", "AGENT"), ("service", "SERVICE")):
            ids[name] = conn.execute(
                text(
                    "INSERT INTO CFG_API_TOKENS (NAME,TOKEN_SHA256,ACCOUNT_ID,TOKEN_KI"
                    "ND) VALUES (:name,:digest,:account,:kind) RETURNING TOKEN_ID AS i"
                    "d"
                ),
                {"name": name, "digest": name * 4, "account": account, "kind": kind},
            ).scalar_one()
        conn.execute(
            text(
                "INSERT INTO AUD_AGENTS (AGENT_ID,POOL,HOST,VERSION,PYTHON_SLOTS,W"
                "AREHOUSE_SLOTS,STARTED_AT,HEARTBEAT_AT,STATUS,RESOURCE_REPORT) VA"
                "LUES (:id,'latest_pool','host','0.3.0',1,1,CURRENT_TIMESTAMP,CURR"
                "ENT_TIMESTAMP,'GONE','{}')"
            ),
            {"id": ids["registered"]},
        )
        old_history = conn.execute(
            text("SELECT COUNT(*) AS count FROM AUD_CFG_API_TOKENS_HISTORY")
        ).scalar_one()
    migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        rows = {
            row.name: row
            for row in conn.execute(
                text(
                    "SELECT NAME AS name,POOL AS pool,REVOKED_AT AS revoked_at,EXPIRES"
                    "_AT AS expires_at,LAST_USED_AT AS last_used_at,ACTIVE_FLAG AS act"
                    "ive_flag FROM CFG_API_TOKENS"
                )
            )
        }
        assert rows["registered"].pool == "latest_pool"
        assert rows["registered"].revoked_at is None
        assert rows["unused"].revoked_at is not None and rows["unused"].active_flag == "N"
        assert rows["service"].expires_at is None and rows["service"].revoked_at is None
        assert all(row.last_used_at is None for row in rows.values())
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS count FROM AUD_CFG_API_TOKENS_HISTORY")
            ).scalar_one()
            >= old_history
        )


def test_rebuilt_tokens_keep_deleted_identity_high_water(empty_engine_db, tmp_path, monkeypatch):

    db = empty_engine_db
    previous = tmp_path / "previous"
    previous.mkdir()
    for file in db.dialect.migrations_dir().glob("*.sql"):
        if file.name < "0065":
            shutil.copyfile(file, previous / file.name)
    with monkeypatch.context() as patch:
        patch.setattr(EngineDialect, "migrations_dir", lambda self: previous)
        # A fresh database stopped at the immediately preceding migration.
        install(db, "0.3.0")
        migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        account = conn.execute(
            text(
                "INSERT INTO CFG_ACCOUNTS (ACCOUNT_NAME,ACCOUNT_KIND,ROLE) "
                "VALUES ('deleted-token','SERVICE','viewer') RETURNING ACCOUNT_ID AS id"
            )
        ).scalar_one()
        old_id = conn.execute(
            text(
                "INSERT INTO CFG_API_TOKENS (NAME,TOKEN_SHA256,ACCOUNT_ID,TOKEN_KIND) "
                "VALUES ('gone','old-hash',:account,'SERVICE') RETURNING TOKEN_ID AS id"
            ),
            {"account": account},
        ).scalar_one()
        conn.execute(text("DELETE FROM CFG_API_TOKENS WHERE TOKEN_ID=:id"), {"id": old_id})
    migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        new_id = conn.execute(
            text(
                "INSERT INTO CFG_API_TOKENS (NAME,TOKEN_SHA256,ACCOUNT_ID,TOKEN_KIND) "
                "VALUES ('new','new-hash',:account,'SERVICE') RETURNING TOKEN_ID AS id"
            ),
            {"account": account},
        ).scalar_one()
    assert new_id > old_id
