"""Common config contracts, safe token history and atomic Engine DB upgrades."""

from dataclasses import replace

import pytest
from sqlalchemy import DateTime, inspect, text
from sqlalchemy.exc import DBAPIError

from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.enums import ApiRole
from etl_craft.core.errors import MigrationError
from etl_craft.core.text import sha256_hex
from etl_craft.core.time import as_utc
from etl_craft.engine import migrations
from etl_craft.services.operations import OperationContext, tokens
from fixtures.metadata import add_pipeline
from fixtures.released_schema import install

ALICE = Actor("alice", ActorKind.HUMAN)
BOB = Actor("bob", ActorKind.HUMAN)


def test_all_config_tables_have_mandatory_base_fields_and_one_key(engine_db):
    with engine_db.engine.connect() as conn:
        inspector = inspect(conn)
        for table in inspector.get_table_names():
            if not table.upper().startswith("CFG_"):
                continue
            columns = {column["name"].upper(): column for column in inspector.get_columns(table)}
            for name in ("CREATED_BY", "CREATE_DATE", "UPDATED_BY", "UPDATED_DATE", "ACTIVE_FLAG"):
                assert not columns[name]["nullable"], (table, name)
                if name.endswith("DATE"):
                    assert isinstance(columns[name]["type"], DateTime)
                    if conn.dialect.name == "postgresql":
                        assert columns[name]["type"].timezone
            assert len(inspector.get_pk_constraint(table)["constrained_columns"]) == 1


@pytest.mark.parametrize(
    "problem", ["missing", "nullable", "date_type", "no_key", "two_keys", "nullable_key"]
)
def test_invalid_project_config_contract_rolls_back(engine_db, tmp_path, problem):
    date = "TIMESTAMPTZ" if engine_db.engine.dialect.name == "postgresql" else "TIMESTAMP"
    columns = (
        "ID BIGINT PRIMARY KEY NOT NULL, CREATED_BY VARCHAR NOT NULL, "
        f"CREATE_DATE {date} NOT NULL, UPDATED_BY VARCHAR NOT NULL, "
        f"UPDATED_DATE {date} NOT NULL, ACTIVE_FLAG VARCHAR NOT NULL"
    )
    if problem == "missing":
        columns = "ID BIGINT PRIMARY KEY"
    elif problem == "nullable":
        columns = columns.replace("CREATED_BY VARCHAR NOT NULL", "CREATED_BY VARCHAR")
    elif problem == "date_type":
        columns = columns.replace(
            f"CREATE_DATE {date}",
            "CREATE_DATE TIMESTAMP" if date == "TIMESTAMPTZ" else "CREATE_DATE VARCHAR",
        )
    elif problem == "no_key":
        columns = columns.replace(" PRIMARY KEY", "")
    elif problem == "nullable_key":
        columns = columns.replace(
            "PRIMARY KEY NOT NULL", "PRIMARY KEY" if date == "TIMESTAMP" else ""
        )
    else:
        columns = columns.replace(" PRIMARY KEY", "") + ", OTHER BIGINT, PRIMARY KEY (ID, OTHER)"
    project = tmp_path / "migrations"
    project.mkdir()
    (project / "0001_config.sql").write_text(f"CREATE TABLE CFG_PROJECT ({columns});")
    with engine_db.engine.begin() as conn:
        migrations.mark_packaged_migrations_applied(conn)
    with pytest.raises(MigrationError, match="common config fields and key"):
        migrations.apply_pending_migrations(engine_db.engine, project)
    with engine_db.engine.connect() as conn:
        assert "CFG_PROJECT" not in {name.upper() for name in inspect(conn).get_table_names()}
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM SCHEMA_MIGRATIONS WHERE SOURCE='PROJECT'")
            ).scalar_one()
            == 0
        )


def test_token_lifecycle_is_attributed_without_credentials(engine_db):
    ctx = OperationContext(engine_db.engine, engine_db.config, ALICE, role=ApiRole.ADMIN)
    created = tokens.create_token(ctx, "dashboard", "viewer")
    assert tokens.authenticate(ctx, created.token)
    with acting_as(BOB), engine_db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_API_TOKENS SET ACTIVE_FLAG='N'"))
    assert tokens.authenticate(ctx, created.token) is None
    with acting_as(BOB), engine_db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_API_TOKENS SET ACTIVE_FLAG='Y'"))
    assert tokens.authenticate(ctx, created.token)
    rotated = "rotated-token"
    with acting_as(BOB), engine_db.engine.begin() as conn:
        conn.execute(
            text("UPDATE CFG_API_TOKENS SET TOKEN_SHA256=:digest"),
            {"digest": sha256_hex(rotated.encode())},
        )
    assert tokens.authenticate(ctx, created.token) is None
    assert tokens.authenticate(ctx, rotated)
    tokens.revoke_token(replace(ctx, actor=BOB), created.metadata.token_id)
    assert tokens.authenticate(ctx, created.token) is None
    with engine_db.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT CREATED_BY, CREATE_DATE, UPDATED_BY, ACTIVE_FLAG, ACTOR "
                "FROM AUD_CFG_API_TOKENS_HISTORY ORDER BY CHANGE_ID"
            )
        ).all()
        assert [(r[0], r[2], r[3], r[4]) for r in rows] == [
            ("alice", "alice", "Y", "alice"),
            ("alice", "bob", "N", "bob"),
            ("alice", "bob", "Y", "bob"),
            ("alice", "bob", "Y", "bob"),
            ("alice", "bob", "N", "bob"),
        ]
        assert {as_utc(row[1]) for row in rows} == {created.metadata.created_at}
        history = conn.execute(
            text(
                "SELECT BEFORE_JSON, AFTER_JSON FROM AUD_METADATA_CHANGES "
                "WHERE TABLE_NAME='CFG_API_TOKENS'"
            )
        ).all()
        assert created.token not in str(history)
        assert sha256_hex(created.token.encode()) not in str(history)
        assert rotated not in str(history)
        assert sha256_hex(rotated.encode()) not in str(history)
        assert "token_sha256" not in str(history).lower()
    for flag in (None, "x"):
        with pytest.raises(DBAPIError), engine_db.engine.begin() as conn:
            conn.execute(text("UPDATE CFG_API_TOKENS SET ACTIVE_FLAG=:flag"), {"flag": flag})


def test_upgrade_preserves_tokens_creation_history_and_rolls_back(empty_engine_db, monkeypatch):
    db = empty_engine_db
    install(db, "0.2.0")
    stream = migrations.migration_streams(db.engine)[0]
    with acting_as(ALICE):
        for migration in stream.files:
            if "0007" <= migration.version < "0025":
                migrations._apply(db.engine, migration)
        with db.engine.begin() as conn:
            pipeline = add_pipeline(conn, "P")
            conn.execute(
                text(
                    "INSERT INTO CFG_API_TOKENS (NAME,TOKEN_SHA256,ROLE,CREATED_BY,CREATED_AT) "
                    "VALUES ('old',:digest,'viewer','alice','2026-01-01 00:00:00')"
                ),
                {"digest": sha256_hex(b"old-token")},
            )
            conn.execute(
                text(
                    "INSERT INTO CFG_API_TOKENS "
                    "(NAME,TOKEN_SHA256,ROLE,CREATED_BY,CREATED_AT,REVOKED_AT) "
                    "VALUES ('revoked',:digest,'viewer','alice','2026-01-01 00:00:00',"
                    "'2026-02-01 00:00:00')"
                ),
                {"digest": sha256_hex(b"revoked-token")},
            )
            removed = conn.execute(
                text(
                    "INSERT INTO CFG_API_TOKENS (NAME,TOKEN_SHA256,ROLE,CREATED_BY,CREATED_AT) "
                    "VALUES ('removed',:digest,'viewer','alice','2026-01-01 00:00:00') "
                    "RETURNING TOKEN_ID"
                ),
                {"digest": sha256_hex(b"removed-token")},
            ).scalar_one()
            conn.execute(text("DELETE FROM CFG_API_TOKENS WHERE TOKEN_ID=:id"), {"id": removed})
            missing = add_pipeline(conn, "MISSING")
            trigger = (
                "DROP TRIGGER trg_audit_cfg_pipelines ON CFG_PIPELINES"
                if conn.dialect.name == "postgresql"
                else "DROP TRIGGER trg_audit_cfg_pipelines_update"
            )
            conn.exec_driver_sql(trigger)
            conn.execute(
                text(
                    "UPDATE CFG_PIPELINES SET CREATED_BY=NULL,CREATE_DATE=NULL,"
                    "UPDATED_BY=NULL,UPDATED_DATE=NULL WHERE PIPELINE_ID=:id"
                ),
                {"id": missing},
            )
            # Model token guards without a token history table.
            conn.exec_driver_sql("DROP VIEW AUD_METADATA_CHANGES")
            conn.exec_driver_sql("DROP TABLE AUD_CFG_API_TOKENS_HISTORY")
            if conn.dialect.name == "sqlite":
                conn.exec_driver_sql(
                    "CREATE INDEX custom_token_created ON CFG_API_TOKENS(CREATED_AT)"
                )
    original = migrations._record

    def refuse(conn, migration):
        if migration.version == "0026_config_base_contract.sql":
            raise MigrationError("ledger refused")
        original(conn, migration)

    monkeypatch.setattr(migrations, "_record", refuse)
    with pytest.raises(MigrationError, match="ledger refused"), acting_as(BOB):
        migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        assert "UPDATED_BY" not in {
            c["name"].upper()
            for c in inspect(conn).get_columns(
                "cfg_api_tokens" if conn.dialect.name == "postgresql" else "CFG_API_TOKENS"
            )
        }
        assert conn.execute(text("SELECT COUNT(*) FROM CFG_API_TOKENS")).scalar_one() == 2
    monkeypatch.setattr(migrations, "_record", original)
    with acting_as(BOB):
        applied = migrations.apply_pending_migrations(db.engine)
        assert {
            "0026_config_base_contract.sql",
            "0027_config_identities.sql",
            "0028_source_contracts.sql",
            "0029_rule_target_contracts.sql",
            "0030_task_target_contracts.sql",
        } <= set(applied)
        assert applied == sorted(applied)
    ctx = OperationContext(db.engine, db.config, BOB, role=ApiRole.ADMIN)
    assert tokens.authenticate(ctx, "old-token")
    assert tokens.authenticate(ctx, "revoked-token") is None
    assert tokens.create_token(ctx, "next", "viewer").metadata.token_id > removed
    with db.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT ACTIVE_FLAG FROM CFG_API_TOKENS WHERE NAME='revoked'")
            ).scalar_one()
            == "N"
        )
        assert (
            conn.execute(
                text("SELECT CREATED_BY FROM CFG_API_TOKENS WHERE NAME='old'")
            ).scalar_one()
            == "alice"
        )
        assert (
            as_utc(
                conn.execute(
                    text("SELECT CREATE_DATE FROM CFG_API_TOKENS WHERE NAME='old'")
                ).scalar_one()
            ).isoformat()
            == "2026-01-01T00:00:00+00:00"
        )
        versions = conn.execute(
            text(
                "SELECT VALID_FROM,VALID_TO,CREATED_BY,CREATE_DATE FROM AUD_CFG_PIPELINES_HISTORY "
                "WHERE PIPELINE_ID=:id ORDER BY CHANGE_ID"
            ),
            {"id": pipeline},
        ).all()
        assert len(versions) == 1
        assert versions[0][2] == "alice"
        filled = conn.execute(
            text(
                "SELECT CREATED_BY,CREATE_DATE,UPDATED_BY,UPDATED_DATE FROM CFG_PIPELINES "
                "WHERE PIPELINE_ID=:id"
            ),
            {"id": missing},
        ).one()
        assert filled[0] == filled[2] == "etl-craft-migration-0026"
        assert as_utc(filled[1]) and as_utc(filled[3])
        boundaries = conn.execute(
            text(
                "SELECT VALID_FROM,VALID_TO FROM AUD_CFG_PIPELINES_HISTORY "
                "WHERE PIPELINE_ID=:id ORDER BY CHANGE_ID"
            ),
            {"id": missing},
        ).all()
        assert as_utc(boundaries[-2][1]) == as_utc(boundaries[-1][0])
        if conn.dialect.name == "sqlite":
            assert conn.exec_driver_sql("PRAGMA foreign_key_check").all() == []
            assert (
                conn.exec_driver_sql(
                    "SELECT sql FROM sqlite_schema WHERE name='custom_token_created'"
                )
                .scalar_one()
                .upper()
                .count("CREATE_DATE")
                == 1
            )
