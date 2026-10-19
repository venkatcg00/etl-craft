"""Team configuration is independent of package guards, audit history and config files."""

import sqlite3
from dataclasses import replace
from pathlib import Path

import psycopg
import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError

from etl_craft.core.errors import MigrationError
from etl_craft.dialects.engine.tables import CONFIG_TABLES
from etl_craft.engine import migrations
from etl_craft.services.config_files import export_config
from fixtures.engine_db import POSTGRES_PASSWORD, POSTGRES_USER
from fixtures.released_schema import install
from fixtures.services import require


def external_connection(db):
    if db.engine.dialect.name == "sqlite":
        return sqlite3.connect(db.engine.url.database)
    service = require("postgres")
    connection = psycopg.connect(
        host=service.host,
        port=service.port,
        dbname=db.engine.url.database,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    connection.execute(f'SET search_path TO "{db.config.engine.schema or "public"}"')
    connection.commit()
    return connection


@pytest.fixture
def plain(engine_db):
    connection = external_connection(engine_db)
    try:
        yield connection
    finally:
        connection.close()


def test_team_table_has_no_contract_actor_or_history(engine_db, plain, tmp_path):
    with engine_db.engine.begin() as conn:
        migrations.mark_packaged_migrations_applied(conn)
    project = tmp_path / "migrations"
    project.mkdir()
    (project / "0001_team.sql").write_text(
        "CREATE TABLE CFG_TEAM (VALUE VARCHAR); INSERT INTO CFG_TEAM VALUES ('team');"
    )
    migrations.apply_pending_migrations(engine_db.engine, project)
    plain.execute("INSERT INTO CFG_TEAM VALUES ('outside')")
    plain.execute("UPDATE CFG_TEAM SET VALUE='updated' WHERE VALUE='team'")
    plain.commit()
    with engine_db.engine.connect() as conn:
        assert {r.value for r in conn.execute(text("SELECT VALUE AS value FROM CFG_TEAM"))} == {
            "updated",
            "outside",
        }
        assert "AUD_CFG_TEAM_HISTORY" not in {t.upper() for t in inspect(conn).get_table_names()}
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS n FROM AUD_METADATA_CHANGES WHERE TABLE_NAME='CFG_TEAM'")
            ).scalar_one()
            == 0
        )
    out = tmp_path / "config"
    export_config(engine_db.engine, out)
    assert not any("TEAM" in path.name.upper() for path in out.iterdir())
    with pytest.raises(
        (sqlite3.DatabaseError, psycopg.Error), match=r"written only by etl-craft|no such function"
    ):
        plain.execute(
            "INSERT INTO CFG_PIPELINES (PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE) "
            "VALUES ('outside','outside','FULL')"
        )
    plain.rollback()


@pytest.mark.parametrize(
    "release",
    sorted(
        p.name for p in (Path(__file__).parents[2] / "fixtures/schemas").iterdir() if p.is_dir()
    ),
)
def test_released_schema_upgrades_with_a_team_table_without_base_fields(empty_engine_db, release):
    db = empty_engine_db
    install(db, release)
    with db.engine.begin() as conn:
        conn.execute(text("CREATE TABLE CFG_TEAM (PASSWORD_HASH VARCHAR)"))
        conn.execute(text("INSERT INTO CFG_TEAM VALUES ('team-owned')"))
    assert migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_TEAM SET PASSWORD_HASH='retained'"))
        assert (
            conn.execute(text("SELECT PASSWORD_HASH AS value FROM CFG_TEAM")).scalar_one()
            == "retained"
        )
        assert "AUD_CFG_TEAM_HISTORY" not in {t.upper() for t in inspect(conn).get_table_names()}


def test_a_project_audit_table_is_refused_and_its_file_rolls_back(engine_db, tmp_path):
    with engine_db.engine.begin() as conn:
        migrations.mark_packaged_migrations_applied(conn)
    project = tmp_path / "migrations"
    project.mkdir()
    (project / "0001_team.sql").write_text(
        "CREATE TABLE CFG_TEAM (VALUE VARCHAR); CREATE TABLE AUD_TEAM (VALUE VARCHAR);"
    )
    with pytest.raises(MigrationError, match=r"(?i)AUD_TEAM.*reserved.*rename"):
        migrations.apply_pending_migrations(engine_db.engine, project)
    with engine_db.engine.connect() as conn:
        assert "CFG_TEAM" not in {t.upper() for t in inspect(conn).get_table_names()}


def test_table_name_collision_refuses_before_an_earlier_pending_migration_writes(empty_engine_db):
    db = empty_engine_db
    install(db, "0.3.0")
    with db.engine.begin() as conn:
        conn.execute(text("CREATE TABLE CFG_ACCOUNTS (TEAM_VALUE VARCHAR)"))
        before = conn.execute(text("SELECT COUNT(*) AS n FROM SCHEMA_MIGRATIONS")).scalar_one()
    with pytest.raises(
        MigrationError, match=r"0034_accounts.sql.*CFG_ACCOUNTS.*rename.*No pending"
    ):
        migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) AS n FROM SCHEMA_MIGRATIONS")).scalar_one() == before
        )
        assert {
            c["name"].upper()
            for c in inspect(conn).get_columns(
                "cfg_accounts" if conn.dialect.name == "postgresql" else "CFG_ACCOUNTS"
            )
        } == {"TEAM_VALUE"}


@pytest.mark.unit
def test_package_config_manifest_matches_the_current_schema():
    import re

    root = Path(__file__).parents[3] / "src/etl_craft/dialects/engine"
    for dialect in ("sqlite", "postgres"):
        declared = set(
            re.findall(
                r"^CREATE TABLE (CFG_\w+)",
                (root / dialect / "schema.sql").read_text(),
                re.MULTILINE,
            )
        )
        assert declared == CONFIG_TABLES


def test_upgrade_releases_generated_team_triggers_but_freezes_old_history(
    empty_engine_db, monkeypatch
):
    db = empty_engine_db
    install(db, "0.3.0")
    streams = migrations.migration_streams

    def before_team_tables(*args, **kwargs):
        return [
            replace(s, files=tuple(f for f in s.files if f.version < "0040"))
            for s in streams(*args, **kwargs)
        ]

    with monkeypatch.context() as older:
        older.setattr(migrations, "migration_streams", before_team_tables)
        migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        conn.execute(text("CREATE TABLE CFG_TEAM (ID BIGINT PRIMARY KEY, VALUE VARCHAR)"))
        conn.execute(text("INSERT INTO CFG_TEAM VALUES (1, 'original')"))
        conn.execute(
            text(
                "CREATE TABLE AUD_CFG_TEAM_HISTORY (CHANGE_ID BIGINT PRIMARY KEY, "
                "VALID_FROM TIMESTAMP, VALID_TO TIMESTAMP, CURRENT_FLAG VARCHAR, OPERATION "
                "VARCHAR, ACTOR VARCHAR, ACTOR_KIND VARCHAR, MIGRATION VARCHAR, ID BIGINT, "
                "VALUE VARCHAR)"
            )
        )
        conn.execute(
            text(
                "INSERT INTO AUD_CFG_TEAM_HISTORY (CHANGE_ID,ID,VALUE) VALUES (800,1,'historical')"
            )
        )
        if db.engine.dialect.name == "sqlite":
            conn.exec_driver_sql(
                "CREATE TRIGGER trg_actor_guard_cfg_team_insert BEFORE INSERT ON CFG_TEAM "
                "BEGIN SELECT etl_craft_actor(); END"
            )
            conn.exec_driver_sql(
                "CREATE TRIGGER trg_actor_guard_aud_cfg_team_history_insert BEFORE INSERT "
                "ON AUD_CFG_TEAM_HISTORY BEGIN SELECT etl_craft_actor(); END"
            )
            conn.exec_driver_sql(
                "CREATE TRIGGER trg_audit_cfg_team_update AFTER UPDATE ON CFG_TEAM WHEN "
                "NEW.VALUE <> 'team-stamped' BEGIN UPDATE CFG_TEAM SET VALUE='team-stamped' "
                "WHERE ID=NEW.ID; END"
            )
        else:
            conn.exec_driver_sql(
                "CREATE TRIGGER trg_actor_guard_cfg_team BEFORE INSERT OR UPDATE OR DELETE "
                "ON CFG_TEAM FOR EACH ROW EXECUTE FUNCTION etl_craft_guard()"
            )
            conn.exec_driver_sql(
                "CREATE TRIGGER trg_actor_guard_aud_cfg_team_history BEFORE INSERT OR "
                "UPDATE OR DELETE ON AUD_CFG_TEAM_HISTORY FOR EACH ROW EXECUTE FUNCTION "
                "etl_craft_guard()"
            )
            conn.exec_driver_sql(
                "CREATE FUNCTION team_stamp() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                "NEW.VALUE='team-stamped'; RETURN NEW; END; $$"
            )
            conn.exec_driver_sql(
                "CREATE TRIGGER trg_audit_cfg_team BEFORE UPDATE ON CFG_TEAM FOR EACH ROW "
                "EXECUTE FUNCTION team_stamp()"
            )
    assert "0040_team_config_tables.sql" in migrations.apply_pending_migrations(db.engine)
    connection = external_connection(db)
    try:
        connection.execute("INSERT INTO CFG_TEAM VALUES (2,'new')")
        connection.execute("UPDATE CFG_TEAM SET VALUE='changed' WHERE ID=2")
        connection.commit()
    finally:
        connection.close()
    with db.engine.begin() as conn:
        assert (
            conn.execute(text("SELECT VALUE AS value FROM CFG_TEAM WHERE ID=2")).scalar_one()
            == "team-stamped"
        )
        assert (
            conn.execute(
                text("SELECT VALUE AS value FROM AUD_CFG_TEAM_HISTORY WHERE CHANGE_ID=800")
            ).scalar_one()
            == "historical"
        )
        assert (
            conn.execute(text("SELECT COUNT(*) AS n FROM AUD_CFG_TEAM_HISTORY")).scalar_one() == 1
        )
    with pytest.raises(DBAPIError, match="read-only"), db.engine.begin() as conn:
        conn.execute(text("INSERT INTO AUD_CFG_TEAM_HISTORY (CHANGE_ID) VALUES (801)"))
    # Refreshing again must recognize the frozen archive and leave the team's trigger intact.
    with db.engine.begin() as conn:
        db.dialect.refresh_metadata_triggers(conn)
        conn.execute(text("UPDATE CFG_TEAM SET VALUE='again' WHERE ID=2"))
        assert (
            conn.execute(text("SELECT VALUE AS value FROM CFG_TEAM WHERE ID=2")).scalar_one()
            == "team-stamped"
        )


def test_legacy_team_json_history_is_converted_without_losing_quoted_field_values(empty_engine_db):
    import json

    db = empty_engine_db
    install(db, "0.3.0")
    with db.engine.begin() as conn:
        conn.execute(text('CREATE TABLE CFG_TEAM (ID BIGINT PRIMARY KEY, "weird""field" VARCHAR)'))
        conn.execute(text("INSERT INTO CFG_TEAM VALUES (1, 'retained')"))
        conn.execute(
            text(
                "INSERT INTO AUD_METADATA_CHANGES "
                "(ACTOR,ACTOR_KIND,TABLE_NAME,ROW_KEY,OPERATION,AFTER_JSON) "
                "VALUES ('old','SYSTEM','CFG_TEAM','1','INSERT',:payload)"
            ),
            {"payload": json.dumps({"id": 1, 'weird"field': "retained"})},
        )
    migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        row = conn.execute(
            text(
                'SELECT ID AS id, "weird""field" AS value, ACTOR AS actor FROM AUD_CFG_TEAM_HISTORY'
            )
        ).one()
        assert row.id == 1 and row.value == "retained" and row.actor == "old"
    with pytest.raises(DBAPIError, match="read-only"), db.engine.begin() as conn:
        conn.execute(text("DELETE FROM AUD_CFG_TEAM_HISTORY"))


def test_a_retired_package_config_name_is_available_to_the_team_after_upgrade(
    engine_db, plain, tmp_path
):
    with engine_db.engine.begin() as conn:
        migrations.mark_packaged_migrations_applied(conn)
        before = conn.execute(
            text("SELECT COUNT(*) AS n FROM AUD_CFG_SOURCE_COLUMNS_HISTORY")
        ).scalar_one()
    project = tmp_path / "migrations"
    project.mkdir()
    (project / "0001_team.sql").write_text("CREATE TABLE CFG_SOURCE_COLUMNS (TEAM_VALUE VARCHAR);")
    migrations.apply_pending_migrations(engine_db.engine, project)
    plain.execute("INSERT INTO CFG_SOURCE_COLUMNS VALUES ('team-owned')")
    plain.commit()
    with engine_db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT TEAM_VALUE AS value FROM CFG_SOURCE_COLUMNS")).scalar_one()
            == "team-owned"
        )
        assert (
            conn.execute(
                text("SELECT COUNT(*) AS n FROM AUD_CFG_SOURCE_COLUMNS_HISTORY")
            ).scalar_one()
            == before
        )
