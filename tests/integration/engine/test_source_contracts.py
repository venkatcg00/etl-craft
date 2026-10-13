"""Scoped source contracts preserve identities, evidence and historical exports."""

import csv
import time
from datetime import UTC, datetime

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from etl_craft.core.errors import MigrationError
from etl_craft.engine import migrations
from etl_craft.services.config_files import export_config, read_config_files, sync_config
from fixtures.catalog import snapshot
from fixtures.metadata import add_source_column
from fixtures.released_schema import install


@pytest.fixture
def legacy_sources_db(empty_engine_db, monkeypatch):
    db = empty_engine_db
    install(db, "0.2.0")
    monkeypatch.delenv("ETL_CRAFT_MIGRATIONS_DIR", raising=False)
    for file in migrations.migration_streams(db.engine)[0].files:
        if "0007" <= file.version < "0028":
            migrations._apply(db.engine, file)
    return db


def seed_legacy(db, second="analytics.raw.orders", column="name"):
    with db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_SOURCE_COLUMNS (TABLE_NAME,COLUMN_NAME,DATA_TYPE,COLUMN_POSITION) "
                "VALUES ('raw.orders','id','INT',1),(:table,:column,'TEXT',2)"
            ),
            {"table": second, "column": column},
        )


def test_conversion_preserves_ids_stamps_and_old_evidence(legacy_sources_db, tmp_path):
    db = legacy_sources_db
    seed_legacy(db)
    moment = datetime.now(UTC)
    with db.engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM CFG_SOURCE_COLUMNS ORDER BY SOURCE_COLUMN_ID")
        ).all()
        history = conn.execute(
            text("SELECT * FROM AUD_CFG_SOURCE_COLUMNS_HISTORY ORDER BY CHANGE_ID")
        ).all()
        evidence = conn.execute(
            text("SELECT * FROM AUD_METADATA_CHANGES WHERE TABLE_NAME='CFG_SOURCE_COLUMNS'")
        ).all()
    assert migrations.apply_pending_migrations(
        db.engine, catalog="analytics", warehouse_dialect="duckdb"
    ) == [
        "0028_source_contracts.sql",
        "0029_rule_target_contracts.sql",
        "0030_task_target_contracts.sql",
    ]
    with db.engine.connect() as conn:
        assert not inspect(conn).has_table(
            "CFG_SOURCE_COLUMNS" if conn.dialect.name == "sqlite" else "cfg_source_columns"
        )
        assert conn.execute(
            text("SELECT CATALOG_NAME,SCHEMA_NAME,TABLE_NAME FROM CFG_TABLES")
        ).one() == ("analytics", "raw", "orders")
        assert conn.execute(
            text(
                "SELECT COLUMN_ID,COLUMN_NAME,DATA_TYPE,COLUMN_POSITION,ACTIVE_FLA"
                "G,CREATED_BY,CREATE_DATE,UPDATED_BY,UPDATED_DATE FROM CFG_COLUMNS"
                " ORDER BY COLUMN_ID"
            )
        ).all() == [(row[0], *row[2:]) for row in rows]
        assert (
            conn.execute(
                text("SELECT * FROM AUD_CFG_SOURCE_COLUMNS_HISTORY ORDER BY CHANGE_ID")
            ).all()
            == history
        )
        assert (
            conn.execute(
                text("SELECT * FROM AUD_METADATA_CHANGES WHERE TABLE_NAME='CFG_SOURCE_COLUMNS'")
            ).all()
            == evidence
        )
        assert (
            conn.execute(
                text("SELECT * FROM AUD_SOURCE_COLUMNS_BASELINE ORDER BY SOURCE_COLUMN_ID")
            ).all()
            == rows
        )
    old = tmp_path / "old"
    export_config(db.engine, old, as_of=moment)
    assert {
        (row["CATALOG_NAME"], row["SCHEMA_NAME"], row["TABLE_NAME"])
        for row in csv.DictReader((old / "tables.csv").open())
    } == {("", "raw", "orders"), ("analytics", "raw", "orders")}
    new = tmp_path / "new"
    export_config(db.engine, new)
    assert not sync_config(db.engine, db.config, read_config_files(new), apply=False).changes
    with db.engine.begin() as conn:
        assert add_source_column(conn, "raw.next", "id", "INT", 1) > max(row[0] for row in rows)
        ids = conn.execute(text("SELECT CHANGE_ID FROM AUD_METADATA_CHANGES")).scalars().all()
        assert len(ids) == len(set(ids))


def test_duplicate_aliases_refuse_conversion_without_any_changes(legacy_sources_db):
    db = legacy_sources_db
    seed_legacy(db, column="id")
    before = snapshot(db.engine)
    with db.engine.connect() as conn:
        ledger = conn.execute(text("SELECT * FROM SCHEMA_MIGRATIONS")).all()
    with pytest.raises(MigrationError, match="SOURCE_COLUMN_IDs"):
        migrations.apply_pending_migrations(
            db.engine, catalog="analytics", warehouse_dialect="duckdb"
        )
    assert snapshot(db.engine) == before
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT * FROM SCHEMA_MIGRATIONS")).all() == ledger


@pytest.mark.parametrize(
    "ddl",
    [
        "ALTER TABLE CFG_SOURCE_COLUMNS ADD COLUMN CUSTOM_OWNER VARCHAR",
        "CREATE INDEX project_source_type ON CFG_SOURCE_COLUMNS (DATA_TYPE)",
        "CREATE VIEW project_source_names AS SELECT TABLE_NAME FROM CFG_SOURCE_COLUMNS",
    ],
)
def test_custom_source_schema_is_not_discarded(legacy_sources_db, ddl):
    db = legacy_sources_db
    with db.engine.begin() as conn:
        conn.exec_driver_sql(ddl)
    before = snapshot(db.engine)
    with pytest.raises(MigrationError, match="CFG_SOURCE_COLUMNS has"):
        migrations.apply_pending_migrations(db.engine)
    assert snapshot(db.engine) == before


def test_retirement_reserves_name_but_releases_active_ordinal(engine_db):
    db = engine_db
    with db.engine.begin() as conn:
        column_id = add_source_column(conn, "raw.orders", "id", "INT", 1)
        conn.execute(
            text("UPDATE CFG_COLUMNS SET ACTIVE_FLAG='N' WHERE COLUMN_ID=:id"), {"id": column_id}
        )
        add_source_column(conn, "raw.orders", "replacement", "INT", 1)
    with pytest.raises(IntegrityError), db.engine.begin() as conn:
        add_source_column(conn, "raw.orders", "id", "INT", 2)
    with pytest.raises(IntegrityError), db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_TABLES (SCHEMA_NAME,TABLE_NAME,ACTIVE_FLAG) "
                "VALUES ('raw','orders','N')"
            )
        )
    with pytest.raises(IntegrityError), db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO CFG_COLUMNS (TABLE_ID,COLUMN_NAME,DATA_TYPE,COLUMN_POSITION) "
                "VALUES (-1,'id','INT',1)"
            )
        )


def test_conversion_baseline_is_immutable(engine_db):
    with pytest.raises(DBAPIError, match="immutable"), engine_db.engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO AUD_SOURCE_COLUMNS_BASELINE (SOURCE_COLUMN_ID,TABLE_NAME,"
                "COLUMN_NAME,DATA_TYPE,COLUMN_POSITION,CREATED_BY,CREATE_DATE,UPDATED_BY,"
                "UPDATED_DATE) VALUES (1,'raw.orders','id','INT',1,'test',CURRENT_TIMESTAMP,"
                "'test',CURRENT_TIMESTAMP)"
            )
        )


def test_catalog_resolution_round_trip_keeps_parent_and_column_ids(
    engine_db, tmp_path, monkeypatch
):
    from etl_craft.engine.repository.source_columns import fetch_source_columns

    db = engine_db
    with db.engine.begin() as conn:
        column_id = add_source_column(conn, "raw.orders", "id", "INT", 1)
        table_id = conn.execute(text("SELECT TABLE_ID FROM CFG_TABLES")).scalar_one()
        definitions = fetch_source_columns(conn)
    monkeypatch.setattr(
        "etl_craft.services.lineage.dialect_and_catalog", lambda config: ("duckdb", "analytics")
    )
    monkeypatch.setattr("etl_craft.services.config_files.seed_sources", lambda *args: definitions)
    folder = tmp_path / "resolved"
    export_config(db.engine, folder, config=db.config)
    tables = list(csv.DictReader((folder / "tables.csv").open()))
    assert len(tables) == 1 and tables[0]["CATALOG_NAME"] == "analytics"
    files = read_config_files(folder)
    planned = sync_config(db.engine, db.config, files, apply=False)
    assert len(planned.changes) == 1 and planned.changes[0].columns[0].column == "CATALOG_NAME"
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT CATALOG_NAME FROM CFG_TABLES")).scalar_one() is None
    done = sync_config(db.engine, db.config, files, apply=True)
    assert done.applied and not done.failed
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT TABLE_ID,CATALOG_NAME FROM CFG_TABLES")).one() == (
            table_id,
            "analytics",
        )
        assert conn.execute(text("SELECT COLUMN_ID,TABLE_ID FROM CFG_COLUMNS")).one() == (
            column_id,
            table_id,
        )
    assert not sync_config(db.engine, db.config, files, apply=False).changes


def test_project_source_resolution_is_audited_before_commit(engine_db, tmp_path):
    db = engine_db
    with db.engine.begin() as conn:
        migrations.mark_packaged_migrations_applied(conn)
    folder = tmp_path / "migrations"
    folder.mkdir()
    (folder / "0001_source.sql").write_text(
        "INSERT INTO CFG_TABLES (SCHEMA_NAME,TABLE_NAME) VALUES ('raw','orders');\n"
        "INSERT INTO CFG_COLUMNS (TABLE_ID,COLUMN_NAME,DATA_TYPE,COLUMN_POSITION) "
        "SELECT TABLE_ID,'id','INT',1 FROM CFG_TABLES WHERE TABLE_NAME='orders';\n"
    )
    migrations.apply_pending_migrations(
        db.engine, folder, catalog="analytics", warehouse_dialect="duckdb"
    )
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT CATALOG_NAME FROM CFG_TABLES")).scalar_one() == "analytics"
        assert (
            conn.execute(
                text("SELECT MIGRATION FROM AUD_CFG_TABLES_HISTORY WHERE CURRENT_FLAG='Y'")
            ).scalar_one()
            == "0001_source.sql"
        )


def test_historical_export_keeps_sources_deleted_before_conversion(legacy_sources_db, tmp_path):
    db = legacy_sources_db
    seed_legacy(db, second="raw.orders")
    moment = datetime.now(UTC)
    time.sleep(0.01)  # Keep SQLite's millisecond history boundaries distinct.
    with db.engine.begin() as conn:
        conn.execute(text("DELETE FROM CFG_SOURCE_COLUMNS"))
    migrations.apply_pending_migrations(db.engine)
    with db.engine.connect() as conn:
        assert not conn.execute(text("SELECT 1 FROM AUD_SOURCE_COLUMNS_BASELINE")).first()
        assert not conn.execute(text("SELECT 1 FROM CFG_TABLES")).first()
    folder = tmp_path / "historical"
    export_config(db.engine, folder, as_of=moment)
    assert {row["COLUMN_NAME"] for row in csv.DictReader((folder / "columns.csv").open())} == {
        "id",
        "name",
    }


def test_malformed_historical_source_has_a_controlled_error(legacy_sources_db, tmp_path):
    from etl_craft.core.errors import MetadataError

    db = legacy_sources_db
    seed_legacy(db, second="raw.orders")
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_SOURCE_COLUMNS SET TABLE_NAME='orders'"))
    moment = datetime.now(UTC)
    time.sleep(0.01)  # Keep SQLite's millisecond history boundaries distinct.
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_SOURCE_COLUMNS SET TABLE_NAME='raw.orders'"))
    migrations.apply_pending_migrations(db.engine)
    with pytest.raises(MetadataError, match=r"SOURCE_COLUMN_ID.*retained legacy"):
        export_config(db.engine, tmp_path / "historical", as_of=moment)


def test_conversion_reserves_deleted_source_ids_and_counter(legacy_sources_db):
    db = legacy_sources_db
    seed_legacy(db, second="raw.orders")
    with db.engine.begin() as conn:
        if conn.dialect.name == "sqlite":
            conn.execute(text("UPDATE sqlite_sequence SET seq=100 WHERE name='CFG_SOURCE_COLUMNS'"))
        else:
            conn.execute(
                text(
                    "SELECT setval(pg_get_serial_sequence('cfg_source_columns',"
                    "'source_column_id'),100,true)"
                )
            )
        conn.execute(text("DELETE FROM CFG_SOURCE_COLUMNS"))
    migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        assert add_source_column(conn, "raw.orders", "fresh", "INT", 1) == 101


def test_failed_conversion_rolls_back_schema_rows_history_and_ledger(legacy_sources_db):
    db = legacy_sources_db
    seed_legacy(db, second="raw.orders")
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_SOURCE_COLUMNS SET DATA_TYPE='' WHERE COLUMN_NAME='id'"))
    before = snapshot(db.engine)
    with db.engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM CFG_SOURCE_COLUMNS ORDER BY SOURCE_COLUMN_ID")
        ).all()
        evidence = conn.execute(text("SELECT * FROM AUD_METADATA_CHANGES ORDER BY CHANGE_ID")).all()
        ledger = conn.execute(text("SELECT * FROM SCHEMA_MIGRATIONS ORDER BY SOURCE,VERSION")).all()
    with pytest.raises(MigrationError, match=r"0028_source_contracts\.sql failed"):
        migrations.apply_pending_migrations(db.engine)
    assert snapshot(db.engine) == before
    with db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT * FROM CFG_SOURCE_COLUMNS ORDER BY SOURCE_COLUMN_ID")).all()
            == rows
        )
        assert (
            conn.execute(text("SELECT * FROM AUD_METADATA_CHANGES ORDER BY CHANGE_ID")).all()
            == evidence
        )
        assert (
            conn.execute(text("SELECT * FROM SCHEMA_MIGRATIONS ORDER BY SOURCE,VERSION")).all()
            == ledger
        )
