"""Source definitions and upstream schemas trace without a warehouse connection."""

from sqlalchemy import text

from etl_craft.services.catalog.collect import build_catalog
from etl_craft.services.config_files import export_config, read_config_files, sync_config
from etl_craft.services.lineage import collect
from fixtures.metadata import add_pipeline, add_source_column, add_task


def test_upstream_stars_join_resolution_and_schema_cache(engine_db, tmp_path, monkeypatch):
    db = engine_db
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(
            conn,
            pipeline,
            "a_consumer",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="mart.output",
            SELECT_QUERY="SELECT * FROM stage.orders",
        )
        add_task(
            conn,
            pipeline,
            "z_producer",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="stage.orders",
            SELECT_QUERY="SELECT * FROM raw.orders",
        )
        add_source_column(conn, "raw.orders", "z", "INT", 1)
        add_source_column(conn, "raw.orders", "a", "INT", 2)

    def no_warehouse(*args, **kwargs):
        raise AssertionError("lineage must stay offline")

    monkeypatch.setattr("etl_craft.services.catalog.collect.open_warehouse", no_warehouse)
    monkeypatch.setattr("etl_craft.warehouse.connection.build_warehouse_engine", no_warehouse)
    first = collect(db.engine, db.config)
    assert all(result.error is None and not result.cached for result in first)
    assert [edge.target_column for edge in first[0].edges] == ["z", "a"]
    assert all(result.cached for result in collect(db.engine, db.config))
    assert [edge.target_column for edge in collect(db.engine, db.config)[0].edges] == ["z", "a"]
    with db.engine.begin() as conn:
        add_source_column(conn, "raw.orders", "new_column", "TEXT", 3)
    changed = collect(db.engine, db.config)
    assert all(not result.cached for result in changed)
    assert [edge.target_column for edge in changed[0].edges] == ["z", "a", "new_column"]
    catalog = build_catalog(db.engine, db.config)
    assert not catalog.untraced
    assert catalog.tables["raw.orders"].columns["new_column"].data_type == "TEXT"
    folder = tmp_path / "export"
    export_config(db.engine, folder)
    assert not sync_config(db.engine, db.config, read_config_files(folder), apply=False).changes
    (folder / "columns.csv").unlink()
    assert not sync_config(db.engine, db.config, read_config_files(folder), apply=True).changes
    with db.engine.connect() as conn:
        assert (
            conn.execute(
                text("SELECT COUNT(*) FROM CFG_COLUMNS WHERE ACTIVE_FLAG='Y'")
            ).scalar_one()
            == 3
        )
    (folder / "tables.csv").unlink()
    assert not sync_config(db.engine, db.config, read_config_files(folder), apply=True).changes


def test_demo_traces_every_task_before_warehouse_exists(engine_db, tmp_path, monkeypatch):
    import shutil
    from dataclasses import replace
    from pathlib import Path

    from etl_craft.config import ConnectionProfile
    from etl_craft.engine.migrations import apply_pending_migrations

    project = tmp_path / "demo"
    shutil.copytree(
        Path(__file__).parents[3] / "examples/demo",
        project,
        ignore=shutil.ignore_patterns(
            "*.db", "*.duckdb", "migrations", "logs", "catalog", "__pycache__"
        ),
    )
    config = replace(
        engine_db.config,
        config_path=project / "craft-connector.yml",
        warehouse=ConnectionProfile(
            "WAREHOUSE", "test", "jdbc:duckdb:warehouse.duckdb", "", "NONE", schema="main"
        ),
    )
    migrations = project / "migrations"
    migrations.mkdir()
    shutil.copy(project / "metadata/support_insights.sql", migrations / "0001_demo.sql")
    from etl_craft.engine.migrations import mark_packaged_migrations_applied

    with engine_db.engine.begin() as conn:
        mark_packaged_migrations_applied(conn)
    apply_pending_migrations(engine_db.engine, migrations)

    def no_warehouse(*args, **kwargs):
        raise AssertionError("offline lineage opened the warehouse")

    monkeypatch.setattr("etl_craft.warehouse.connection.build_warehouse_engine", no_warehouse)
    results = collect(engine_db.engine, config)
    assert results
    assert not [(result.task, result.error) for result in results if result.error]


def test_source_config_reorders_and_records_history(engine_db, tmp_path):
    import csv
    from datetime import UTC, datetime

    from etl_craft.services.validate import validate_on

    db = engine_db
    with db.engine.begin() as conn:
        add_source_column(conn, "raw.orders", "id", "INT", 1)
        add_source_column(conn, "raw.orders", "name", "TEXT", 2)
    moment = datetime.now(UTC)
    directory = tmp_path / "config"
    export_config(db.engine, directory)
    path = directory / "columns.csv"
    rows = list(csv.DictReader(path.open()))
    for row in rows:
        row["COLUMN_POSITION"] = "2" if row["COLUMN_NAME"] == "id" else "1"
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    done = sync_config(db.engine, db.config, read_config_files(directory), apply=True)
    assert not done.failed and len(done.changes) == 2
    with db.engine.connect() as conn:
        assert conn.execute(
            text(
                "SELECT COLUMN_NAME FROM CFG_COLUMNS WHERE ACTIVE_FLAG='Y' ORDER BY COLUMN_POSITION"
            )
        ).scalars().all() == ["name", "id"]
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_CFG_COLUMNS_HISTORY")).scalar_one() == 6
    historical = tmp_path / "historical"
    export_config(db.engine, historical, as_of=moment, config=db.config)
    past = list(csv.DictReader((historical / "columns.csv").open()))
    assert {row["COLUMN_NAME"]: row["COLUMN_POSITION"] for row in past} == {"id": "1", "name": "2"}
    with db.engine.begin() as conn:
        conn.execute(text("UPDATE CFG_COLUMNS SET DATA_TYPE='not a type' WHERE COLUMN_NAME='id'"))
        assert any(
            "DATA_TYPE" in finding.message for finding in validate_on(conn, db.config).findings
        )


def test_sequential_self_reads_do_not_create_lineage_cycles(engine_db):
    from fixtures.metadata import add_dependency

    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        create = add_task(
            conn,
            pipeline,
            "create",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="stage.orders",
            SELECT_QUERY="SELECT 1 AS id",
        )
        first = add_task(
            conn,
            pipeline,
            "append_a",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT="stage.orders",
            SELECT_QUERY="SELECT id FROM stage.orders",
        )
        second = add_task(
            conn,
            pipeline,
            "append_b",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT="stage.orders",
            SELECT_QUERY="SELECT id FROM stage.orders",
        )
        add_dependency(conn, pipeline, first, create)
        add_dependency(conn, pipeline, second, first)
    assert all(result.error is None for result in collect(engine_db.engine, engine_db.config))


def test_catalog_reports_unavailable_warehouse_without_losing_definitions(engine_db, monkeypatch):
    from contextlib import contextmanager
    from dataclasses import replace

    from etl_craft.config import ConnectionProfile
    from etl_craft.core.errors import MetadataError

    with engine_db.engine.begin() as conn:
        add_source_column(conn, "raw.orders", "id", "INT", 1)

    @contextmanager
    def unavailable(*args, **kwargs):
        raise MetadataError("warehouse unavailable")
        yield

    monkeypatch.setattr("etl_craft.services.catalog.collect.open_warehouse", unavailable)
    config = replace(
        engine_db.config,
        warehouse=ConnectionProfile(
            "WAREHOUSE", "test", "jdbc:duckdb:warehouse.duckdb", "", "NONE", schema="main"
        ),
    )
    table = build_catalog(engine_db.engine, config, with_warehouse=True).tables["raw.orders"]
    assert not table.warehouse_checked
    assert table.columns["id"].declared_type == "INT"
    assert table.warehouse_findings == ["comparison unavailable: warehouse unavailable"]
