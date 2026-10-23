"""A plain-SQL cache reset starts lineage history on both Engine DBs."""

from dataclasses import replace

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError

from etl_craft.engine import migrations
from etl_craft.services.lineage import collect
from fixtures.metadata import add_pipeline, add_task
from fixtures.released_schema import install


@pytest.fixture
def before_lineage_upgrade(empty_engine_db, monkeypatch):
    db = empty_engine_db
    install(db, "0.2.0")
    original = migrations.migration_streams
    with monkeypatch.context() as patch:
        patch.setattr(
            migrations,
            "migration_streams",
            lambda *args, **kwargs: [
                replace(stream, files=tuple(f for f in stream.files if f.version < "0031"))
                if stream.source == migrations.ENGINE
                else stream
                for stream in original(*args, **kwargs)
            ],
        )
        migrations.apply_pending_migrations(db.engine)
    return db


def test_pre_slice_cache_is_discarded_without_conversion(before_lineage_upgrade):
    db = before_lineage_upgrade
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        task = add_task(
            conn,
            pipeline,
            "load",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="stage.output",
            SELECT_QUERY="SELECT id FROM raw.input",
        )
        conn.execute(
            text(
                "INSERT INTO AUD_COLUMN_LINEAGE (TASK_ID,SOURCE_SQL_HASH,TARGET_OBJECT,"
                "TARGET_COLUMN,SOURCE_OBJECT,SOURCE_COLUMN,TRANSFORMATION) "
                "VALUES (:task,'old','not.a.valid.target.name','id','unknown','id','copy')"
            ),
            {"task": task},
        )
    before = collect(db.engine, db.config, refresh=True, record=False)
    applied = migrations.apply_pending_migrations(db.engine)
    assert "0031_lineage_versions.sql" in applied
    assert applied == sorted(applied)
    with db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_COLUMN_LINEAGE")).scalar_one() == 0
        assert conn.execute(text("SELECT COUNT(*) FROM AUD_LINEAGE_VERSIONS")).scalar_one() == 0
        fields = {c["name"].lower() for c in inspect(conn).get_columns("aud_column_lineage")}
        assert "target_object" not in fields and "source_object" not in fields
        assert {"target_catalog_name", "target_schema_name", "target_table_name"} <= fields
    traced = collect(db.engine, db.config)
    assert traced[0].edges[0].source_object == "raw.input"
    assert traced[0].edges == before[0].edges
    with db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT VERSION_NUMBER FROM AUD_LINEAGE_VERSIONS")).scalar_one() == 1
        )
    stream = migrations.migration_streams(db.engine)[0]
    migration = next(f for f in stream.files if f.version == "0031_lineage_versions.sql")
    assert not migration.markers


def test_table_versions_and_edges_cannot_be_rewritten(engine_db):
    db = engine_db
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "P")
        add_task(
            conn,
            pipeline,
            "load",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="stage.output",
            SELECT_QUERY="SELECT id FROM raw.input",
        )
    collect(db.engine, db.config)
    for sql in (
        "UPDATE AUD_LINEAGE_VERSIONS SET EDGES_HASH='rewritten'",
        "UPDATE AUD_LINEAGE_VERSIONS SET VERSION_NUMBER=2",
        "DELETE FROM AUD_LINEAGE_VERSIONS",
        "UPDATE AUD_LINEAGE_VERSION_EDGES SET SOURCE_COLUMN='rewritten'",
        "DELETE FROM AUD_LINEAGE_VERSION_EDGES",
    ):
        with pytest.raises(DBAPIError, match="append-only"), db.engine.begin() as conn:
            conn.execute(text(sql))
    with db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT VERSION_NUMBER FROM AUD_LINEAGE_VERSIONS")).scalar_one() == 1
        )
        assert (
            conn.execute(text("SELECT SOURCE_COLUMN FROM AUD_LINEAGE_VERSION_EDGES")).scalar_one()
            == "id"
        )


def test_demo_lineage_graph_and_documentation_survive_the_cache_reset(
    before_lineage_upgrade, tmp_path
):
    import shutil

    from etl_craft.config import ConnectionProfile
    from etl_craft.core.enums import SqlAction
    from etl_craft.services.catalog.collect import build_catalog
    from etl_craft.services.catalog.site import table_url, write_site
    from etl_craft.services.lineage import LineageGraph
    from fixtures.demo import DEMO

    db = before_lineage_upgrade
    project = tmp_path / "demo"
    shutil.copytree(
        DEMO,
        project,
        ignore=shutil.ignore_patterns(
            "*.db", "*.duckdb", "migrations", "logs", "catalog", "__pycache__"
        ),
    )
    config = replace(
        db.config,
        config_path=project / "craft-connector.yml",
        warehouse=ConnectionProfile(
            "WAREHOUSE", "test", "jdbc:duckdb:warehouse.duckdb", "", "NONE", schema="main"
        ),
    )
    with db.engine.begin() as conn:
        from etl_craft.engine.queries import run_script

        statements = db.dialect.split_statements(
            (project / "metadata/support_insights.sql").read_text()
        )
        # The graph fixture predates placement columns; keep its graph SQL unchanged.
        placement = [sql for sql in statements if "MAX_PARALLEL_TASKS" in sql]
        assert len(placement) == 1
        run_script(conn, [sql for sql in statements if sql not in placement])
    before = collect(db.engine, config, refresh=True, record=False)
    assert before and not [item.error for item in before if item.error]
    migrations.apply_pending_migrations(db.engine)
    after = collect(db.engine, config)
    assert [(item.task, item.edges) for item in after] == [
        (item.task, item.edges) for item in before
    ]
    catalog = build_catalog(db.engine, config)
    assert set(catalog.column_edges) == {
        edge
        for edge in LineageGraph.of(before).edges
        if catalog.tasks[edge.task].params.get("SQL_ACTION") != SqlAction.DELETE_ROWS
    }
    site = tmp_path / "demo-site"
    write_site(catalog, config, site)
    targets = {edge.target_object for item in before for edge in item.edges}
    for name in targets:
        assert "Current lineage version 1" in (site / table_url(name)).read_text()


@pytest.mark.parametrize(
    "empty_engine_db",
    [pytest.param("postgresql", marks=pytest.mark.engine_postgres)],
    indirect=True,
)
@pytest.mark.parametrize("upgrade", [False, True])
def test_postgres_lineage_tables_have_matching_descriptions(empty_engine_db, upgrade):
    from fixtures.engine_db import apply_schema

    db = empty_engine_db
    if upgrade:
        install(db, "0.2.0")
        migrations.apply_pending_migrations(db.engine)
    else:
        apply_schema(db.engine)
    with db.engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT c.relname AS table_name, "
                "obj_description(c.oid,'pg_class') AS description "
                "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname=current_schema() AND c.relname IN "
                "('aud_column_lineage','aud_lineage_versions','aud_lineage_version_edges')"
            )
        )
        comments = {row.table_name: row.description for row in rows}
    assert comments["aud_column_lineage"] == (
        "Derived SQL task lineage cache keyed by SQL, parser version and source schemas."
    )
    assert comments["aud_lineage_versions"] == (
        "Append-only per-target table lineage versions and validity intervals."
    )
    assert comments["aud_lineage_version_edges"] == (
        "Recorded column sources and writing tasks for a table lineage version."
    )
