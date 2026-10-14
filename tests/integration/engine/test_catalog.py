"""The catalog and its site: assets, lineage across pipelines, search index, both Engine DBs."""

import json
import logging
import re

import pytest
import yaml
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.config import load_config
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.engine import runlog, transitions
from etl_craft.execution.interventions import mark_task
from etl_craft.services.catalog import build_catalog
from etl_craft.services.catalog_graph import lineage_drawing
from etl_craft.services.catalog_site import MARKER, table_url, write_site
from fixtures.metadata import (
    add_dependency,
    add_pipeline,
    add_pipeline_dependency,
    add_source_column,
    add_table,
    add_task,
    start_run,
)
from fixtures.metadata import insert as insert_row


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def project(engine_db, tmp_path, monkeypatch):
    """crm → raw.orders (script) → staging.orders → sales.orders → mart.daily, plus gaps."""
    profile = engine_db.config.engine
    schema = "public" if profile.jdbc_url.startswith("jdbc:postgresql") else "main"
    block = {"jdbc_url": profile.jdbc_url, "schema": schema}
    if profile.auth_mode != "none":
        block |= {
            "user": profile.user,
            "auth_mode": profile.auth_mode,
            "secret": profile.secret_var,
        }
    root = (engine_db.config.config_path or tmp_path / "craft-connector.yml").parent
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {"dev": block},
        "Warehouse": {"dev": {"jdbc_url": "jdbc:duckdb:analytics.duckdb", "schema": "main"}},
    }
    (root / "craft-connector.yml").write_text(yaml.safe_dump(raw, sort_keys=False), "utf-8")
    (root / "sql_files").mkdir(exist_ok=True)
    (root / "sql_files" / "convert.sql").write_text(
        "SELECT o.id, o.amount * r.rate AS amount_usd\n"
        "FROM staging.orders o JOIN ref.rates r ON r.currency = o.currency\n",
        encoding="utf-8",
    )
    (root / "ingestion_scripts" / "crm").mkdir(parents=True, exist_ok=True)
    (root / "ingestion_scripts" / "crm" / "orders.py").write_text("def run():\n    pass\n", "utf-8")
    monkeypatch.chdir(root)
    engine = engine_db.engine
    with engine.begin() as conn:
        ingest = add_pipeline(conn, "INGEST")
        pull = add_task(
            conn,
            ingest,
            "pull",
            "PYTHON",
            SCRIPT_NAME="crm/orders.py",
            TARGET_OBJECT="raw.orders",
            SOURCE_OBJECT="CRM API",
        )
        stage = add_task(
            conn,
            ingest,
            "stage",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="staging.orders",
            SOURCE_SQL="SELECT id, amt AS amount, currency FROM raw.orders WHERE amt > 0",
            DOCUMENTATION="Stages positive orders.",
        )
        sales = add_pipeline(conn, "SALES")
        convert = add_task(
            conn,
            sales,
            "convert",
            SQL_ACTION="OVERWRITE_TABLE",
            TARGET_OBJECT="analytics.sales.orders",
            SOURCE_SQL_FILE="convert.sql",
        )
        rules = add_task(conn, sales, "rules", "BUSINESS_RULES")
        insert_row(
            conn,
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
            "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE_ID, "
            "SEQUENCE_NUMBER) VALUES ('negative usd', :p, :t, 'SELECT 1 WHERE t.amount_usd < 0', "
            "'REJECT', 'id', :target, 1)",
            "BUSINESS_RULE_ID",
            p=sales,
            t=rules,
            target=add_table(conn, "sales.orders"),
        )
        mart = add_pipeline(conn, "MART")
        add_task(
            conn,
            mart,
            "daily",
            SQL_ACTION="OVERWRITE_TABLE",
            TARGET_OBJECT="mart.daily",
            SOURCE_SQL="SELECT COUNT(*) AS n, SUM(amount_usd) AS total FROM sales.orders",
        )
        add_task(
            conn,
            mart,
            "copy_all",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="mart.everything",
            SOURCE_SQL="SELECT * FROM sales.orders o JOIN ref.rates r ON o.id = r.id",
        )
        add_pipeline_dependency(conn, mart, sales)
        add_dependency(conn, ingest, stage, pull)
        add_dependency(conn, sales, convert, stage, upstream_pipeline=ingest)
        add_dependency(conn, sales, rules, convert, "HAS_DATA")
        run_id = start_run(conn, ingest)
        insert_row(
            conn,
            "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS, END_DATE, "
            "SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT) VALUES (:t, :r, 'SUCCESS', "
            "CURRENT_TIMESTAMP, 12, 12, 12)",
            "TASK_RUN_ID",
            t=stage,
            r=run_id,
        )
        transitions.finalize_pipeline_run(conn, run_id, "SUCCESS")
    with engine.begin() as conn:
        for position, column in enumerate(("id", "amt", "currency"), 1):
            add_source_column(conn, "raw.orders", column, "VARCHAR", position)
    return engine, load_config(root / "craft-connector.yml"), root


def test_the_catalog_joins_tasks_tables_and_rules(project):
    engine, config, _ = project
    catalog = build_catalog(engine, config)
    assert sorted(catalog.pipelines) == ["INGEST", "MART", "SALES"]
    assert catalog.pipelines["MART"].depends_on == [("SALES", "SUCCESS")]
    assert catalog.pipelines["SALES"].depended_on_by == ["MART"]
    tables = catalog.tables
    assert sorted(tables) == [
        "external:crm api",
        "mart.daily",
        "mart.everything",
        "raw.orders",
        "ref.rates",
        "sales.orders",
        "staging.orders",
    ]
    assert tables["sales.orders"].writers == ["SALES.convert"]
    assert tables["sales.orders"].readers == ["MART.copy_all", "MART.daily"]
    assert [c.name for c in tables["sales.orders"].columns.values() if not c.engine_managed] == [
        "id",
        "amount_usd",
    ]
    assert tables["raw.orders"].writers == ["INGEST.pull"]
    assert tables["external:crm api"].external
    (rule,) = catalog.rules.values()
    assert rule.table == "sales.orders" and tables["sales.orders"].rules == [
        rule.row.business_rule_id
    ]
    stage = catalog.tasks["INGEST.stage"]
    assert stage.documentation == "Stages positive orders."
    assert stage.row.last_run.status == "SUCCESS" and stage.row.last_run.target_count == 12
    assert catalog.tasks["INGEST.pull"].row.last_run is None
    assert catalog.tasks["INGEST.stage"].row.has_successful_run
    # The SELECT * task keeps its table edge and says why its columns are unknown.
    (untraced,) = catalog.untraced
    assert untraced.label == "MART.copy_all" and "SELECT *" in untraced.lineage_error
    edges = {(e.source, e.target, e.task) for e in catalog.table_edges}
    assert ("sales.orders", "mart.everything", "MART.copy_all") in edges
    assert ("external:crm api", "raw.orders", "INGEST.pull") in edges
    assert catalog.scripts["crm/orders.py"].exists


def test_a_tables_graph_runs_to_the_first_source_and_the_last_consumer(project):
    engine, config, _ = project
    drawing = lineage_drawing(build_catalog(engine, config), "sales.orders")
    levels = {name: node.level for name, node in drawing.nodes.items()}
    assert levels == {
        "external:crm api": -3,
        "raw.orders": -2,
        "staging.orders": -1,
        "ref.rates": -1,
        "sales.orders": 0,
        "mart.daily": 1,
        "mart.everything": 1,
    }
    assert {"id", "amount_usd"} <= set(drawing.nodes["sales.orders"].columns)
    assert drawing.nodes["mart.daily"].derived == {"n": "COUNT(*)"}
    column_pairs = {
        (f"{e.source_object}.{e.source_column}", f"{e.target_object}.{e.target_column}")
        for e in drawing.column_edges
    }
    assert ("staging.orders.amount", "sales.orders.amount_usd") in column_pairs
    assert ("raw.orders.amt", "staging.orders.amount") in column_pairs
    # Table level only: the script, and the task whose columns cannot be traced.
    assert {(s, t) for s, t, _ in drawing.table_edges} == {
        ("external:crm api", "raw.orders"),
        ("sales.orders", "mart.everything"),
        ("ref.rates", "mart.everything"),
    }
    # Every box sits inside the drawing, a level to a column, left to right.
    xs = {node.level: node.x for node in drawing.nodes.values()}
    assert [xs[level] for level in sorted(xs)] == sorted(xs.values())
    assert all(n.y + n.height <= drawing.height for n in drawing.nodes.values())


def test_the_site_has_a_page_per_asset_and_a_search_index(project, tmp_path):
    engine, config, _ = project
    site = write_site(build_catalog(engine, config), config, tmp_path / "site")
    folder = site.folder
    assert (folder / MARKER).is_file()
    pages = sorted(str(p.relative_to(folder)) for p in folder.rglob("*.html"))
    assert [p for p in pages if not p.startswith(("scripts/", "runs/"))] == sorted(
        [
            "gaps.html",
            "dags.html",
            "warehouse.html",
            "index.html",
            "pipelines/INGEST.html",
            "pipelines/MART.html",
            "pipelines/SALES.html",
            "rules/1.html",
            "tables/mart.daily.html",
            "tables/mart.everything.html",
            "tables/raw.orders.html",
            "tables/ref.rates.html",
            "tables/sales.orders.html",
            "tables/staging.orders.html",
            "tasks/INGEST.pull.html",
            "tasks/INGEST.stage.html",
            "tasks/MART.copy_all.html",
            "tasks/MART.daily.html",
            "tasks/SALES.convert.html",
            "tasks/SALES.rules.html",
        ]
    )
    assert any(re.fullmatch(r"scripts/crm_orders\.py-[0-9a-f]{8}\.html", p) for p in pages)
    assert site.pages == len(pages)
    table = (folder / table_url("sales.orders")).read_text("utf-8")
    assert '<svg class="lineage"' in table and 'data-col="sales.orders|amount_usd"' in table
    assert 'data-from="staging.orders|amount"' in table
    # Every table's box has a header to show its columns and a button to open its page.
    assert '<a class="open" href="../tables/staging.orders.html">' in table
    assert table.count('class="head"') == 7 and 'marker-end="url(#arrow)"' in table
    assert "negative usd" in table and "../tasks/SALES.convert.html" in table
    task = (folder / "tasks/SALES.convert.html").read_text("utf-8")
    assert "o.amount * r.rate" in task and "JOIN ref.rates" in task
    gap = (folder / "tasks/MART.copy_all.html").read_text("utf-8")
    assert "column lineage unavailable" in gap
    # Nothing is loaded from anywhere but the site itself.
    for page in folder.rglob("*.html"):
        assert not re.search(r"(src|href)=\"(https?:)?//", page.read_text("utf-8")), page
    index_js = (folder / "search-index.js").read_text("utf-8")
    entries = json.loads(index_js.removeprefix("window.CATALOG_INDEX = ").rstrip(";\n"))
    kinds = {kind for kind, *_ in entries}
    assert kinds == {"pipeline", "task", "table", "column", "rule", "script"}
    assert [
        "column",
        "sales.orders.amount_usd",
        "",
        "tables/sales.orders.html#col=amount_usd",
    ] in entries


def test_a_folder_that_holds_other_files_is_refused(project, tmp_path):
    engine, config, _ = project
    folder = tmp_path / "mine"
    folder.mkdir()
    (folder / "notes.txt").write_text("keep me", "utf-8")
    with pytest.raises(UsageError, match="already holds files that generate-docs did not write"):
        write_site(build_catalog(engine, config), config, folder)
    assert (folder / "notes.txt").read_text("utf-8") == "keep me"
    site = tmp_path / "site"
    write_site(build_catalog(engine, config), config, site)
    (site / "stale.html").write_text("old", "utf-8")
    write_site(build_catalog(engine, config), config, site)
    assert not (site / "stale.html").exists()
    # The new site is built beside the old one and swapped in; nothing is left behind.
    assert not [p.name for p in tmp_path.iterdir() if p.name.startswith(".site")]
    page = (site / "index.html").read_text("utf-8")
    assert 'data-generated="' in page and "run details are as of then" in page


def test_the_command(project, capsys):
    _, _, root = project
    assert main(["generate-docs", "--strict"]) == ExitCode.FAILURE
    out = capsys.readouterr().out.splitlines()
    assert out[0].startswith(
        "column lineage unavailable: MART.copy_all: history not updated: SELECT * cannot"
    )
    assert out[-1] == "generate-docs: nothing written; 1 task(s) not traced"
    assert not (root / "catalog").exists()
    assert main(["generate-docs", "--with-warehouse"]) == ExitCode.SUCCESS
    assert capsys.readouterr().out.splitlines()[-1] == (
        f"generate-docs: wrote {len(list((root / 'catalog').rglob('*.html')))} "
        f"page(s) to {root / 'catalog'}"
    )
    assert (root / "catalog" / "index.html").is_file()


def test_warehouse_columns_add_types(project, tmp_path):
    engine, config, root = project
    import duckdb

    with duckdb.connect(str(root / "analytics.duckdb")) as conn:
        conn.execute("CREATE SCHEMA sales")
        conn.execute(
            "CREATE TABLE sales.orders (id BIGINT, amount_usd DECIMAL(18, 2), note VARCHAR)"
        )
        conn.execute("COMMENT ON COLUMN sales.orders.note IS 'free text'")
    catalog = build_catalog(engine, config, with_warehouse=True)
    columns = catalog.tables["sales.orders"].columns
    assert [c.name for c in columns.values() if not c.engine_managed] == [
        "id",
        "amount_usd",
        "note",
    ]
    assert columns["id"].data_type == "BIGINT"
    assert columns["note"].comment == "free text"
    write_site(catalog, config, tmp_path / "commented")
    page = (tmp_path / "commented" / "tables" / "sales.orders.html").read_text()
    assert "<th>Comment</th>" in page and "free text" in page
    assert catalog.tables["sales.orders"].in_warehouse
    assert not catalog.tables["mart.daily"].in_warehouse


def test_gap_snapshot_and_rows_show_both_directions_and_types(project, tmp_path):
    import duckdb

    engine, config, root = project
    with duckdb.connect(str(root / "analytics.duckdb")) as conn:
        conn.execute("CREATE SCHEMA raw")
        conn.execute("CREATE TABLE raw.orders (id INT, currency VARCHAR, extra VARCHAR)")
    catalog = build_catalog(engine, config, with_warehouse=True)
    folder = write_site(catalog, config, tmp_path / "site").folder
    html = (folder / "tables/raw.orders.html").read_text()
    assert 'class="gap-amber"' in html and "Deployed only" in html and "extra" in html
    assert 'class="gap-red"' in html and "Config only" in html and "amt" in html
    assert 'class="gap-purple"' in html and "Type mismatch" in html
    assert "⚠" in html and 'aria-hidden="true"' in html
    assert "Script column lineage is unknown" in html
    snapshot = json.loads((folder / "gaps.json").read_text())
    outcomes = {
        (r["column"], r["attribute"]): r for r in snapshot["records"] if r["table"] == "raw.orders"
    }
    assert outcomes["extra", "column"]["state"] == "Deployed only"
    assert outcomes["amt", "column"]["state"] == "Config only"
    assert outcomes["id", "type"]["state"] == "Type mismatch"
    assert outcomes["currency", "type"]["state"] == "Matched"
    assert outcomes["id", "type"]["tasks"] == ["INGEST.pull", "INGEST.stage"]
    assert snapshot["compared_at"] and snapshot["contract_version"] and snapshot["warehouse"]
    summary = (folder / "gaps.html").read_text()
    assert 'href="tables/raw.orders.html"' in summary
    assert 'href="tasks/INGEST.pull.html"' in summary
    assert "Filter gaps by state, object, task or pipeline" in summary


def test_offline_gap_pages_never_claim_checked_or_missing(project, tmp_path, monkeypatch):
    from etl_craft.services import catalog as catalog_service

    def forbidden(*args, **kwargs):
        pytest.fail("offline generation opened the warehouse")

    monkeypatch.setattr(catalog_service, "open_warehouse", forbidden)
    engine, config, _ = project
    folder = write_site(build_catalog(engine, config), config, tmp_path / "site").folder
    html = (folder / "gaps.html").read_text()
    assert "Unchecked" in html and "Not checked" in html
    assert "Matched" not in html and "Config only" not in html
    assert "Unknown" not in html
    assert (folder / "tables/sales.orders.html").read_text().count("Engine managed") == 5
    snapshot = json.loads((folder / "gaps.json").read_text())
    assert snapshot["compared_at"] is None
    assert {r["state"] for r in snapshot["records"]} == {"Unchecked", "Lineage gap"}
    assert any(
        "SALES.rules" in r["tasks"] for r in snapshot["records"] if r["table"] == "sales.orders"
    )


def test_failed_comparison_is_unavailable_in_summary_and_table(project, tmp_path, monkeypatch):
    from sqlalchemy.exc import SQLAlchemyError

    from etl_craft.services import catalog as catalog_service

    def inaccessible(*args, **kwargs):
        raise SQLAlchemyError("metadata access denied")

    monkeypatch.setattr(catalog_service, "open_warehouse", inaccessible)
    engine, config, _ = project
    folder = write_site(
        build_catalog(engine, config, with_warehouse=True), config, tmp_path / "site"
    ).folder
    snapshot = json.loads((folder / "gaps.json").read_text())
    assert {r["state"] for r in snapshot["records"]} == {"Unavailable", "Lineage gap"}
    for path in ("gaps.html", "tables/sales.orders.html"):
        html = (folder / path).read_text()
        assert "Unavailable" in html and "metadata access denied" in html
        assert "Matched" not in html


def test_lineage_warning_graph_has_reason_and_keyboard_controls(project, tmp_path):
    engine, config, _ = project
    folder = write_site(build_catalog(engine, config), config, tmp_path / "site").folder
    html = (folder / "tables/mart.everything.html").read_text()
    assert "has-gap" in html and "SELECT *" in html and "⚠" in html
    assert "edge table gap-edge" in html and 'role="group"' in html
    assert 'role="button"' in html and 'tabindex="0"' in html and "aria-label=" in html
    assert 'href="../tasks/MART.copy_all.html"' in html
    assert (folder / "tables/mart.daily.html").read_text().count("Lineage gap") >= 1


def test_missing_objects_distinguish_expected_creations_and_existing_targets(project, tmp_path):
    import duckdb

    engine, config, root = project
    with duckdb.connect(str(root / "analytics.duckdb")):
        pass
    catalog = build_catalog(engine, config, with_warehouse=True)
    folder = write_site(catalog, config, tmp_path / "site").folder
    snapshot = json.loads((folder / "gaps.json").read_text())
    states = {r["table"]: r["state"] for r in snapshot["records"] if r["attribute"] == "object"}
    assert states["mart.everything"] == "Expected before first deployment"
    assert states["sales.orders"] == "Config only"
    assert states["raw.orders"] == "Config only"
    assert "Matched" not in (folder / "gaps.html").read_text()


def test_constant_lineage_has_no_gap_and_failed_reasons_are_escaped(project, tmp_path):
    engine, config, _ = project
    with engine.begin() as conn:
        pipeline = add_pipeline(conn, "CONSTANTS")
        add_task(
            conn,
            pipeline,
            "make",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="mart.constant",
            SOURCE_SQL="SELECT 1 AS id",
        )
    catalog = build_catalog(engine, config)
    catalog.tasks["MART.copy_all"].lineage_error = '<script>alert("bad")</script>'
    folder = write_site(catalog, config, tmp_path / "site").folder
    html = (folder / "tables/mart.constant.html").read_text()
    assert 'class="lineage"' in html and "no source column" in html
    snapshot = json.loads((folder / "gaps.json").read_text())
    assert not [
        r
        for r in snapshot["records"]
        if r["table"] == "mart.constant" and r["state"] == "Lineage gap"
    ]
    for path in ("gaps.html", "tables/mart.everything.html"):
        page = (folder / path).read_text()
        assert '<script>alert("bad")</script>' not in page
        assert "&lt;script&gt;" in page


def test_comparison_version_uses_inputs_and_stale_checks_are_not_healthy(project, tmp_path):
    from datetime import timedelta

    engine, config, _ = project
    catalog = build_catalog(engine, config)
    original = catalog.contract_version
    with engine.begin() as conn:
        conn.execute(text("UPDATE CFG_COLUMNS SET DATA_TYPE='BIGINT' WHERE COLUMN_NAME='id'"))
    changed = build_catalog(engine, config)
    assert changed.contract_version != original
    changed.compared_at = changed.generated_at - timedelta(hours=27)
    table = changed.tables["raw.orders"]
    table.warehouse_checked = table.in_warehouse = True
    folder = write_site(changed, config, tmp_path / "site").folder
    snapshot = json.loads((folder / "gaps.json").read_text())
    records = [
        r for r in snapshot["records"] if r["table"] == "raw.orders" and r["attribute"] == "object"
    ]
    assert records[0]["state"] == "Stale"
    assert "Matched" not in (folder / "tables/raw.orders.html").read_text()


def test_graph_column_warnings_do_not_spread_to_other_columns(project):
    from xml.etree import ElementTree

    import duckdb

    from etl_craft.services.catalog_graph import render_svg

    engine, config, root = project
    with duckdb.connect(str(root / "analytics.duckdb")) as conn:
        conn.execute("CREATE SCHEMA sales")
        conn.execute("CREATE TABLE sales.orders (id BIGINT, amount_usd DECIMAL(18,2), extra INT)")
    catalog = build_catalog(engine, config, with_warehouse=True)
    svg = ElementTree.fromstring(render_svg(lineage_drawing(catalog, "sales.orders"), lambda n: n))
    column = svg.find('.//{*}g[@data-col="sales.orders|id"]/{*}title')
    extra = svg.find('.//{*}g[@data-col="sales.orders|extra"]/{*}title')
    assert column is not None and column.text == "id"
    assert extra is not None and "Deployed only" in extra.text


def test_catalog_reports_quoted_components_as_unavailable_without_splitting_them(project, tmp_path):
    import duckdb

    engine, config, root = project
    with engine.begin() as conn:
        add_table(conn, '"raw.archive".orders')
    with duckdb.connect(str(root / "analytics.duckdb")):
        pass
    catalog = build_catalog(engine, config, with_warehouse=True)
    folder = write_site(catalog, config, tmp_path / "site").folder
    snapshot = json.loads((folder / "gaps.json").read_text())
    record = next(r for r in snapshot["records"] if r["table"] == "raw.archive.orders")
    assert record["state"] == "Unavailable" and "quoted" in record["reason"]
    assert catalog.where("raw.archive.orders") == ("analytics", "raw.archive", "orders")


def test_failed_sql_output_does_not_guess_deployed_only_columns(project, tmp_path):
    import duckdb

    engine, config, root = project
    build_catalog(engine, config)
    with duckdb.connect(str(root / "analytics.duckdb")) as conn:
        conn.execute("CREATE SCHEMA sales")
        conn.execute("CREATE TABLE sales.orders (id BIGINT, unresolved_output DECIMAL(18,2))")
    (config.sql_files_dir / "convert.sql").write_text("SELECT * FROM missing.source")
    folder = write_site(
        build_catalog(engine, config, with_warehouse=True), config, tmp_path / "site"
    ).folder
    snapshot = json.loads((folder / "gaps.json").read_text())
    records = [
        r
        for r in snapshot["records"]
        if r["table"] == "sales.orders" and r["column"] == "unresolved_output"
    ]
    assert records and all(r["state"] == "Unavailable" for r in records)
    assert all("SALES.convert" in r["reason"] for r in records)


def test_action_spelling_uses_execution_normalization(project):
    engine, config, _ = project
    with engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE CFG_TASK_PARAMETERS SET PARAMETER_VALUE='  overwrite_table  ' "
                "WHERE PARAMETER_NAME='SQL_ACTION' AND TASK_ID IN "
                "(SELECT TASK_ID FROM CFG_TASKS WHERE TASK_CODE='convert')"
            )
        )
    table = build_catalog(engine, config).tables["sales.orders"]
    assert {c.name for c in table.columns.values() if c.engine_managed} == {
        "pipeline_id",
        "pipeline_run_id",
        "task_run_id",
        "row_id",
        "update_date",
    }


def test_every_page_chains_to_what_it_mentions(project, tmp_path):
    engine, config, _ = project
    catalog = build_catalog(engine, config)
    assert catalog.tasks["SALES.convert"].upstream == [("INGEST.stage", "SUCCESS")]
    assert catalog.tasks["INGEST.stage"].downstream == [("SALES.convert", "SUCCESS")]
    folder = write_site(catalog, config, tmp_path / "site").folder

    def page(path):
        return (folder / path).read_text("utf-8")

    # DAGs tab → pipeline → its DAG, with tasks, the tables they touch, and other pipelines' tasks.
    assert '<a href="pipelines/SALES.html">SALES</a>' in page("dags.html")
    sales = page("pipelines/SALES.html")
    assert '<nav class="crumbs"><a href="../dags.html">DAGs</a> &rsaquo; SALES</nav>' in sales
    assert '<svg class="dag"' in sales
    for link in (
        "../tasks/SALES.convert.html",
        "../tasks/SALES.rules.html",
        "../tasks/INGEST.stage.html",
        "../tables/sales.orders.html",
        "../tables/ref.rates.html",
    ):
        assert f'href="{link}"' in sales, link
    assert "SALES.convert waits for INGEST.stage: SUCCESS" in sales
    assert 'class="edge dep has-data"' in sales and "✓ sales.orders" in sales
    # Task → its pipeline, the tasks around it, and what it reads and writes.
    convert = page("tasks/SALES.convert.html")
    assert (
        '<nav class="crumbs"><a href="../dags.html">DAGs</a> &rsaquo; '
        '<a href="../pipelines/SALES.html">SALES</a> &rsaquo; convert</nav>'
    ) in convert
    assert '<a href="../tasks/INGEST.stage.html">INGEST.stage</a> (SUCCESS)' in convert
    assert '<a href="../tasks/SALES.rules.html">SALES.rules</a> (HAS_DATA)' in convert
    assert 'Checks</dt><dd><a href="../tables/sales.orders.html">' in page("tasks/SALES.rules.html")
    # Warehouse tab → database → schema → table, and back up again.
    warehouse = page("warehouse.html")
    assert 'id="w-analytics.sales"' in warehouse
    assert '<a href="tables/sales.orders.html">orders</a>' in warehouse
    table = page("tables/sales.orders.html")
    assert '<a href="../warehouse.html#w-analytics.sales">sales</a>' in table
    assert 'Pipelines</dt><dd><a href="../pipelines/MART.html">MART</a>' in table


def test_a_pipeline_page_lists_what_operators_changed_in_its_last_run(project, tmp_path):
    engine, config, _ = project
    run_id = build_catalog(engine, config).pipelines["INGEST"].runs[0].pipeline_run_id
    mark_task(
        engine,
        config,
        "INGEST",
        "pull",
        "SKIPPED",
        "no file today",
        requested_by="op@h",
        selector=runlog.RunSelector(run_id=run_id),
    )
    catalog = build_catalog(engine, config)
    assert [(c.task_code, c.action) for c in catalog.pipelines["INGEST"].interventions] == [
        ("pull", "MARK"),
        (None, "REOPEN"),
    ]
    assert catalog.pipelines["SALES"].interventions == []
    folder = write_site(catalog, config, tmp_path / "site").folder
    page = (folder / "pipelines/INGEST.html").read_text("utf-8")
    assert "<h2>Interventions on the last run</h2>" in page
    assert '<a href="../tasks/INGEST.pull.html">INGEST.pull</a>' in page
    assert "no file today" in page and "op@h" in page
    assert "Interventions" not in (folder / "pipelines/SALES.html").read_text("utf-8")


def test_runs_have_pages_with_what_they_were_built_from_and_used_by(project, tmp_path):
    engine, config, _ = project
    with engine.begin() as conn:
        ids = dict(conn.execute(text("SELECT PIPELINE_CODE, PIPELINE_ID FROM CFG_PIPELINES")).all())
        ingest_run, stage_run = conn.execute(
            text(
                "SELECT r.PIPELINE_RUN_ID, r.TASK_RUN_ID FROM AUD_TASK_RUN_LOG r "
                "JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID WHERE t.TASK_CODE = 'stage'"
            )
        ).one()
        sales_run = start_run(conn, ids["SALES"])
        dependency, convert = conn.execute(
            text(
                "SELECT d.TASK_DEPENDENCY_ID, d.TASK_ID FROM CFG_TASK_DEPENDENCY d "
                "JOIN CFG_TASKS t ON t.TASK_ID = d.TASK_ID WHERE t.TASK_CODE = 'convert'"
            )
        ).one()
        conn.execute(
            text(
                "INSERT INTO AUD_DEPENDENCY_CONSUMPTION (TASK_DEPENDENCY_ID, PIPELINE_ID, "
                "PIPELINE_RUN_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, CONSUMED_PIPELINE_RUN_ID, "
                "CONSUMED_TASK_RUN_ID) VALUES (:d, :p, :r, :t, :up, :ur, :utr)"
            ),
            {
                "d": dependency,
                "p": ids["SALES"],
                "r": sales_run,
                "t": convert,
                "up": ids["INGEST"],
                "ur": ingest_run,
                "utr": stage_run,
            },
        )
        transitions.finalize_pipeline_run(conn, sales_run, "FAILED")
    catalog = build_catalog(engine, config)
    assert [run.pipeline_run_id for run in catalog.pipelines["INGEST"].runs] == [ingest_run]
    assert [c.upstream_run_id for c in catalog.built_from[sales_run]] == [ingest_run]
    folder = write_site(catalog, config, tmp_path / "site").folder

    pipeline = (folder / "pipelines/INGEST.html").read_text("utf-8")
    assert '<h2 id="runs">Runs</h2>' in pipeline and 'class="bars"' in pipeline
    assert f'href="../runs/{ingest_run}.html"' in pipeline
    upstream = (folder / f"runs/{ingest_run}.html").read_text("utf-8")
    assert "INGEST.stage" in upstream and "source 12, target 12, inserted 12" in upstream
    assert f'<a href="../runs/{sales_run}.html">SALES run {sales_run}</a> (convert)' in upstream
    downstream = (folder / f"runs/{sales_run}.html").read_text("utf-8")
    assert f'<a href="../runs/{ingest_run}.html">INGEST run {ingest_run}</a> (stage)' in downstream
    task = (folder / "tasks/INGEST.stage.html").read_text("utf-8")
    assert "Rows written of each run" in task and "Average rows written" in task
    dags = (folder / "dags.html").read_text("utf-8")
    assert "<th>Succeeded</th>" in dags and "100% of 1" in dags and "0% of 1" in dags
