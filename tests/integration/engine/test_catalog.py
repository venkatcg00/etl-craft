"""The catalog and its site: assets, lineage across pipelines, search index, both Engine DBs."""

import json
import logging
import re

import pytest
import yaml

from etl_craft.cli import main
from etl_craft.config import load_config
from etl_craft.core.errors import ExitCode, UsageError
from etl_craft.engine import runlog
from etl_craft.execution.interventions import mark_task
from etl_craft.services.catalog import build_catalog
from etl_craft.services.catalog_graph import lineage_drawing
from etl_craft.services.catalog_site import MARKER, table_url, write_site
from fixtures.metadata import add_dependency, add_pipeline, add_pipeline_dependency, add_task
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
    profile = engine_db.config.engine.active
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
            "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, "
            "SEQUENCE_NUMBER) VALUES ('negative usd', :p, :t, 'SELECT 1 WHERE t.amount_usd < 0', "
            "'REJECT', 'id', 'sales.orders', 1)",
            "BUSINESS_RULE_ID",
            p=sales,
            t=rules,
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
            SOURCE_SQL="SELECT * FROM sales.orders",
        )
        add_pipeline_dependency(conn, mart, sales)
        add_dependency(conn, ingest, stage, pull)
        add_dependency(conn, sales, convert, stage, upstream_pipeline=ingest)
        add_dependency(conn, sales, rules, convert, "HAS_DATA")
        run_id = runlog.find_or_create_active_run(conn, ingest)
        insert_row(
            conn,
            "INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS, END_DATE, "
            "SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT) VALUES (:t, :r, 'SUCCESS', "
            "CURRENT_TIMESTAMP, 12, 12, 12)",
            "TASK_RUN_ID",
            t=stage,
            r=run_id,
        )
        runlog.finalize_pipeline_run(conn, run_id, "SUCCESS")
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
    assert list(tables["sales.orders"].columns) == ["id", "amount_usd"]
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
    assert drawing.nodes["sales.orders"].columns == ["id", "amount_usd"]
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
    assert [p for p in pages if not p.startswith("scripts/")] == sorted(
        [
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
    assert out[0].startswith("column lineage unavailable: MART.copy_all: SELECT * cannot")
    assert out[-1] == "generate-docs: nothing written; 1 task(s) not traced"
    assert not (root / "catalog").exists()
    assert main(["generate-docs", "--with-warehouse"]) == ExitCode.SUCCESS
    assert capsys.readouterr().out.splitlines()[-1] == (
        f"generate-docs: wrote 20 page(s) to {root / 'catalog'}"
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
    assert list(columns) == ["id", "amount_usd", "note"]
    assert columns["id"].data_type == "BIGINT"
    assert columns["note"].comment == "free text"
    assert catalog.tables["sales.orders"].in_warehouse
    assert not catalog.tables["mart.daily"].in_warehouse


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
    mark_task(engine, config, "INGEST", "pull", "SKIPPED", "no file today", requested_by="op@h")
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
