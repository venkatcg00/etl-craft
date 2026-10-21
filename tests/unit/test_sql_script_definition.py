"""Unmanaged files validate every input before their first side effect."""

from dataclasses import replace
from datetime import date

import pytest
from sqlalchemy import text

from etl_craft.config import ConnectionProfile, ConnectorConfig, SourceConfig
from etl_craft.core.enums import Handler, RefreshType, SecretsSource
from etl_craft.core.errors import HandlerError, MetadataError
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.sql_script import bindable_sql, read_script, statement_text
from etl_craft.services.lineage import script_lineage

pytestmark = pytest.mark.unit


@pytest.fixture
def context(tmp_path):
    folder = tmp_path / "sql_scripts"
    folder.mkdir()
    (folder / "script.sql").write_text(
        "SELECT :pipeline_id, :pipeline_run_id, :task_run_id, :run_date;", "utf-8"
    )
    config = ConnectorConfig(
        source=SourceConfig(SecretsSource.ENVIRONMENT),
        engine=ConnectionProfile("Engine", "dev", "jdbc:sqlite:engine.db", "", "NONE"),
        warehouse=ConnectionProfile("Warehouse", "dev", "jdbc:duckdb:warehouse.duckdb", "", "NONE"),
        config_path=tmp_path / "craft-connector.yml",
    )
    return TaskContext(
        config,
        11,
        "P",
        12,
        "T",
        13,
        14,
        1,
        Handler.SQL_SCRIPT,
        RefreshType.FULL,
        {"SCRIPT_FILE": "sql_scripts/script.sql"},
        run_date=date(2026, 10, 21),
    )


def test_engine_inputs_are_bound_values(context):
    script = read_script(context)
    assert script.inputs == {
        "pipeline_id": 11,
        "pipeline_run_id": 13,
        "task_run_id": 14,
        "run_date": date(2026, 10, 21),
    }
    assert ":pipeline_id" in script.statements[0]


@pytest.mark.parametrize(
    ("body", "inputs", "expected"),
    [
        ("SELECT 1; SELECT :missing;", "{}", "statement 2: missing binds missing"),
        ("-- only a comment", "{}", "contains no statements"),
        ("SELECT 1;", '{"run_date": "override"}', "overrides run_date"),
        ("SELECT 1;", "[]", "must be a JSON object"),
    ],
)
def test_bad_inputs_are_refused_before_execution(context, body, inputs, expected):
    (context.config.sql_scripts_dir / "script.sql").write_text(body, "utf-8")
    context = replace(
        context, task_params={"SCRIPT_FILE": "sql_scripts/script.sql", "INPUT_PARAMS": inputs}
    )
    with pytest.raises(HandlerError, match=expected):
        read_script(context)


@pytest.mark.parametrize("name", ["../outside.sql", "/outside.sql", "sql_scripts/script.txt"])
def test_script_paths_cannot_escape_sql_scripts(context, name):
    with pytest.raises(MetadataError, match="SCRIPT_FILE"):
        read_script(replace(context, task_params={"SCRIPT_FILE": name}))


def test_script_symlink_cannot_read_outside_the_project(context):
    external = context.config.project_dir / "outside.sql"
    external.write_text("SELECT 999", "utf-8")
    (context.config.sql_scripts_dir / "linked.sql").symlink_to(external)
    with pytest.raises(MetadataError, match="resolves outside"):
        read_script(replace(context, task_params={"SCRIPT_FILE": "sql_scripts/linked.sql"}))


def test_only_code_colons_are_binds_and_audit_hides_literals():
    sql = "SELECT :input, ':literal', $$ BEGIN PERFORM :body; END $$ -- :comment"
    compiled = text(bindable_sql(sql)).compile()
    assert set(compiled.params) == {"input"}
    assert ":literal" in str(compiled)
    assert ":body" in str(compiled)
    audit = statement_text(sql)
    assert ":input" in audit
    assert ":literal" not in audit and ":body" not in audit and ":comment" not in audit


def test_unparseable_procedure_is_unmanaged_and_does_not_invent_lineage(context):
    lineage = script_lineage("CALL maintenance(:run_date)", "P.T", context.config)
    assert "unmanaged" in lineage.error
    assert lineage.targets == ()


@pytest.mark.parametrize(
    "sql",
    [
        "TRUNCATE TABLE main.target",
        "DROP TABLE main.target",
        "ALTER TABLE main.target ADD COLUMN id INTEGER",
        "DELETE FROM main.target",
        "UPDATE main.target SET id=1",
    ],
)
def test_ddl_and_dml_report_the_unmanaged_target(context, sql):
    lineage = script_lineage(sql, "P.T", context.config)
    assert lineage.targets == ("main.target",)


def test_unqualified_script_targets_use_the_configured_schema(context):
    context = replace(
        context,
        config=replace(context.config, warehouse=replace(context.config.warehouse, schema="main")),
    )
    lineage = script_lineage("INSERT INTO target SELECT id FROM source", "P.T", context.config)
    assert lineage.targets == ("main.target",)
    assert lineage.sources == ("main.source",)
