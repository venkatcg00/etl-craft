"""``etl-craft config``: the project's config files loaded into the ``CFG_`` tables, on both
Engine DBs.

The examples in docs/examples/migrations/ load a realistic configuration, ``export`` writes it
as files, and each test edits those files the way a team would.
"""

import csv
import json
import shutil
from pathlib import Path

import pytest

from etl_craft.cli import main
from etl_craft.core.actor import Actor, ActorKind, acting_as
from etl_craft.core.errors import EngineDbError, ExitCode, MetadataFileError
from etl_craft.services.config_files import (
    ColumnChange,
    export_config,
    read_config_files,
    sync_config,
)

ALICE = Actor("github:alice", ActorKind.HUMAN)
EXAMPLE = Path(__file__).parents[3] / "docs" / "examples" / "config"


def edit(path, change):
    """Rewrite a config file's rows with ``change(rows)``, keeping its header."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        header, rows = reader.fieldnames, list(reader)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, header, lineterminator="\n")
        writer.writeheader()
        writer.writerows(change(rows))


def without(*codes):
    """Drop every row naming one of the pipeline ``codes``, at either end."""

    def change(rows):
        return [
            row
            for row in rows
            if row["PIPELINE_CODE"] not in codes
            and row.get("DEPENDS_ON_PIPELINE_CODE") not in codes
        ]

    return change


def sync(project, *, apply=True):
    files = read_config_files(project.config_dir)
    with acting_as(ALICE):
        return sync_config(project.engine, project.config, files, apply=apply)


def active(project, table):
    return project.rows(f"SELECT COUNT(*) AS n FROM {table} WHERE ACTIVE_FLAG = 'Y'")[0][0]


def changes_logged(project):
    return project.rows("SELECT COUNT(*) AS n FROM AUD_METADATA_CHANGES")[0][0]


@pytest.fixture
def exported(metadata_project):
    metadata_project.load_examples()
    export_config(metadata_project.engine, metadata_project.config_dir)
    return metadata_project


def test_exported_files_load_back_without_a_change(exported):
    done = sync(exported, apply=False)
    assert (done.changes, done.failed, done.findings) == ((), False, ())

    header = (exported.config_dir / "tasks.csv").read_text("utf-8").splitlines()[0]
    assert header == (
        "PIPELINE_CODE,TASK_CODE,TASK_TYPE,HANDLER,RUN_CONDITION,RUN_CONDITION_COUNT,ACTIVE_FLAG"
    )
    pipelines = {
        row["PIPELINE_CODE"]: row
        for row in csv.DictReader((exported.config_dir / "pipelines.csv").open(encoding="utf-8"))
    }
    assert {code: row["ACTIVE_FLAG"] for code, row in pipelines.items()} == {
        "SALES_DAILY": "Y",
        "SALES_DAILY_EU": "Y",
        "SALES_DAILY_US": "N",
        "SALES_MART": "Y",
    }
    daily = pipelines["SALES_DAILY"]
    assert (daily["SLA_IN_HOURS"], daily["SCHEDULE_START_DATE"], daily["MAX_CATCHUP_RUNS"]) == (
        "1.5",
        "2026-11-01",
        "3",
    )
    assert json.loads(daily["PIPELINE_PARAMETERS"])["TAGS"] == ["sales", "daily"]


def test_apply_inserts_updates_and_retires_by_key(exported):
    folder = exported.config_dir
    (parameter_id,) = exported.rows(
        "SELECT x.TASK_PARAMETER_ID AS id FROM CFG_TASK_PARAMETERS x "
        "JOIN CFG_TASKS t ON t.TASK_ID = x.TASK_ID "
        "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
        "WHERE p.PIPELINE_CODE = 'SALES_DAILY' AND p.ACTIVE_FLAG = 'Y' AND t.TASK_CODE = 'alert' "
        "AND t.ACTIVE_FLAG = 'Y' AND x.PARAMETER_NAME = 'EMAIL_TO' AND x.ACTIVE_FLAG = 'Y'"
    )[0]
    for name in (
        "pipelines.csv",
        "tasks.csv",
        "task_parameters.csv",
        "task_dependencies.csv",
        "pipeline_dependencies.csv",
        "business_rules.csv",
    ):
        edit(folder / name, without("SALES_DAILY_EU"))
    edit(
        folder / "task_parameters.csv",
        lambda rows: (
            [
                {**row, "PARAMETER_VALUE": "ops@example.com"}
                if (row["PIPELINE_CODE"], row["TASK_CODE"], row["PARAMETER_NAME"])
                == ("SALES_DAILY", "alert", "EMAIL_TO")
                else row
                for row in rows
            ]
            + [
                {
                    "PIPELINE_CODE": "SALES_MART",
                    "TASK_CODE": "archive",
                    "PARAMETER_NAME": name,
                    "PARAMETER_VALUE": value,
                }
                for name, value in (("SCRIPT_NAME", "sales/publish.py"), ("RETRIES", "2"))
            ]
        ),
    )
    edit(
        folder / "tasks.csv",
        lambda rows: [
            *rows,
            {
                "PIPELINE_CODE": "SALES_MART",
                "TASK_CODE": "archive",
                "TASK_TYPE": "ETL",
                "HANDLER": "PYTHON",
                "RUN_CONDITION": "",
                "RUN_CONDITION_COUNT": "",
            },
        ],
    )
    edit(
        folder / "task_dependencies.csv",
        lambda rows: (
            [
                {**row, "DEPENDENCY_TYPE": "HAS_DATA"}
                if (row["TASK_CODE"], row["DEPENDS_ON_TASK_CODE"])
                == ("check_orders", "load_orders")
                else row
                for row in rows
            ]
            + [
                {
                    "PIPELINE_CODE": "SALES_MART",
                    "TASK_CODE": "archive",
                    "DEPENDS_ON_PIPELINE_CODE": "SALES_MART",
                    "DEPENDS_ON_TASK_CODE": "publish",
                    "DEPENDENCY_TYPE": "SUCCESS",
                    "CONSUME_REPAIRS": "Y",
                },
                {
                    "PIPELINE_CODE": "SALES_MART",
                    "TASK_CODE": "alert",
                    "DEPENDS_ON_PIPELINE_CODE": "SALES_MART",
                    "DEPENDS_ON_TASK_CODE": "archive",
                    "DEPENDENCY_TYPE": "ALWAYS",
                    "CONSUME_REPAIRS": "Y",
                },
            ]
        ),
    )
    logged = changes_logged(exported)

    done = sync(exported)

    assert (done.applied, done.failed) == (True, False), done.findings
    by_operation = {}
    for change in done.changes:
        by_operation.setdefault(change.operation, set()).add((change.file, change.row))
    assert by_operation["insert"] == {
        ("tasks.csv", "SALES_MART.archive"),
        ("task_parameters.csv", "SALES_MART.archive SCRIPT_NAME"),
        ("task_parameters.csv", "SALES_MART.archive RETRIES"),
        ("task_dependencies.csv", "SALES_MART.archive on SALES_MART.publish SUCCESS"),
        ("task_dependencies.csv", "SALES_MART.alert on SALES_MART.archive ALWAYS"),
        ("task_dependencies.csv", "SALES_DAILY.check_orders on SALES_DAILY.load_orders HAS_DATA"),
    }
    assert [
        (change.row, change.columns[0].before, change.columns[0].after)
        for change in done.changes
        if change.operation == "update"
    ] == [
        (
            "SALES_DAILY.alert EMAIL_TO",
            "sales-data@example.com|sales-leads@example.com",
            "ops@example.com",
        )
    ]
    retired = by_operation["retire"]
    assert ("pipelines.csv", "SALES_DAILY_EU") in retired
    assert ("pipeline_dependencies.csv", "SALES_MART on SALES_DAILY_EU SUCCESS") in retired
    assert (
        "task_dependencies.csv",
        "SALES_DAILY.check_orders on SALES_DAILY.load_orders SUCCESS",
    ) in retired
    assert {file for file, _ in retired} == {
        "pipelines.csv",
        "tasks.csv",
        "task_parameters.csv",
        "task_dependencies.csv",
        "pipeline_dependencies.csv",
        "business_rules.csv",
    }

    assert exported.rows(
        "SELECT TASK_PARAMETER_ID AS id, PARAMETER_VALUE AS value FROM CFG_TASK_PARAMETERS "
        "WHERE TASK_PARAMETER_ID = :id",
        id=parameter_id,
    ) == [(parameter_id, "ops@example.com")]
    assert exported.rows(
        "SELECT COUNT(*) AS n FROM CFG_TASKS t "
        "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
        "WHERE p.PIPELINE_CODE = 'SALES_DAILY_EU' AND t.ACTIVE_FLAG = 'Y'"
    ) == [(0,)]
    labels = exported.rows(
        "SELECT DISTINCT ACTOR AS actor, MIGRATION AS migration FROM AUD_METADATA_CHANGES "
        "WHERE CHANGE_ID > (SELECT MAX(CHANGE_ID) - :n FROM AUD_METADATA_CHANGES)",
        n=changes_logged(exported) - logged,
    )
    assert labels == [("github:alice", f"config@{done.revision}")]

    again = sync(exported)
    assert (again.applied, again.changes) == (True, ())


def test_plan_reports_what_apply_would_do_and_changes_nothing(exported):
    edit(exported.config_dir / "pipelines.csv", without("SALES_DAILY_EU", "SALES_MART"))
    for name in (
        "tasks.csv",
        "task_parameters.csv",
        "task_dependencies.csv",
        "pipeline_dependencies.csv",
        "business_rules.csv",
    ):
        edit(exported.config_dir / name, without("SALES_DAILY_EU", "SALES_MART"))
    pipelines = active(exported, "CFG_PIPELINES")
    logged = changes_logged(exported)

    done = sync(exported, apply=False)

    assert (done.applied, done.failed) == (False, False)
    assert {change.row for change in done.changes if change.file == "pipelines.csv"} == {
        "SALES_DAILY_EU",
        "SALES_MART",
    }
    assert (active(exported, "CFG_PIPELINES"), changes_logged(exported)) == (
        pipelines,
        logged,
    )


def test_a_configuration_that_validate_fails_is_not_applied(exported):
    edit(
        exported.config_dir / "tasks.csv",
        lambda rows: [
            {**row, "RUN_CONDITION": "N", "RUN_CONDITION_COUNT": "5"}
            if (row["PIPELINE_CODE"], row["TASK_CODE"]) == ("SALES_MART", "publish")
            else row
            for row in rows
        ],
    )
    logged = changes_logged(exported)

    done = sync(exported)

    assert (done.applied, done.failed) == (False, True)
    assert [change.row for change in done.changes] == ["SALES_MART.publish"]
    assert [(f.where, f.message) for f in done.findings] == [
        (
            "SALES_MART",
            "task publish requires 5 satisfied dependencies but only has 2 — it could never run",
        )
    ]
    assert changes_logged(exported) == logged
    assert exported.rows(
        "SELECT t.RUN_CONDITION AS run_condition FROM CFG_TASKS t "
        "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
        "WHERE p.PIPELINE_CODE = 'SALES_MART' "
        "AND t.TASK_CODE = 'publish' AND t.ACTIVE_FLAG = 'Y'"
    ) == [("ANY",)]


def test_the_command_plans_applies_and_exports(metadata_project, capsys):
    metadata_project.load_examples("0002")
    assert main(["config", "plan"]) == ExitCode.METADATA_FILE
    assert "no config folder" in capsys.readouterr().err

    assert main(["config", "export"]) == ExitCode.SUCCESS
    assert "pipelines.csv 2" in capsys.readouterr().out
    assert main(["config", "export"]) == ExitCode.METADATA_FILE
    assert "--force" in capsys.readouterr().err

    edit(
        metadata_project.config_dir / "pipelines.csv",
        lambda rows: [{**row, "SLA_IN_HOURS": "4"} for row in rows],
    )
    assert main(["config", "plan"]) == ExitCode.SUCCESS
    planned = capsys.readouterr().out
    assert "SALES_MART  (SLA_IN_HOURS: '' -> '4')" in planned
    assert planned.endswith("plan only: nothing was changed\n")

    assert main(["config", "apply", "--format", "json"]) == ExitCode.SUCCESS
    document = json.loads(capsys.readouterr().out)
    assert (document["schema"], document["applied"], len(document["changes"])) == (
        "etl-craft/config-sync/1",
        True,
        2,
    )
    assert metadata_project.rows(
        "SELECT COMMAND AS command, OUTCOME AS outcome FROM AUD_ACTIONS "
        "WHERE COMMAND = 'config apply'"
    ) == [("config apply", "REQUESTED")]


def flag(code, value):
    """Set ``ACTIVE_FLAG`` on the rows of a file whose key starts with ``code``."""

    def change(rows):
        return [
            {**row, "ACTIVE_FLAG": value} if ".".join(_key(row)).startswith(code) else row
            for row in rows
        ]

    return change


def _key(row):
    names = ("PIPELINE_CODE", "TASK_CODE", "PARAMETER_NAME")
    return [row[name] for name in names if name in row]


def test_flags_turn_rows_off_and_on_again_keeping_their_ids(exported):
    folder = exported.config_dir
    (mart_id,) = exported.rows(
        "SELECT PIPELINE_ID AS id FROM CFG_PIPELINES "
        "WHERE PIPELINE_CODE = 'SALES_MART' AND ACTIVE_FLAG = 'Y'"
    )[0]

    edit(folder / "pipelines.csv", flag("SALES_MART", "N"))
    off = sync(exported)
    assert (off.applied, [(c.operation, c.row, c.reason) for c in off.changes]) == (
        True,
        [("retire", "SALES_MART", "ACTIVE_FLAG is N")],
    )
    assert exported.rows(
        "SELECT COUNT(*) AS n FROM CFG_TASKS WHERE PIPELINE_ID = :id AND ACTIVE_FLAG = 'Y'",
        id=mart_id,
    ) == [(4,)]

    edit(folder / "pipelines.csv", flag("SALES_MART", "Y"))
    on = sync(exported)
    assert [(c.operation, c.row) for c in on.changes] == [("reactivate", "SALES_MART")]
    assert exported.rows(
        "SELECT PIPELINE_ID AS id FROM CFG_PIPELINES "
        "WHERE PIPELINE_CODE = 'SALES_MART' AND ACTIVE_FLAG = 'Y'"
    ) == [(mart_id,)]

    parameter = "SALES_DAILY.fetch_orders.RETRIES"
    edit(folder / "task_parameters.csv", flag(parameter, "N"))
    assert [(c.operation, c.row) for c in sync(exported).changes] == [
        ("retire", "SALES_DAILY.fetch_orders RETRIES")
    ]
    edit(
        folder / "task_parameters.csv",
        lambda rows: [row for row in rows if ".".join(_key(row)) != parameter],
    )
    assert sync(exported).changes == ()
    edit(
        folder / "task_parameters.csv",
        lambda rows: [
            *rows,
            {
                "PIPELINE_CODE": "SALES_DAILY",
                "TASK_CODE": "fetch_orders",
                "PARAMETER_NAME": "RETRIES",
                "PARAMETER_VALUE": "4",
                "ACTIVE_FLAG": "Y",
            },
        ],
    )
    back = sync(exported)
    assert [(c.operation, c.row, c.columns[-1]) for c in back.changes] == [
        (
            "reactivate",
            "SALES_DAILY.fetch_orders RETRIES",
            ColumnChange("PARAMETER_VALUE", "2", "4"),
        )
    ]

    edit(
        folder / "pipelines.csv",
        lambda rows: [
            *rows,
            {**rows[0], "PIPELINE_CODE": "NEVER_LOADED", "ACTIVE_FLAG": "N"},
        ],
    )
    assert sync(exported).changes == ()


def test_an_active_row_under_a_pipeline_without_a_row_is_refused(exported):
    folder = exported.config_dir
    edit(
        folder / "pipelines.csv",
        lambda rows: [*rows, {**rows[0], "PIPELINE_CODE": "NEVER_LOADED", "ACTIVE_FLAG": "N"}],
    )
    edit(
        folder / "tasks.csv",
        lambda rows: [
            *rows,
            {**rows[0], "PIPELINE_CODE": "NEVER_LOADED", "TASK_CODE": "load", "ACTIVE_FLAG": "Y"},
        ],
    )

    with pytest.raises(
        MetadataFileError, match="pipeline NEVER_LOADED has no row in the Engine DB"
    ):
        sync(exported, apply=False)


def test_the_example_folder_is_the_migration_examples_exported(metadata_project, tmp_path):
    metadata_project.load_examples()
    export_config(metadata_project.engine, tmp_path / "exported")
    for path in sorted(EXAMPLE.glob("*.csv")):
        assert (tmp_path / "exported" / path.name).read_text("utf-8") == path.read_text("utf-8")


def test_the_example_folder_loads_into_a_new_engine_db(metadata_project):
    shutil.copytree(EXAMPLE, metadata_project.config_dir)

    done = sync(metadata_project)

    assert (done.applied, done.failed, done.findings) == (True, False, ())
    assert {change.operation for change in done.changes} == {"insert"}
    assert metadata_project.rows(
        "SELECT PIPELINE_CODE AS code FROM CFG_PIPELINES WHERE ACTIVE_FLAG = 'Y' ORDER BY 1"
    ) == [("SALES_DAILY",), ("SALES_DAILY_EU",), ("SALES_MART",)]
    assert sync(metadata_project).changes == ()


def test_an_engine_db_without_tables_is_refused_with_the_remedy(empty_engine_db, tmp_path):
    with pytest.raises(EngineDbError, match=r"no CFG_PIPELINES, .* run `etl-craft setup`"):
        export_config(empty_engine_db.engine, tmp_path / "config")
