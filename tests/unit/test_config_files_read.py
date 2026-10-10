"""Reading the project's config files: every problem named with its file, line and column."""

from datetime import date

import pytest

from etl_craft.cli import main
from etl_craft.core.errors import ExitCode, MetadataFileError
from etl_craft.engine.repository.config_rows import TABLES
from etl_craft.services.config_files import cell, read_config_files

pytestmark = pytest.mark.unit

GOOD = {
    "pipelines.csv": "PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE,SLA_IN_HOURS,PIPELINE_PARAMETERS\n"
    'SALES,Sales,FULL,1.5,"{""TAGS"": [""sales""]}"\n'
    "MART,Mart,FULL,,\n",
    "tasks.csv": "PIPELINE_CODE,TASK_CODE,TASK_TYPE,HANDLER,RUN_CONDITION,RUN_CONDITION_COUNT,"
    "ACTIVE_FLAG\n"
    "SALES,load,ETL,SQL,,,Y\n"
    "SALES,check,ETL,BUSINESS_RULES,N,1,Y\n"
    "MART,build,ETL,SQL,,,N\n",
    "task_parameters.csv": "PIPELINE_CODE,TASK_CODE,PARAMETER_NAME,PARAMETER_VALUE\n"
    'SALES,load,SOURCE_SQL,"SELECT id,\n       name FROM raw.orders"\n',
    "task_dependencies.csv": "PIPELINE_CODE,TASK_CODE,DEPENDS_ON_PIPELINE_CODE,"
    "DEPENDS_ON_TASK_CODE,DEPENDENCY_TYPE\n"
    "SALES,check,SALES,load,SUCCESS\n",
    "pipeline_dependencies.csv": "PIPELINE_CODE,DEPENDS_ON_PIPELINE_CODE,DEPENDENCY_TYPE\n"
    "MART,SALES,SUCCESS\n",
    "business_rules.csv": "PIPELINE_CODE,TASK_CODE,BUSINESS_RULE_NAME,SEQUENCE_NUMBER,"
    "BUSINESS_RULE_TYPE,BUSINESS_RULE_KEY_COLUMN,TARGET_TABLE,BUSINESS_RULE_SQL\n"
    "SALES,check,closed,1,REJECT,ROW_ID,sales.orders,"
    "\"SELECT 1 FROM sales.customers c WHERE c.id = t.customer_id AND c.status = 'closed'\"\n",
}


def folder(tmp_path, **files):
    for name, content in (GOOD | {f"{key}.csv": value for key, value in files.items()}).items():
        if content is not None:
            (tmp_path / name).write_text(content, "utf-8")
    return tmp_path


def problems(tmp_path, **files):
    with pytest.raises(MetadataFileError) as caught:
        read_config_files(folder(tmp_path, **files))
    return str(caught.value)


def test_good_files_read_with_types_defaults_and_quoted_cells(tmp_path):
    read = read_config_files(folder(tmp_path))

    sales = read.rows["pipelines.csv"][("SALES",)].values
    assert (sales["SLA_IN_HOURS"], sales["PIPELINE_PARAMETERS"], sales["CATCHUP"]) == (
        1.5,
        {"TAGS": ["sales"]},
        "N",
    )
    assert sales["ACTIVE_FLAG"] == "Y"
    assert read.rows["tasks.csv"][("SALES", "check")].values["RUN_CONDITION_COUNT"] == 1
    assert not read.rows["tasks.csv"][("MART", "build")].active
    query = read.rows["task_parameters.csv"][("SALES", "load", "SOURCE_SQL")]
    assert (query.line, query.values["PARAMETER_VALUE"]) == (
        3,
        "SELECT id,\n       name FROM raw.orders",
    )
    assert len(read.revision) == 12
    assert read_config_files(tmp_path).revision == read.revision
    (tmp_path / "tasks.csv").write_text(GOOD["tasks.csv"] + "SALES,extra,ETL,SQL,,,Y\n", "utf-8")
    assert read_config_files(tmp_path).revision != read.revision


def test_a_byte_order_mark_is_ignored(tmp_path):
    (folder(tmp_path) / "pipelines.csv").write_text("﻿" + GOOD["pipelines.csv"], "utf-8")
    assert ("SALES",) in read_config_files(tmp_path).rows["pipelines.csv"]


def test_a_missing_folder_and_a_missing_file_are_named(tmp_path):
    with pytest.raises(MetadataFileError, match=r"no config folder at .*config export"):
        read_config_files(tmp_path / "config")
    found = problems(tmp_path, business_rules=None)
    assert "business_rules.csv is missing: every config file must exist" in found
    assert "BUSINESS_RULE_SQL,ACTIVE_FLAG)" in found


def test_every_header_problem_is_named(tmp_path):
    found = problems(
        tmp_path,
        tasks="PIPELINE_CODE,TASK_CODE,HANDLER,OWNER,HANDLER\n",
        pipelines="",
    )
    assert "pipelines.csv is empty: write its header row (PIPELINE_CODE," in found
    assert "tasks.csv line 1: unknown column 'OWNER'; its columns are PIPELINE_CODE," in found
    assert "tasks.csv line 1: column HANDLER appears more than once" in found
    assert "tasks.csv line 1: column TASK_TYPE is missing; it needs a value in every row" in found


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        ("SALES,load,ETL", "line 2: 3 values for 7 columns; quote a value that holds a comma"),
        ("SALES,2load,ETL,SQL,,,Y", "column TASK_CODE: '2load' is not a code"),
        ("SALES,load,BATCH,SQL,,,Y", "column TASK_TYPE: 'BATCH' is not one of INGESTION, ETL"),
        ("SALES,load,ETL,SQL,,,y", "column ACTIVE_FLAG: 'y' is not one of Y, N"),
        ("SALES,load,ETL,SQL,N,two,Y", "column RUN_CONDITION_COUNT: 'two' is not a whole number"),
        ("SALES,load,ETL,SQL,N,0,Y", "column RUN_CONDITION_COUNT: 0 is less than 1"),
        ("SALES,load,,SQL,,,Y", "column TASK_TYPE: is empty; this column needs a value"),
        ("SALES,load,ETL,SQL,N,,Y", "line 2: RUN_CONDITION N needs a RUN_CONDITION_COUNT"),
        ("SALES,load,ETL,SQL,ANY,2,Y", "RUN_CONDITION_COUNT goes with RUN_CONDITION N only"),
    ],
)
def test_a_bad_task_row_is_named_with_its_line_and_column(tmp_path, row, expected):
    header = GOOD["tasks.csv"].splitlines()[0]
    found = problems(tmp_path, tasks=f"{header}\n{row}\n")
    assert expected in found


@pytest.mark.parametrize(
    ("cells", "expected"),
    [
        ("SALES,Sales,FULL,1.5x,", "column SLA_IN_HOURS: '1.5x' is not a number"),
        ("SALES,Sales,FULL,inf,", "column SLA_IN_HOURS: 'inf' is not a finite number"),
        ('SALES,Sales,FULL,,"[1, 2]"', "column PIPELINE_PARAMETERS: is not a JSON object"),
        ("SALES,Sales,FULL,,{tags", "column PIPELINE_PARAMETERS: is not JSON"),
    ],
)
def test_a_bad_pipeline_value_is_named(tmp_path, cells, expected):
    header = GOOD["pipelines.csv"].splitlines()[0]
    assert expected in problems(tmp_path, pipelines=f"{header}\n{cells}\nMART,Mart,FULL,,\n")


def test_a_bad_date_is_named(tmp_path):
    found = problems(
        tmp_path,
        pipelines="PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE,SCHEDULE_START_DATE\n"
        "SALES,Sales,FULL,2026-02-30\nMART,Mart,FULL,11/01/2026\n",
    )
    assert "line 2, column SCHEDULE_START_DATE: '2026-02-30' is not a date" in found
    assert (
        "line 3, column SCHEDULE_START_DATE: '11/01/2026' is not a date written YYYY-MM-DD" in found
    )


def test_duplicate_keys_self_dependencies_and_missing_references_are_named(tmp_path):
    found = problems(
        tmp_path,
        task_parameters="PIPELINE_CODE,TASK_CODE,PARAMETER_NAME,PARAMETER_VALUE\n"
        "SALES,load,RETRIES,1\nSALES,load,RETRIES,2\n",
        pipeline_dependencies="PIPELINE_CODE,DEPENDS_ON_PIPELINE_CODE,DEPENDENCY_TYPE\n"
        "MART,MART,SUCCESS\n",
        task_dependencies="PIPELINE_CODE,TASK_CODE,DEPENDS_ON_PIPELINE_CODE,DEPENDS_ON_TASK_CODE,"
        "DEPENDENCY_TYPE\nSALES,load,SALES,load,ALWAYS\n",
    )
    assert (
        "task_parameters.csv lines 2 and 3 both hold SALES.load RETRIES; keep one of them" in found
    )
    assert "pipeline_dependencies.csv line 2: pipeline MART depends on itself" in found
    assert "task_dependencies.csv line 2: SALES.load depends on itself" in found

    missing = problems(
        tmp_path,
        task_dependencies="PIPELINE_CODE,TASK_CODE,DEPENDS_ON_PIPELINE_CODE,DEPENDS_ON_TASK_CODE,"
        "DEPENDENCY_TYPE\nSALES,check,SALES,lod,SUCCESS\n",
        tasks=GOOD["tasks.csv"] + "SALSE,extra,ETL,SQL,,,Y\n",
        task_parameters="PIPELINE_CODE,TASK_CODE,PARAMETER_NAME,PARAMETER_VALUE\n"
        "SALES,load,SOURCE_SQL,SELECT 1\n",
        pipeline_dependencies="PIPELINE_CODE,DEPENDS_ON_PIPELINE_CODE,DEPENDENCY_TYPE\n"
        "MART,SALES,SUCCESS\n",
    )
    assert "tasks.csv line 5: pipeline SALSE is not in pipelines.csv" in missing
    assert "task_dependencies.csv line 2: task SALES.lod is not in tasks.csv" in missing
    assert missing.startswith("2 problem(s) in ")


def test_every_file_holds_its_table_with_active_flag_last():
    assert [table.file for table in TABLES] == [
        "pipelines.csv",
        "tasks.csv",
        "task_parameters.csv",
        "task_dependencies.csv",
        "pipeline_dependencies.csv",
        "business_rules.csv",
    ]
    assert all(table.columns[-1].name == "ACTIVE_FLAG" for table in TABLES)
    assert all(set(table.key) <= {column.name for column in table.columns} for table in TABLES)


@pytest.mark.parametrize(
    ("value", "written"),
    [
        (None, ""),
        (2.0, "2"),
        (0.0002, "0.0002"),
        (1e-05, "0.00001"),
        (date(2026, 11, 1), "2026-11-01"),
        ({"TAGS": ["sales"]}, '{"TAGS": ["sales"]}'),
        (3, "3"),
        ("it's", "it's"),
    ],
)
def test_values_are_written_as_the_files_hold_them(value, written):
    assert cell(value) == written


@pytest.mark.parametrize("verb", ["plan", "apply", "export"])
def test_the_config_option_after_a_verb_is_refused_not_taken_for_the_folder(verb, capsys):
    with pytest.raises(SystemExit) as stopped:
        main(["config", verb, "--config", "elsewhere/craft-connector.yml"])
    assert stopped.value.code == ExitCode.USAGE
    assert "unrecognized arguments: --config" in capsys.readouterr().err
