"""A SQL task's parameters are checked before the warehouse is touched."""

import pytest

from etl_craft.config import ConnectionProfile, ConnectionSection, ConnectorConfig, SourceConfig
from etl_craft.core.enums import Mode, SqlAction
from etl_craft.core.errors import HandlerError, MetadataError
from etl_craft.handlers.registry import TaskContext
from etl_craft.handlers.sql.spec import parse_flag, read_sql_task

pytestmark = pytest.mark.unit


@pytest.fixture
def project(tmp_path):
    root = tmp_path / "etl-craft"
    (root / "sql_files").mkdir(parents=True)
    return root


def context(project, refresh_type="INCREMENTAL", **params):
    engine = ConnectionProfile("ENGINE", "dev", "jdbc:sqlite:e.db", "", "none")
    config = ConnectorConfig(
        mode=Mode.LOCAL,
        source=SourceConfig(type="environment"),
        engine=ConnectionSection("dev", {"dev": engine}),
        config_path=project / "craft-connector.yml",
    )
    return TaskContext(
        config=config,
        pipeline_id=1,
        pipeline_code="P",
        task_id=2,
        task_code="T",
        pipeline_run_id=42,
        task_run_id=7,
        attempt=1,
        handler="SQL",
        refresh_type=refresh_type,
        task_params=params,
    )


BASE = {"SQL_ACTION": "OVERWRITE_TABLE", "TARGET_OBJECT": "sales.orders"}


def test_a_complete_merge_task(project):
    task = read_sql_task(
        context(
            project,
            SQL_ACTION="scd2_merge",
            TARGET_OBJECT=" sales.orders ",
            SOURCE_SQL="SELECT id, name FROM raw.orders WHERE $$pipeline_id_filter;",
            PIPELINE_ID_FILTER="TRUE",
            MERGE_KEY="id | region",
            MERGE_COMPARE_COLUMNS="name",
            MERGE_DEDUPE_ORDER="updated_at DESC NULLS LAST,id",
            SCHEMA_EVOLUTION="true",
        )
    )
    assert task.action == SqlAction.SCD2_MERGE
    assert task.target_object == "sales.orders"
    assert task.select_sql == "SELECT id, name FROM raw.orders WHERE pipeline_run_id = 42"
    assert (task.merge_key, task.merge_compare_columns) == (("id", "region"), ("name",))
    assert task.dedupe_order == "updated_at DESC NULLS LAST, id"
    assert task.schema_evolution and not task.preserve_target
    assert task.source == "SOURCE_SQL"


def test_a_sql_file_is_read_from_sql_files(project):
    (project / "sql_files" / "orders.sql").write_text(
        "-- all orders on a full refresh\nSELECT $$pipeline_id AS run\nFROM raw.orders\n"
        "WHERE $$pipeline_id_filter\n",
        encoding="utf-8",
    )
    task = read_sql_task(
        context(
            project,
            refresh_type="FULL",
            SOURCE_SQL_FILE="orders.sql",
            PIPELINE_ID_SUBSTITUTION="true",
            PIPELINE_ID_FILTER="true",
            **BASE,
        )
    )
    assert task.select_sql.endswith("SELECT 42 AS run\nFROM raw.orders\nWHERE 1=1")
    assert task.source == "SOURCE_SQL_FILE='orders.sql'"


def test_drop_table_takes_no_select(project):
    task = read_sql_task(context(project, SQL_ACTION="DROP_TABLE", TARGET_OBJECT="s.t"))
    assert (task.select_sql, task.source) == (None, "none")
    with pytest.raises(HandlerError, match="DROP_TABLE takes no SELECT, but SOURCE_SQL is set"):
        read_sql_task(
            context(project, SQL_ACTION="DROP_TABLE", TARGET_OBJECT="s.t", SOURCE_SQL="SELECT 1")
        )


@pytest.mark.parametrize(
    ("params", "message"),
    [
        ({"TARGET_OBJECT": "s.t"}, "SQL_ACTION is required; one of CREATE_TABLE"),
        (
            {"SQL_ACTION": "SCD1_MERG", "TARGET_OBJECT": "s.t"},
            "SQL_ACTION='SCD1_MERG' is not one of .* did you mean: SCD1_MERGE",
        ),
        ({"SQL_ACTION": "CREATE_TABLE"}, "TARGET_OBJECT is required for SQL_ACTION=CREATE_TABLE"),
        (
            {"SQL_ACTION": "CREATE_TABLE", "TARGET_OBJECT": "orders"},
            "TARGET_OBJECT='orders' must be 'schema.table' or 'database.schema.table'",
        ),
        (BASE, "needs a SELECT: set SOURCE_SQL, or SOURCE_SQL_FILE"),
        (
            {**BASE, "SOURCE_SQL": "SELECT 1", "SOURCE_SQL_FILE": "a.sql"},
            "set SOURCE_SQL or SOURCE_SQL_FILE, not both",
        ),
        (
            {**BASE, "SOURCE_SQL": "SELECT 1; SELECT 2"},
            "SOURCE_SQL must hold exactly one SELECT; it holds 2 statements",
        ),
        (
            {**BASE, "SOURCE_SQL": "DELETE FROM s.t"},
            "SOURCE_SQL must be a read-only SELECT .* starts with 'DELETE'",
        ),
        ({**BASE, "SOURCE_SQL": "SELECT 1", "SCHEMA_EVOLUTION": "yes"}, "must be true or false"),
        (
            {**BASE, "SOURCE_SQL": "SELECT 1", "HARD_DELETE": "true"},
            "HARD_DELETE applies only to DELETE_ROWS, not SQL_ACTION=OVERWRITE_TABLE",
        ),
        (
            {**BASE, "SOURCE_SQL": "SELECT 1", "MERGE_KEY": "id"},
            "MERGE_KEY does not apply to SQL_ACTION=OVERWRITE_TABLE",
        ),
        (
            {**BASE, "SOURCE_SQL": "SELECT 1", "MERGE_DEDUPE_ORDER": "id"},
            "MERGE_DEDUPE_ORDER applies only to SCD1_MERGE and SCD2_MERGE",
        ),
        (
            {"SQL_ACTION": "DELETE_ROWS", "TARGET_OBJECT": "s.t", "SOURCE_SQL": "SELECT 1"},
            "MERGE_KEY is required for SQL_ACTION=DELETE_ROWS",
        ),
        (
            {
                "SQL_ACTION": "SCD1_MERGE",
                "TARGET_OBJECT": "s.t",
                "SOURCE_SQL": "SELECT 1",
                "MERGE_KEY": "id; drop",
                "MERGE_COMPARE_COLUMNS": "a",
            },
            "MERGE_KEY='id; drop': 'id; drop' is not a plain column name",
        ),
        (
            {
                "SQL_ACTION": "SCD1_MERGE",
                "TARGET_OBJECT": "s.t",
                "SOURCE_SQL": "SELECT 1",
                "MERGE_KEY": "id",
                "MERGE_COMPARE_COLUMNS": "a",
                "MERGE_DEDUPE_ORDER": "lower(a) DESC",
            },
            "MERGE_DEDUPE_ORDER='lower\\(a\\) DESC': 'lower\\(a\\) DESC' is not",
        ),
    ],
)
def test_definition_mistakes_fail_with_the_remedy(project, params, message):
    with pytest.raises(HandlerError, match=message):
        read_sql_task(context(project, **params))


def test_setup_for_names_the_writing_action(project):
    setup = {"SQL_ACTION": "SETUP_TABLE", "TARGET_OBJECT": "s.t", "SOURCE_SQL": "SELECT 1"}
    assert read_sql_task(context(project, **setup)).setup_for is None
    assert read_sql_task(context(project, SETUP_FOR="scd2_merge", **setup)).setup_for == (
        SqlAction.SCD2_MERGE
    )
    with pytest.raises(HandlerError, match="SETUP_FOR='DROP_TABLE' must name the action that"):
        read_sql_task(context(project, SETUP_FOR="DROP_TABLE", **setup))
    with pytest.raises(HandlerError, match="SETUP_FOR applies only to SETUP_TABLE"):
        read_sql_task(context(project, SETUP_FOR="SCD1_MERGE", SOURCE_SQL="SELECT 1", **BASE))
    with pytest.raises(
        HandlerError, match=r"SCHEMA_EVOLUTION applies only to .* not SQL_ACTION=APP"
    ):
        read_sql_task(
            context(
                project,
                SQL_ACTION="APPEND_TABLE",
                TARGET_OBJECT="s.t",
                SOURCE_SQL="SELECT 1",
                SCHEMA_EVOLUTION="true",
            )
        )


def test_a_missing_or_unreadable_sql_file_is_metadata(project):
    with pytest.raises(MetadataError, match=r"SOURCE_SQL_FILE='nope\.sql': no such file"):
        read_sql_task(context(project, SOURCE_SQL_FILE="nope.sql", **BASE))
    (project / "sql_files" / "latin1.sql").write_bytes(b"SELECT '\xe9'")
    with pytest.raises(MetadataError, match=r"SOURCE_SQL_FILE='latin1\.sql': cannot read"):
        read_sql_task(context(project, SOURCE_SQL_FILE="latin1.sql", **BASE))


def test_flags():
    assert parse_flag({}, "X") is False
    assert parse_flag({"X": " "}, "X") is False
    assert parse_flag({"X": " False "}, "X") is False
    assert parse_flag({"X": "TRUE"}, "X") is True
