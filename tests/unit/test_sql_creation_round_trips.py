"""Identity replacements populate independent candidates before publication."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from etl_craft.core.errors import HandlerError
from etl_craft.dialects.warehouse import for_key
from etl_craft.handlers.sql.context import ActionContext
from etl_craft.handlers.sql.creation import create_table
from etl_craft.handlers.sql.session import Session

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("key", ["snowflake", "databricks"])
@pytest.mark.parametrize("publication", ["new", "existing", "external"])
@pytest.mark.parametrize("insert_fails", [False, True])
def test_identity_candidate_is_populated_before_rename_or_clone(key, publication, insert_fails):
    session = Mock(spec=Session)
    session.conn = Mock()
    session.uploads = ()
    session.dialect = for_key(key)
    session.target = "c.s.target"
    session.token = "test"
    session.clear_hash_version = False
    session.params = (
        {"EXTERNAL_LOCATION": "s3://bucket/target"} if publication == "external" else {}
    )
    session.target_columns.return_value = [("id", "BIGINT")] if publication == "existing" else []
    session.describe_select.return_value = None
    session.scratch.return_value = "c.s.candidate"
    session.column_types.return_value = {"id": "BIGINT"}
    session.columns.return_value = [("id", "BIGINT")]
    # Existing definitions come from the warehouse and must survive the clone.
    identity = (
        "AUTOINCREMENT START 1 INCREMENT 1 ORDER"
        if key == "snowflake"
        else "GENERATED ALWAYS AS IDENTITY"
    )
    session.conn.execute.return_value.scalar_one.return_value = (
        f"CREATE TABLE c.s.target (id BIGINT, ROW_ID BIGINT {identity}) COMMENT 'kept'"
    )
    events = []

    def run(sql, *, step):
        events.append(sql)
        if sql.startswith("INSERT") and insert_fails:
            raise HandlerError("populate the identity replacement failed")
        return Mock(rowcount=2)

    session.run.side_effect = run
    session.rename.side_effect = lambda before, after: events.append(f"rename {before} to {after}")
    session.drop.side_effect = lambda name: events.append(f"drop {name}")
    action = Mock(spec=ActionContext)
    action.uploads = ()
    action.task = SimpleNamespace(watermark=None, allow_empty=True)
    action.staged = None
    action.build_stage.return_value = "stage"
    action.context = SimpleNamespace(pipeline_run_id=11, pipeline_id=12, task_run_id=13)
    if insert_fails:
        with pytest.raises(HandlerError, match="populate the identity replacement failed"):
            create_table(session, action)
        assert len(events) == 2
        assert session.clear_hash_version is False
        session.rename.assert_not_called()
        session.drop.assert_not_called()
    else:
        result = create_table(session, action)
        assert (result.source_count, result.target_count, result.insert_count) == (2, 2, 2)
        if publication == "new":
            assert events[2] == "rename c.s.candidate to c.s.target"
        else:
            assert events[2] == (
                "CREATE OR REPLACE TABLE c.s.target CLONE c.s.candidate COPY GRANTS"
                if key == "snowflake"
                else "CREATE OR REPLACE TABLE c.s.target DEEP CLONE c.s.candidate LOCATION 's3://bucket/target'"
                if publication == "external"
                else "CREATE OR REPLACE TABLE c.s.target DEEP CLONE c.s.candidate"
            )
        assert events[-1] == "drop stage"
        assert session.clear_hash_version is True
    assert events[0].startswith("CREATE TABLE c.s.candidate")
    assert "ROW_ID BIGINT" in events[0]
    assert events[1] == (
        "INSERT INTO c.s.candidate (id, PIPELINE_RUN_ID, PIPELINE_ID, TASK_RUN_ID) "
        "SELECT s.*, CAST(11 AS BIGINT) AS PIPELINE_RUN_ID, CAST(12 AS BIGINT) AS PIPELINE_ID, "
        "CAST(13 AS BIGINT) AS TASK_RUN_ID FROM stage s"
    )
    session.count.assert_not_called()
