"""Exact vendor descriptions and conservative shape fallbacks without cloud credentials."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from etl_craft.core.enums import TableFormat
from etl_craft.core.errors import HandlerError, SqlGuardError
from etl_craft.dialects.warehouse import for_key
from etl_craft.dialects.warehouse.metadata import (
    ddl_tables,
    declarable_source,
    projection_names,
)

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    "description, expected",
    [
        ({"name": "decimal", "precision": 18, "scale": 4}, "DECIMAL(18,4)"),
        ({"name": "varchar", "length": 23}, "VARCHAR(23)"),
        ({"name": "char", "length": 7}, "CHAR(7)"),
        ({"name": "array", "element_type": {"name": "string"}}, "ARRAY<STRING>"),
        (
            {"name": "map", "key_type": {"name": "string"}, "value_type": {"name": "int"}},
            "MAP<STRING,INT>",
        ),
        (
            {
                "name": "struct",
                "fields": [{"name": "odd`name", "type": {"name": "bigint"}}],
            },
            "STRUCT<`odd``name`:BIGINT>",
        ),
        ({"name": "timestamp_ntz"}, "TIMESTAMP_NTZ"),
    ],
)
def test_databricks_description_preserves_nested_types_and_sizes(description, expected):
    assert for_key("databricks").json_type(description) == expected


@pytest.mark.parametrize("description", ["STRING", {"name": "future_type"}])
def test_databricks_unknown_type_requires_explicit_migration(description):
    with pytest.raises(
        SqlGuardError, match=r"Databricks column type.*expected.*(?:repair|migrate)"
    ):
        for_key("databricks").json_type(description)


@pytest.mark.parametrize(
    "table_format, enabled", [(TableFormat.NATIVE, ""), (TableFormat.ICEBERG, "iceberg")]
)
def test_databricks_one_description_supplies_types_nullability_and_storage(table_format, enabled):
    conn = Mock()
    conn.execute.return_value.scalar_one.return_value = json.dumps(
        {
            "type": "MANAGED",
            "provider": "delta",
            "table_properties": {"delta.universalFormat.enabledFormats": enabled},
            "columns": [
                {"name": "id", "type": {"name": "bigint"}, "nullable": False},
                {"name": "name", "type": {"name": "string"}, "nullable": True},
            ],
        }
    )
    found = for_key("databricks").read_metadata(conn, "cat.s.t")
    assert found.columns == (
        ("id", "BIGINT", None, None, "NO"),
        ("name", "STRING", None, None, "YES"),
    )
    assert found.table_format == table_format
    conn.execute.assert_called_once()
    assert str(conn.execute.call_args.args[0]) == "DESCRIBE TABLE EXTENDED cat.s.t AS JSON"


@pytest.mark.parametrize("description", ["{", "{}", '{"columns": null}', '{"columns": [{}]}'])
def test_databricks_incomplete_description_is_not_a_missing_table(description):
    conn = Mock()
    conn.execute.return_value.scalar_one.return_value = description
    with pytest.raises(
        SqlGuardError, match=r"cat.s.t: DESCRIBE returned invalid column metadata; expected.*repair"
    ):
        for_key("databricks").read_metadata(conn, "cat.s.t")


@pytest.mark.parametrize("key, code", [("databricks", "42P01"), ("snowflake", "42S02")])
@pytest.mark.parametrize("missing", [True, False])
def test_only_vendor_missing_table_errors_become_empty_metadata(key, code, missing):
    conn = Mock()
    error = SQLAlchemyError(f"SQLSTATE: {code if missing else '42501'}")
    error.sqlstate = code if missing else "42501"
    conn.execute.side_effect = error
    if missing:
        assert for_key(key).read_metadata(conn, "cat.s.t").columns == ()
    else:
        with pytest.raises(SQLAlchemyError) as caught:
            for_key(key).read_metadata(conn, "cat.s.t")
        assert caught.value is error


@pytest.mark.parametrize("generated", [None, "IDENTITY START 1", "AUTOINCREMENT START 1"])
def test_snowflake_description_supplies_identity_without_an_extra_read(generated):
    conn = Mock()
    conn.execute.return_value.mappings.return_value.all.return_value = [
        {"name": "ROW_ID", "type": "NUMBER(38,0)", "null?": "N", "default": generated},
        {"name": "NAME", "type": "VARCHAR(23)", "null?": "Y", "default": None},
    ]
    found = for_key("snowflake").read_metadata(conn, "cat.s.t")
    assert found.columns == (
        ("ROW_ID", "NUMBER(38,0)", None, None, "NO"),
        ("NAME", "VARCHAR(23)", None, None, "YES"),
    )
    assert found.generated is (generated is not None)
    conn.execute.assert_called_once()


@pytest.mark.parametrize(
    "attributes, expected",
    [
        ({"type_code": 0, "precision": 18, "scale": 4}, "NUMBER(18,4)"),
        ({"type_code": 0, "precision": 18}, None),
        ({"type_code": 2, "internal_size": 23}, "VARCHAR(23)"),
        ({"type_code": 2}, None),
        ({"type_code": 13}, "BOOLEAN"),
        ({"type_code": 8}, "TIMESTAMP_NTZ"),
        ({"type_code": 999}, None),
        ({"type_code": "unknown"}, None),
        ({"type_code": 9, "fields": ["nested"]}, None),
    ],
)
def test_snowflake_protocol_types_are_exact_or_require_materialization(attributes, expected):
    assert for_key("snowflake").result_type(SimpleNamespace(**attributes)) == expected


@pytest.mark.parametrize(
    "name, attributes, expected",
    [
        ("varchar", {"display_size": 23}, "varchar(23)"),
        ("varchar", {}, "VARCHAR"),
        ("bpchar", {"display_size": 7}, "CHAR(7)"),
        ("bpchar", {}, None),
        ("numeric", {"precision": 18, "scale": 4}, "NUMERIC(18,4)"),
        ("numeric", {"precision": 18}, None),
        ("int8", {}, "BIGINT"),
        ("uuid", {}, "UUID"),
        ("extension_type", {}, None),
        (None, {}, None),
    ],
)
def test_postgres_protocol_types_keep_sizes_and_refuse_unknown_modifiers(
    name, attributes, expected
):
    registry = Mock()
    registry.get.return_value = SimpleNamespace(name=name) if name else None
    conn = SimpleNamespace(
        connection=SimpleNamespace(
            dbapi_connection=SimpleNamespace(adapters=SimpleNamespace(types=registry))
        )
    )
    assert (
        for_key("postgres").result_type(conn, SimpleNamespace(type_code=42, **attributes))
        == expected
    )
    registry.get.assert_called_once_with(42)


def test_postgres_closed_driver_cannot_describe_types():
    conn = SimpleNamespace(connection=SimpleNamespace(dbapi_connection=None))
    assert for_key("postgres").result_type(conn, SimpleNamespace(type_code=42)) is None


@pytest.mark.parametrize("unknown", [False, True])
def test_snowflake_describes_source_once_and_defers_incomplete_protocol_types(unknown):
    conn = MagicMock()
    result = MagicMock()
    # Driver descriptions expose both named fields and tuple indexing.
    column = MagicMock(type_code=999 if unknown else 13, fields=None)
    column.__getitem__.return_value = "READY"
    result.cursor.description = [column]
    conn.execute.return_value.__enter__.return_value = result
    found = for_key("snowflake").describe_select(conn, "SELECT true AS ready")
    assert found == (None if unknown else [("READY", "BOOLEAN")])
    assert "LIMIT 0" in str(conn.execute.call_args.args[0])
    conn.execute.assert_called_once()
    assert for_key("snowflake_iceberg").describe_select(conn, "SELECT true AS ready") is None
    conn.execute.assert_called_once()


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT id, name AS label FROM source", ["ID", "LABEL"]),
        ("SELECT id FROM source UNION ALL SELECT id FROM other", ["ID"]),
        ('SELECT id AS "Mixed" FROM source', None),
        ("SELECT * FROM source", None),
        ("SELECT 1 + 2", None),
        ("SELECT id AS x, name AS X FROM source", None),
        ("DELETE FROM source", None),
        ("SELECT (", None),
    ],
)
def test_only_unambiguous_projection_names_skip_the_probe(sql, expected):
    assert projection_names(sql, "snowflake", "upper") == expected


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("ALTER TABLE s.old RENAME TO new", {"old", "new"}),
        ("DROP TABLE s.removed", None),
        ("CREATE INDEX idx ON s.old(id)", set()),
        ("RENAME TABLE s.old TO s.new", None),
        ("ALTER TABLE (", None),
        ("COMMENT ON TABLE s.old IS 'hello'", None),
        ("-- comment\nALTER TABLE s.old ADD value INT", None),
        ("SELECT 1", set()),
        ("", set()),
    ],
)
def test_unknown_ddl_invalidates_all_metadata_and_known_ddl_only_its_tables(sql, expected):
    assert ddl_tables(sql, "postgres") == expected


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT 1 AS id", True),
        ("SELECT id FROM s.t", False),
        ("SELECT 'a' COLLATE \"C\"", False),
        ("SELECT (", False),
    ],
)
def test_source_types_with_inherited_collation_require_catalog_metadata(sql, expected):
    assert declarable_source(sql, "postgres") is expected


@pytest.mark.parametrize("key", ["snowflake", "databricks"])
@pytest.mark.parametrize("existing", [False, True])
def test_identity_replacement_keeps_generator_and_publishes_only_from_candidate(key, existing):
    conn = Mock()
    identity = (
        "AUTOINCREMENT START 1 INCREMENT 1"
        if key == "snowflake"
        else "GENERATED ALWAYS AS IDENTITY"
    )
    conn.execute.return_value.scalar_one.return_value = (
        f"CREATE TABLE c.s.target (id BIGINT, ROW_ID BIGINT {identity}) COMMENT 'preserved'"
    )
    create, publish = for_key(key).identity_replacement_ddl(
        conn, "c.s.target", "c.s.candidate", "id BIGINT", {}, existing=existing
    )
    assert create.startswith("CREATE TABLE c.s.candidate")
    assert "ROW_ID BIGINT" in create
    assert identity.split()[0] in create
    assert publish == (
        "CREATE OR REPLACE TABLE c.s.target CLONE c.s.candidate COPY GRANTS"
        if key == "snowflake"
        else "CREATE OR REPLACE TABLE c.s.target DEEP CLONE c.s.candidate"
    )
    assert ("preserved" in create) is existing
    assert conn.execute.call_count == int(existing)


def test_existing_delta_identity_replacement_refuses_a_new_external_location_before_sql():
    conn = Mock()
    with pytest.raises(
        SqlGuardError,
        match=r"c\.s\.target: replacement cannot change EXTERNAL_LOCATION; use OVERWRITE_TABLE",
    ):
        for_key("databricks").identity_replacement_ddl(
            conn,
            "c.s.target",
            "c.s.candidate",
            "id BIGINT",
            {"EXTERNAL_LOCATION": "s3://new"},
            existing=True,
        )
    conn.execute.assert_not_called()


@pytest.mark.parametrize("count, expected", [(7, 7), (-1, 8)])
def test_databricks_uses_exact_protocol_or_dml_result_counts(count, expected):
    result = Mock(rowcount=count)
    result.mappings.return_value.one.return_value = {"num_affected_rows": 8}
    assert for_key("databricks").affected_rows(result) == expected
    assert result.mappings.call_count == int(count < 0)


@pytest.mark.parametrize("key", ["postgres", "databricks"])
def test_unavailable_dml_count_is_refused_instead_of_recording_negative_rows(key):
    result = Mock(rowcount=-1)
    result.mappings.return_value.one.return_value = {}
    with pytest.raises(
        SqlGuardError,
        match="DML returned rowcount=-1; expected an affected-row count; use a driver",
    ):
        for_key(key).affected_rows(result)


def test_postgres_source_description_uses_protocol_once_and_does_not_probe_inherited_types():
    conn = MagicMock()
    conn.connection.dbapi_connection.adapters.types.get.return_value = SimpleNamespace(name="int8")
    column = MagicMock(type_code=20)
    column.__getitem__.return_value = "id"
    conn.execute.return_value.__enter__.return_value.cursor.description = [column]
    dialect = for_key("postgres")
    assert dialect.describe_select(conn, "SELECT CAST(1 AS BIGINT) AS id") == [("id", "BIGINT")]
    conn.execute.assert_called_once()
    assert dialect.describe_select(conn, "SELECT id FROM c.s.t") is None
    conn.execute.assert_called_once()


@pytest.mark.parametrize("step", ["columns", "source"])
def test_metadata_errors_keep_task_action_target_and_original_cause(monkeypatch, step):
    from etl_craft.handlers.sql.session import Session

    monkeypatch.setattr("etl_craft.handlers.sql.session.event", Mock())
    dialect = Mock()
    error = SQLAlchemyError("permission denied")
    dialect.read_metadata.side_effect = error
    dialect.describe_select.side_effect = error
    session = Session(
        Mock(),
        dialect,
        catalog="c",
        action="CREATE_TABLE",
        target_object="s.t",
        task_run_id=1,
        params={},
    )
    try:
        with pytest.raises(
            HandlerError, match=r"CREATE_TABLE c.s.t: .* failed: SQLAlchemyError: permission denied"
        ) as caught:
            session.columns(session.target) if step == "columns" else session.describe_select(
                "SELECT 1 AS id"
            )
        assert caught.value.__cause__ is error
    finally:
        session.sweep()


def test_known_add_column_updates_cache_and_failed_ddl_forces_reread():
    from sqlalchemy import create_engine, text

    from etl_craft.handlers.sql.session import Session

    engine = create_engine("sqlite://")
    dialect = Mock(wraps=for_key("snowflake"))
    dialect.sqlglot_dialect = "snowflake"
    dialect.metadata_types_complete = False
    dialect.read_metadata.return_value = SimpleNamespace(
        columns=(("id", "BIGINT", None, None, "NO"),),
        generated=True,
        table_format=TableFormat.NATIVE,
    )
    dialect.full_column_types.return_value = {"id": "BIGINT"}
    try:
        with engine.connect() as conn:
            conn.execute(text("CREATE TABLE shape (id BIGINT NOT NULL)"))
            session = Session(
                conn,
                dialect,
                catalog="c",
                action="APPEND_TABLE",
                target_object="s.shape",
                task_run_id=1,
                params={},
            )
            try:
                assert session.columns("shape") == [("id", "BIGINT")]
                assert session.column_types("shape") == {"id": "BIGINT"}
                session.add_column("shape", "name", "VARCHAR(23)")
                assert session.columns("shape") == [("id", "BIGINT"), ("name", "VARCHAR(23)")]
                assert session.column_types("shape") == {"id": "BIGINT", "name": "VARCHAR(23)"}
                assert session.not_null_columns("shape") == {"id"}
                dialect.read_metadata.assert_called_once()
                dialect.full_column_types.assert_called_once()
                with pytest.raises(HandlerError, match=r"add column name.*duplicate column"):
                    session.add_column("shape", "name", "VARCHAR(23)")
                # Invalidated on the failed DDL, so the next shape read must consult the DB.
                dialect.read_metadata.return_value = SimpleNamespace(
                    columns=(
                        ("id", "BIGINT", None, None, "NO"),
                        ("name", "VARCHAR(23)", None, None, "YES"),
                    ),
                    generated=True,
                    table_format=TableFormat.NATIVE,
                )
                assert session.columns("shape") == [("id", "BIGINT"), ("name", "VARCHAR(23)")]
                assert dialect.read_metadata.call_count == 2
            finally:
                session.sweep()
    finally:
        engine.dispose()
