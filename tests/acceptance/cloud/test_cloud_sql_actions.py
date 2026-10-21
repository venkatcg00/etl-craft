"""Every SQL action on Databricks (Delta, and Delta with UniForm) and Snowflake (native, Iceberg).

One test per warehouse and table format walks the whole action vocabulary against the live
warehouse, in its configured schema, with table names unique to the run; the tables are dropped
afterwards. Credentials come from the same variables as ``test_cloud_connections``.
"""

import os
import uuid
from dataclasses import replace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import TableFormat
from etl_craft.core.errors import HandlerError
from etl_craft.handlers import business_rules
from etl_craft.warehouse.connection import build_warehouse_engine, warehouse_dialect
from fixtures.cloud import DATABRICKS_VARS, SNOWFLAKE_VARS, require_variables, write_config
from fixtures.engine_db import apply_schema, sqlite_engine_db
from fixtures.metadata import add_pipeline, add_table, insert, start_run
from fixtures.scd_lifecycle import check_scd1_upgrade, check_scd_lifecycle
from fixtures.sql_appends import check_append_retries, check_identity_inputs, check_legacy_upgrade
from fixtures.sql_evolution import check_evolution, check_interrupted_evolution
from fixtures.sql_merges import check_composite_merge
from fixtures.sql_rejects import check_rejects
from fixtures.sql_replacement import check_replacement_failure
from fixtures.sql_row_ids import check_row_id_generation
from fixtures.sql_warehouse import SqlWorld
from fixtures.sql_watermarks import check_watermark_literals, check_watermarks

# Some forty statements against a remote warehouse, one to three seconds each, cold start aside.
pytestmark = [pytest.mark.timeout(1200), pytest.mark.usefixtures("engine_actor")]


def cloud_world(tmp_path, name, fields, table_format, schema):
    project = tmp_path / "etl-craft"
    (project / "sql_scripts").mkdir(parents=True)
    config = write_config(project, name, fields, table_format)
    engine_db = sqlite_engine_db(project).engine
    apply_schema(engine_db)
    with engine_db.begin() as conn:
        pipeline_id = add_pipeline(conn, "P")
        run_id = start_run(conn, pipeline_id)
    warehouse = build_warehouse_engine(config)
    return SqlWorld(
        "cloud",
        config,
        engine_db,
        warehouse,
        active_catalog(config),
        schema,
        pipeline_id,
        run_id,
    )


def walk_every_action(w):
    suffix = uuid.uuid4().hex[:8]
    names = {
        base: f"etl_craft_{base}_{suffix}"
        for base in (
            "orders",
            "customers",
            "history",
            "events",
            "joined_scd1",
            "joined_scd2",
            "replace_create",
            "replace_overwrite",
        )
    }
    scd = {"MERGE_KEY": "id", "MERGE_COMPARE_COLUMNS": "name"}
    # Typed like a real source column: Snowflake gives a bare literal its own length, and the
    # target takes the SELECT's types.
    ann = "CAST('Ann' AS VARCHAR(20))"
    try:
        created = w.run(
            "create",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=names["orders"],
            SELECT_QUERY="SELECT 1 AS id, 'a' AS name UNION ALL SELECT 2, 'b'",
        )
        assert created.insert_count == 2
        assert sorted(w.rows(f"SELECT row_id FROM {w.name(names['orders'])}")) == [(1,), (2,)]

        copy = names["orders"] + "_copy"
        w.setup(copy, f"SELECT id, name FROM {w.name(names['orders'])}", "OVERWRITE_TABLE")
        overwrite = w.run(
            "overwrite",
            SQL_ACTION="OVERWRITE_TABLE",
            TARGET_OBJECT=names["orders"] + "_copy",
            SELECT_QUERY=f"SELECT id, name FROM {w.name(names['orders'])}",
        )
        assert overwrite.insert_count == 2

        shape = f"SELECT 1 AS id, {ann} AS name"
        w.setup(names["customers"], shape, "SCD1_MERGE")
        w.setup(names["history"], shape, "SCD2_MERGE")
        first = w.run(
            "scd1",
            SQL_ACTION="SCD1_MERGE",
            TARGET_OBJECT=names["customers"],
            SELECT_QUERY=f"SELECT 1 AS id, {ann} AS name UNION ALL SELECT 2, 'Bo'",
            **scd,
        )
        assert (first.insert_count, first.update_count) == (2, 0)
        second = w.run(
            "scd1",
            SQL_ACTION="SCD1_MERGE",
            TARGET_OBJECT=names["customers"],
            SELECT_QUERY=f"SELECT 1 AS id, {ann} AS name UNION ALL SELECT 2, 'Bob'",
            **scd,
        )
        assert (second.insert_count, second.update_count) == (0, 1)

        w.run(
            "scd2",
            SQL_ACTION="SCD2_MERGE",
            TARGET_OBJECT=names["history"],
            SELECT_QUERY=f"SELECT 1 AS id, {ann} AS name",
            **scd,
        )
        changed = w.run(
            "scd2",
            SQL_ACTION="SCD2_MERGE",
            TARGET_OBJECT=names["history"],
            SELECT_QUERY="SELECT 1 AS id, 'Anna' AS name",
            **scd,
        )
        assert (changed.update_count, changed.insert_count, changed.target_count) == (1, 1, 2)
        for target in (names["customers"], names["history"]):
            with w.warehouse.connect() as conn:
                rows = conn.execute(
                    text(
                        f"SELECT CREATED_BY AS created_by, UPDATED_BY AS updated_by "
                        f"FROM {w.name(target)}"
                    )
                ).all()
            assert rows
            assert all(
                row.created_by == "etl-craft" and row.updated_by == "etl-craft" for row in rows
            )

        w.setup(names["events"], "SELECT 1 AS id", "APPEND_TABLE")
        appended = w.run(
            "append",
            SQL_ACTION="APPEND_TABLE",
            TARGET_OBJECT=names["events"],
            SELECT_QUERY="SELECT 1 AS id UNION ALL SELECT 2",
        )
        assert appended.insert_count == 2

        # A business rule over the merged customers: flag those that have an order.
        rule_context = w.task("rules")
        with w.engine_db.begin() as conn:
            insert(
                conn,
                "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
                "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE_ID, "
                "SEQUENCE_NUMBER) VALUES ('has_order', :p, :t, :rule_sql, 'REPORT', 'id', "
                ":target, 1)",
                "BUSINESS_RULE_ID",
                p=w.pipeline_id,
                t=w.tasks["rules"],
                rule_sql=f"SELECT 1 FROM {w.name(names['orders'])} o WHERE o.id = t.id "
                f"AND :pipeline_id = {w.pipeline_id} "
                f"AND :pipeline_run_id = {w.pipeline_run_id} "
                f"AND :task_run_id = {rule_context.task_run_id}",
                target=add_table(conn, f"{w.schema}.{names['customers']}"),
            )
        rules = business_rules.run(
            replace(w.task("rules"), handler="BUSINESS_RULES", force=True), w.engine_db
        )
        assert rules.insert_count == 2

        soft = w.run(
            "soft",
            SQL_ACTION="DELETE_ROWS",
            TARGET_OBJECT=names["customers"],
            MERGE_KEY="id",
            SELECT_QUERY="SELECT 1 AS id",
        )
        hard = w.run(
            "hard",
            SQL_ACTION="DELETE_ROWS",
            TARGET_OBJECT=names["customers"],
            MERGE_KEY="id",
            HARD_DELETE="true",
            SELECT_QUERY="SELECT 2 AS id",
        )
        assert (soft.delete_count, hard.delete_count) == (1, 1)
        assert w.rows(f"SELECT id, delete_flag FROM {w.name(names['customers'])}") == [(1, "Y")]

        check_composite_merge(w, names["joined_scd1"], "SCD1_MERGE")
        check_composite_merge(w, names["joined_scd2"], "SCD2_MERGE")

        check_replacement_failure(w, names["replace_create"], "CREATE_TABLE")
        check_replacement_failure(w, names["replace_overwrite"], "OVERWRITE_TABLE")

        w.finish("create")
        w.run("drop", SQL_ACTION="DROP_TABLE", TARGET_OBJECT=names["orders"])
        with pytest.raises(DBAPIError):
            w.rows(f"SELECT 1 FROM {w.name(names['orders'])}")
    finally:
        for base in (*names.values(), names["orders"] + "_copy"):
            with w.warehouse.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {w.name(base)}"))
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_every_action_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    schema = os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"]
    walk_every_action(cloud_world(tmp_path, "Databricks", fields, table_format, schema))


@pytest.mark.parametrize(
    "name,variables",
    [
        pytest.param("Databricks", DATABRICKS_VARS, marks=pytest.mark.cloud_databricks),
        pytest.param("Snowflake", SNOWFLAKE_VARS, marks=pytest.mark.cloud_snowflake),
    ],
)
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_scd_lifecycle_and_upgrade(tmp_path, name, variables, table_format):
    prefix = name.upper()
    require_variables(prefix, variables)
    fields = {key.lower(): f"ETL_CRAFT_TEST_{prefix}_{key}" for key in variables}
    schema = os.environ[f"ETL_CRAFT_TEST_{prefix}_SCHEMA"]
    w = cloud_world(tmp_path, name, fields, table_format, schema)
    suffix = uuid.uuid4().hex[:8]
    targets = [f"etl_lifecycle_{kind}_{suffix}" for kind in ("scd1", "scd2", "upgrade")]
    try:
        check_scd_lifecycle(w, targets[0], "SCD1_MERGE")
        check_scd_lifecycle(w, targets[1], "SCD2_MERGE")
        check_scd1_upgrade(w, targets[2])
    finally:
        for target in targets:
            with w.warehouse.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {w.name(target)}"))
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_every_action_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    schema = os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"]
    walk_every_action(cloud_world(tmp_path, "Snowflake", fields, table_format, schema))


def walk_views(w):
    """A view is built, replaced with its grants kept, made secure where the warehouse can, and
    never put in place of a table."""
    suffix = uuid.uuid4().hex[:8]
    orders, view, secure = (f"etl_craft_{base}_{suffix}" for base in ("vorders", "view", "secure"))
    snowflake = warehouse_dialect(w.config).spec.key.startswith("snowflake")
    grantee = "ROLE PUBLIC" if snowflake else "`account users`"
    try:
        w.run(
            "orders",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=orders,
            SELECT_QUERY="SELECT 1 AS id UNION ALL SELECT 2 AS id",
        )
        params = {"SQL_ACTION": "CREATE_VIEW", "TARGET_OBJECT": view}
        w.run("view", SELECT_QUERY=f"SELECT id FROM {w.name(orders)} WHERE id > 1", **params)
        assert w.rows(f"SELECT id FROM {w.name(view)}") == [(2,)]
        w.execute(f"GRANT SELECT ON VIEW {w.name(view)} TO {grantee}")

        w.run("view", SELECT_QUERY=f"SELECT id, id * 10 AS tens FROM {w.name(orders)}", **params)
        assert sorted(w.rows(f"SELECT id, tens FROM {w.name(view)}")) == [(1, 10), (2, 20)]
        with w.warehouse.connect() as conn:
            grants = [
                {key.lower().replace("_", ""): value for key, value in row.items()}
                for row in conn.execute(text(f"SHOW GRANTS ON VIEW {w.name(view)}")).mappings()
            ]
        if snowflake:
            assert any(g["privilege"] == "SELECT" and g["granteename"] == "PUBLIC" for g in grants)
        else:
            assert any(
                g["principal"] == "account users" and g["actiontype"] == "SELECT" for g in grants
            ), grants

        with pytest.raises(HandlerError, match="CREATE_VIEW replaces only views"):
            w.run(
                "on_table", SQL_ACTION="CREATE_VIEW", TARGET_OBJECT=orders, SELECT_QUERY="SELECT 1"
            )
        assert len(w.rows(f"SELECT id FROM {w.name(orders)}")) == 2

        secured = {
            "SQL_ACTION": "CREATE_VIEW",
            "TARGET_OBJECT": secure,
            "SELECT_QUERY": f"SELECT id FROM {w.name(orders)}",
            "SECURE_VIEW": "true",
        }
        if snowflake:
            w.run("secure", **secured)
            with w.warehouse.connect() as conn:
                shown = conn.execute(
                    text(f"SHOW VIEWS LIKE '{secure.upper()}' IN SCHEMA {w.catalog}.{w.schema}")
                ).mappings()
                assert [str(row["is_secure"]).lower() for row in shown] == ["true"]
        else:
            with pytest.raises(HandlerError, match="has no secure views"):
                w.run("secure", **secured)
    finally:
        with w.warehouse.begin() as conn:
            for name in (view, secure):
                conn.execute(text(f"DROP VIEW IF EXISTS {w.name(name)}"))
            conn.execute(text(f"DROP TABLE IF EXISTS {w.name(orders)}"))
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
def test_views_on_databricks(tmp_path):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    schema = os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"]
    walk_views(cloud_world(tmp_path, "Databricks", fields, TableFormat.NATIVE, schema))


@pytest.mark.cloud_snowflake
def test_views_on_snowflake(tmp_path):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    schema = os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"]
    walk_views(cloud_world(tmp_path, "Snowflake", fields, TableFormat.NATIVE, schema))


def walk_schema_evolution(w):
    suffix = uuid.uuid4().hex[:8]
    targets = [f"etl_craft_evolve_{suffix}", f"etl_craft_partial_{suffix}"]
    try:
        check_evolution(w, targets[0])
        check_interrupted_evolution(w, targets[1])
    finally:
        for target in targets:
            w.execute(f"DROP TABLE IF EXISTS {w.name(target)}")
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_schema_evolution_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    schema = os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"]
    walk_schema_evolution(cloud_world(tmp_path, "Databricks", fields, table_format, schema))


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_schema_evolution_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    schema = os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"]
    walk_schema_evolution(cloud_world(tmp_path, "Snowflake", fields, table_format, schema))


def walk_row_id_generation(w):
    target = f"etl_row_ids_{uuid.uuid4().hex[:8]}"
    names = [target, target + "_create", target + "_append", target + "_legacy"]
    try:
        check_row_id_generation(w, target)
        from etl_craft.warehouse.connection import warehouse_dialect

        dialect = warehouse_dialect(w.config)
        if dialect.identity_in_create:
            legacy = target + "_legacy"
            w.execute(
                f"CREATE TABLE {w.name(legacy)} AS SELECT CAST(1 AS BIGINT) AS id, "
                "CAST(1 AS BIGINT) AS pipeline_run_id, CURRENT_TIMESTAMP AS create_date, "
                "CAST(NULL AS BIGINT) AS task_run_id, CAST(7 AS BIGINT) AS row_id"
            )
            w.run(
                legacy,
                SQL_ACTION="APPEND_TABLE",
                TARGET_OBJECT=legacy,
                SELECT_QUERY="SELECT CAST(2 AS BIGINT) AS id",
            )
            assert sorted(w.rows(f"SELECT row_id FROM {w.name(legacy)}")) == [(7,), (8,)]
    finally:
        for name in names:
            w.execute(f"DROP TABLE IF EXISTS {w.name(name)}")
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_row_id_generation_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    schema = os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"]
    walk_row_id_generation(cloud_world(tmp_path, "Databricks", fields, table_format, schema))


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_row_id_generation_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    schema = os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"]
    walk_row_id_generation(cloud_world(tmp_path, "Snowflake", fields, table_format, schema))


def walk_table_format_checks(w):
    """Refuse both format changes, including replacement and no-op setup, before source reads."""
    from etl_craft.config.targets import parse_warehouse_url
    from etl_craft.core.errors import HandlerError
    from etl_craft.dialects.warehouse import resolve

    target = f"etl_craft_format_{uuid.uuid4().hex[:8]}"
    opposite = (
        TableFormat.ICEBERG
        if w.config.warehouse_table_format == TableFormat.NATIVE
        else TableFormat.NATIVE
    )
    actual = w.config.warehouse_table_format
    replacement = target + "_replace"
    try:
        w.setup(target, "SELECT CAST(1 AS BIGINT) AS id", "OVERWRITE_TABLE")
        w.run(
            "seed",
            SQL_ACTION="OVERWRITE_TABLE",
            TARGET_OBJECT=target,
            SELECT_QUERY="SELECT CAST(1 AS BIGINT) AS id",
        )
        before = w.rows(f"SELECT * FROM {w.name(target)}")
        for requested in (actual, opposite):
            dialect = resolve(parse_warehouse_url(w.config.warehouse.jdbc_url).dialect, requested)
            with w.warehouse.connect() as conn:
                assert dialect.existing_table_format(conn, w.name(target)) == actual
        for action in (
            "CREATE_TABLE",
            "SETUP_TABLE",
            "OVERWRITE_TABLE",
            "SCD1_MERGE",
            "SCD2_MERGE",
        ):
            params = (
                {"MERGE_KEY": "id", "MERGE_COMPARE_COLUMNS": "id"}
                if action.startswith("SCD")
                else {}
            )
            with pytest.raises(
                HandlerError, match=f"existing table format is {actual}.*resolves to {opposite}"
            ):
                w.run(
                    "conflict",
                    SQL_ACTION=action,
                    TARGET_OBJECT=target,
                    TABLE_FORMAT=opposite,
                    SELECT_QUERY="SELECT id FROM nonexistent_source",
                    **params,
                )
        assert w.rows(f"SELECT * FROM {w.name(target)}") == before
        w.run(
            "evolve",
            SQL_ACTION="OVERWRITE_TABLE",
            TARGET_OBJECT=target,
            TABLE_FORMAT=actual,
            SCHEMA_EVOLUTION="true",
            SELECT_QUERY="SELECT CAST(2 AS BIGINT) AS id, CAST('ok' AS VARCHAR(20)) AS added",
        )
        assert w.rows(f"SELECT id, added FROM {w.name(target)}") == [(2, "ok")]
        w.run(
            "create_replacement",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=replacement,
            TABLE_FORMAT=actual,
            SELECT_QUERY="SELECT CAST(3 AS BIGINT) AS id",
        )
        w.run(
            "replace",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=replacement,
            TABLE_FORMAT=actual,
            SELECT_QUERY="SELECT CAST(4 AS BIGINT) AS id",
        )
        assert w.rows(f"SELECT id FROM {w.name(replacement)}") == [(4,)]
        with w.warehouse.connect() as conn:
            for name in (target, replacement):
                assert dialect.existing_table_format(conn, w.name(name)) == actual
    finally:
        for name in (target, replacement):
            w.execute(f"DROP TABLE IF EXISTS {w.name(name)}")
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_table_formats_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    walk_table_format_checks(
        cloud_world(
            tmp_path,
            "Databricks",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"],
        )
    )


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_table_formats_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    walk_table_format_checks(
        cloud_world(
            tmp_path,
            "Snowflake",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"],
        )
    )


def walk_append_retries(w):
    """Real autocommit retries converge; legacy upgrades preserve the prior loads."""
    target = f"etl_craft_append_{uuid.uuid4().hex[:8]}"
    try:
        check_append_retries(w, target)
        check_legacy_upgrade(w, target + "_legacy")
    finally:
        for name in (target, target + "_legacy"):
            w.execute(f"DROP TABLE IF EXISTS {w.name(name)}")
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_append_retries_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    walk_append_retries(
        cloud_world(
            tmp_path,
            "Databricks",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"],
        )
    )


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_append_retries_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    walk_append_retries(
        cloud_world(
            tmp_path,
            "Snowflake",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"],
        )
    )


def walk_identity_inputs(w):
    target = f"etl_craft_identity_{uuid.uuid4().hex[:8]}"
    try:
        check_identity_inputs(w, target)
    finally:
        w.execute(f"DROP TABLE IF EXISTS {w.name(target)}")
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_identity_inputs_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    walk_identity_inputs(
        cloud_world(
            tmp_path,
            "Databricks",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"],
        )
    )


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_identity_inputs_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    walk_identity_inputs(
        cloud_world(
            tmp_path,
            "Snowflake",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"],
        )
    )


def walk_rejects(w):
    """A messy merge loads its good rows and sets the rest aside; over the limit, nothing."""
    target = f"etl_craft_rejects_{uuid.uuid4().hex[:8]}"
    try:
        check_rejects(w, target)
    finally:
        for name in (target, target + "__rejects"):
            w.execute(f"DROP TABLE IF EXISTS {w.name(name)}")
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_rejects_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    schema = os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"]
    walk_rejects(cloud_world(tmp_path, "Databricks", fields, table_format, schema))


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_rejects_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    schema = os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"]
    walk_rejects(cloud_world(tmp_path, "Snowflake", fields, table_format, schema))


def walk_ingestion_upgrade(w):
    from fixtures.sql_appends import check_ingestion_target_upgrade

    target = f"etl_craft_ingestion_{uuid.uuid4().hex[:8]}"
    try:
        check_ingestion_target_upgrade(w, target)
    finally:
        w.execute(f"DROP TABLE IF EXISTS {w.name(target)}")
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_ingestion_upgrade_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    walk_ingestion_upgrade(
        cloud_world(
            tmp_path,
            "Databricks",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"],
        )
    )


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_ingestion_upgrade_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    walk_ingestion_upgrade(
        cloud_world(
            tmp_path,
            "Snowflake",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"],
        )
    )


def walk_watermarks(w):
    """Incremental SQL batches commit their positions and retain failed ranges."""
    target = f"etl_craft_watermark_{uuid.uuid4().hex[:8]}"
    try:
        check_watermarks(w, target)
    finally:
        for name in (target,):
            w.execute(f"DROP TABLE IF EXISTS {w.name(name)}")
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_watermarks_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    walk_watermarks(
        cloud_world(
            tmp_path,
            "Databricks",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"],
        )
    )


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_watermarks_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    walk_watermarks(
        cloud_world(
            tmp_path,
            "Snowflake",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"],
        )
    )


def walk_watermark_literals(w):
    """TEXT and TIMESTAMP positions are rendered as typed warehouse literals."""
    target = f"etl_craft_watermark_literal_{uuid.uuid4().hex[:8]}"
    try:
        check_watermark_literals(w, target)
    finally:
        for name in (target, target + "_ts"):
            w.execute(f"DROP TABLE IF EXISTS {w.name(name)}")
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_watermark_literals_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    walk_watermark_literals(
        cloud_world(
            tmp_path,
            "Databricks",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"],
        )
    )


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_watermark_literals_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    walk_watermark_literals(
        cloud_world(
            tmp_path,
            "Snowflake",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"],
        )
    )


def walk_events(w):
    """Read-only warehouse event waits and portable file literals."""
    from fixtures.event_waits import check_event_queries

    target = f"etl_craft_event_{uuid.uuid4().hex[:8]}"
    try:
        check_event_queries(w, target)
    finally:
        w.execute(f"DROP TABLE IF EXISTS {w.name(target)}")
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_event_waits_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    walk_events(
        cloud_world(
            tmp_path,
            "Databricks",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"],
        )
    )


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_event_waits_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    walk_events(
        cloud_world(
            tmp_path,
            "Snowflake",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"],
        )
    )


def walk_source_definitions(w, folder):
    """Seed live source shapes and detect column drift without running a consumer."""
    from etl_craft.services.catalog.collect import build_catalog
    from etl_craft.services.config_files import export_config, read_config_files, sync_config
    from etl_craft.services.validate import validate

    target = f"etl_craft_source_{uuid.uuid4().hex[:8]}"
    source = f"{w.schema}.{target}".lower()
    try:
        w.run(
            "land_source",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=target,
            SELECT_QUERY="SELECT CAST(1 AS BIGINT) AS id, CAST(2 AS BIGINT) AS removed",
        )
        with w.engine_db.begin() as conn:
            conn.execute(
                text("UPDATE CFG_TASKS SET ACTIVE_FLAG='N' WHERE TASK_ID=:id"),
                {"id": w.tasks["land_source"]},
            )
        w.task(
            "source_reader",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=target + "_consumer",
            SELECT_QUERY=f"SELECT id FROM {source}",
        )
        exported = export_config(w.engine_db, folder, config=w.config)
        assert exported.rows["columns.csv"] >= 2
        done = sync_config(w.engine_db, w.config, read_config_files(folder), apply=True)
        assert not done.failed
        export_config(w.engine_db, folder, config=w.config, overwrite=True)
        assert not sync_config(
            w.engine_db, w.config, read_config_files(folder), apply=False
        ).changes
        with w.engine_db.begin() as conn:
            conn.execute(text("UPDATE CFG_COLUMNS SET DATA_TYPE='BIGINT' WHERE COLUMN_NAME='id'"))
        assert not [
            finding
            for finding in validate(w.engine_db, w.config).findings
            if finding.where.startswith("source ")
        ]
        with w.engine_db.begin() as conn:
            conn.execute(text("UPDATE CFG_COLUMNS SET ACTIVE_FLAG='N' WHERE COLUMN_NAME='removed'"))
            conn.execute(text("UPDATE CFG_COLUMNS SET DATA_TYPE='VARCHAR' WHERE COLUMN_NAME='id'"))
            conn.execute(
                text(
                    "INSERT INTO CFG_COLUMNS (TABLE_ID,COLUMN_NAME,DATA_TYPE,COLUMN_POSITION) "
                    "SELECT TABLE_ID,'missing','INT',100 FROM CFG_COLUMNS WHERE COLUMN_NAME='id'"
                )
            )
        findings = [
            finding.message
            for finding in validate(w.engine_db, w.config).findings
            if finding.where == f"source {source}"
        ]
        assert any("adds column removed" in message for message in findings)
        assert any("lacks column missing" in message for message in findings)
        assert any("id has warehouse type" in message for message in findings)
        compared = build_catalog(w.engine_db, w.config, with_warehouse=True).tables[source]
        assert compared.warehouse_checked and len(compared.warehouse_findings) == 3
        assert compared.columns["id"].declared_type == "VARCHAR"
        assert compared.columns["id"].warehouse_type != "VARCHAR"
    finally:
        w.execute(f"DROP TABLE IF EXISTS {w.name(target)}")
        w.warehouse.dispose()
        w.engine_db.dispose()


@pytest.mark.cloud_databricks
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_source_definitions_on_databricks(tmp_path, table_format):
    require_variables("DATABRICKS", DATABRICKS_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_DATABRICKS_{name}" for name in DATABRICKS_VARS}
    walk_source_definitions(
        cloud_world(
            tmp_path,
            "Databricks",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_DATABRICKS_SCHEMA"],
        ),
        tmp_path / "export",
    )


@pytest.mark.cloud_snowflake
@pytest.mark.parametrize("table_format", [TableFormat.NATIVE, TableFormat.ICEBERG])
def test_source_definitions_on_snowflake(tmp_path, table_format):
    require_variables("SNOWFLAKE", SNOWFLAKE_VARS)
    fields = {name.lower(): f"ETL_CRAFT_TEST_SNOWFLAKE_{name}" for name in SNOWFLAKE_VARS}
    walk_source_definitions(
        cloud_world(
            tmp_path,
            "Snowflake",
            fields,
            table_format,
            os.environ["ETL_CRAFT_TEST_SNOWFLAKE_SCHEMA"],
        ),
        tmp_path / "export",
    )
