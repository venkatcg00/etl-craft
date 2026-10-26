"""Rule conversion and active-flag uniqueness retain evidence on both Engine DBs."""

import shutil
from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import inspect, text

from etl_craft.core.errors import MigrationError
from etl_craft.dialects.engine.base import EngineDialect
from etl_craft.engine import migration_steps, migrations, transitions
from etl_craft.engine.repository.business_rules import (
    begin_rule_run,
    fetch_business_rules_for_task,
    flag_rule_keys,
)
from etl_craft.services.config_files import export_config, read_config_files
from fixtures.metadata import add_pipeline, add_table, add_task, insert, start_run
from fixtures.released_schema import install


@pytest.fixture
def legacy_rules(empty_engine_db, monkeypatch, tmp_path):
    db = empty_engine_db
    install(db, "0.3.0")
    old = tmp_path / "previous"
    old.mkdir()
    for file in db.dialect.migrations_dir().glob("*.sql"):
        if file.name < "0060":
            shutil.copyfile(file, old / file.name)
    with monkeypatch.context() as patch:
        patch.setattr(EngineDialect, "migrations_dir", lambda self: old)
        migrations.apply_pending_migrations(db.engine)
    with db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "BR")
        task = add_task(conn, pipeline, "rules", "BUSINESS_RULES")
        target = add_table(conn, "analytics.sales.orders")
        rule_id = insert(
            conn,
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME,PIPELINE_ID,TASK_ID,"
            "BUSINESS_RULE_SQL,BUSINESS_RULE_TYPE,BUSINESS_RULE_KEY_COLUMN,TARGET_TABLE_ID,"
            "SEQUENCE_NUMBER) VALUES "
            "('negative',:pipeline,:task,:rule_sql,'REPORT','id',:target,1)",
            "BUSINESS_RULE_ID",
            pipeline=pipeline,
            task=task,
            target=target,
            rule_sql="SELECT 1 WHERE t.amount < 0 -- keep this body",
        )
        run = start_run(conn, pipeline)
        task_run = transitions.find_or_create_task_run(conn, task, run).task_run_id
        binding = begin_rule_run(conn, rule_id, task_run, datetime.now(UTC))
        rule = fetch_business_rules_for_task(conn, task)[0]
        flag_rule_keys(conn, rule, binding.business_rule_run_id, ["11"], datetime.now(UTC))
    return db, task, rule, binding


def state(db):
    with db.engine.connect() as conn:
        return (
            [
                dict(row)
                for row in conn.execute(text("SELECT * FROM CFG_BUSINESS_RULES")).mappings()
            ],
            [
                dict(row)
                for row in conn.execute(text("SELECT * FROM AUD_BUSINESS_RULES_RESULTS")).mappings()
            ],
        )


def test_conversion_preserves_results_and_exports_the_new_form(legacy_rules, tmp_path):
    db, *_ = legacy_rules
    before_rules, before_results = state(db)
    migrations.apply_pending_migrations(db.engine, warehouse_dialect="duckdb")
    after_rules, after_results = state(db)
    assert after_results == before_results
    sql_key = next(key for key in after_rules[0] if key.lower() == "business_rule_sql")
    assert after_rules[0][sql_key] == (
        'SELECT t."id" FROM $$rows_in_scope t WHERE EXISTS (\n'
        "SELECT 1 WHERE t.amount < 0 -- keep this body\n)"
    )
    before_rules[0][sql_key] = after_rules[0][sql_key]
    assert after_rules == before_rules
    export_config(db.engine, tmp_path / "export")
    exported = read_config_files(tmp_path / "export")
    assert (
        next(iter(exported.rows["business_rules.csv"].values())).values["BUSINESS_RULE_SQL"]
        == after_rules[0][sql_key]
    )
    with db.engine.connect() as conn:
        indexes = inspect(conn).get_indexes(
            "cfg_business_rules" if db.dialect.name == "postgresql" else "CFG_BUSINESS_RULES"
        )
        assert indexes


def test_legacy_duplicates_refuse_before_any_pending_upgrade_without_picking_a_winner(
    legacy_rules, monkeypatch, tmp_path
):
    db, _task, rule, binding = legacy_rules
    with db.engine.begin() as conn:
        flag_rule_keys(conn, rule, binding.business_rule_run_id, ["11"], datetime.now(UTC))
    packaged = tmp_path / "packaged"
    packaged.mkdir()
    for file in db.dialect.migrations_dir().glob("*.sql"):
        shutil.copyfile(file, packaged / file.name)
    (packaged / "0059z_probe.sql").write_text("CREATE TABLE must_not_exist (id INTEGER);")
    monkeypatch.setattr(EngineDialect, "migrations_dir", lambda self: packaged)
    before = state(db)
    with pytest.raises(MigrationError) as failure:
        migrations.apply_pending_migrations(db.engine, warehouse_dialect="duckdb")
    for part in (
        "AUD_BUSINESS_RULES_RESULTS",
        f"rule={rule.business_rule_id}",
        "key='11'",
        "result id=1",
        "result id=2",
        "expected one ACTIVE_FLAG=Y",
        "previous package",
        "retaining their history",
    ):
        assert part in str(failure.value)
    assert state(db) == before
    assert not inspect(db.engine).has_table("must_not_exist")
    assert "engine/0060_business_rule_queries.sql" in migrations.pending_migrations(db.engine)


def test_marker_checks_again_inside_the_transaction_and_rolls_back(legacy_rules, monkeypatch):
    db, _task, rule, binding = legacy_rules
    step = migration_steps.STEPS["check-business-rule-flags"]
    calls = []

    def check(conn, target):
        calls.append(conn.in_transaction())
        if len(calls) == 2:
            flag_rule_keys(conn, rule, binding.business_rule_run_id, ["11"], datetime.now(UTC))
        return step.check(conn, target)

    monkeypatch.setitem(
        migration_steps.STEPS, "check-business-rule-flags", replace(step, check=check)
    )
    before = state(db)
    with pytest.raises(MigrationError, match="conflicting active flags"):
        migrations.apply_pending_migrations(db.engine, warehouse_dialect="duckdb")
    assert len(calls) == 2
    assert state(db) == before
    assert "engine/0060_business_rule_queries.sql" in migrations.pending_migrations(db.engine)


def test_rule_conversion_requires_the_actual_warehouse_dialect(legacy_rules):
    db, *_ = legacy_rules
    before = state(db)
    with pytest.raises(MigrationError) as failure:
        migrations.apply_pending_migrations(db.engine)
    for part in (
        "CFG_BUSINESS_RULES",
        "dialect=None",
        "expected the target",
        "configure Warehouse",
    ):
        assert part in str(failure.value)
    assert state(db) == before


def test_failure_after_conversion_rolls_back_sql_index_history_and_ledger(
    legacy_rules, monkeypatch
):
    from etl_craft.engine.repository import target_contracts

    db, *_ = legacy_rules
    before = state(db)
    with db.engine.connect() as conn:
        history = conn.execute(
            text("SELECT COUNT(*) FROM AUD_CFG_BUSINESS_RULES_HISTORY")
        ).scalar_one()
    original = target_contracts.record_conversion

    def fail_after_recording(*args, **kwargs):
        original(*args, **kwargs)
        raise MigrationError("injected conversion failure")

    monkeypatch.setattr(target_contracts, "record_conversion", fail_after_recording)
    with pytest.raises(MigrationError, match="injected conversion failure"):
        migrations.apply_pending_migrations(db.engine, warehouse_dialect="duckdb")
    assert state(db) == before
    with db.engine.connect() as conn:
        assert (
            conn.execute(text("SELECT COUNT(*) FROM AUD_CFG_BUSINESS_RULES_HISTORY")).scalar_one()
            == history
        )
        indexes = inspect(conn).get_indexes(
            "aud_business_rules_results"
            if db.dialect.name == "postgresql"
            else "AUD_BUSINESS_RULES_RESULTS"
        )
        assert "ux_brresults_active_key" not in {index["name"] for index in indexes}
    assert "engine/0060_business_rule_queries.sql" in migrations.pending_migrations(db.engine)


def add_project_rule(db, code):
    with db.engine.begin() as conn:
        project = insert(
            conn,
            "INSERT INTO CFG_PROJECTS (PROJECT_CODE,PROJECT_NAME,DEFAULT_POOL) "
            "VALUES (:code,:code,'default')",
            "PROJECT_ID",
            code=code,
        )
        pipeline = insert(
            conn,
            "INSERT INTO CFG_PIPELINES (PROJECT_ID,PIPELINE_CODE,PIPELINE_NAME,REFRESH_TYPE) "
            "VALUES (:project,'BR','BR','FULL')",
            "PIPELINE_ID",
            project=project,
        )
        task = add_task(conn, pipeline, "rules", "BUSINESS_RULES")
        target = insert(
            conn,
            "INSERT INTO CFG_TABLES (PROJECT_ID,SCHEMA_NAME,TABLE_NAME) "
            "VALUES (:project,'sales','orders')",
            "TABLE_ID",
            project=project,
        )
        return insert(
            conn,
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME,PIPELINE_ID,TASK_ID,"
            "BUSINESS_RULE_SQL,BUSINESS_RULE_TYPE,BUSINESS_RULE_KEY_COLUMN,TARGET_TABLE_ID,"
            "SEQUENCE_NUMBER) VALUES ('foreign_rule',:pipeline,:task,'SELECT 1 WHERE t.amount<0',"
            "'REPORT','id',:target,1)",
            "BUSINESS_RULE_ID",
            pipeline=pipeline,
            task=task,
            target=target,
        )


def test_shared_projects_require_their_own_dialects_before_any_write(legacy_rules):
    db, *_ = legacy_rules
    add_project_rule(db, "spark")
    before = state(db)
    with pytest.raises(MigrationError) as failure:
        migrations.apply_pending_migrations(db.engine, warehouse_dialect="duckdb")
    for part in (
        "foreign_rule",
        "project='spark'",
        "Warehouse dialect=None",
        "expected the target project's",
        "--project-config PATH",
    ):
        assert part in str(failure.value)
    assert state(db) == before


def test_shared_projects_convert_with_each_warehouse_dialect(legacy_rules):
    db, *_ = legacy_rules
    spark = add_project_rule(db, "spark")
    snow = add_project_rule(db, "snow")
    before_results = state(db)[1]
    migrations.apply_pending_migrations(
        db.engine,
        project_dialects={
            "default": "duckdb",
            "spark": "databricks",
            "snow": "snowflake",
        },
    )
    with db.engine.connect() as conn:
        rows = conn.execute(
            text("SELECT BUSINESS_RULE_ID AS id, BUSINESS_RULE_SQL AS sql FROM CFG_BUSINESS_RULES")
        ).mappings()
        actual = {row["id"]: row["sql"] for row in rows}
    assert (
        actual[spark]
        == "SELECT t.`id` FROM $$rows_in_scope t WHERE EXISTS (\nSELECT 1 WHERE t.amount<0\n)"
    )
    assert (
        actual[snow]
        == 'SELECT t."ID" FROM $$rows_in_scope t WHERE EXISTS (\nSELECT 1 WHERE t.amount<0\n)'
    )
    assert state(db)[1] == before_results


def connector(path, db, project):
    import yaml

    profile = db.config.engine
    engine = {"Jdbc_url": profile.jdbc_url, "Schema": profile.schema}
    if profile.auth_mode != "NONE":
        engine |= {
            "User": profile.user,
            "Auth_mode": profile.auth_mode,
            "Secret": profile.extra["secret_var"],
        }
    raw = {
        "Secrets": {"Source_type": "ENVIRONMENT"},
        "Orchestration": {"Project": project},
        "Engine": {"test": engine},
        "Warehouse": {"test": {"Jdbc_url": "jdbc:duckdb:warehouse.duckdb", "Schema": "main"}},
    }
    path.write_text(yaml.safe_dump(raw, sort_keys=False))
    return path


def test_migrate_operation_loads_project_configs_without_opening_the_warehouse(
    legacy_rules, tmp_path
):
    from etl_craft.config import load_config
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.services.operations.context import OperationContext
    from etl_craft.services.operations.maintenance import MigrateInput, migrate

    db, *_ = legacy_rules
    add_project_rule(db, "second")
    primary = load_config(connector(tmp_path / "primary.yml", db, "default"))
    extra = connector(tmp_path / "second.yml", db, "second")
    applied = migrate(
        OperationContext(db.engine, primary, SYSTEM_ACTOR), MigrateInput(project_configs=(extra,))
    )
    assert "0060_business_rule_queries.sql" in applied
    assert not (tmp_path / "warehouse.duckdb").exists()
    assert all(
        "$$rows_in_scope"
        in next(value for key, value in row.items() if key.lower() == "business_rule_sql")
        for row in state(db)[0]
    )


@pytest.mark.parametrize(
    "problem", ["duplicate", "foreign-db", "missing-warehouse", "unreadable", "encoding"]
)
def test_project_config_refusals_leave_migrations_untouched(
    engine_db, tmp_path, monkeypatch, problem
):
    from pathlib import Path

    import yaml

    from etl_craft.config import load_config
    from etl_craft.core.actor import SYSTEM_ACTOR
    from etl_craft.core.errors import ConfigurationError
    from etl_craft.services.operations.context import OperationContext
    from etl_craft.services.operations.maintenance import MigrateInput, migrate

    primary = load_config(connector(tmp_path / "primary.yml", engine_db, "default"))
    extra = connector(
        tmp_path / "second.yml", engine_db, "default" if problem == "duplicate" else "second"
    )
    raw = yaml.safe_load(extra.read_text())
    expected = "expected one config with a Warehouse per project"
    remedy = "remove duplicate"
    if problem == "foreign-db":
        raw["Engine"] = {
            "test": {"Jdbc_url": f"jdbc:sqlite:{tmp_path / 'foreign.db'}", "Schema": "main"}
        }
        expected, remedy = "expected the selected Engine DB", "same Engine DB and schema"
    elif problem == "missing-warehouse":
        raw.pop("Warehouse")
        remedy = "configure its Warehouse"
    extra.write_text(yaml.safe_dump(raw, sort_keys=False))
    if problem == "encoding":
        extra.write_bytes(b"\xff")
        expected, remedy = "expected a readable UTF-8", "restore its permissions/encoding"
    if problem == "unreadable":
        original = Path.read_text

        def read(path, *args, **kwargs):
            if path == extra:
                raise PermissionError("permission denied by test")
            return original(path, *args, **kwargs)

        monkeypatch.setattr(Path, "read_text", read)
        expected, remedy = "expected a readable UTF-8", "restore its permissions/encoding"
    with engine_db.engine.connect() as conn:
        before = conn.execute(text("SELECT COUNT(*) FROM SCHEMA_MIGRATIONS")).scalar_one()
    with pytest.raises(ConfigurationError) as failure:
        migrate(
            OperationContext(engine_db.engine, primary, SYSTEM_ACTOR),
            MigrateInput(project_configs=(extra,)),
        )
    for part in (str(extra), expected, remedy):
        assert part in str(failure.value)
    with engine_db.engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM SCHEMA_MIGRATIONS")).scalar_one() == before
