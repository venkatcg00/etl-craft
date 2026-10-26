"""Complete rule validation and dialect-safe legacy conversion."""

import pytest

from etl_craft.core.errors import SqlGuardError
from etl_craft.dialects.business_rules import converted_rule, expand_scope
from etl_craft.engine.repository.business_rules import BusinessRule
from etl_craft.handlers.business_rules import check_rule

pytestmark = pytest.mark.unit


def rule(sql, key="id"):
    return BusinessRule(17, "check_amount", sql, "REPORT", key, "sales.orders", 1)


@pytest.mark.parametrize(
    ("sql", "parts"),
    [
        ("SELECT amount FROM $$rows_in_scope", ["returns ['amount']", "KEY_COLUMN='id'", "select"]),
        ("VALUES (1)", ["read-only SELECT", "select target row keys"]),
        ("DELETE FROM sales.orders", ["read-only SELECT", "DELETE", "rewrite"]),
        ("SELECT id FROM $$rows_in_scope; DELETE FROM sales.orders", ["2 statements", "rewrite"]),
        (
            "WITH changed AS (DELETE FROM sales.orders RETURNING id) SELECT id FROM changed",
            ["DELETE", "read-only SELECT", "rewrite"],
        ),
        (
            "SELECT 1 WHERE t.amount < 0",
            [
                "old correlated fragment",
                'SELECT t."id" FROM $$rows_in_scope t WHERE EXISTS',
                "SELECT 1 WHERE t.amount < 0",
            ],
        ),
    ],
)
def test_invalid_rules_name_the_value_expected_form_and_remedy(sql, parts):
    with pytest.raises(SqlGuardError) as failure:
        check_rule(rule(sql), "analytics", "postgres")
    assert "check_amount" in str(failure.value)
    for part in parts:
        assert part in str(failure.value)


def test_scope_expansion_preserves_literals_comments_and_similar_names():
    sql = "SELECT '$$rows_in_scope' AS id FROM $$rows_in_scope -- $$rows_in_scope\n"
    assert expand_scope(sql) == (
        "SELECT '$$rows_in_scope' AS id FROM etl_craft_rows_in_scope -- $$rows_in_scope\n"
    )
    assert expand_scope("SELECT '$$rows_in_scope_extra'") == "SELECT '$$rows_in_scope_extra'"


@pytest.mark.parametrize("key", ["id; DELETE FROM x", 'a"b', "id --", "a.b"])
def test_conversion_refuses_attacker_identifiers(key):
    with pytest.raises(SqlGuardError, match="expected a plain column name"):
        converted_rule("SELECT 1 WHERE t.amount < 0", key, "postgres")


@pytest.mark.parametrize(
    ("dialect", "key"),
    [
        ("postgres", '"row_id"'),
        ("snowflake", '"ROW_ID"'),
        ("databricks", "`row_id`"),
        ("trino", '"row_id"'),
    ],
)
def test_conversion_quotes_the_key_and_preserves_the_legacy_body(dialect, key):
    original = "SELECT 1 FROM sales.customers c WHERE c.id=t.customer_id -- original comment"
    converted = converted_rule(original, "ROW_ID", dialect)
    assert converted == f"SELECT t.{key} FROM $$rows_in_scope t WHERE EXISTS (\n{original}\n)"


def test_demo_rules_return_the_configured_fact_key():
    import re
    from pathlib import Path

    path = Path(__file__).parents[2] / "examples/demo/metadata/support_insights.sql"
    rules = path.read_text().split("INSERT INTO CFG_BUSINESS_RULES", 1)[1]
    queries = re.findall(r"'(SELECT [^']*)'", rules)
    assert queries
    for query in queries:
        check_rule(rule(query, "fact_key"), "analytics", "duckdb")


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM x",
        "SELECT 1; DELETE FROM x",
        "WITH changed AS (DELETE FROM x RETURNING id) SELECT id FROM changed",
    ],
)
def test_conversion_refuses_a_legacy_write_or_second_statement(sql):
    with pytest.raises(SqlGuardError, match="expected one read-only SELECT"):
        converted_rule(sql, "id", "postgres")


def test_conversion_accepts_a_legacy_trailing_semicolon_and_comment():
    assert converted_rule("SELECT 1 WHERE t.amount < 0; -- trailing comment", "id", "postgres") == (
        'SELECT t."id" FROM $$rows_in_scope t WHERE EXISTS (\nSELECT 1 WHERE t.amount < 0\n)'
    )


@pytest.mark.parametrize(
    "target", ["sales.orders; DELETE FROM x", 'sales."orders"', "sales.orders --"]
)
def test_rule_targets_cannot_inject_generated_scope_sql(target):
    from dataclasses import replace

    with pytest.raises(SqlGuardError) as failure:
        check_rule(
            replace(rule("SELECT id FROM $$rows_in_scope"), target_table=target),
            "analytics",
            "postgres",
        )
    for part in (
        "check_amount",
        target,
        "expected schema.table",
        "plain SQL identifiers",
        "correct the CFG_TABLES",
    ):
        assert part in str(failure.value)


def test_cli_project_config_is_repeatable_and_the_operation_input_round_trips(
    tmp_path, monkeypatch
):
    import argparse

    from etl_craft.cli.commands import migrate as command
    from etl_craft.cli.output import Output
    from etl_craft.services.operations.maintenance import MigrateInput
    from etl_craft.services.operations.serialization import encode, from_json

    parser = argparse.ArgumentParser()
    command.COMMAND.configure(parser)
    paths = (tmp_path / "first.yml", tmp_path / "second.yml")
    args = parser.parse_args(["--project-config", str(paths[0]), "--project-config", str(paths[1])])
    captured = []

    def call(args, action, request):
        captured.append(request)
        return []

    monkeypatch.setattr(command, "call", call)
    assert command.COMMAND.run(args, Output()) == 0
    assert captured == [MigrateInput(project_configs=paths)]
    assert from_json(MigrateInput, encode(captured[0])) == captured[0]
