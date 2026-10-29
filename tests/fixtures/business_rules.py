"""Business-rule assertions shared by local and live warehouse acceptance."""

from dataclasses import replace
from uuid import uuid4

from etl_craft.handlers import business_rules
from fixtures.metadata import add_table, insert


def add_rule(
    w, name, sql, *, sequence=1, key="order_id", rule_type="REJECT", target="orders", level="ROW"
):
    with w.engine_db.begin() as conn:
        return insert(
            conn,
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
            "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE_ID, "
            "SEQUENCE_NUMBER,BUSINESS_RULE_LEVEL) VALUES "
            "(:name, :p, :t, :rule_sql, :type, :key, :target, :seq,:level)",
            "BUSINESS_RULE_ID",
            name=name,
            p=w.pipeline_id,
            t=w.tasks["rules"],
            rule_sql=sql,
            type=rule_type,
            key=key,
            target=add_table(conn, f"{w.schema}.{target}"),
            seq=sequence,
            level=level,
        )


def run_rules(w, *, force=False, rerun=False):
    context = replace(w.task("rules"), handler="BUSINESS_RULES", force=force, rerun=rerun)
    return business_rules.run(context, w.engine_db)


def check_business_rule_queries(w):
    target = f"etl_craft_rule_{uuid4().hex[:8]}"
    table = w.name(target)
    try:
        w.run(
            "rule_source",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=table,
            SELECT_QUERY="SELECT 1 AS order_id, 7 AS amount "
            "UNION ALL SELECT 2, -1 UNION ALL SELECT 3, -2",
        )
        w.task("rules")
        queries = [
            "SELECT * FROM $$rows_in_scope",
            "SELECT order_id FROM $$rows_in_scope WHERE amount < 0",
            "WITH broken AS (SELECT * FROM $$rows_in_scope WHERE amount < 0) SELECT "
            "order_id FROM broken",
            "SELECT order_id FROM (SELECT order_id, ROW_NUMBER() OVER (ORDER BY "
            "order_id DESC) AS n FROM $$rows_in_scope) ranked WHERE n > 1",
            "SELECT t.order_id FROM $$rows_in_scope t JOIN (SELECT 1 AS g FROM "
            "$$rows_in_scope HAVING COUNT(*) > 1) g ON g.g = 1",
            f"SELECT t.order_id FROM $$rows_in_scope t LEFT JOIN {table} x ON "
            f"x.order_id=t.order_id AND x.amount > 0 WHERE x.order_id IS NULL",
        ]
        for index, query in enumerate(queries):
            add_rule(w, f"native_{index}", query, target=target)
        result = run_rules(w)
        assert result.insert_count == 14
        assert result.update_count == 0
        for values in result.variables.values():
            assert values["dropped"] == 0
        w.execute(f"UPDATE {table} SET amount=9")
        assert run_rules(w, rerun=True).update_count == 6
    finally:
        w.execute(f"DROP TABLE IF EXISTS {table}")


def check_table_business_rules(w):
    """Check both types, scopes and outcomes through one query on each supported cloud."""
    from sqlalchemy import text

    from etl_craft.core.errors import HandlerError
    from etl_craft.warehouse.connection import warehouse_dialect

    string_type = warehouse_dialect(w.config).string_type
    target = f"etl_table_rule_{uuid4().hex[:8]}"
    try:
        w.run(
            "table_source",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT=target,
            SELECT_QUERY="SELECT 1 AS order_id UNION ALL SELECT 2",
        )
        w.task("rules")
        for scoped in (True, False):
            for rule_type in ("REJECT", "REPORT"):
                for passed in ("Y", "N"):
                    with w.engine_db.begin() as conn:
                        conn.execute(text("UPDATE CFG_BUSINESS_RULES SET ACTIVE_FLAG='N'"))
                    source = "$$rows_in_scope" if scoped else w.name(target)
                    rule = add_rule(
                        w,
                        f"{scoped}_{rule_type}_{passed}",
                        f"SELECT '{passed}' AS PASSED, "
                        f"CAST(COUNT(*) AS {string_type}) AS OBSERVED FROM {source}",
                        key=None,
                        level="TABLE",
                        rule_type=rule_type,
                        target=target,
                    )
                    try:
                        run_rules(w)
                    except HandlerError:
                        assert rule_type == "REJECT" and passed == "N"
                    else:
                        assert rule_type != "REJECT" or passed != "N"
                    with w.engine_db.connect() as conn:
                        row = conn.execute(
                            text(
                                "SELECT PASSED AS passed,OBSERVED AS observed "
                                "FROM AUD_BUSINESS_RULES_RUN_LOG WHERE BUSINESS_RULE_ID=:rule"
                            ),
                            {"rule": rule},
                        ).one()
                        assert (row.passed, row.observed) == (passed, "2")
                        assert (
                            conn.execute(
                                text("SELECT COUNT(*) FROM AUD_BUSINESS_RULES_RESULTS")
                            ).scalar_one()
                            == 0
                        )
    finally:
        w.execute(f"DROP TABLE IF EXISTS {w.name(target)}")
