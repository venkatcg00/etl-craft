"""Business-rule assertions shared by local and live warehouse acceptance."""

from dataclasses import replace
from uuid import uuid4

from etl_craft.handlers import business_rules
from fixtures.metadata import add_table, insert


def add_rule(w, name, sql, *, sequence=1, key="order_id", rule_type="REJECT", target="orders"):
    with w.engine_db.begin() as conn:
        return insert(
            conn,
            "INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, "
            "BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE_ID, "
            "SEQUENCE_NUMBER) VALUES (:name, :p, :t, :rule_sql, :type, :key, :target, :seq)",
            "BUSINESS_RULE_ID",
            name=name,
            p=w.pipeline_id,
            t=w.tasks["rules"],
            rule_sql=sql,
            type=rule_type,
            key=key,
            target=add_table(conn, f"{w.schema}.{target}"),
            seq=sequence,
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
