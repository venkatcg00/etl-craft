"""The configuration uploads in docs/examples/migrations/, applied in order on both Engine DBs.

Each file is a project migration a team copies. Every one must apply as written, leave metadata
that ``validate`` passes without a finding, and stop the upload, changing nothing, when a code it
names has no active row.
"""

import shutil

import pytest

from etl_craft.core.errors import MigrationError
from etl_craft.engine.migrations import apply_pending_migrations
from etl_craft.services.validate import validate
from fixtures.metadata_project import EXAMPLES


def test_every_example_applies_in_order_and_validates(metadata_project):
    engine, config, migrations = (
        metadata_project.engine,
        metadata_project.config,
        metadata_project.migrations,
    )
    examples = sorted(EXAMPLES.glob("*.sql"))
    assert [path.name[:4] for path in examples] == ["0001", "0002", "0003", "0004", "0005", "0006"]
    checked = []
    for example in examples:
        shutil.copy(example, migrations)
        assert apply_pending_migrations(engine, migrations) == [example.name]
        report = validate(engine, config)
        assert report.findings == [], example.name
        checked.append((report.pipelines, report.tasks))
    assert checked == [(1, 6), (2, 11), (4, 23), (4, 23), (4, 24), (3, 17)]

    assert metadata_project.rows(
        "SELECT p.PIPELINE_CODE AS pipeline_code, t.TASK_CODE AS task_code, t.HANDLER AS handler "
        "FROM CFG_TASKS t JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
        "WHERE p.ACTIVE_FLAG = 'Y' AND t.ACTIVE_FLAG = 'Y' AND p.PIPELINE_CODE <> 'SALES_DAILY_EU' "
        "ORDER BY 1, 2",
    ) == [
        ("SALES_DAILY", "alert", "EMAIL_ALERT"),
        ("SALES_DAILY", "check_orders", "BUSINESS_RULES"),
        ("SALES_DAILY", "fetch_orders", "PYTHON"),
        ("SALES_DAILY", "load_orders", "SQL"),
        ("SALES_DAILY", "on_failure", "EMAIL_ALERT"),
        ("SALES_DAILY", "setup_orders", "SQL"),
        ("SALES_DAILY", "validate_feed", "PYTHON"),
        ("SALES_MART", "alert", "EMAIL_ALERT"),
        ("SALES_MART", "daily_totals", "SQL"),
        ("SALES_MART", "publish", "PYTHON"),
        ("SALES_MART", "totals_by_region", "SQL"),
    ]
    copy = (
        "SELECT t.TASK_CODE AS task_code, x.PARAMETER_NAME AS name, x.PARAMETER_VALUE AS value "
        "FROM CFG_TASK_PARAMETERS x JOIN CFG_TASKS t ON t.TASK_ID = x.TASK_ID "
        "JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID "
        "WHERE p.PIPELINE_CODE = 'SALES_DAILY_EU' AND p.ACTIVE_FLAG = 'Y' AND x.ACTIVE_FLAG = 'Y' "
        "AND x.PARAMETER_NAME IN ('INPUT_PARAMS', 'TARGET_OBJECT', 'SOURCE_SQL_FILE', 'EMAIL_TO') "
        "ORDER BY 1, 2"
    )
    assert metadata_project.rows(copy) == [
        ("alert", "EMAIL_TO", "sales-data@example.com|sales-leads@example.com"),
        ("fetch_orders", "INPUT_PARAMS", '{"region": "eu", "days": 1}'),
        ("fetch_orders", "TARGET_OBJECT", "lnd.orders_eu"),
        ("load_orders", "SOURCE_SQL_FILE", "sales/orders_eu.sql"),
        ("load_orders", "TARGET_OBJECT", "sales.orders_eu"),
        ("on_failure", "EMAIL_TO", "sales-oncall@example.com"),
        ("setup_orders", "SOURCE_SQL_FILE", "sales/orders_eu.sql"),
        ("setup_orders", "TARGET_OBJECT", "sales.orders_eu"),
    ]
    assert metadata_project.rows(
        "SELECT p.PIPELINE_CODE AS pipeline_code, u.PIPELINE_CODE AS upstream "
        "FROM CFG_PIPELINE_DEPENDENCY d JOIN CFG_PIPELINES p ON p.PIPELINE_ID = d.PIPELINE_ID "
        "JOIN CFG_PIPELINES u ON u.PIPELINE_ID = d.DEPENDS_ON_PIPELINE_ID "
        "WHERE d.ACTIVE_FLAG = 'Y' ORDER BY 1, 2",
    ) == [("SALES_MART", "SALES_DAILY"), ("SALES_MART", "SALES_DAILY_EU")]
    assert metadata_project.rows(
        "SELECT BUSINESS_RULE_NAME AS name, BUSINESS_RULE_TYPE AS kind, BUSINESS_RULE_SQL AS sql "
        "FROM CFG_BUSINESS_RULES r JOIN CFG_PIPELINES p ON p.PIPELINE_ID = r.PIPELINE_ID "
        "WHERE p.PIPELINE_CODE = 'SALES_DAILY_EU' AND r.ACTIVE_FLAG = 'Y' "
        "AND BUSINESS_RULE_NAME IN ('test customer', 'large order this run') ORDER BY 1",
    ) == [
        (
            "large order this run",
            "REPORT",
            "SELECT 1 FROM sales.customers c WHERE c.customer_id = t.customer_id "
            "AND t.amount > c.credit_limit AND t.PIPELINE_RUN_ID = :pipeline_run_id",
        ),
        (
            "test customer",
            "REPORT",
            "SELECT 1 FROM sales.customers c WHERE c.customer_id = t.customer_id "
            "AND c.name LIKE 'TEST%'",
        ),
    ]
    changes = metadata_project.rows(
        "SELECT MIGRATION AS migration, COUNT(*) AS changes FROM AUD_METADATA_CHANGES "
        "GROUP BY MIGRATION ORDER BY 1",
    )
    assert [name for name, _ in changes] == [path.name for path in examples]


@pytest.mark.parametrize("example", ["0001_new_pipeline.sql", "0004_change_values.sql"])
def test_a_code_with_no_active_row_stops_the_upload(metadata_project, example):
    engine, migrations = metadata_project.engine, metadata_project.migrations
    for earlier in sorted(EXAMPLES.glob("*.sql")):
        if earlier.name >= example:
            break
        shutil.copy(earlier, migrations)
    apply_pending_migrations(engine, migrations)
    misspelt = (EXAMPLES / example).read_text("utf-8").replace("'load_orders'", "'load_order'", 1)
    (migrations / example).write_text(misspelt, "utf-8")
    before = metadata_project.rows("SELECT COUNT(*) AS n FROM AUD_METADATA_CHANGES")

    with pytest.raises(MigrationError, match=example):
        apply_pending_migrations(engine, migrations)
    assert metadata_project.rows("SELECT COUNT(*) AS n FROM AUD_METADATA_CHANGES") == before
