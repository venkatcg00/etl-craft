-- A pipeline that waits on another. SALES_MART starts once SALES_DAILY has succeeded, and one of
-- its tasks also waits on a task in SALES_DAILY: it runs only when that load wrote rows.

INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, DESCRIPTION, REFRESH_TYPE, RUN_SCHEDULE,
                           SCHEDULE_TIMEZONE)
VALUES
    ('SALES_MART', 'Sales mart', 'Daily, regional and customer totals of the checked orders.',
     'FULL', '30 6 * * *', 'Europe/London');

-- Pipeline dependencies: the pipeline, the pipeline it waits on, DEPENDENCY_TYPE, and
-- CONSUME_REPAIRS: 'Y' (the default) takes a repaired upstream run as new output, 'N' only a new
-- upstream run.
INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE,
                                     CONSUME_REPAIRS)
SELECT p.PIPELINE_ID, u.PIPELINE_ID, v.column3, v.column4
FROM (VALUES
    ('SALES_MART', 'SALES_DAILY', 'SUCCESS', 'Y')
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_PIPELINES u ON u.PIPELINE_CODE = v.column2 AND u.ACTIVE_FLAG = 'Y';

-- publish runs once 2 of the 3 totals are built.
INSERT INTO CFG_TASKS (PIPELINE_ID, TASK_CODE, TASK_TYPE, HANDLER, RUN_CONDITION,
                       RUN_CONDITION_COUNT)
SELECT p.PIPELINE_ID, v.column2, v.column3, v.column4, v.column5, CAST(v.column6 AS INTEGER)
FROM (VALUES
    ('SALES_MART', 'daily_totals', 'ETL', 'SQL', NULL, NULL),
    ('SALES_MART', 'region_totals', 'ETL', 'SQL', NULL, NULL),
    ('SALES_MART', 'customer_totals', 'ETL', 'SQL', NULL, NULL),
    ('SALES_MART', 'publish', 'ETL', 'PYTHON', 'N', 2),
    ('SALES_MART', 'alert', 'ETL', 'EMAIL_ALERT', NULL, NULL)
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y';

-- An inline SELECT goes in SOURCE_SQL, its quotes doubled like any other text.
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT t.TASK_ID, v.column3, v.column4
FROM (VALUES
    ('SALES_MART', 'daily_totals', 'SQL_ACTION', 'CREATE_TABLE'),
    ('SALES_MART', 'daily_totals', 'TARGET_OBJECT', 'mart.daily_totals'),
    ('SALES_MART', 'daily_totals', 'SOURCE_SQL',
     'SELECT CAST(ordered_at AS DATE) AS order_date, SUM(amount) AS total_amount FROM sales.orders_uk GROUP BY CAST(ordered_at AS DATE)'),
    ('SALES_MART', 'region_totals', 'SQL_ACTION', 'CREATE_TABLE'),
    ('SALES_MART', 'region_totals', 'TARGET_OBJECT', 'mart.region_totals'),
    ('SALES_MART', 'region_totals', 'SOURCE_SQL',
     'SELECT ''uk'' AS region, SUM(amount) AS total_amount FROM sales.orders_uk'),
    ('SALES_MART', 'customer_totals', 'SQL_ACTION', 'CREATE_TABLE'),
    ('SALES_MART', 'customer_totals', 'TARGET_OBJECT', 'mart.customer_totals'),
    ('SALES_MART', 'customer_totals', 'SOURCE_SQL',
     'SELECT customer_id, SUM(amount) AS total_amount FROM sales.orders_uk GROUP BY customer_id'),
    ('SALES_MART', 'publish', 'SCRIPT_NAME', 'sales/publish.py'),
    ('SALES_MART', 'alert', 'EMAIL_TO', 'sales-data@example.com'),
    ('SALES_MART', 'alert', 'EMAIL_SUBJECT', '$$pipeline_code: $$status'),
    ('SALES_MART', 'alert', 'EMAIL_BODY', 'Run $$pipeline_run_id ended $$status.')
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = v.column2
                     AND t.ACTIVE_FLAG = 'Y';

-- customer_totals waits on a task in another pipeline: the upstream's pipeline code names it.
INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID,
                                 DEPENDENCY_TYPE)
SELECT t.PIPELINE_ID, t.TASK_ID, u.PIPELINE_ID, u.TASK_ID, v.column5
FROM (VALUES
    ('SALES_MART', 'customer_totals', 'SALES_DAILY', 'load_orders', 'HAS_DATA'),
    ('SALES_MART', 'publish', 'SALES_MART', 'daily_totals', 'SUCCESS'),
    ('SALES_MART', 'publish', 'SALES_MART', 'region_totals', 'SUCCESS'),
    ('SALES_MART', 'publish', 'SALES_MART', 'customer_totals', 'SUCCESS'),
    ('SALES_MART', 'alert', 'SALES_MART', 'publish', 'ALWAYS')
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = v.column2
                     AND t.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_PIPELINES up ON up.PIPELINE_CODE = v.column3 AND up.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS u ON u.PIPELINE_ID = up.PIPELINE_ID AND u.TASK_CODE = v.column4
                     AND u.ACTIVE_FLAG = 'Y';
