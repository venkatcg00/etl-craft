-- A new pipeline, with rows in every table a pipeline uses. SALES_DAILY fetches the day's UK
-- orders, loads and checks them, emails on-call when a step fails, and emails the team when it
-- ends.
--
-- Rows refer to each other by code, never by id, so the same file loads every Engine DB. Each
-- statement LEFT JOINs the codes it names: a code with no active row leaves an id NULL, and the
-- NOT NULL constraint stops the whole file, where a plain JOIN would drop the row and say nothing.
--
-- Values are SQL literals: text in single quotes, with each quote inside it doubled
-- ('the day''s'), NULL unquoted for no value, and numbers as they are.

-- Pipelines. RUN_SCHEDULE is a five-field cron expression, or NULL for a pipeline that runs only
-- when started; SCHEDULE_TIMEZONE is an IANA name, or NULL for Orchestration.Timezone. CATCHUP,
-- MAX_CATCHUP_RUNS, OVERLAP_POLICY and SCHEDULE_START_DATE can be left out for their defaults.
INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, DESCRIPTION, REFRESH_TYPE, RUN_SCHEDULE,
                           SCHEDULE_TIMEZONE, CATCHUP, MAX_CATCHUP_RUNS, OVERLAP_POLICY,
                           SCHEDULE_START_DATE, SLA_IN_HOURS, PIPELINE_PARAMETERS)
VALUES
    ('SALES_DAILY', 'Daily sales, UK', 'Fetches the day''s UK orders, loads them and checks them.',
     'INCREMENTAL', '0 6 * * *', 'Europe/London', 'Y', 3, 'QUEUE', '2026-11-01', 2,
     '{"TAGS": ["sales"], "EMAIL_RECIPIENTS": ["sales-data@example.com"]}');

-- Tasks: pipeline code, task code, TASK_TYPE (INGESTION or ETL), HANDLER (PYTHON, SQL,
-- BUSINESS_RULES or EMAIL_ALERT), then RUN_CONDITION and RUN_CONDITION_COUNT: NULL and NULL to
-- wait for every dependency, 'ANY' and NULL for any one of them, 'N' and a number for that many.
INSERT INTO CFG_TASKS (PIPELINE_ID, TASK_CODE, TASK_TYPE, HANDLER, RUN_CONDITION,
                       RUN_CONDITION_COUNT)
SELECT p.PIPELINE_ID, v.column2, v.column3, v.column4, v.column5, CAST(v.column6 AS INTEGER)
FROM (VALUES
    ('SALES_DAILY', 'fetch_orders', 'INGESTION', 'PYTHON', NULL, NULL),
    ('SALES_DAILY', 'setup_orders', 'ETL', 'SQL', NULL, NULL),
    ('SALES_DAILY', 'load_orders', 'ETL', 'SQL', NULL, NULL),
    ('SALES_DAILY', 'check_orders', 'ETL', 'BUSINESS_RULES', NULL, NULL),
    ('SALES_DAILY', 'on_failure', 'ETL', 'EMAIL_ALERT', 'ANY', NULL),
    ('SALES_DAILY', 'alert', 'ETL', 'EMAIL_ALERT', NULL, NULL)
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y';

-- Task parameters: pipeline code, task code, PARAMETER_NAME, PARAMETER_VALUE. Every value is
-- text, JSON included. Each handler's guide lists the parameters it reads; any task can also set
-- RETRIES, RETRY_DELAY_SECONDS, RETRY_BACKOFF, TASK_TIMEOUT_SECONDS and DOCUMENTATION.
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT t.TASK_ID, v.column3, v.column4
FROM (VALUES
    ('SALES_DAILY', 'fetch_orders', 'SCRIPT_NAME', 'sales/fetch_orders.py'),
    ('SALES_DAILY', 'fetch_orders', 'INPUT_PARAMS', '{"region": "uk", "days": 1}'),
    ('SALES_DAILY', 'fetch_orders', 'TARGET_OBJECT', 'lnd.orders_uk'),
    ('SALES_DAILY', 'fetch_orders', 'SOURCE_OBJECT', 'Order service API'),
    ('SALES_DAILY', 'fetch_orders', 'RETRIES', '2'),
    ('SALES_DAILY', 'fetch_orders', 'RETRY_DELAY_SECONDS', '300'),
    ('SALES_DAILY', 'fetch_orders', 'TASK_TIMEOUT_SECONDS', '1800'),
    ('SALES_DAILY', 'setup_orders', 'SQL_ACTION', 'SETUP_TABLE'),
    ('SALES_DAILY', 'setup_orders', 'TARGET_OBJECT', 'sales.orders_uk'),
    ('SALES_DAILY', 'setup_orders', 'SOURCE_SQL_FILE', 'sales/orders_uk.sql'),
    ('SALES_DAILY', 'load_orders', 'SQL_ACTION', 'APPEND_TABLE'),
    ('SALES_DAILY', 'load_orders', 'TARGET_OBJECT', 'sales.orders_uk'),
    ('SALES_DAILY', 'load_orders', 'SOURCE_SQL_FILE', 'sales/orders_uk.sql'),
    ('SALES_DAILY', 'load_orders', 'DOCUMENTATION', 'Appends the day''s orders to the region''s table.'),
    ('SALES_DAILY', 'on_failure', 'EMAIL_TO', 'sales-oncall@example.com'),
    ('SALES_DAILY', 'on_failure', 'EMAIL_ON_STATUS', 'FAILED'),
    ('SALES_DAILY', 'on_failure', 'EMAIL_SUBJECT', '$$pipeline_code: a step failed'),
    ('SALES_DAILY', 'on_failure', 'EMAIL_BODY', 'Run $$pipeline_run_id failed: $$error_message'),
    ('SALES_DAILY', 'alert', 'EMAIL_TO', 'sales-data@example.com'),
    ('SALES_DAILY', 'alert', 'EMAIL_SUBJECT', '$$pipeline_code: $$status'),
    ('SALES_DAILY', 'alert', 'EMAIL_BODY', 'Run $$pipeline_run_id ended $$status.')
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = v.column2
                     AND t.ACTIVE_FLAG = 'Y';

-- Task dependencies: the task (pipeline code, task code), the task it waits on (pipeline code,
-- task code, in this pipeline or another), and DEPENDENCY_TYPE: SUCCESS, FAILURE, ALWAYS, or
-- HAS_DATA for a success that wrote rows. on_failure runs when ANY of its upstreams fails.
INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID,
                                 DEPENDENCY_TYPE)
SELECT t.PIPELINE_ID, t.TASK_ID, u.PIPELINE_ID, u.TASK_ID, v.column5
FROM (VALUES
    ('SALES_DAILY', 'load_orders', 'SALES_DAILY', 'fetch_orders', 'SUCCESS'),
    ('SALES_DAILY', 'load_orders', 'SALES_DAILY', 'setup_orders', 'SUCCESS'),
    ('SALES_DAILY', 'check_orders', 'SALES_DAILY', 'load_orders', 'HAS_DATA'),
    ('SALES_DAILY', 'on_failure', 'SALES_DAILY', 'fetch_orders', 'FAILURE'),
    ('SALES_DAILY', 'on_failure', 'SALES_DAILY', 'load_orders', 'FAILURE'),
    ('SALES_DAILY', 'alert', 'SALES_DAILY', 'check_orders', 'ALWAYS')
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = v.column2
                     AND t.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_PIPELINES up ON up.PIPELINE_CODE = v.column3 AND up.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS u ON u.PIPELINE_ID = up.PIPELINE_ID AND u.TASK_CODE = v.column4
                     AND u.ACTIVE_FLAG = 'Y';

-- Business rules: pipeline code, task code, BUSINESS_RULE_NAME, SEQUENCE_NUMBER (the wave the
-- rule runs in), BUSINESS_RULE_TYPE (INCOMPLETE, REJECT or REPORT), BUSINESS_RULE_KEY_COLUMN,
-- TARGET_TABLE, and BUSINESS_RULE_SQL: a SELECT that returns a row when the table's row t breaks
-- the rule. Bound inputs such as :pipeline_run_id are written as they are.
INSERT INTO CFG_BUSINESS_RULES (PIPELINE_ID, TASK_ID, BUSINESS_RULE_NAME, SEQUENCE_NUMBER,
                                BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE,
                                BUSINESS_RULE_SQL)
SELECT t.PIPELINE_ID, t.TASK_ID, v.column3, CAST(v.column4 AS INTEGER), v.column5, v.column6,
       v.column7, v.column8
FROM (VALUES
    ('SALES_DAILY', 'check_orders', 'closed customer', 1, 'REJECT', 'ROW_ID', 'sales.orders_uk',
     'SELECT 1 FROM sales.customers c WHERE c.customer_id = t.customer_id AND c.status = ''closed'''),
    ('SALES_DAILY', 'check_orders', 'missing amount', 1, 'INCOMPLETE', 'ROW_ID', 'sales.orders_uk',
     'SELECT 1 FROM sales.customers c WHERE c.customer_id = t.customer_id AND t.amount IS NULL'),
    ('SALES_DAILY', 'check_orders', 'test customer', 1, 'REPORT', 'ROW_ID', 'sales.orders_uk',
     'SELECT 1 FROM sales.customers c WHERE c.customer_id = t.customer_id AND c.name LIKE ''TEST%'''),
    ('SALES_DAILY', 'check_orders', 'large order this run', 2, 'REPORT', 'ROW_ID', 'sales.orders_uk',
     'SELECT 1 FROM sales.customers c WHERE c.customer_id = t.customer_id AND t.amount > c.credit_limit AND t.PIPELINE_RUN_ID = :pipeline_run_id')
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = v.column2
                     AND t.ACTIVE_FLAG = 'Y';
