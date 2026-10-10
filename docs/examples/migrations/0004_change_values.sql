-- Changes in place: a schedule, an SLA, PIPELINE_PARAMETERS, a parameter on several pipelines at
-- once, a dependency type, a run condition, a business rule and a task's code.
--
-- Each UPDATE finds its rows by code. An UPDATE that matches no row changes nothing and reports
-- no error, so the file first lists every code it changes in a temporary table: a code with no
-- active row leaves ID NULL there, and the NOT NULL constraint stops the file.

CREATE TEMPORARY TABLE codes_in_use (CODE VARCHAR(300) NOT NULL, ID BIGINT NOT NULL);

INSERT INTO codes_in_use (CODE, ID)
SELECT v.column1, p.PIPELINE_ID
FROM (VALUES ('SALES_DAILY'), ('SALES_DAILY_EU'), ('SALES_DAILY_US'), ('SALES_MART')) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y';

INSERT INTO codes_in_use (CODE, ID)
SELECT v.column1 || '.' || v.column2, t.TASK_ID
FROM (VALUES
    ('SALES_DAILY', 'alert'),
    ('SALES_DAILY_EU', 'alert'),
    ('SALES_DAILY_US', 'alert'),
    ('SALES_DAILY', 'check_orders'),
    ('SALES_DAILY', 'load_orders'),
    ('SALES_MART', 'publish'),
    ('SALES_MART', 'region_totals')
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = v.column2
                     AND t.ACTIVE_FLAG = 'Y';

-- A schedule and an SLA.
UPDATE CFG_PIPELINES
SET RUN_SCHEDULE = '0 5 * * 1-5', OVERLAP_POLICY = 'SKIP', SLA_IN_HOURS = 1.5
WHERE PIPELINE_CODE = 'SALES_DAILY' AND ACTIVE_FLAG = 'Y';

-- PIPELINE_PARAMETERS is replaced whole: write the complete JSON object.
UPDATE CFG_PIPELINES
SET PIPELINE_PARAMETERS = '{"TAGS": ["sales", "daily"], "EMAIL_RECIPIENTS": ["sales-data@example.com", "sales-leads@example.com"]}'
WHERE PIPELINE_CODE = 'SALES_DAILY' AND ACTIVE_FLAG = 'Y';

-- One parameter on several pipelines at once.
UPDATE CFG_TASK_PARAMETERS
SET PARAMETER_VALUE = 'sales-data@example.com|sales-leads@example.com'
WHERE PARAMETER_NAME = 'EMAIL_TO' AND ACTIVE_FLAG = 'Y'
  AND TASK_ID IN (SELECT t.TASK_ID
                  FROM CFG_TASKS t
                  JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID AND p.ACTIVE_FLAG = 'Y'
                  WHERE p.PIPELINE_CODE IN ('SALES_DAILY', 'SALES_DAILY_EU', 'SALES_DAILY_US')
                    AND t.TASK_CODE = 'alert' AND t.ACTIVE_FLAG = 'Y');

-- A dependency's type, found by both of its tasks.
UPDATE CFG_TASK_DEPENDENCY
SET DEPENDENCY_TYPE = 'SUCCESS'
WHERE ACTIVE_FLAG = 'Y'
  AND TASK_ID = (SELECT ID FROM codes_in_use WHERE CODE = 'SALES_DAILY.check_orders')
  AND DEPENDS_ON_TASK_ID = (SELECT ID FROM codes_in_use WHERE CODE = 'SALES_DAILY.load_orders');

-- A run condition: RUN_CONDITION and RUN_CONDITION_COUNT change together.
UPDATE CFG_TASKS
SET RUN_CONDITION = 'ANY', RUN_CONDITION_COUNT = NULL
WHERE TASK_ID = (SELECT ID FROM codes_in_use WHERE CODE = 'SALES_MART.publish');

-- A business rule, found by its task and name.
UPDATE CFG_BUSINESS_RULES
SET BUSINESS_RULE_TYPE = 'REJECT'
WHERE BUSINESS_RULE_NAME = 'large order this run' AND ACTIVE_FLAG = 'Y'
  AND TASK_ID = (SELECT ID FROM codes_in_use WHERE CODE = 'SALES_DAILY.check_orders');

-- A task's code. Runs refer to a task by id, so its history stays with it; commands, logs and
-- generated DAGs use the new code from now on. Rename while no run of the pipeline is active.
UPDATE CFG_TASKS
SET TASK_CODE = 'totals_by_region'
WHERE TASK_ID = (SELECT ID FROM codes_in_use WHERE CODE = 'SALES_MART.region_totals');

DROP TABLE codes_in_use;
