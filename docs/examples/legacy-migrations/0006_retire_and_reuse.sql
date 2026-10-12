-- Retiring rows: two tasks, a whole pipeline, and a retired task code used again for a new task.
-- Rows are retired with ACTIVE_FLAG = 'N', never deleted: run history refers to them, and
-- etl-craft reads only active rows. A retired code is free for a new row. Retire a pipeline, or
-- its tasks, while no run of it is active.
--
-- Two temporary tables list what is retired; the statements after them retire everything that
-- belongs to those rows or refers to them. A code with no active row stops the file.

CREATE TEMPORARY TABLE retired_pipelines (CODE VARCHAR(128) NOT NULL, ID BIGINT NOT NULL);
CREATE TEMPORARY TABLE retired_tasks (CODE VARCHAR(300) NOT NULL, ID BIGINT NOT NULL);

INSERT INTO retired_pipelines (CODE, ID)
SELECT v.column1, p.PIPELINE_ID
FROM (VALUES ('SALES_DAILY_US')) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y';

-- Tasks retired on their own. publish is replaced at the end by a new task with the same code.
INSERT INTO retired_tasks (CODE, ID)
SELECT v.column1 || '.' || v.column2, t.TASK_ID
FROM (VALUES
    ('SALES_MART', 'customer_totals'),
    ('SALES_MART', 'publish')
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = v.column2
                     AND t.ACTIVE_FLAG = 'Y';

-- Every task of a retired pipeline.
INSERT INTO retired_tasks (CODE, ID)
SELECT r.CODE || '.' || t.TASK_CODE, t.TASK_ID
FROM retired_pipelines r
JOIN CFG_TASKS t ON t.PIPELINE_ID = r.ID AND t.ACTIVE_FLAG = 'Y';

UPDATE CFG_TASK_PARAMETERS
SET ACTIVE_FLAG = 'N'
WHERE ACTIVE_FLAG = 'Y' AND TASK_ID IN (SELECT ID FROM retired_tasks);

UPDATE CFG_BUSINESS_RULES
SET ACTIVE_FLAG = 'N'
WHERE ACTIVE_FLAG = 'Y' AND TASK_ID IN (SELECT ID FROM retired_tasks);

-- Dependencies both ways: what a retired task waits on, and what waits on it. A task that waited
-- on one starts without it from now on; validate fails a RUN_CONDITION_COUNT larger than the
-- dependencies left.
UPDATE CFG_TASK_DEPENDENCY
SET ACTIVE_FLAG = 'N'
WHERE ACTIVE_FLAG = 'Y'
  AND (TASK_ID IN (SELECT ID FROM retired_tasks)
       OR DEPENDS_ON_TASK_ID IN (SELECT ID FROM retired_tasks));

UPDATE CFG_TASKS
SET ACTIVE_FLAG = 'N'
WHERE ACTIVE_FLAG = 'Y' AND TASK_ID IN (SELECT ID FROM retired_tasks);

UPDATE CFG_PIPELINE_DEPENDENCY
SET ACTIVE_FLAG = 'N'
WHERE ACTIVE_FLAG = 'Y'
  AND (PIPELINE_ID IN (SELECT ID FROM retired_pipelines)
       OR DEPENDS_ON_PIPELINE_ID IN (SELECT ID FROM retired_pipelines));

UPDATE CFG_PIPELINES
SET ACTIVE_FLAG = 'N'
WHERE ACTIVE_FLAG = 'Y' AND PIPELINE_ID IN (SELECT ID FROM retired_pipelines);

-- A new publish under the retired code. Its history starts afresh, and its dependencies, both
-- ways, are written again.
INSERT INTO CFG_TASKS (PIPELINE_ID, TASK_CODE, TASK_TYPE, HANDLER, RUN_CONDITION)
SELECT p.PIPELINE_ID, v.column2, v.column3, v.column4, v.column5
FROM (VALUES
    ('SALES_MART', 'publish', 'ETL', 'PYTHON', 'ANY')
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y';

INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT t.TASK_ID, v.column3, v.column4
FROM (VALUES
    ('SALES_MART', 'publish', 'SCRIPT_NAME', 'sales/publish_to_partner.py'),
    ('SALES_MART', 'publish', 'INPUT_PARAMS', '{"partner": "acme"}')
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = v.column2
                     AND t.ACTIVE_FLAG = 'Y';

INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID,
                                 DEPENDENCY_TYPE)
SELECT t.PIPELINE_ID, t.TASK_ID, u.PIPELINE_ID, u.TASK_ID, v.column5
FROM (VALUES
    ('SALES_MART', 'publish', 'SALES_MART', 'daily_totals', 'SUCCESS'),
    ('SALES_MART', 'publish', 'SALES_MART', 'totals_by_region', 'SUCCESS'),
    ('SALES_MART', 'alert', 'SALES_MART', 'publish', 'ALWAYS')
) v
LEFT JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = v.column2
                     AND t.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_PIPELINES up ON up.PIPELINE_CODE = v.column3 AND up.ACTIVE_FLAG = 'Y'
LEFT JOIN CFG_TASKS u ON u.PIPELINE_ID = up.PIPELINE_ID AND u.TASK_CODE = v.column4
                     AND u.ACTIVE_FLAG = 'Y';

DROP TABLE retired_tasks;
DROP TABLE retired_pipelines;
