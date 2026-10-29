-- Retiring rows: two tasks and a whole pipeline, then reactivating a task in place.
-- Rows are retired with ACTIVE_FLAG = 'N', never deleted: run history refers to them, and
-- etl-craft reads only active rows. A retired code still belongs to its row. Retire a pipeline, or
-- its tasks, while no run of it is active.
--
-- Two temporary tables list what is retired; the statements after them retire everything that
-- belongs to those rows or refers to them. A code with no active row stops the file.

CREATE TEMPORARY TABLE retired_pipelines (CODE VARCHAR(128) NOT NULL, ID BIGINT NOT NULL);
CREATE TEMPORARY TABLE retired_tasks (CODE VARCHAR(300) NOT NULL, ID BIGINT NOT NULL);

INSERT INTO retired_pipelines (CODE, ID)
SELECT v.column1, p.PIPELINE_ID
FROM (VALUES ('SALES_DAILY_US')) v
LEFT JOIN CFG_PIPELINES p ON p.PROJECT_ID = (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)
    AND p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y';

-- Tasks retired on their own. publish is reactivated at the end with its existing ID.
INSERT INTO retired_tasks (CODE, ID)
SELECT v.column1 || '.' || v.column2, t.TASK_ID
FROM (VALUES
    ('SALES_MART', 'customer_totals'),
    ('SALES_MART', 'publish')
) v
LEFT JOIN CFG_PIPELINES p ON p.PROJECT_ID = (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)
    AND p.PIPELINE_CODE = v.column1 AND p.ACTIVE_FLAG = 'Y'
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
SET ACTIVE_FLAG = 'N', TARGET_ACTIVE_FLAG=CASE WHEN TARGET_TABLE_ID IS NULL THEN TARGET_ACTIVE_FLAG ELSE 'N' END
WHERE ACTIVE_FLAG = 'Y' AND TASK_ID IN (SELECT ID FROM retired_tasks);

UPDATE CFG_PIPELINE_DEPENDENCY
SET ACTIVE_FLAG = 'N'
WHERE ACTIVE_FLAG = 'Y'
  AND (PIPELINE_ID IN (SELECT ID FROM retired_pipelines)
       OR DEPENDS_ON_PIPELINE_ID IN (SELECT ID FROM retired_pipelines));

UPDATE CFG_PIPELINES
SET ACTIVE_FLAG = 'N'
WHERE ACTIVE_FLAG = 'Y' AND PIPELINE_ID IN (SELECT ID FROM retired_pipelines);

-- Reactivate publish without replacing its ID or its existing history.
UPDATE CFG_TASKS SET ACTIVE_FLAG='Y', HANDLER='PYTHON', RUN_CONDITION='ANY', RUN_CONDITION_COUNT=NULL
WHERE TASK_ID IN (SELECT ID FROM retired_tasks WHERE CODE='SALES_MART.publish');

-- Change the existing script setting and reactivate the existing parameters.
UPDATE CFG_TASK_PARAMETERS SET ACTIVE_FLAG='Y',
    PARAMETER_VALUE=CASE WHEN PARAMETER_NAME='SCRIPT_FILE' THEN 'python_scripts/sales/publish_to_partner.py'
                        ELSE PARAMETER_VALUE END
WHERE TASK_ID IN (SELECT ID FROM retired_tasks WHERE CODE='SALES_MART.publish');

INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT ID, 'INPUT_PARAMS', '{"partner": "acme"}' FROM retired_tasks
WHERE CODE='SALES_MART.publish';

-- Reactivate the dependency rows by their original IDs, both ways.
UPDATE CFG_TASK_DEPENDENCY SET ACTIVE_FLAG='Y'
WHERE (TASK_ID IN (SELECT ID FROM retired_tasks WHERE CODE='SALES_MART.publish')
       OR DEPENDS_ON_TASK_ID IN (SELECT ID FROM retired_tasks WHERE CODE='SALES_MART.publish'))
  AND TASK_ID IN (SELECT TASK_ID FROM CFG_TASKS WHERE ACTIVE_FLAG='Y')
  AND DEPENDS_ON_TASK_ID IN (SELECT TASK_ID FROM CFG_TASKS WHERE ACTIVE_FLAG='Y');

DROP TABLE retired_tasks;
DROP TABLE retired_pipelines;

-- Table-level checks use the same task and target contracts, with no row key.
INSERT INTO CFG_BUSINESS_RULES (PIPELINE_ID,TASK_ID,BUSINESS_RULE_NAME,SEQUENCE_NUMBER,
 BUSINESS_RULE_TYPE,BUSINESS_RULE_LEVEL,BUSINESS_RULE_KEY_COLUMN,TARGET_TABLE_ID,BUSINESS_RULE_SQL)
SELECT t.PIPELINE_ID,t.TASK_ID,v.column1,1,v.column2,'TABLE',NULL,o.TABLE_ID,v.column3
FROM (VALUES
 ('at least one row this run','REJECT','SELECT CASE WHEN COUNT(*)>0 THEN ''Y'' ELSE ''N'' END AS PASSED, CAST(COUNT(*) AS VARCHAR) AS OBSERVED FROM $$rows_in_scope'),
 ('null rate under five percent','REPORT','SELECT CASE WHEN COUNT(*)>0 AND SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END)*100.0<COUNT(*)*5 THEN ''Y'' ELSE ''N'' END AS PASSED, CAST(SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) AS VARCHAR) AS OBSERVED FROM $$rows_in_scope')
) v JOIN CFG_PIPELINES p ON p.PROJECT_ID = (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)
    AND p.PIPELINE_CODE='SALES_DAILY'
JOIN CFG_TASKS t ON t.PIPELINE_ID=p.PIPELINE_ID AND t.TASK_CODE='check_orders'
JOIN CFG_TABLES o ON o.PROJECT_ID = (SELECT PROJECT_ID FROM ETL_CRAFT_PROJECT)
    AND o.SCHEMA_NAME='sales' AND o.TABLE_NAME='orders_uk';
