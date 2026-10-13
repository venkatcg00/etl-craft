-- The active parameters of :task_id.
SELECT PARAMETER_NAME AS parameter_name, PARAMETER_VALUE AS parameter_value, 'Y' AS target_active_flag
FROM CFG_TASK_PARAMETERS
WHERE TASK_ID = :task_id AND ACTIVE_FLAG = 'Y'
UNION ALL
SELECT 'TARGET_OBJECT' AS parameter_name,
       CASE WHEN o.CATALOG_NAME IS NULL THEN '' ELSE o.CATALOG_NAME || '.' END
       || o.SCHEMA_NAME || '.' || o.TABLE_NAME AS parameter_value,
       o.ACTIVE_FLAG AS target_active_flag
FROM CFG_TASKS t JOIN CFG_TABLES o ON o.TABLE_ID=t.TARGET_TABLE_ID
WHERE t.TASK_ID=:task_id AND t.TARGET_ACTIVE_FLAG='Y'
