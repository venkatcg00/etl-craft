-- Active task parameters and normalized targets, restricted to active pipelines.
SELECT v.TASK_ID AS task_id, v.PARAMETER_NAME AS parameter_name,
       v.PARAMETER_VALUE AS parameter_value, v.TARGET_ACTIVE_FLAG AS target_active_flag
FROM (
    SELECT TASK_ID, PARAMETER_NAME, PARAMETER_VALUE, 'Y' AS target_active_flag
    FROM CFG_TASK_PARAMETERS WHERE ACTIVE_FLAG = 'Y'
    UNION ALL
    SELECT t.TASK_ID, 'TARGET_OBJECT',
           CASE WHEN o.CATALOG_NAME IS NULL THEN '' ELSE o.CATALOG_NAME || '.' END
           || o.SCHEMA_NAME || '.' || o.TABLE_NAME, o.ACTIVE_FLAG
    FROM CFG_TASKS t JOIN CFG_TABLES o ON o.TABLE_ID = t.TARGET_TABLE_ID
    WHERE t.TARGET_ACTIVE_FLAG = 'Y'
) v
JOIN CFG_TASKS t ON t.TASK_ID = v.TASK_ID
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
WHERE t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y' AND p.PROJECT_ID=:project_id
  AND (CAST(:handler AS varchar) IS NULL OR t.HANDLER = :handler)
ORDER BY v.TASK_ID, v.PARAMETER_NAME
