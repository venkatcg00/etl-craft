-- The active task dependencies of the active tasks in :pipeline_id, with both tasks' codes and
-- whether the upstream task is active.
SELECT d.TASK_ID AS task_id, d.DEPENDS_ON_TASK_ID AS depends_on_task_id,
       d.DEPENDS_ON_PIPELINE_ID AS depends_on_pipeline_id, d.DEPENDENCY_TYPE AS dependency_type,
       t.TASK_CODE AS task_code, u.TASK_CODE AS depends_on_task_code,
       u.ACTIVE_FLAG AS depends_on_active_flag
FROM CFG_TASK_DEPENDENCY d
JOIN CFG_TASKS t ON t.TASK_ID = d.TASK_ID
JOIN CFG_TASKS u ON u.TASK_ID = d.DEPENDS_ON_TASK_ID
WHERE d.PIPELINE_ID = :pipeline_id AND d.ACTIVE_FLAG = 'Y' AND t.ACTIVE_FLAG = 'Y'
ORDER BY d.TASK_DEPENDENCY_ID
