-- The active task dependencies declared in :pipeline_id.
SELECT TASK_ID AS task_id, DEPENDS_ON_TASK_ID AS depends_on_task_id,
       DEPENDS_ON_PIPELINE_ID AS depends_on_pipeline_id, DEPENDENCY_TYPE AS dependency_type
FROM CFG_TASK_DEPENDENCY
WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'
ORDER BY TASK_DEPENDENCY_ID
