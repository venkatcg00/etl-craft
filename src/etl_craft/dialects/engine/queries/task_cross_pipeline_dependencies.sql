-- The active dependencies of :task_id on tasks in other pipelines.
SELECT TASK_DEPENDENCY_ID AS task_dependency_id, PIPELINE_ID AS pipeline_id,
       DEPENDS_ON_PIPELINE_ID AS depends_on_pipeline_id,
       DEPENDS_ON_TASK_ID AS depends_on_task_id, DEPENDENCY_TYPE AS dependency_type
FROM CFG_TASK_DEPENDENCY
WHERE TASK_ID = :task_id AND ACTIVE_FLAG = 'Y' AND DEPENDS_ON_PIPELINE_ID <> PIPELINE_ID
ORDER BY TASK_DEPENDENCY_ID
