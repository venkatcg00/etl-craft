-- Every active dependency of an active task of an active pipeline, with the upstream task's
-- handler, pipeline and whether they are active. DEPENDS_ON_PIPELINE_ID is as written, so it
-- can be compared with the pipeline the upstream task belongs to.
SELECT p.PIPELINE_CODE AS pipeline_code, t.TASK_CODE AS task_code,
       d.DEPENDENCY_TYPE AS dependency_type, d.DEPENDS_ON_PIPELINE_ID AS written_pipeline_id,
       u.TASK_ID AS depends_on_task_id, u.TASK_CODE AS depends_on_task_code,
       u.HANDLER AS depends_on_handler, u.ACTIVE_FLAG AS depends_on_task_active,
       u.PIPELINE_ID AS depends_on_pipeline_id, up.PIPELINE_CODE AS depends_on_pipeline_code,
       up.ACTIVE_FLAG AS depends_on_pipeline_active
FROM CFG_TASK_DEPENDENCY d
JOIN CFG_TASKS t ON t.TASK_ID = d.TASK_ID
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
JOIN CFG_TASKS u ON u.TASK_ID = d.DEPENDS_ON_TASK_ID
JOIN CFG_PIPELINES up ON up.PIPELINE_ID = u.PIPELINE_ID
WHERE d.ACTIVE_FLAG = 'Y' AND t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y'
ORDER BY p.PIPELINE_CODE, t.TASK_CODE, d.TASK_DEPENDENCY_ID
