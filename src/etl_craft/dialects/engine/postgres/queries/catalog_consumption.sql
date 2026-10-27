-- Consumption between runs of the selected project, touching its shown history.
WITH shown AS (
    SELECT r.PIPELINE_RUN_ID AS pipeline_run_id
    FROM CFG_PIPELINES p
    CROSS JOIN LATERAL (
        SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG
        WHERE PIPELINE_ID = p.PIPELINE_ID ORDER BY PIPELINE_RUN_ID DESC LIMIT :limit
    ) r
    WHERE p.ACTIVE_FLAG = 'Y' AND p.PROJECT_ID=:project_id
), relevant AS (
    SELECT DISTINCT selected.consumption_id FROM shown
    CROSS JOIN LATERAL (
        SELECT c.CONSUMPTION_ID AS consumption_id FROM AUD_DEPENDENCY_CONSUMPTION c
        WHERE c.PIPELINE_RUN_ID=shown.pipeline_run_id
        UNION ALL
        SELECT c.CONSUMPTION_ID AS consumption_id FROM AUD_DEPENDENCY_CONSUMPTION c
        WHERE c.CONSUMED_PIPELINE_RUN_ID=shown.pipeline_run_id
    ) selected
)

SELECT dp.PIPELINE_CODE AS pipeline_code, c.PIPELINE_RUN_ID AS pipeline_run_id,
       dt.TASK_CODE AS task_code,
       up.PIPELINE_CODE AS upstream_pipeline, c.CONSUMED_PIPELINE_RUN_ID AS upstream_run_id,
       ut.TASK_CODE AS upstream_task
FROM relevant selected
JOIN AUD_DEPENDENCY_CONSUMPTION c ON c.CONSUMPTION_ID=selected.consumption_id
JOIN CFG_PIPELINES dp ON dp.PIPELINE_ID = c.PIPELINE_ID
JOIN CFG_PIPELINES up ON up.PIPELINE_ID = c.DEPENDS_ON_PIPELINE_ID
LEFT JOIN CFG_TASKS dt ON dt.TASK_ID = c.TASK_ID
LEFT JOIN AUD_TASK_RUN_LOG ur ON ur.TASK_RUN_ID = c.CONSUMED_TASK_RUN_ID
LEFT JOIN CFG_TASKS ut ON ut.TASK_ID = ur.TASK_ID
WHERE dp.PROJECT_ID=:project_id AND up.PROJECT_ID=:project_id
ORDER BY c.CONSUMPTION_ID
