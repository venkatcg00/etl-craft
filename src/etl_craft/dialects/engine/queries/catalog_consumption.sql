-- Consumption touching a shown run, preserving both directions of its explanation.
WITH shown AS (
    SELECT pipeline_run_id FROM (
        SELECT r.PIPELINE_RUN_ID AS pipeline_run_id,
               ROW_NUMBER() OVER (PARTITION BY r.PIPELINE_ID ORDER BY r.PIPELINE_RUN_ID DESC) AS rn
        FROM AUD_PIPELINES_RUN_LOG r
        JOIN CFG_PIPELINES p ON p.PIPELINE_ID = r.PIPELINE_ID
        WHERE p.ACTIVE_FLAG = 'Y'
    ) ranked WHERE rn <= :limit
)
SELECT dp.PIPELINE_CODE AS pipeline_code, c.PIPELINE_RUN_ID AS pipeline_run_id,
       dt.TASK_CODE AS task_code,
       up.PIPELINE_CODE AS upstream_pipeline, c.CONSUMED_PIPELINE_RUN_ID AS upstream_run_id,
       ut.TASK_CODE AS upstream_task
FROM AUD_DEPENDENCY_CONSUMPTION c
JOIN CFG_PIPELINES dp ON dp.PIPELINE_ID = c.PIPELINE_ID
JOIN CFG_PIPELINES up ON up.PIPELINE_ID = c.DEPENDS_ON_PIPELINE_ID
LEFT JOIN CFG_TASKS dt ON dt.TASK_ID = c.TASK_ID
LEFT JOIN AUD_TASK_RUN_LOG ur ON ur.TASK_RUN_ID = c.CONSUMED_TASK_RUN_ID
LEFT JOIN CFG_TASKS ut ON ut.TASK_ID = ur.TASK_ID
WHERE c.PIPELINE_RUN_ID IN (SELECT pipeline_run_id FROM shown)
   OR c.CONSUMED_PIPELINE_RUN_ID IN (SELECT pipeline_run_id FROM shown)
ORDER BY c.CONSUMPTION_ID
