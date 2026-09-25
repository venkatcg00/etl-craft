-- Every active task of an active pipeline, with its handler and the pipeline's refresh type.
SELECT t.TASK_ID AS task_id, t.PIPELINE_ID AS pipeline_id, p.PIPELINE_CODE AS pipeline_code,
       t.TASK_CODE AS task_code, t.HANDLER AS handler, p.REFRESH_TYPE AS refresh_type
FROM CFG_TASKS t
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
WHERE t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y'
ORDER BY p.PIPELINE_CODE, t.TASK_CODE
