-- What running :task_id needs beyond its parameters.
SELECT t.HANDLER AS handler, t.TASK_CODE AS task_code, t.PIPELINE_ID AS pipeline_id,
       p.PIPELINE_CODE AS pipeline_code, p.REFRESH_TYPE AS refresh_type
FROM CFG_TASKS t
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
WHERE t.TASK_ID = :task_id
