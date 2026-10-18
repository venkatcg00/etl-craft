SELECT t.PIPELINE_RUN_ID AS pipeline_run_id, t.TASK_ID AS task_id
FROM AUD_TASK_RUN_LOG t WHERE t.TASK_RUN_ID=:task_run_id
