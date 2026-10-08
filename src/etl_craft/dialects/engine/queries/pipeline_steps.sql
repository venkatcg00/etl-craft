-- Active task definitions and their summaries under one exact pipeline run.
SELECT t.TASK_ID AS task_id, t.TASK_CODE AS task_code, t.TASK_TYPE AS task_type,
       t.HANDLER AS handler, t.RUN_CONDITION AS run_condition,
       t.RUN_CONDITION_COUNT AS run_condition_count, r.TASK_RUN_ID AS task_run_id,
       r.STATUS AS status
FROM CFG_TASKS t
LEFT JOIN AUD_TASK_RUN_LOG r ON r.TASK_ID = t.TASK_ID AND r.PIPELINE_RUN_ID = :run_id
WHERE t.PIPELINE_ID = :pipeline_id AND t.ACTIVE_FLAG = 'Y'
ORDER BY t.TASK_CODE
