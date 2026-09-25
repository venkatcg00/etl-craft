-- The active tasks of :pipeline_id, by code.
SELECT TASK_ID AS task_id, TASK_CODE AS task_code, TASK_TYPE AS task_type, HANDLER AS handler,
       RUN_CONDITION AS run_condition, RUN_CONDITION_COUNT AS run_condition_count
FROM CFG_TASKS
WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'
ORDER BY TASK_CODE
