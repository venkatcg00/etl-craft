-- Every active task in :pipeline_id, with its run condition.
SELECT TASK_ID AS task_id, RUN_CONDITION AS run_condition,
       RUN_CONDITION_COUNT AS run_condition_count
FROM CFG_TASKS
WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'
ORDER BY TASK_ID
