-- Every active task in :pipeline_id, by id.
SELECT TASK_ID AS task_id, TASK_CODE AS task_code
FROM CFG_TASKS
WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'
ORDER BY TASK_CODE
