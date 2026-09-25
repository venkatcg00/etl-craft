-- The active task with :task_code in :pipeline_id.
SELECT TASK_ID AS task_id
FROM CFG_TASKS
WHERE PIPELINE_ID = :pipeline_id AND TASK_CODE = :task_code AND ACTIVE_FLAG = 'Y'
