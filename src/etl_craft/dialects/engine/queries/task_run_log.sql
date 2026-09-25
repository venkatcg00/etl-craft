-- The TASK_LOG of :task_run_id.
SELECT TASK_LOG AS task_log
FROM AUD_TASK_RUN_LOG
WHERE TASK_RUN_ID = :task_run_id
