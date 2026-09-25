-- The latest row of :task_id, in any status.
SELECT TASK_RUN_ID AS task_run_id, STATUS AS status, START_DATE AS start_date
FROM AUD_TASK_RUN_LOG
WHERE TASK_ID = :task_id
ORDER BY TASK_RUN_ID DESC
LIMIT 1
