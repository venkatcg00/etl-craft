-- The status and error message of :task_run_id.
SELECT STATUS AS status, ERROR_MESSAGE AS error_message, ATTEMPT_COUNT AS attempt_count
FROM AUD_TASK_RUN_LOG
WHERE TASK_RUN_ID = :task_run_id
