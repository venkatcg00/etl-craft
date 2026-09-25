-- Replace the TASK_LOG of :task_run_id.
UPDATE AUD_TASK_RUN_LOG
SET TASK_LOG = :task_log
WHERE TASK_RUN_ID = :task_run_id
