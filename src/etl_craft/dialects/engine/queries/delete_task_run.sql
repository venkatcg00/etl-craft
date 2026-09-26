-- Remove the row of a task the engine SKIPPED without running, so the resumed run decides again.
DELETE FROM AUD_TASK_RUN_LOG
WHERE TASK_RUN_ID = :task_run_id AND STATUS = 'SKIPPED'
