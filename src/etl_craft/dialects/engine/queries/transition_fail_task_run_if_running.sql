-- Record :task_run_id FAILED with :error_message, only while it is still IN-PROGRESS: an attempt
-- whose task process could not be started, or whose parent was stopped, never overwrites an
-- outcome the task process recorded itself.
UPDATE AUD_TASK_RUN_LOG
SET STATUS = 'FAILED', END_DATE = :now, ERROR_MESSAGE = :error_message
WHERE TASK_RUN_ID = :task_run_id AND STATUS = 'IN-PROGRESS'
AND NOT EXISTS (SELECT 1 FROM AUD_TASK_ATTEMPTS a WHERE a.TASK_RUN_ID = :task_run_id AND a.STATUS IN ('QUEUED','CLAIMED','RUNNING'))
