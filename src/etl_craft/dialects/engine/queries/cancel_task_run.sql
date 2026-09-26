-- End the IN-PROGRESS :task_run_id CANCELLED; its process sees it and stops.
UPDATE AUD_TASK_RUN_LOG
SET STATUS = 'CANCELLED', END_DATE = :now, ERROR_MESSAGE = :error_message
WHERE TASK_RUN_ID = :task_run_id AND STATUS = 'IN-PROGRESS'
