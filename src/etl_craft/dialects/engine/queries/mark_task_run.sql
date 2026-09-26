-- Set :task_run_id to :status, as an operator marked it. A marked SUCCESS (:sets_count = 1) gets
-- :target_count, NULL unless a row count was stated; other statuses keep the counts the task
-- reported.
UPDATE AUD_TASK_RUN_LOG
SET STATUS = :status, END_DATE = :now, ERROR_MESSAGE = :error_message,
    TARGET_COUNT = CASE WHEN :sets_count = 1 THEN :target_count ELSE TARGET_COUNT END
WHERE TASK_RUN_ID = :task_run_id
