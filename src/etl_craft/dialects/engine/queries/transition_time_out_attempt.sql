UPDATE AUD_TASK_ATTEMPTS SET STATUS = 'TIMED_OUT', ENDED_AT = :now,
SOURCE_COUNT = :source_count, TARGET_COUNT = :target_count, INSERT_COUNT = :insert_count,
UPDATE_COUNT = :update_count, DELETE_COUNT = :delete_count, ROWS_WRITTEN = :rows_written,
ERROR_MESSAGE = :error_message, TASK_LOG = :task_log, EXIT_CODE = :exit_code
WHERE ATTEMPT_ID = :row_id AND STATUS = 'RUNNING' AND OWNER_ID = :owner
RETURNING TASK_RUN_ID AS task_run_id
