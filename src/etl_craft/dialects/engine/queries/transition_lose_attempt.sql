UPDATE AUD_TASK_ATTEMPTS SET RESERVED_CPU=0, RESERVED_MEMORY_MB=0, STATUS = 'LOST', ENDED_AT = :now,
SOURCE_COUNT = :source_count, TARGET_COUNT = :target_count, INSERT_COUNT = :insert_count,
UPDATE_COUNT = :update_count, DELETE_COUNT = :delete_count, ROWS_WRITTEN = :rows_written,
REJECT_COUNT = :reject_count, ERROR_MESSAGE = :error_message, TASK_LOG = :task_log, EXIT_CODE = :exit_code
WHERE ATTEMPT_ID = :row_id AND STATUS IN ('CLAIMED','RUNNING') AND (LEASE_EXPIRES_AT IS NULL OR LEASE_EXPIRES_AT <= :now)
AND (OWNER_ID = :owner OR (OWNER_ID IS NULL AND :owner IS NULL))
RETURNING TASK_RUN_ID AS task_run_id
