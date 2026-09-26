-- Record the outcome of :task_run_id's current attempt, unless an operator cancelled it.
UPDATE AUD_TASK_RUN_LOG
SET STATUS = :status, END_DATE = :now,
    SOURCE_COUNT = :source_count, TARGET_COUNT = :target_count,
    INSERT_COUNT = :insert_count, UPDATE_COUNT = :update_count,
    DELETE_COUNT = :delete_count, ERROR_MESSAGE = :error_message, TASK_LOG = :task_log
WHERE TASK_RUN_ID = :task_run_id AND STATUS <> 'CANCELLED'
