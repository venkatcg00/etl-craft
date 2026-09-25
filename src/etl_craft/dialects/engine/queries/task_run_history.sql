-- The latest :limit runs of :task_id, newest first.
SELECT PIPELINE_RUN_ID AS pipeline_run_id, STATUS AS status, START_DATE AS start_date,
       END_DATE AS end_date, ATTEMPT_COUNT AS attempt_count, SOURCE_COUNT AS source_count,
       TARGET_COUNT AS target_count, ERROR_MESSAGE AS error_message
FROM AUD_TASK_RUN_LOG
WHERE TASK_ID = :task_id
ORDER BY START_DATE DESC, TASK_RUN_ID DESC
LIMIT :limit
