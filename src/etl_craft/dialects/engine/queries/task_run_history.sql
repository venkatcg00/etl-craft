-- Bounded task history, newest first.
SELECT p.PIPELINE_ID AS pipeline_id, r.PIPELINE_RUN_ID AS pipeline_run_id,
       t.TASK_ID AS task_id, t.TASK_CODE AS task_code, r.TASK_RUN_ID AS task_run_id,
       r.STATUS AS status, r.START_DATE AS start_date, r.END_DATE AS end_date,
       r.ATTEMPT_COUNT AS attempt_count, r.SOURCE_COUNT AS source_count,
       r.TARGET_COUNT AS target_count, r.INSERT_COUNT AS insert_count,
       r.UPDATE_COUNT AS update_count, r.DELETE_COUNT AS delete_count,
       r.ROWS_WRITTEN AS rows_written, r.ERROR_MESSAGE AS error_message
FROM AUD_TASK_RUN_LOG r
JOIN AUD_PIPELINES_RUN_LOG p ON p.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID
JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID
WHERE p.PIPELINE_ID = :pipeline_id AND r.TASK_ID = :task_id
AND (CAST(:run_id AS bigint) IS NULL OR r.PIPELINE_RUN_ID = :run_id)
ORDER BY r.START_DATE DESC, r.TASK_RUN_ID DESC
LIMIT :limit
