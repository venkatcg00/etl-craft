-- The latest :limit runs of every active task of an active pipeline, newest first.
SELECT r.TASK_ID AS task_id, r.PIPELINE_RUN_ID AS pipeline_run_id, r.STATUS AS status,
       r.ATTEMPT_COUNT AS attempts, r.START_DATE AS start_date, r.END_DATE AS end_date,
       r.SOURCE_COUNT AS source_count, r.TARGET_COUNT AS target_count,
       r.INSERT_COUNT AS insert_count, r.UPDATE_COUNT AS update_count,
       r.DELETE_COUNT AS delete_count, r.ERROR_MESSAGE AS error_message
FROM (
    SELECT l.*, ROW_NUMBER() OVER (PARTITION BY l.TASK_ID ORDER BY l.TASK_RUN_ID DESC) AS rn
    FROM AUD_TASK_RUN_LOG l
) r
JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
WHERE r.rn <= :limit AND t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y'
ORDER BY r.TASK_ID, r.PIPELINE_RUN_ID DESC
