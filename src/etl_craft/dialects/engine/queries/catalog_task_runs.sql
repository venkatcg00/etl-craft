-- The latest :limit runs of every active task of an active pipeline, newest first.
SELECT r.TASK_RUN_ID AS task_run_id, r.TASK_ID AS task_id, r.PIPELINE_RUN_ID AS pipeline_run_id, r.STATUS AS status,
       CASE WHEN pr.OUTPUT_REVISION > 1 OR pr.REPAIR_PENDING = 'Y' THEN 'REPAIR'
            WHEN pr.BACKFILL = 'Y' THEN 'BACKFILL'
            WHEN pr.TRIGGER_KIND = 'SCHEDULE' THEN 'SCHEDULED' ELSE 'OTHER' END AS run_kind,
       r.ATTEMPT_COUNT AS attempts, r.START_DATE AS "start", r.END_DATE AS "end",
       r.SOURCE_COUNT AS source_count, r.TARGET_COUNT AS target_count,
       r.INSERT_COUNT AS insert_count, r.UPDATE_COUNT AS update_count,
       r.DELETE_COUNT AS delete_count, r.REJECT_COUNT AS reject_count,
       r.WATERMARK_FROM AS watermark_from, r.WATERMARK_TO AS watermark_to,
       r.ERROR_MESSAGE AS error_message,
       (SELECT 1 AS has_item FROM AUD_TASK_ITEMS i WHERE i.TASK_RUN_ID=r.TASK_RUN_ID LIMIT 1) AS has_items
FROM (
    SELECT l.*, ROW_NUMBER() OVER (PARTITION BY l.TASK_ID ORDER BY l.TASK_RUN_ID DESC) AS rn
    FROM AUD_TASK_RUN_LOG l
    WHERE l.TASK_ID IN (
        SELECT t.TASK_ID FROM CFG_TASKS t JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
        WHERE t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y'
    )
) r
JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
JOIN AUD_PIPELINES_RUN_LOG pr ON pr.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID
WHERE r.rn <= :limit AND t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y'
ORDER BY r.TASK_ID, r.PIPELINE_RUN_ID DESC
