-- Indexed top-N history for each active definition.
SELECT r.TASK_ID AS task_id, r.PIPELINE_RUN_ID AS pipeline_run_id, r.STATUS AS status,
       CASE WHEN pr.OUTPUT_REVISION > 1 OR pr.REPAIR_PENDING = 'Y' THEN 'REPAIR'
            WHEN pr.BACKFILL = 'Y' THEN 'BACKFILL'
            WHEN pr.TRIGGER_KIND = 'SCHEDULE' THEN 'SCHEDULED' ELSE 'OTHER' END AS run_kind,
       r.ATTEMPT_COUNT AS attempts, r.START_DATE AS "start", r.END_DATE AS "end",
       r.SOURCE_COUNT AS source_count, r.TARGET_COUNT AS target_count,
       r.INSERT_COUNT AS insert_count, r.UPDATE_COUNT AS update_count,
       r.DELETE_COUNT AS delete_count, r.REJECT_COUNT AS reject_count,
       r.WATERMARK_FROM AS watermark_from, r.WATERMARK_TO AS watermark_to,
       r.ERROR_MESSAGE AS error_message
FROM CFG_TASKS t
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
CROSS JOIN LATERAL (
    SELECT * FROM AUD_TASK_RUN_LOG
    WHERE TASK_ID = t.TASK_ID ORDER BY TASK_RUN_ID DESC LIMIT :limit
) r
JOIN AUD_PIPELINES_RUN_LOG pr ON pr.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID
WHERE t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y'
ORDER BY r.TASK_ID, r.PIPELINE_RUN_ID DESC
