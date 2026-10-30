-- Each active pipeline's five newest runs, newest first; one index range per pipeline.
SELECT r.PIPELINE_ID AS pipeline_id, r.PIPELINE_RUN_ID AS pipeline_run_id,
       r.STATUS AS status, r.SLA_STATUS AS sla_status
FROM CFG_PIPELINES p
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID = p.PIPELINE_ID
WHERE p.ACTIVE_FLAG = 'Y'
AND r.PIPELINE_RUN_ID IN (
    SELECT recent.PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG recent
    WHERE recent.PIPELINE_ID = p.PIPELINE_ID
    ORDER BY recent.PIPELINE_RUN_ID DESC
    LIMIT 5
)
ORDER BY r.PIPELINE_ID, r.PIPELINE_RUN_ID DESC
