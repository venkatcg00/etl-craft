-- Each active pipeline's five newest runs, newest first; one index range per pipeline.
SELECT p.PIPELINE_ID AS pipeline_id, r.PIPELINE_RUN_ID AS pipeline_run_id,
       r.STATUS AS status, r.SLA_STATUS AS sla_status
FROM CFG_PIPELINES p
CROSS JOIN LATERAL (
    SELECT recent.PIPELINE_RUN_ID, recent.STATUS, recent.SLA_STATUS
    FROM AUD_PIPELINES_RUN_LOG recent
    WHERE recent.PIPELINE_ID = p.PIPELINE_ID
    ORDER BY recent.PIPELINE_RUN_ID DESC
    LIMIT 5
) r
WHERE p.ACTIVE_FLAG = 'Y'
ORDER BY p.PIPELINE_ID, r.PIPELINE_RUN_ID DESC
