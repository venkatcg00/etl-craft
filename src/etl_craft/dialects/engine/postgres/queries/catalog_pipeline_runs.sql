-- Indexed top-N history for each active definition.
SELECT p.PIPELINE_CODE AS pipeline_code, r.PIPELINE_RUN_ID AS pipeline_run_id,
       r.STATUS AS status,
       CASE WHEN r.OUTPUT_REVISION > 1 OR r.REPAIR_PENDING = 'Y' THEN 'REPAIR'
            WHEN r.BACKFILL = 'Y' THEN 'BACKFILL'
            WHEN r.TRIGGER_KIND = 'SCHEDULE' THEN 'SCHEDULED' ELSE 'OTHER' END AS run_kind, r.RUN_DATE AS run_date, (r.BACKFILL = 'Y') AS backfill,
       r.START_DATE AS "start", r.END_DATE AS "end", r.SLA_STATUS AS sla_status, r.STARTED_BY AS started_by,
       r.STARTED_BY_KIND AS started_by_kind, r.ENDED_BY AS ended_by,
       r.ENDED_BY_KIND AS ended_by_kind
FROM CFG_PIPELINES p
CROSS JOIN LATERAL (
    SELECT * FROM AUD_PIPELINES_RUN_LOG
    WHERE PIPELINE_ID = p.PIPELINE_ID ORDER BY PIPELINE_RUN_ID DESC LIMIT :limit
) r
WHERE p.ACTIVE_FLAG = 'Y'
ORDER BY p.PIPELINE_CODE, r.PIPELINE_RUN_ID DESC
