-- Indexed top-N history for each active definition.
SELECT p.PIPELINE_CODE AS pipeline_code, r.PIPELINE_RUN_ID AS pipeline_run_id,
       r.BUNDLE_ID AS bundle_id, r.METADATA_VERSION AS metadata_version,
       r.STATUS AS status,
       COALESCE((SELECT i.FROM_STATUS FROM AUD_RUN_INTERVENTIONS i
        WHERE i.PIPELINE_RUN_ID=r.PIPELINE_RUN_ID AND i.TASK_ID IS NULL
        AND i.FROM_STATUS IN ('SUCCESS','FAILED','CANCELLED')
        ORDER BY i.INTERVENTION_ID LIMIT 1), r.STATUS) AS first_status,
       CASE WHEN r.BACKFILL = 'Y' THEN 'BACKFILL'
            WHEN r.TRIGGER_KIND = 'SCHEDULE' THEN 'SCHEDULED' ELSE 'OTHER' END AS run_kind, r.RUN_DATE AS run_date, (r.BACKFILL = 'Y') AS backfill,
       r.START_DATE AS "start", r.END_DATE AS "end", r.SLA_STATUS AS sla_status, r.STARTED_BY AS started_by,
       r.STARTED_BY_KIND AS started_by_kind, r.ENDED_BY AS ended_by,
       r.ENDED_BY_KIND AS ended_by_kind
FROM CFG_PIPELINES p
CROSS JOIN LATERAL (
    SELECT * FROM AUD_PIPELINES_RUN_LOG
    WHERE PIPELINE_ID = p.PIPELINE_ID ORDER BY PIPELINE_RUN_ID DESC LIMIT :limit
) r
WHERE p.ACTIVE_FLAG = 'Y' AND p.PROJECT_ID=:project_id
ORDER BY p.PIPELINE_CODE, r.PIPELINE_RUN_ID DESC
