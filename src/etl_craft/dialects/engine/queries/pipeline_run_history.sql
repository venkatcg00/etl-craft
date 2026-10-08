-- Bounded pipeline history, newest first.
SELECT p.PIPELINE_ID AS pipeline_id, p.PIPELINE_CODE AS pipeline_code,
       r.PIPELINE_RUN_ID AS pipeline_run_id, r.RUN_KEY AS run_key,
       r.TRIGGER_KIND AS trigger_kind, r.RUN_DATE AS run_date, r.STATUS AS status,
       r.START_DATE AS start_date, r.END_DATE AS end_date, r.SLA_STATUS AS sla_status,
       r.BACKFILL AS backfill, r.STARTED_BY AS started_by, r.STARTED_BY_KIND AS started_by_kind,
       r.ENDED_BY AS ended_by, r.ENDED_BY_KIND AS ended_by_kind
FROM AUD_PIPELINES_RUN_LOG r
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = r.PIPELINE_ID
WHERE r.PIPELINE_ID = :pipeline_id
AND (CAST(:run_id AS bigint) IS NULL OR r.PIPELINE_RUN_ID = :run_id)
ORDER BY r.START_DATE DESC, r.PIPELINE_RUN_ID DESC
LIMIT :limit
