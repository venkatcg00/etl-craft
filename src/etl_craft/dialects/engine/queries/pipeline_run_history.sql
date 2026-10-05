-- The latest :limit runs of :pipeline_id, newest first.
SELECT PIPELINE_RUN_ID AS pipeline_run_id, STATUS AS status, START_DATE AS start_date,
       END_DATE AS end_date, SLA_STATUS AS sla_status, RUN_DATE AS run_date,
       BACKFILL AS backfill, STARTED_BY AS started_by, STARTED_BY_KIND AS started_by_kind,
       ENDED_BY AS ended_by, ENDED_BY_KIND AS ended_by_kind
FROM AUD_PIPELINES_RUN_LOG
WHERE PIPELINE_ID = :pipeline_id
ORDER BY START_DATE DESC, PIPELINE_RUN_ID DESC
LIMIT :limit
