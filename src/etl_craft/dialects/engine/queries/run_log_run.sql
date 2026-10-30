-- The run's own record: how it started, how it ended and its SLA outcome.
SELECT r.PIPELINE_RUN_ID AS pipeline_run_id, p.PIPELINE_CODE AS pipeline_code,
       r.START_DATE AS start_date, r.END_DATE AS end_date, r.STATUS AS status,
       r.SLA_STATUS AS sla_status, r.SLA_BREACHED_AT AS sla_breached_at,
       r.RUN_DATE AS run_date, r.RUN_KEY AS run_key, r.TRIGGER_KIND AS trigger_kind,
       r.BACKFILL AS backfill, r.STARTED_BY AS started_by, r.STARTED_BY_KIND AS started_by_kind,
       r.ENDED_BY AS ended_by, r.ENDED_BY_KIND AS ended_by_kind
FROM AUD_PIPELINES_RUN_LOG r JOIN CFG_PIPELINES p ON p.PIPELINE_ID = r.PIPELINE_ID
WHERE r.PIPELINE_RUN_ID = :pipeline_run_id
