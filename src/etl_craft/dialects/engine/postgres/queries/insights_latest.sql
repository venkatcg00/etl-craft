-- The newest runs of each of the project's pipelines started in the window: one bounded index
-- range of (PIPELINE_ID, START_DATE) per pipeline.
SELECT p.PIPELINE_ID AS pipeline_id, r.pipeline_run_id AS pipeline_run_id, r.status AS status,
       r.sla_status AS sla_status, r.start_date AS start_date, r.end_date AS end_date,
       r.trigger_kind AS trigger_kind
FROM CFG_PIPELINES p
CROSS JOIN LATERAL (
    SELECT newest.PIPELINE_RUN_ID AS pipeline_run_id, newest.STATUS AS status,
           newest.SLA_STATUS AS sla_status, newest.START_DATE AS start_date,
           newest.END_DATE AS end_date, newest.TRIGGER_KIND AS trigger_kind
    FROM AUD_PIPELINES_RUN_LOG newest
    WHERE newest.PIPELINE_ID = p.PIPELINE_ID
      AND newest.START_DATE >= :window_start AND newest.START_DATE < :window_end
    ORDER BY newest.START_DATE DESC LIMIT :limit
) r
WHERE p.PROJECT_ID = :project_id
