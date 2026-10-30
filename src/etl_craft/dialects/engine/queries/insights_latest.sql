-- The newest runs of each of the project's pipelines started in the window.
SELECT r.PIPELINE_ID AS pipeline_id, r.PIPELINE_RUN_ID AS pipeline_run_id, r.STATUS AS status,
       r.SLA_STATUS AS sla_status, r.START_DATE AS start_date, r.END_DATE AS end_date,
       r.TRIGGER_KIND AS trigger_kind
FROM CFG_PIPELINES p
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID = p.PIPELINE_ID
WHERE p.PROJECT_ID = :project_id AND r.PIPELINE_RUN_ID IN (
    SELECT newest.PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG newest
    WHERE newest.PIPELINE_ID = p.PIPELINE_ID
      AND newest.START_DATE >= :window_start AND newest.START_DATE < :window_end
    ORDER BY newest.START_DATE DESC, newest.PIPELINE_RUN_ID DESC LIMIT :limit
)
