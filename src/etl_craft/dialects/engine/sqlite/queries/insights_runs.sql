-- Runs of the project's pipelines started in [:window_start, :window_end), grouped by pipeline,
-- UTC day, status and SLA result, with the durations of those that ended.
SELECT r.PIPELINE_ID AS pipeline_id, date(r.START_DATE) AS day,
       r.STATUS AS status, r.SLA_STATUS AS sla_status, COUNT(*) AS runs,
       COUNT(r.END_DATE) AS ended,
       SUM((julianday(r.END_DATE) - julianday(r.START_DATE)) * 86400.0) AS seconds,
       MAX((julianday(r.END_DATE) - julianday(r.START_DATE)) * 86400.0) AS longest
FROM CFG_PIPELINES p
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID = p.PIPELINE_ID
 AND r.START_DATE >= :window_start AND r.START_DATE < :window_end
WHERE p.PROJECT_ID = :project_id
GROUP BY r.PIPELINE_ID, date(r.START_DATE), r.STATUS, r.SLA_STATUS
