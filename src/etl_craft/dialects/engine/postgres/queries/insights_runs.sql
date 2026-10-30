-- Runs of the project's pipelines started in [:window_start, :window_end), grouped by pipeline,
-- UTC day, status and SLA result, with the durations of those that ended.
SELECT r.PIPELINE_ID AS pipeline_id, (r.START_DATE AT TIME ZONE 'UTC')::date AS day,
       r.STATUS AS status, r.SLA_STATUS AS sla_status, COUNT(*) AS runs,
       COUNT(r.END_DATE) AS ended,
       SUM(EXTRACT(EPOCH FROM (r.END_DATE - r.START_DATE))) AS seconds,
       MAX(EXTRACT(EPOCH FROM (r.END_DATE - r.START_DATE))) AS longest
FROM CFG_PIPELINES p
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID = p.PIPELINE_ID
 AND r.START_DATE >= :window_start AND r.START_DATE < :window_end
WHERE p.PROJECT_ID = :project_id
GROUP BY r.PIPELINE_ID, (r.START_DATE AT TIME ZONE 'UTC')::date, r.STATUS, r.SLA_STATUS
