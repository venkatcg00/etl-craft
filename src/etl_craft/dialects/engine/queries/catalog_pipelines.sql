-- Every active pipeline, with its latest run.
SELECT p.PIPELINE_ID AS pipeline_id, p.PIPELINE_CODE AS pipeline_code,
       p.PIPELINE_NAME AS pipeline_name, p.DESCRIPTION AS description,
       p.RUN_SCHEDULE AS run_schedule, p.SLA_IN_HOURS AS sla_in_hours,
       p.REFRESH_TYPE AS refresh_type, r.PIPELINE_RUN_ID AS pipeline_run_id,
       r.STATUS AS run_status, r.START_DATE AS run_start, r.END_DATE AS run_end,
       r.SLA_STATUS AS sla_status
FROM CFG_PIPELINES p
LEFT JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID = (
    SELECT MAX(recent.PIPELINE_RUN_ID) FROM AUD_PIPELINES_RUN_LOG recent
    WHERE recent.PIPELINE_ID = p.PIPELINE_ID
)
WHERE p.ACTIVE_FLAG = 'Y'
ORDER BY p.PIPELINE_CODE
