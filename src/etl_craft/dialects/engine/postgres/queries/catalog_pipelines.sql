-- Every active pipeline, with its latest run.
SELECT p.PIPELINE_ID AS pipeline_id, p.PIPELINE_CODE AS pipeline_code, p.PIPELINE_PARAMETERS AS pipeline_parameters,
       p.PIPELINE_NAME AS pipeline_name, p.DESCRIPTION AS description,
       p.RUN_SCHEDULE AS run_schedule, p.SLA_IN_HOURS AS sla_in_hours,
       p.REFRESH_TYPE AS refresh_type, r.PIPELINE_RUN_ID AS pipeline_run_id,
       r.STATUS AS run_status, r.START_DATE AS run_start, r.END_DATE AS run_end,
       r.SLA_STATUS AS sla_status
FROM CFG_PIPELINES p
LEFT JOIN LATERAL (
    SELECT * FROM AUD_PIPELINES_RUN_LOG
    WHERE PIPELINE_ID = p.PIPELINE_ID
    ORDER BY PIPELINE_RUN_ID DESC LIMIT 1
) r ON TRUE
WHERE p.ACTIVE_FLAG = 'Y' AND p.PROJECT_ID=:project_id
ORDER BY p.PIPELINE_CODE
