-- Every active pipeline, with its latest run.
SELECT p.PIPELINE_ID AS pipeline_id, p.PIPELINE_CODE AS pipeline_code,
       p.PIPELINE_NAME AS pipeline_name, p.DESCRIPTION AS description,
       p.RUN_SCHEDULE AS run_schedule, p.SLA_IN_HOURS AS sla_in_hours,
       p.REFRESH_TYPE AS refresh_type, r.PIPELINE_RUN_ID AS pipeline_run_id,
       r.STATUS AS run_status, r.START_DATE AS run_start, r.END_DATE AS run_end,
       r.SLA_STATUS AS sla_status
FROM CFG_PIPELINES p
LEFT JOIN (
    SELECT PIPELINE_ID, PIPELINE_RUN_ID, STATUS, START_DATE, END_DATE, SLA_STATUS,
           ROW_NUMBER() OVER (PARTITION BY PIPELINE_ID ORDER BY PIPELINE_RUN_ID DESC) AS rn
    FROM AUD_PIPELINES_RUN_LOG
) r ON r.PIPELINE_ID = p.PIPELINE_ID AND r.rn = 1
WHERE p.ACTIVE_FLAG = 'Y'
ORDER BY p.PIPELINE_CODE
