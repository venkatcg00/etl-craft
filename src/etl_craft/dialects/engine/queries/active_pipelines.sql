-- Every active pipeline, by code.
SELECT PIPELINE_CODE AS pipeline_code, PIPELINE_NAME AS pipeline_name,
       REFRESH_TYPE AS refresh_type, RUN_SCHEDULE AS run_schedule, SLA_IN_HOURS AS sla_in_hours
FROM CFG_PIPELINES
WHERE ACTIVE_FLAG = 'Y'
ORDER BY PIPELINE_CODE
