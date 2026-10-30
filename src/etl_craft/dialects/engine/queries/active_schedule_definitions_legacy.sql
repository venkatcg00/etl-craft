-- Schedule validation reads definitions without runtime admission or run-history state.
SELECT PIPELINE_CODE AS pipeline_code, RUN_SCHEDULE AS run_schedule,
       SCHEDULE_TIMEZONE AS schedule_timezone, SCHEDULE_START_DATE AS schedule_start_date,
       NULL AS calendar_id, 'SKIP' AS non_business_day
FROM CFG_PIPELINES WHERE ACTIVE_FLAG='Y'
ORDER BY PIPELINE_ID
