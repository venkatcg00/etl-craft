-- One pipeline's row, :pipeline_id.
SELECT PIPELINE_CODE AS pipeline_code, PIPELINE_NAME AS pipeline_name,
       DESCRIPTION AS description, RUN_SCHEDULE AS run_schedule,
       CREATE_DATE AS create_date, SCHEDULE_TIMEZONE AS schedule_timezone, SCHEDULE_START_DATE AS schedule_start_date,
       SLA_IN_HOURS AS sla_in_hours, REFRESH_TYPE AS refresh_type,
       CREATED_BY AS created_by, PIPELINE_PARAMETERS AS pipeline_parameters
FROM CFG_PIPELINES
WHERE PIPELINE_ID = :pipeline_id
