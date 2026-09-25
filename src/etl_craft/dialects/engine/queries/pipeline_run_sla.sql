-- When :pipeline_run_id started, and its SLA status so far.
SELECT START_DATE AS start_date, SLA_STATUS AS sla_status
FROM AUD_PIPELINES_RUN_LOG
WHERE PIPELINE_RUN_ID = :pipeline_run_id
