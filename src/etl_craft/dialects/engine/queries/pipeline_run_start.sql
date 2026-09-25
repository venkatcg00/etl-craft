-- When :pipeline_run_id started.
SELECT START_DATE AS start_date
FROM AUD_PIPELINES_RUN_LOG
WHERE PIPELINE_RUN_ID = :pipeline_run_id
