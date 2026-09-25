-- Mark :pipeline_run_id BREACHED while it is still going, unless it already is.
UPDATE AUD_PIPELINES_RUN_LOG
SET SLA_STATUS = 'BREACHED'
WHERE PIPELINE_RUN_ID = :pipeline_run_id AND STATUS = 'IN-PROGRESS'
  AND COALESCE(SLA_STATUS, '') <> 'BREACHED'
