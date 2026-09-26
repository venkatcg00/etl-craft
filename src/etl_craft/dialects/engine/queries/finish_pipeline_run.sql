-- End :pipeline_run_id with :status and END_DATE :now, and :sla_status when the pipeline has an
-- SLA; a pipeline without one keeps what it had. A run an operator cancelled stays CANCELLED.
UPDATE AUD_PIPELINES_RUN_LOG
SET STATUS = :status, END_DATE = :now, SLA_STATUS = COALESCE(:sla_status, SLA_STATUS)
WHERE PIPELINE_RUN_ID = :pipeline_run_id AND STATUS <> 'CANCELLED'
