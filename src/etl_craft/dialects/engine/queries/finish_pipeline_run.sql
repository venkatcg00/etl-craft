-- End :pipeline_run_id with :status and END_DATE :now; :sla_status is kept only when set, so a
-- run judged without SLA enforcement keeps what it had.
UPDATE AUD_PIPELINES_RUN_LOG
SET STATUS = :status, END_DATE = :now, SLA_STATUS = COALESCE(:sla_status, SLA_STATUS)
WHERE PIPELINE_RUN_ID = :pipeline_run_id
