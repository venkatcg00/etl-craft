-- End the IN-PROGRESS :pipeline_run_id with :status and END_DATE :now. The SLA is decided once:
-- an SLA_STATUS already recorded (by the SLA watcher, or before the run was reopened) is kept,
-- and a pipeline without an SLA passes NULL. A run that is no longer IN-PROGRESS (cancelled by
-- an operator meanwhile, say) is left as it is.
UPDATE AUD_PIPELINES_RUN_LOG
SET STATUS = :status, END_DATE = :now, SLA_STATUS = COALESCE(SLA_STATUS, :sla_status)
WHERE PIPELINE_RUN_ID = :pipeline_run_id AND STATUS = 'IN-PROGRESS'
