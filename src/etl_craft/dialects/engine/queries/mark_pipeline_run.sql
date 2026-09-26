-- Set :pipeline_run_id to :status, as an operator marked it, ending it now if it had not ended.
UPDATE AUD_PIPELINES_RUN_LOG
SET STATUS = :status, END_DATE = COALESCE(END_DATE, :now)
WHERE PIPELINE_RUN_ID = :pipeline_run_id
