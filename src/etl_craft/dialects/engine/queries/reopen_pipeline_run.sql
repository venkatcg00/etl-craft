-- Put the finished :pipeline_run_id back IN-PROGRESS, with no END_DATE until it ends again.
UPDATE AUD_PIPELINES_RUN_LOG
SET STATUS = 'IN-PROGRESS', END_DATE = NULL
WHERE PIPELINE_RUN_ID = :pipeline_run_id AND STATUS <> 'IN-PROGRESS'
