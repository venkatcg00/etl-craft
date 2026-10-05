-- End :pipeline_run_id with :status and END_DATE :now, only while it is still :from_status.
UPDATE AUD_PIPELINES_RUN_LOG
SET STATUS = :status, END_DATE = :now, ENDED_BY = :ended_by, ENDED_BY_KIND = :ended_by_kind
WHERE PIPELINE_RUN_ID = :pipeline_run_id AND STATUS = :from_status
