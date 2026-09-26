-- The date :pipeline_run_id runs as of, and whether it is part of a backfill.
SELECT RUN_DATE AS run_date, BACKFILL AS backfill
FROM AUD_PIPELINES_RUN_LOG
WHERE PIPELINE_RUN_ID = :pipeline_run_id
