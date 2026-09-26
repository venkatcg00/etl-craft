-- Start a run of :pipeline_id as of :run_date, part of a backfill when :backfill is 'Y'. The
-- unique index on IN-PROGRESS runs refuses a second one.
INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS, RUN_DATE, BACKFILL)
VALUES (:pipeline_id, 'IN-PROGRESS', :run_date, :backfill)
RETURNING PIPELINE_RUN_ID AS pipeline_run_id
