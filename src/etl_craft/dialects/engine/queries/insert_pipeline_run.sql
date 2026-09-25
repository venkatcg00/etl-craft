-- Start a run of :pipeline_id. The unique index on IN-PROGRESS runs refuses a
-- second one.
INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS)
VALUES (:pipeline_id, 'IN-PROGRESS')
RETURNING PIPELINE_RUN_ID AS pipeline_run_id
