INSERT INTO AUD_PIPELINES_RUN_LOG
(PIPELINE_ID, STATUS, RUN_DATE, BACKFILL, RUN_KEY, TRIGGER_KIND, STARTED_BY, STARTED_BY_KIND)
VALUES (:pipeline_id, 'IN-PROGRESS', :run_date, :backfill, :run_key, :trigger_kind,
:actor, :actor_kind) RETURNING PIPELINE_RUN_ID AS pipeline_run_id
