-- Active schedule definitions and their latest durable tick, never entire run history.
SELECT p.PIPELINE_ID AS pipeline_id, p.PIPELINE_CODE AS pipeline_code,
       p.RUN_SCHEDULE AS run_schedule, p.SCHEDULE_TIMEZONE AS schedule_timezone,
       p.CATCHUP AS catchup, p.MAX_CATCHUP_RUNS AS max_catchup_runs,
       p.OVERLAP_POLICY AS overlap_policy, p.SCHEDULE_START_DATE AS schedule_start_date,
       p.CREATE_DATE AS create_date,
       (SELECT MAX(r.RUN_KEY) FROM AUD_PIPELINES_RUN_LOG r
        WHERE r.PIPELINE_ID=p.PIPELINE_ID AND r.TRIGGER_KIND='SCHEDULE') AS last_run_key,
       EXISTS (SELECT 1 FROM AUD_PIPELINES_RUN_LOG a WHERE a.PIPELINE_ID=p.PIPELINE_ID
               AND a.STATUS IN ('QUEUED','IN-PROGRESS')) AS pending
FROM CFG_PIPELINES p WHERE p.ACTIVE_FLAG='Y'
ORDER BY p.PIPELINE_ID
