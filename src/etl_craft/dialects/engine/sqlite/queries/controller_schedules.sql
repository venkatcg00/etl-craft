-- Active schedule definitions and their latest durable tick, never entire run history.
SELECT p.PIPELINE_ID AS pipeline_id, p.PIPELINE_CODE AS pipeline_code,
       (SELECT pause.RESUMED_AT FROM AUD_PIPELINE_PAUSES pause WHERE pause.PIPELINE_ID=p.PIPELINE_ID AND pause.TASK_ID IS NULL AND pause.RESUMED_AT IS NOT NULL ORDER BY pause.RESUMED_AT DESC LIMIT 1) AS resumed_at,
       project.TIMEZONE AS project_timezone, CASE WHEN :refresh_required THEN json_extract(old.value,'$.run_schedule') ELSE p.RUN_SCHEDULE END AS run_schedule, CASE WHEN :refresh_required THEN json_extract(old.value,'$.schedule_timezone') ELSE p.SCHEDULE_TIMEZONE END AS schedule_timezone,
       CASE WHEN :refresh_required THEN json_extract(old.value,'$.catchup') ELSE p.CATCHUP END AS catchup, CASE WHEN :refresh_required THEN json_extract(old.value,'$.max_catchup_runs') ELSE p.MAX_CATCHUP_RUNS END AS max_catchup_runs,
       CASE WHEN :refresh_required THEN json_extract(old.value,'$.overlap_policy') ELSE p.OVERLAP_POLICY END AS overlap_policy, CASE WHEN :refresh_required THEN json_extract(old.value,'$.schedule_start_date') ELSE p.SCHEDULE_START_DATE END AS schedule_start_date,
       CASE WHEN :refresh_required THEN json_extract(old.value,'$.create_date') ELSE p.CREATE_DATE END AS create_date,
       CASE WHEN :refresh_required THEN json_extract(old.value,'$.calendar_id') ELSE p.CALENDAR_ID END AS calendar_id, CASE WHEN :refresh_required THEN json_extract(old.value,'$.non_business_day') ELSE p.NON_BUSINESS_DAY END AS non_business_day,
       (SELECT MAX(r.RUN_KEY) FROM AUD_PIPELINES_RUN_LOG r
        WHERE r.PIPELINE_ID=p.PIPELINE_ID AND r.TRIGGER_KIND='SCHEDULE') AS last_run_key,
       EXISTS (SELECT 1 FROM AUD_PIPELINES_RUN_LOG a WHERE a.PIPELINE_ID=p.PIPELINE_ID
               AND a.EXCLUSIVE_RUN='Y' AND a.STATUS IN ('QUEUED','IN-PROGRESS')) AS pending
FROM CFG_PIPELINES p JOIN CFG_PROJECTS project ON project.PROJECT_ID=p.PROJECT_ID

LEFT JOIN AUD_PIPELINE_REFRESHES good ON good.REFRESH_ID=(SELECT MAX(r.REFRESH_ID) FROM AUD_PIPELINE_REFRESHES r WHERE r.PIPELINE_ID=p.PIPELINE_ID AND r.STATUS='SUCCESS' AND :refresh_required)
LEFT JOIN AUD_PIPELINE_REFRESHES image ON image.REFRESH_ID=good.SNAPSHOT_REFRESH_ID
LEFT JOIN json_each(COALESCE(good.METADATA_SNAPSHOT,image.METADATA_SNAPSHOT),'$.CFG_PIPELINES.rows') old ON CAST(json_extract(old.value,'$.pipeline_id') AS bigint)=p.PIPELINE_ID
WHERE p.ACTIVE_FLAG='Y' AND project.ACTIVE_FLAG='Y'
AND (CAST(:pipeline_id AS bigint) IS NULL OR p.PIPELINE_ID=:pipeline_id)
AND (NOT :refresh_required OR (good.REFRESH_ID IS NOT NULL AND (p.BLOCK_ON_REFRESH_ERROR='N' OR (SELECT latest.STATUS FROM AUD_PIPELINE_REFRESHES latest WHERE latest.PIPELINE_ID=p.PIPELINE_ID ORDER BY latest.REFRESH_ID DESC LIMIT 1)='SUCCESS')))
ORDER BY p.PIPELINE_ID
