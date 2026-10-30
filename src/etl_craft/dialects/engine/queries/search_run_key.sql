-- Runs of the project's pipelines with exactly this run key, newest first.
SELECT r.PIPELINE_RUN_ID AS pipeline_run_id, p.PIPELINE_CODE AS pipeline_code,
       r.STATUS AS status
FROM CFG_PIPELINES p
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID = p.PIPELINE_ID AND r.RUN_KEY = :run_key
WHERE p.PROJECT_ID = :project_id
ORDER BY r.PIPELINE_RUN_ID DESC
