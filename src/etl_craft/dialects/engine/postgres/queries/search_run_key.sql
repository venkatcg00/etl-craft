-- Runs with exactly this run key: one unique (PIPELINE_ID, RUN_KEY) probe per project pipeline.
SELECT r.pipeline_run_id AS pipeline_run_id, p.PIPELINE_CODE AS pipeline_code,
       r.status AS status
FROM CFG_PIPELINES p
CROSS JOIN LATERAL (
    SELECT keyed.PIPELINE_RUN_ID AS pipeline_run_id, keyed.STATUS AS status
    FROM AUD_PIPELINES_RUN_LOG keyed
    WHERE keyed.PIPELINE_ID = p.PIPELINE_ID AND keyed.RUN_KEY = :run_key
    LIMIT 1
) r
WHERE p.PROJECT_ID = :project_id
ORDER BY r.pipeline_run_id DESC
