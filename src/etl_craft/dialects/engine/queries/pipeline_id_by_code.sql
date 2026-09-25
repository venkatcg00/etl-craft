-- The active pipeline with :pipeline_code.
SELECT PIPELINE_ID AS pipeline_id
FROM CFG_PIPELINES
WHERE PIPELINE_CODE = :pipeline_code AND ACTIVE_FLAG = 'Y'
