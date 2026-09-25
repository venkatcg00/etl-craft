-- The active pipeline dependencies of :pipeline_id.
SELECT d.PIPELINE_DEPENDENCY_ID AS pipeline_dependency_id,
       d.DEPENDS_ON_PIPELINE_ID AS depends_on_pipeline_id, d.DEPENDENCY_TYPE AS dependency_type,
       p.PIPELINE_CODE AS depends_on_pipeline_code
FROM CFG_PIPELINE_DEPENDENCY d
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = d.DEPENDS_ON_PIPELINE_ID
WHERE d.PIPELINE_ID = :pipeline_id AND d.ACTIVE_FLAG = 'Y'
ORDER BY d.PIPELINE_DEPENDENCY_ID
