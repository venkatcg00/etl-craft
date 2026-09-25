-- The active pipeline dependencies of :pipeline_id.
SELECT PIPELINE_DEPENDENCY_ID AS pipeline_dependency_id,
       DEPENDS_ON_PIPELINE_ID AS depends_on_pipeline_id, DEPENDENCY_TYPE AS dependency_type
FROM CFG_PIPELINE_DEPENDENCY
WHERE PIPELINE_ID = :pipeline_id AND ACTIVE_FLAG = 'Y'
ORDER BY PIPELINE_DEPENDENCY_ID
