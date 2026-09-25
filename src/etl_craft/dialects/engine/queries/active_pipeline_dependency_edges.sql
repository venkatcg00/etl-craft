-- Every active dependency of an active pipeline on another, and whether the upstream is active.
SELECT p.PIPELINE_CODE AS pipeline_code, u.PIPELINE_CODE AS depends_on_pipeline_code,
       d.DEPENDENCY_TYPE AS dependency_type, u.ACTIVE_FLAG AS depends_on_pipeline_active
FROM CFG_PIPELINE_DEPENDENCY d
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = d.PIPELINE_ID
JOIN CFG_PIPELINES u ON u.PIPELINE_ID = d.DEPENDS_ON_PIPELINE_ID
WHERE d.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y'
ORDER BY p.PIPELINE_CODE, u.PIPELINE_CODE
