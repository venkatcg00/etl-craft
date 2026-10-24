-- The active pipeline dependencies of :pipeline_id.
SELECT d.PIPELINE_DEPENDENCY_ID AS pipeline_dependency_id,
       d.DEPENDS_ON_PIPELINE_ID AS depends_on_pipeline_id, d.DEPENDENCY_TYPE AS dependency_type, (d.CONSUME_REPAIRS = 'Y') AS consume_repairs,
       CASE WHEN p.PROJECT_ID=:project_id THEN p.PIPELINE_CODE ELSE project.PROJECT_CODE || '.' || p.PIPELINE_CODE END AS depends_on_pipeline_code
FROM CFG_PIPELINE_DEPENDENCY d
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = d.DEPENDS_ON_PIPELINE_ID
JOIN CFG_PROJECTS project ON project.PROJECT_ID=p.PROJECT_ID
WHERE d.PIPELINE_ID = :pipeline_id AND d.ACTIVE_FLAG = 'Y'
ORDER BY d.PIPELINE_DEPENDENCY_ID
