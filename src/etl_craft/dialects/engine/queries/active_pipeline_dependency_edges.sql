-- Every active dependency of an active pipeline on another, and whether the upstream is active.
SELECT CASE WHEN p.PROJECT_ID=:project_id THEN p.PIPELINE_CODE ELSE project.PROJECT_CODE || '.' || p.PIPELINE_CODE END AS pipeline_code, CASE WHEN u.PROJECT_ID=:project_id THEN u.PIPELINE_CODE ELSE upstream.PROJECT_CODE || '.' || u.PIPELINE_CODE END AS depends_on_pipeline_code,
       d.DEPENDENCY_TYPE AS dependency_type, (u.ACTIVE_FLAG = 'Y') AS depends_on_pipeline_active
FROM CFG_PIPELINE_DEPENDENCY d
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = d.PIPELINE_ID
JOIN CFG_PIPELINES u ON u.PIPELINE_ID = d.DEPENDS_ON_PIPELINE_ID
JOIN CFG_PROJECTS project ON project.PROJECT_ID=p.PROJECT_ID
JOIN CFG_PROJECTS upstream ON upstream.PROJECT_ID=u.PROJECT_ID
WHERE d.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y'
ORDER BY p.PIPELINE_CODE, u.PIPELINE_CODE
