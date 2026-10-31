WITH eligible AS ({{permission_eligible}}), project_levels AS (
 SELECT p.PROJECT_ID AS project_id,p.PROJECT_CODE AS project_code,
 CASE WHEN :role='admin' THEN 4 ELSE COALESCE(MAX(CASE e.permission
 WHEN 'CAN_MANAGE' THEN 4 WHEN 'CAN_RUN' THEN 3 WHEN 'CAN_UPLOAD' THEN 2 WHEN 'CAN_VIEW' THEN 1 ELSE 0 END),0) END AS level
 FROM CFG_PROJECTS p LEFT JOIN eligible e ON e.project_id=p.PROJECT_ID
 AND e.pipeline_code IS NULL AND e.tag IS NULL
 WHERE p.ACTIVE_FLAG='Y' AND (CAST(:project_id AS bigint) IS NULL OR p.PROJECT_ID=:project_id)
 GROUP BY p.PROJECT_ID,p.PROJECT_CODE
), selected_projects AS (
 SELECT project_id,project_code,level FROM project_levels WHERE level>0
 AND (CAST(:after_project AS text) IS NULL OR project_code>:after_project)
 ORDER BY project_code LIMIT :project_limit
)
SELECT project_id,project_code,level FROM selected_projects
