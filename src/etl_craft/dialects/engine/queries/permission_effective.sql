WITH eligible AS ({{permission_eligible}}), selected_projects AS ({{permission_projects}}), applicable AS (
 SELECT p.PIPELINE_ID AS pipeline_id,x.PIPELINE_GRANT_ID AS grant_id
 FROM CFG_PIPELINES p JOIN selected_projects pr ON pr.project_id=p.PROJECT_ID
 JOIN CFG_PIPELINE_GRANTS x ON x.PROJECT_ID=p.PROJECT_ID AND x.ACTIVE_FLAG='Y'
 AND (x.PIPELINE_CODE=p.PIPELINE_CODE OR x.TAG IN (
 SELECT value AS tag FROM json_each(COALESCE(p.PIPELINE_PARAMETERS,'{}'),'$.TAGS')))
), pipeline_levels AS (
 SELECT p.PIPELINE_ID AS pipeline_id,p.PIPELINE_CODE AS pipeline_code,pr.project_id,pr.project_code,
 CASE WHEN pr.level=4 THEN 4 WHEN NOT EXISTS (
 SELECT 1 AS present FROM applicable x WHERE x.pipeline_id=p.PIPELINE_ID)
 THEN pr.level ELSE COALESCE((SELECT MAX(CASE e.permission
 WHEN 'CAN_MANAGE' THEN 4 WHEN 'CAN_RUN' THEN 3 WHEN 'CAN_UPLOAD' THEN 2 WHEN 'CAN_VIEW' THEN 1 ELSE 0 END)
 FROM applicable x JOIN eligible e ON e.grant_id=x.grant_id
 WHERE x.pipeline_id=p.PIPELINE_ID),0) END AS level,
 CASE WHEN pr.level=4 OR NOT EXISTS (
 SELECT 1 AS present FROM applicable x WHERE x.pipeline_id=p.PIPELINE_ID)
 THEN json_array('project:' || CASE pr.level WHEN 4 THEN 'CAN_MANAGE' WHEN 3 THEN 'CAN_RUN' WHEN 2 THEN 'CAN_UPLOAD' ELSE 'CAN_VIEW' END)
 ELSE COALESCE((SELECT json_group_array(e.source) AS sources
 FROM applicable x JOIN eligible e ON e.grant_id=x.grant_id
 WHERE x.pipeline_id=p.PIPELINE_ID),'[]') END AS sources
 FROM CFG_PIPELINES p JOIN selected_projects pr ON pr.project_id=p.PROJECT_ID
)
SELECT project_id,project_code,NULL AS pipeline_id,NULL AS pipeline_code,level,'[]' AS sources
FROM selected_projects
UNION ALL SELECT project_id,project_code,pipeline_id,pipeline_code,level,sources FROM pipeline_levels
