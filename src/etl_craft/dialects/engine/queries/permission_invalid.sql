WITH selected_projects AS ({{permission_projects}})
SELECT p.PIPELINE_CODE AS pipeline_code,p.PIPELINE_PARAMETERS AS pipeline_parameters
FROM CFG_PIPELINES p JOIN selected_projects pr ON pr.project_id=p.PROJECT_ID
WHERE p.PIPELINE_PARAMETERS IS NOT NULL AND (
 json_type(p.PIPELINE_PARAMETERS)<>'object' OR
 (json_type(p.PIPELINE_PARAMETERS,'$.TAGS') IS NOT NULL AND (
 json_type(p.PIPELINE_PARAMETERS,'$.TAGS')<>'array' OR
 json_array_length(p.PIPELINE_PARAMETERS,'$.TAGS')>20 OR EXISTS (
 SELECT 1 AS invalid FROM json_each(p.PIPELINE_PARAMETERS,'$.TAGS') t
 WHERE t.type<>'text' OR length(t.value) NOT BETWEEN 1 AND 64
 OR t.value GLOB '*[^a-z0-9_:-]*') OR
 (SELECT COUNT(*) AS count FROM json_each(p.PIPELINE_PARAMETERS,'$.TAGS'))<>
 (SELECT COUNT(DISTINCT value) AS count FROM json_each(p.PIPELINE_PARAMETERS,'$.TAGS')))))
LIMIT 1
