WITH selected_projects AS ({{permission_projects}})
SELECT p.PIPELINE_CODE AS pipeline_code, p.PIPELINE_PARAMETERS AS pipeline_parameters
FROM CFG_PIPELINES p JOIN selected_projects pr ON pr.project_id=p.PROJECT_ID
WHERE p.PIPELINE_PARAMETERS IS NOT NULL AND (
 jsonb_typeof(CAST(p.PIPELINE_PARAMETERS AS jsonb))<>'object' OR
 (CAST(p.PIPELINE_PARAMETERS AS jsonb) ? 'TAGS' AND (
 jsonb_typeof(CAST(p.PIPELINE_PARAMETERS AS jsonb)->'TAGS')<>'array' OR
 jsonb_array_length(CASE WHEN jsonb_typeof(CAST(p.PIPELINE_PARAMETERS AS jsonb)->'TAGS')='array' THEN CAST(p.PIPELINE_PARAMETERS AS jsonb)->'TAGS' ELSE '[]'::jsonb END)>20 OR EXISTS (
 SELECT 1 AS invalid FROM jsonb_array_elements(CASE WHEN jsonb_typeof(CAST(p.PIPELINE_PARAMETERS AS jsonb)->'TAGS')='array' THEN CAST(p.PIPELINE_PARAMETERS AS jsonb)->'TAGS' ELSE '[]'::jsonb END) t(value)
 WHERE jsonb_typeof(t.value)<>'string' OR (t.value #>> '{}') !~ '^[a-z0-9_:-]{1,64}$') OR
 (SELECT COUNT(*) AS count FROM jsonb_array_elements(CASE WHEN jsonb_typeof(CAST(p.PIPELINE_PARAMETERS AS jsonb)->'TAGS')='array' THEN CAST(p.PIPELINE_PARAMETERS AS jsonb)->'TAGS' ELSE '[]'::jsonb END))<>
 (SELECT COUNT(DISTINCT value) AS count FROM jsonb_array_elements(CASE WHEN jsonb_typeof(CAST(p.PIPELINE_PARAMETERS AS jsonb)->'TAGS')='array' THEN CAST(p.PIPELINE_PARAMETERS AS jsonb)->'TAGS' ELSE '[]'::jsonb END)))))
LIMIT 1
