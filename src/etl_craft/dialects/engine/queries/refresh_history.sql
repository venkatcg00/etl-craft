WITH selected AS (
SELECT PIPELINE_ID AS pipeline_id,PIPELINE_CODE AS pipeline_code FROM CFG_PIPELINES
WHERE PROJECT_ID=:project_id AND (CAST(:pipeline_code AS varchar) IS NULL OR PIPELINE_CODE=:pipeline_code)
), records AS (
SELECT source.*,
LAG(METADATA_VERSION) OVER (PARTITION BY PIPELINE_ID ORDER BY REFRESH_ID) AS previous_version,
LAG(BUNDLE_ID) OVER (PARTITION BY PIPELINE_ID ORDER BY REFRESH_ID) AS previous_bundle,
LAG(REFRESHED_AT) OVER (PARTITION BY PIPELINE_ID ORDER BY REFRESH_ID) AS previous_time,
LAG(STATUS) OVER (PARTITION BY PIPELINE_ID ORDER BY REFRESH_ID) AS previous_status
FROM AUD_PIPELINE_REFRESHES source
WHERE PROJECT_ID=:project_id AND PIPELINE_ID IN (SELECT pipeline_id FROM selected)
)
SELECT r.REFRESH_ID AS refresh_id,r.PIPELINE_ID AS pipeline_id,p.pipeline_code AS pipeline_code,
r.REFRESHED_AT AS refreshed_at,r.ACTOR AS actor,r.STATUS AS status,
r.METADATA_VERSION AS metadata_version,r.BUNDLE_ID AS bundle_id,r.FINDINGS AS findings,
r.previous_version AS previous_version,r.previous_bundle AS previous_bundle,
r.previous_time AS previous_time,r.previous_status AS previous_status
FROM records r JOIN selected p ON p.pipeline_id=r.PIPELINE_ID
ORDER BY r.REFRESH_ID DESC
