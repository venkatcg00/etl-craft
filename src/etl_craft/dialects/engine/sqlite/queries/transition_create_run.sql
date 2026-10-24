WITH image AS ( {{metadata_capture}} ),
bundle AS (SELECT b.BUNDLE_ID AS bundle_id FROM AUD_PROJECT_BUNDLES b JOIN CFG_PIPELINES p ON p.PROJECT_ID=b.PROJECT_ID WHERE p.PIPELINE_ID=:pipeline_id AND b.CONNECTIONS_APPROVED_FLAG='Y' ORDER BY b.CREATED_AT DESC,b.BUNDLE_ID DESC LIMIT 1)
INSERT INTO AUD_PIPELINES_RUN_LOG
(PIPELINE_ID, STATUS, RUN_DATE, BACKFILL, RUN_KEY, TRIGGER_KIND, STARTED_BY, STARTED_BY_KIND,
END_DATE, ENDED_BY, ENDED_BY_KIND, SSO_GROUP_IDS, BUNDLE_ID, METADATA_VERSION, METADATA_SNAPSHOT)
SELECT :pipeline_id AS pipeline_id,:status AS status,:run_date AS run_date,:backfill AS backfill,
:run_key AS run_key,:trigger_kind AS trigger_kind,:actor AS actor,:actor_kind AS actor_kind,
:end_date AS end_date,:ended_by AS ended_by,:ended_by_kind AS ended_by_kind,:sso_group_ids AS sso_group_ids,
CASE WHEN :use_bundles THEN (SELECT bundle_id FROM bundle) ELSE NULL END AS bundle_id,
etl_craft_sha256(snapshot) AS metadata_version,CAST(snapshot AS text) AS metadata_snapshot
FROM image WHERE NOT :use_bundles OR EXISTS (SELECT 1 AS found FROM bundle) OR :status='SKIPPED'
RETURNING PIPELINE_RUN_ID AS pipeline_run_id
