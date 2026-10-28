INSERT INTO AUD_PIPELINE_REFRESHES
(PROJECT_ID,PIPELINE_ID,REFRESHED_AT,ACTOR,ACTOR_KIND,STATUS,METADATA_VERSION,METADATA_SNAPSHOT,SNAPSHOT_REFRESH_ID,BUNDLE_ID,FINDINGS)
VALUES (:project_id,:pipeline_id,:now,:actor,:actor_kind,:status,:metadata_version,:snapshot,:snapshot_refresh_id,:bundle_id,:findings)
RETURNING REFRESH_ID AS refresh_id
