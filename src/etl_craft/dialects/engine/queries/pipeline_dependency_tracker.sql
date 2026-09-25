-- The upstream run :pipeline_dependency_id last consumed.
SELECT LAST_CONSUMED_PIPELINE_RUN_ID AS last_consumed
FROM AUD_PIPELINE_DEPENDENCY_TRACKER
WHERE PIPELINE_DEPENDENCY_ID = :dependency_id
