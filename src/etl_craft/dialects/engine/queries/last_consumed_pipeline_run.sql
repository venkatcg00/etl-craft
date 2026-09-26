-- The upstream run :dependency_id, a pipeline dependency, last consumed: its latest log row.
SELECT CONSUMED_PIPELINE_RUN_ID AS last_consumed
FROM AUD_DEPENDENCY_CONSUMPTION
WHERE PIPELINE_DEPENDENCY_ID = :dependency_id
ORDER BY CONSUMPTION_ID DESC
LIMIT 1
