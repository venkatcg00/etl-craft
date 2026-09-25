-- The upstream task run :task_dependency_id last consumed.
SELECT LAST_CONSUMED_TASK_RUN_ID AS last_consumed
FROM AUD_TASK_DEPENDENCY_TRACKER
WHERE TASK_DEPENDENCY_ID = :dependency_id
