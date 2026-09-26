-- The upstream task run :dependency_id, a task's dependency on another pipeline's task, last
-- consumed: its latest log row.
SELECT CONSUMED_TASK_RUN_ID AS last_consumed
FROM AUD_DEPENDENCY_CONSUMPTION
WHERE TASK_DEPENDENCY_ID = :dependency_id
ORDER BY CONSUMPTION_ID DESC
LIMIT 1
