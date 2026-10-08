-- The latest row of :task_id, in any status, under a run that is not part of a backfill: the
-- task run a gate on the task waits for.
SELECT t.TASK_RUN_ID AS run_id, t.STATUS AS status, t.START_DATE AS start_date
FROM AUD_TASK_RUN_LOG t
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
WHERE t.TASK_ID = :task_id AND r.BACKFILL = 'N'
ORDER BY t.TASK_RUN_ID DESC
LIMIT 1
