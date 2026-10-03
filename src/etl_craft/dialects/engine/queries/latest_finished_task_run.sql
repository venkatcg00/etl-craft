-- The latest finished row of :task_id under a run that is not part of a backfill, and whether
-- it wrote rows.
SELECT t.TASK_RUN_ID AS run_id, t.STATUS AS status,
       CASE WHEN t.TARGET_COUNT > 0 THEN 1 ELSE 0 END AS has_data
FROM AUD_TASK_RUN_LOG t
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID = t.PIPELINE_RUN_ID
WHERE t.TASK_ID = :task_id AND t.STATUS IN ('SUCCESS', 'FAILED', 'SKIPPED', 'CANCELLED')
  AND r.BACKFILL = 'N'
ORDER BY t.TASK_RUN_ID DESC
LIMIT 1
