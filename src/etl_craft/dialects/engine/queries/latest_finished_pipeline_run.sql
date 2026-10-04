-- The latest run of :pipeline_id that finished by :ended_by and is not part of a backfill, and
-- whether any of its tasks wrote rows (see latest_finished_task_run.sql).
SELECT r.PIPELINE_RUN_ID AS run_id, r.STATUS AS status,
       CASE WHEN EXISTS (
           SELECT 1 FROM AUD_TASK_RUN_LOG t
           WHERE t.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID
             AND COALESCE(t.ROWS_WRITTEN, t.TARGET_COUNT) > 0
       ) THEN 1 ELSE 0 END AS has_data
FROM AUD_PIPELINES_RUN_LOG r
WHERE r.PIPELINE_ID = :pipeline_id
  AND r.STATUS IN ('SUCCESS', 'FAILED', 'SKIPPED', 'CANCELLED')
  AND r.END_DATE <= :ended_by
  AND r.BACKFILL = 'N'
ORDER BY r.PIPELINE_RUN_ID DESC
LIMIT 1
