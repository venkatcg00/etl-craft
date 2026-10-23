-- The task's definition and admitted run, without resolving by recency.
SELECT l.TASK_ID AS task_id, l.PIPELINE_RUN_ID AS pipeline_run_id,
       l.ATTEMPT_COUNT AS attempt_count, l.STATUS AS status, r.WRITER AS writer,
       t.HANDLER AS handler, t.TASK_CODE AS task_code, t.PIPELINE_ID AS pipeline_id,
       p.PIPELINE_CODE AS pipeline_code, p.REFRESH_TYPE AS refresh_type,
       r.RUN_DATE AS run_date, r.BACKFILL AS backfill,
       r.TASK_SELECTION AS task_selection, r.EXCLUSIVE_RUN AS exclusive_run
FROM AUD_TASK_RUN_LOG l
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID = l.PIPELINE_RUN_ID
JOIN CFG_TASKS t ON t.TASK_ID=l.TASK_ID
JOIN CFG_PIPELINES p ON p.PIPELINE_ID=t.PIPELINE_ID
WHERE l.TASK_RUN_ID = :task_run_id
