-- Successful task runs of runs started in the window, per pipeline and task: each handler count
-- summed with how many runs reported it, so an unreported count stays "not measured".
SELECT r.PIPELINE_ID AS pipeline_id, k.TASK_CODE AS task_code, COUNT(*) AS runs,
       SUM(t.SOURCE_COUNT) AS source_rows, COUNT(t.SOURCE_COUNT) AS source_measured,
       SUM(t.TARGET_COUNT) AS target_rows, COUNT(t.TARGET_COUNT) AS target_measured,
       SUM(t.INSERT_COUNT) AS inserted, COUNT(t.INSERT_COUNT) AS insert_measured,
       SUM(t.UPDATE_COUNT) AS updated, COUNT(t.UPDATE_COUNT) AS update_measured,
       SUM(t.DELETE_COUNT) AS deleted, COUNT(t.DELETE_COUNT) AS delete_measured,
       SUM(t.REJECT_COUNT) AS rejected, COUNT(t.REJECT_COUNT) AS reject_measured
FROM CFG_PIPELINES p
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID = p.PIPELINE_ID
 AND r.START_DATE >= :window_start AND r.START_DATE < :window_end
JOIN AUD_TASK_RUN_LOG t ON t.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID AND t.STATUS = 'SUCCESS'
JOIN CFG_TASKS k ON k.TASK_ID = t.TASK_ID
WHERE p.PROJECT_ID = :project_id
GROUP BY r.PIPELINE_ID, k.TASK_CODE
