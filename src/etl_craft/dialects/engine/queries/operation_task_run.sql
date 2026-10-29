-- One exact task run, or the task's row beneath one exact pipeline run.
SELECT p.PIPELINE_ID AS pipeline_id, r.PIPELINE_RUN_ID AS pipeline_run_id,
       t.TASK_ID AS task_id, t.TASK_CODE AS task_code, r.TASK_RUN_ID AS task_run_id,
       r.STATUS AS status, r.START_DATE AS start_date, r.END_DATE AS end_date,
       r.ATTEMPT_COUNT AS attempt_count, r.SOURCE_COUNT AS source_count,
       r.TARGET_COUNT AS target_count, r.INSERT_COUNT AS insert_count,
       r.UPDATE_COUNT AS update_count, r.DELETE_COUNT AS delete_count,
       r.ROWS_WRITTEN AS rows_written, r.REJECT_COUNT AS reject_count,
       r.WATERMARK_FROM AS watermark_from, r.WATERMARK_TO AS watermark_to,
       r.ERROR_MESSAGE AS error_message,
       (SELECT 1 AS has_item FROM AUD_TASK_ITEMS i WHERE i.TASK_RUN_ID=r.TASK_RUN_ID LIMIT 1) AS has_items,
       (SELECT 1 AS has_table_rule FROM AUD_BUSINESS_RULES_RUN_LOG br WHERE br.TASK_RUN_ID=r.TASK_RUN_ID
        AND (br.PASSED IS NOT NULL OR br.DETAIL IS NOT NULL) LIMIT 1) AS has_table_rules
FROM AUD_TASK_RUN_LOG r
JOIN AUD_PIPELINES_RUN_LOG p ON p.PIPELINE_RUN_ID = r.PIPELINE_RUN_ID
JOIN CFG_TASKS t ON t.TASK_ID = r.TASK_ID
WHERE p.PIPELINE_ID = :pipeline_id
AND (CAST(:task_run_id AS bigint) IS NULL OR r.TASK_RUN_ID = :task_run_id)
AND (CAST(:pipeline_run_id AS bigint) IS NULL OR r.PIPELINE_RUN_ID = :pipeline_run_id)
AND (CAST(:task_id AS bigint) IS NULL OR r.TASK_ID = :task_id)
