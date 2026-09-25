-- Every active task of an active pipeline, with its latest finished run.
SELECT t.TASK_ID AS task_id, p.PIPELINE_CODE AS pipeline_code, t.TASK_CODE AS task_code,
       t.TASK_TYPE AS task_type, t.HANDLER AS handler, t.RUN_CONDITION AS run_condition,
       r.STATUS AS run_status, r.START_DATE AS run_start, r.END_DATE AS run_end,
       r.SOURCE_COUNT AS source_count, r.TARGET_COUNT AS target_count,
       r.INSERT_COUNT AS insert_count, r.UPDATE_COUNT AS update_count,
       r.DELETE_COUNT AS delete_count, r.ERROR_MESSAGE AS error_message
FROM CFG_TASKS t
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
LEFT JOIN (
    SELECT TASK_ID, STATUS, START_DATE, END_DATE, SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT,
           UPDATE_COUNT, DELETE_COUNT, ERROR_MESSAGE,
           ROW_NUMBER() OVER (PARTITION BY TASK_ID ORDER BY TASK_RUN_ID DESC) AS rn
    FROM AUD_TASK_RUN_LOG
    WHERE STATUS <> 'IN-PROGRESS'
) r ON r.TASK_ID = t.TASK_ID AND r.rn = 1
WHERE t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y'
ORDER BY p.PIPELINE_CODE, t.TASK_CODE
