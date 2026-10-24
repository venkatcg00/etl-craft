-- Every active task of an active pipeline, with its latest finished run.
SELECT t.TASK_ID AS task_id, p.PIPELINE_CODE AS pipeline_code, t.TASK_CODE AS task_code,
       t.TASK_TYPE AS task_type, t.HANDLER AS handler, t.RUN_CONDITION AS run_condition,
       r.STATUS AS run_status, r.START_DATE AS run_start, r.END_DATE AS run_end,
       r.SOURCE_COUNT AS source_count, r.TARGET_COUNT AS target_count,
       r.INSERT_COUNT AS insert_count, r.UPDATE_COUNT AS update_count,
       r.DELETE_COUNT AS delete_count, r.REJECT_COUNT AS reject_count,
       r.ERROR_MESSAGE AS error_message,
       EXISTS (SELECT 1 FROM AUD_TASK_RUN_LOG done
               WHERE done.TASK_ID=t.TASK_ID AND done.STATUS='SUCCESS') AS has_successful_run
FROM CFG_TASKS t
JOIN CFG_PIPELINES p ON p.PIPELINE_ID = t.PIPELINE_ID
LEFT JOIN AUD_TASK_RUN_LOG r ON r.TASK_RUN_ID = (
    SELECT MAX(recent.TASK_RUN_ID) FROM AUD_TASK_RUN_LOG recent
    WHERE recent.TASK_ID = t.TASK_ID AND recent.STATUS <> 'IN-PROGRESS'
)
WHERE t.ACTIVE_FLAG = 'Y' AND p.ACTIVE_FLAG = 'Y' AND p.PROJECT_ID=:project_id
ORDER BY p.PIPELINE_CODE, t.TASK_CODE
