-- Each task :task_id watches through an active FAILURE dependency, with the
-- error message of its latest run under any pipeline run.
SELECT dt.TASK_CODE AS depends_on_task_code,
       (SELECT l.ERROR_MESSAGE FROM AUD_TASK_RUN_LOG l
        WHERE l.TASK_ID = dt.TASK_ID
        ORDER BY l.START_DATE DESC, l.TASK_RUN_ID DESC
        LIMIT 1) AS error_message
FROM CFG_TASK_DEPENDENCY d
JOIN CFG_TASKS dt ON dt.TASK_ID = d.DEPENDS_ON_TASK_ID
WHERE d.TASK_ID = :task_id AND d.ACTIVE_FLAG = 'Y' AND d.DEPENDENCY_TYPE = 'FAILURE'
ORDER BY dt.TASK_CODE
