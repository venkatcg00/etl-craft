-- Recorded admission decisions, in immutable audit order.
SELECT d.DECISION_ID AS decision_id, d.ATTEMPT_ID AS attempt_id,
       d.TASK_DEPENDENCY_ID AS task_dependency_id,
       d.PIPELINE_DEPENDENCY_ID AS pipeline_dependency_id,
       d.SELECTED_PIPELINE_RUN_ID AS selected_pipeline_run_id,
       d.SELECTED_TASK_RUN_ID AS selected_task_run_id, d.SELECTED_REVISION AS selected_revision,
       d.RESULT AS result, d.REASON AS reason,
       COALESCE(t.STATUS,p.STATUS) AS upstream_status
FROM AUD_GATE_DECISIONS d
LEFT JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID=d.SELECTED_TASK_RUN_ID
LEFT JOIN AUD_PIPELINES_RUN_LOG p ON p.PIPELINE_RUN_ID=d.SELECTED_PIPELINE_RUN_ID
WHERE d.PIPELINE_RUN_ID=:pipeline_run_id ORDER BY d.DECISION_ID
