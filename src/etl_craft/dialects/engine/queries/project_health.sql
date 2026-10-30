WITH visibility AS ({{permission_effective}}), visible AS (
 SELECT project_id,project_code,pipeline_id FROM visibility WHERE pipeline_id IS NOT NULL AND level>0
), current_runs AS (
 SELECT v.project_id AS project_id,COUNT(*) AS running
 FROM visible v JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID=v.pipeline_id
 WHERE r.STATUS='IN-PROGRESS' GROUP BY v.project_id
), recent_runs AS (
 SELECT v.project_id AS project_id,
 SUM(CASE WHEN r.STATUS='FAILED' THEN 1 ELSE 0 END) AS failed,
 SUM(CASE WHEN r.SLA_STATUS='BREACHED' THEN 1 ELSE 0 END) AS sla_misses
 FROM visible v JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_ID=v.pipeline_id
 WHERE r.END_DATE>=:window_start AND r.END_DATE<:window_end GROUP BY v.project_id
), queued AS (
 SELECT v.project_id AS project_id,COUNT(DISTINCT a.TASK_RUN_ID) AS queued_tasks
 FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID=a.TASK_RUN_ID
 JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID=t.PIPELINE_RUN_ID
 JOIN visible v ON v.pipeline_id=r.PIPELINE_ID
 WHERE a.STATUS='QUEUED' GROUP BY v.project_id
)
SELECT p.project_id AS project_id,p.project_code AS project,
 metadata.PROJECT_NAME AS project_name,metadata.ENVIRONMENT_TYPE AS environment_type,
 metadata.ENVIRONMENT_NAME AS environment_name,metadata.GOVERNANCE AS governance,
 COALESCE(r.running,0) AS running,COALESCE(h.failed,0) AS failed,
 COALESCE(h.sla_misses,0) AS sla_misses,COALESCE(q.queued_tasks,0) AS queued_tasks
FROM visibility p JOIN CFG_PROJECTS metadata ON metadata.PROJECT_ID=p.project_id AND metadata.ACTIVE_FLAG='Y' LEFT JOIN current_runs r ON r.project_id=p.project_id
LEFT JOIN recent_runs h ON h.project_id=p.project_id LEFT JOIN queued q ON q.project_id=p.project_id
WHERE p.pipeline_id IS NULL AND p.level>0 ORDER BY p.project_code
