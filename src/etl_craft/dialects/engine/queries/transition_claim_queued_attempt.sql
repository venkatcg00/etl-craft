-- PostgreSQL only: serialize admission with the deployment pool lock before this statement.
WITH requests AS (SELECT a.ATTEMPT_ID AS attempt_id, origin.PIPELINE_ID AS pipeline_id,
p.MAX_PARALLEL_TASKS AS pipeline_limit, p.PIPELINE_CODE AS pipeline_code,
project.PROJECT_ID AS project_id, project.MAX_RUNNING_TASKS AS project_limit,
CASE WHEN origin.BUNDLE_ID IS NULL THEN CAST(:warehouse_limit AS integer) ELSE
 (SELECT b.MAX_WAREHOUSE_QUERIES FROM AUD_PROJECT_BUNDLES b WHERE b.BUNDLE_ID=origin.BUNDLE_ID) END AS warehouse_limit,
a.PRIORITY AS priority, a.QUEUED_AT AS queued_at, a.SLOT_KIND AS slot_kind,
COALESCE(CAST((SELECT v.PARAMETER_VALUE FROM CFG_TASK_PARAMETERS v WHERE v.TASK_ID=r.TASK_ID
 AND v.PARAMETER_NAME='CPU_REQUEST' AND v.ACTIVE_FLAG='Y') AS real),
 CASE WHEN a.SLOT_KIND='python' THEN :python_cpu ELSE :warehouse_cpu END) AS cpu,
COALESCE(CAST((SELECT v.PARAMETER_VALUE FROM CFG_TASK_PARAMETERS v WHERE v.TASK_ID=r.TASK_ID
 AND v.PARAMETER_NAME='MEMORY_REQUEST_MB' AND v.ACTIVE_FLAG='Y') AS bigint),
 CASE WHEN a.SLOT_KIND='python' THEN :python_memory_mb ELSE :warehouse_memory_mb END) AS memory_mb
FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG r ON r.TASK_RUN_ID=a.TASK_RUN_ID
JOIN AUD_PIPELINES_RUN_LOG origin ON origin.PIPELINE_RUN_ID=r.PIPELINE_RUN_ID
JOIN CFG_PIPELINES p ON p.PIPELINE_ID=origin.PIPELINE_ID
JOIN CFG_PIPELINES project_pipeline ON project_pipeline.PIPELINE_ID=origin.PIPELINE_ID
JOIN CFG_PROJECTS project ON project.PROJECT_ID=project_pipeline.PROJECT_ID AND project.ACTIVE_FLAG='Y'
JOIN CFG_POOLS g ON g.POOL_NAME=a.POOL AND g.ACTIVE_FLAG='Y'
JOIN CFG_POOLS admission ON admission.POOL_NAME='default'
WHERE a.STATUS='QUEUED' AND a.OWNER_ID IS NULL
AND (a.NOT_BEFORE IS NULL OR a.NOT_BEFORE<=clock_timestamp()) AND a.POOL=:pool), eligible AS (
 SELECT q.* FROM requests q WHERE (SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID=x.TASK_RUN_ID
 JOIN AUD_PIPELINES_RUN_LOG owner_run ON owner_run.PIPELINE_RUN_ID=t.PIPELINE_RUN_ID
 WHERE owner_run.PIPELINE_ID=q.pipeline_id AND x.STATUS IN ('CLAIMED','RUNNING')) < q.pipeline_limit
AND (SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x
 JOIN AUD_TASK_RUN_LOG tr ON tr.TASK_RUN_ID=x.TASK_RUN_ID
 JOIN AUD_PIPELINES_RUN_LOG pr ON pr.PIPELINE_RUN_ID=tr.PIPELINE_RUN_ID
 JOIN CFG_PIPELINES pp ON pp.PIPELINE_ID=pr.PIPELINE_ID
 WHERE pp.PROJECT_ID=q.project_id AND x.STATUS IN ('CLAIMED','RUNNING')) < q.project_limit
AND (q.slot_kind='python' OR q.warehouse_limit IS NULL OR
 (SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x
 JOIN AUD_TASK_RUN_LOG tr ON tr.TASK_RUN_ID=x.TASK_RUN_ID
 JOIN AUD_PIPELINES_RUN_LOG pr ON pr.PIPELINE_RUN_ID=tr.PIPELINE_RUN_ID
 JOIN CFG_PIPELINES pp ON pp.PIPELINE_ID=pr.PIPELINE_ID
 WHERE pp.PROJECT_ID=q.project_id AND x.SLOT_KIND='warehouse'
 AND x.STATUS IN ('CLAIMED','RUNNING')) < q.warehouse_limit)
 AND ((q.slot_kind='python' AND :python_free) OR (q.slot_kind='warehouse' AND :warehouse_free))
 AND q.cpu<=:cpu_free AND q.memory_mb<=:memory_free_mb
 AND EXISTS (SELECT 1 FROM AUD_AGENTS g WHERE g.AGENT_ID=:agent_id AND g.STATUS='ACTIVE'
 AND g.HEARTBEAT_AT>clock_timestamp()-interval '30 seconds')
 ORDER BY q.priority DESC, q.queued_at, q.attempt_id LIMIT 1
), selected AS (
 SELECT a.ATTEMPT_ID AS attempt_id, e.cpu AS cpu, e.memory_mb AS memory_mb
 FROM AUD_TASK_ATTEMPTS a JOIN eligible e ON e.attempt_id=a.ATTEMPT_ID
 WHERE a.STATUS='QUEUED' FOR UPDATE OF a SKIP LOCKED
)
UPDATE AUD_TASK_ATTEMPTS a SET STATUS='CLAIMED', OWNER_ID=:owner, ERROR_MESSAGE=NULL,
CLAIMED_AT=clock_timestamp(), HEARTBEAT_AT=clock_timestamp(), LEASE_EXPIRES_AT=clock_timestamp()+interval '60 seconds',
RESERVED_CPU=s.cpu, RESERVED_MEMORY_MB=s.memory_mb FROM selected s WHERE a.ATTEMPT_ID=s.attempt_id
RETURNING a.ATTEMPT_ID AS attempt_id, a.LEASE_EXPIRES_AT AS lease_expires_at, clock_timestamp() AS db_now
