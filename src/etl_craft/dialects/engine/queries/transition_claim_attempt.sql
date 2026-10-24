WITH requests AS (SELECT a.ATTEMPT_ID AS attempt_id, origin.PIPELINE_ID AS pipeline_id,
CAST(:event_wait AS boolean) AS event_wait, (SELECT p.MAX_PARALLEL_TASKS FROM CFG_PIPELINES p
WHERE p.PIPELINE_ID=origin.PIPELINE_ID) AS pipeline_limit,
a.PRIORITY AS priority, a.QUEUED_AT AS queued_at, a.SLOT_KIND AS slot_kind,
CASE WHEN CAST(:event_wait AS boolean) THEN 0 ELSE COALESCE(CAST((SELECT v.PARAMETER_VALUE FROM CFG_TASK_PARAMETERS v WHERE v.TASK_ID=r.TASK_ID
 AND v.PARAMETER_NAME='CPU_REQUEST' AND v.ACTIVE_FLAG='Y') AS real),
 CASE WHEN a.SLOT_KIND='python' THEN :python_cpu ELSE :warehouse_cpu END) END AS cpu,
CASE WHEN CAST(:event_wait AS boolean) THEN 0 ELSE COALESCE(CAST((SELECT v.PARAMETER_VALUE FROM CFG_TASK_PARAMETERS v WHERE v.TASK_ID=r.TASK_ID
 AND v.PARAMETER_NAME='MEMORY_REQUEST_MB' AND v.ACTIVE_FLAG='Y') AS bigint),
 CASE WHEN a.SLOT_KIND='python' THEN :python_memory_mb ELSE :warehouse_memory_mb END) END AS memory_mb
FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG r ON r.TASK_RUN_ID=a.TASK_RUN_ID
JOIN AUD_PIPELINES_RUN_LOG origin ON origin.PIPELINE_RUN_ID=r.PIPELINE_RUN_ID
JOIN CFG_POOLS g ON g.POOL_NAME=a.POOL AND g.ACTIVE_FLAG='Y'
JOIN CFG_POOLS admission ON admission.POOL_NAME='default'
WHERE a.STATUS='QUEUED' AND a.OWNER_ID IS NULL
AND (a.NOT_BEFORE IS NULL OR a.NOT_BEFORE<=:now) AND a.ATTEMPT_ID IN :candidate_ids), eligible AS (SELECT q.attempt_id AS attempt_id, q.cpu AS cpu, q.memory_mb AS memory_mb
FROM requests q WHERE q.event_wait OR ((SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID=x.TASK_RUN_ID
 JOIN AUD_PIPELINES_RUN_LOG owner_run ON owner_run.PIPELINE_RUN_ID=t.PIPELINE_RUN_ID
 WHERE owner_run.PIPELINE_ID=q.pipeline_id AND x.STATUS IN ('CLAIMED','RUNNING')) < q.pipeline_limit
AND (q.slot_kind='python' OR CAST(:warehouse_limit AS integer) IS NULL OR
 (SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x WHERE x.SLOT_KIND='warehouse'
 AND x.STATUS IN ('CLAIMED','RUNNING')) < :warehouse_limit)
AND (CAST(:local_cpu AS real) IS NULL OR q.cpu + (SELECT COALESCE(SUM(x.RESERVED_CPU),0)
 FROM AUD_TASK_ATTEMPTS x WHERE x.HOST=:host AND x.STATUS IN ('CLAIMED','RUNNING'))<=CAST(:local_cpu AS real))
AND (CAST(:local_memory_mb AS bigint) IS NULL OR q.memory_mb+(SELECT COALESCE(SUM(x.RESERVED_MEMORY_MB),0)
 FROM AUD_TASK_ATTEMPTS x WHERE x.HOST=:host AND x.STATUS IN ('CLAIMED','RUNNING'))<=CAST(:local_memory_mb AS bigint))
AND (CAST(:local_slots AS integer) IS NULL OR (SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x WHERE x.HOST=:host
 AND x.STATUS IN ('CLAIMED','RUNNING'))<CAST(:local_slots AS integer))
AND (CAST(:local_slots AS integer) IS NULL OR (SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x WHERE x.HOST=:host
 AND x.SLOT_KIND=q.slot_kind AND x.STATUS IN ('CLAIMED','RUNNING'))<
 CASE WHEN q.slot_kind='python' THEN CAST(:python_slots AS integer) ELSE CAST(:warehouse_slots AS integer) END))
ORDER BY q.priority DESC, q.queued_at, q.attempt_id LIMIT 1)
UPDATE AUD_TASK_ATTEMPTS AS a SET STATUS='CLAIMED', OWNER_ID=:owner, HOST=:host,
LEASE_EXPIRES_AT=:lease, HEARTBEAT_AT=:now, CLAIMED_AT=:now, ERROR_MESSAGE=NULL,
RESERVED_CPU=(SELECT cpu FROM eligible), RESERVED_MEMORY_MB=(SELECT memory_mb FROM eligible)
WHERE a.ATTEMPT_ID=:row_id AND a.ATTEMPT_ID=(SELECT attempt_id FROM eligible)
RETURNING ATTEMPT_ID AS attempt_id
