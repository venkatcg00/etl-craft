WITH scope AS (
 SELECT a.ATTEMPT_ID AS attempt_id,p.PROJECT_ID AS project_id,
        project.MAX_RUNNING_TASKS AS project_limit,
        CASE WHEN r.BUNDLE_ID IS NULL THEN CAST(:warehouse_limit AS integer)
             ELSE b.MAX_WAREHOUSE_QUERIES END AS warehouse_limit
 FROM AUD_TASK_ATTEMPTS a JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID=a.TASK_RUN_ID
 JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID=t.PIPELINE_RUN_ID
 JOIN CFG_PIPELINES p ON p.PIPELINE_ID=r.PIPELINE_ID
 JOIN CFG_PROJECTS project ON project.PROJECT_ID=p.PROJECT_ID
 LEFT JOIN AUD_PROJECT_BUNDLES b ON b.BUNDLE_ID=r.BUNDLE_ID
)
UPDATE AUD_TASK_ATTEMPTS AS a SET ERROR_MESSAGE=CASE
WHEN NOT EXISTS (SELECT 1 FROM CFG_POOLS p WHERE p.POOL_NAME='default')
THEN 'CFG_POOLS.default is missing; expected the deployment admission row; restore default using etl-craft pool add --name default --reason REASON'
WHEN NOT EXISTS (SELECT 1 FROM CFG_POOLS p WHERE p.POOL_NAME=a.POOL AND p.ACTIVE_FLAG='Y')
THEN 'pool ' || a.POOL || ' is missing or ACTIVE_FLAG=N; choose an active pool or use etl-craft pool add --name POOL --reason REASON'
WHEN (SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x JOIN AUD_TASK_RUN_LOG r ON r.TASK_RUN_ID=x.TASK_RUN_ID
 JOIN AUD_PIPELINES_RUN_LOG origin ON origin.PIPELINE_RUN_ID=r.PIPELINE_RUN_ID
 WHERE x.STATUS IN ('CLAIMED','RUNNING') AND origin.PIPELINE_ID=(SELECT owner_run.PIPELINE_ID FROM AUD_PIPELINES_RUN_LOG owner_run JOIN AUD_TASK_RUN_LOG tr ON tr.PIPELINE_RUN_ID=owner_run.PIPELINE_RUN_ID WHERE tr.TASK_RUN_ID=a.TASK_RUN_ID))
 >= (SELECT p.MAX_PARALLEL_TASKS FROM CFG_PIPELINES p JOIN AUD_PIPELINES_RUN_LOG owner_run ON owner_run.PIPELINE_ID=p.PIPELINE_ID JOIN AUD_TASK_RUN_LOG t ON t.PIPELINE_RUN_ID=owner_run.PIPELINE_RUN_ID WHERE t.TASK_RUN_ID=a.TASK_RUN_ID)
THEN 'pipeline ' || (SELECT p.PIPELINE_CODE FROM CFG_PIPELINES p JOIN AUD_PIPELINES_RUN_LOG owner_run ON owner_run.PIPELINE_ID=p.PIPELINE_ID JOIN AUD_TASK_RUN_LOG t ON t.PIPELINE_RUN_ID=owner_run.PIPELINE_RUN_ID WHERE t.TASK_RUN_ID=a.TASK_RUN_ID)
 || ' at MAX_PARALLEL_TASKS=' || CAST((SELECT p.MAX_PARALLEL_TASKS FROM CFG_PIPELINES p JOIN AUD_PIPELINES_RUN_LOG owner_run ON owner_run.PIPELINE_ID=p.PIPELINE_ID JOIN AUD_TASK_RUN_LOG t ON t.PIPELINE_RUN_ID=owner_run.PIPELINE_RUN_ID WHERE t.TASK_RUN_ID=a.TASK_RUN_ID) AS varchar)
WHEN (SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x JOIN scope own ON own.attempt_id=x.ATTEMPT_ID
 WHERE own.project_id=(SELECT project_id FROM scope WHERE attempt_id=a.ATTEMPT_ID)
 AND x.STATUS IN ('CLAIMED','RUNNING')) >= (SELECT project_limit FROM scope WHERE attempt_id=a.ATTEMPT_ID)
THEN 'project at MAX_RUNNING_TASKS=' || CAST((SELECT project_limit FROM scope WHERE attempt_id=a.ATTEMPT_ID) AS varchar)
WHEN a.SLOT_KIND='warehouse' AND
 (SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x JOIN scope own ON own.attempt_id=x.ATTEMPT_ID
 WHERE own.project_id=(SELECT project_id FROM scope WHERE attempt_id=a.ATTEMPT_ID)
 AND x.STATUS IN ('CLAIMED','RUNNING') AND x.SLOT_KIND='warehouse') >=
 (SELECT warehouse_limit FROM scope WHERE attempt_id=a.ATTEMPT_ID)
THEN 'warehouse at MAX_WAREHOUSE_QUERIES=' || CAST((SELECT warehouse_limit FROM scope WHERE attempt_id=a.ATTEMPT_ID) AS varchar)
ELSE 'needs ' || COALESCE((SELECT v.PARAMETER_VALUE FROM CFG_TASK_PARAMETERS v JOIN AUD_TASK_RUN_LOG r ON r.TASK_ID=v.TASK_ID WHERE r.TASK_RUN_ID=a.TASK_RUN_ID AND v.PARAMETER_NAME='MEMORY_REQUEST_MB' AND v.ACTIVE_FLAG='Y'),CAST(CASE WHEN a.SLOT_KIND='python' THEN :python_memory_mb ELSE :warehouse_memory_mb END AS varchar)) || ' MB and ' || COALESCE((SELECT v.PARAMETER_VALUE FROM CFG_TASK_PARAMETERS v JOIN AUD_TASK_RUN_LOG r ON r.TASK_ID=v.TASK_ID WHERE r.TASK_RUN_ID=a.TASK_RUN_ID AND v.PARAMETER_NAME='CPU_REQUEST' AND v.ACTIVE_FLAG='Y'),CAST(CASE WHEN a.SLOT_KIND='python' THEN :python_cpu ELSE :warehouse_cpu END AS varchar)) || ' CPU; ' || :capacity_label
 || ' has ' || CAST(:memory_free_mb AS varchar) || ' MB free of ' || CAST(:memory_capacity_mb AS varchar)
 || ' and ' || CAST(:cpu_free AS varchar) || ' CPU free of ' || CAST(:cpu_capacity AS varchar)
 || '; ' || a.SLOT_KIND || ' slots ' || CAST(CASE WHEN a.SLOT_KIND='python' THEN :python_free_slots ELSE :warehouse_free_slots END AS varchar)
 || ' free of ' || CAST(CASE WHEN a.SLOT_KIND='python' THEN :python_capacity_slots ELSE :warehouse_capacity_slots END AS varchar)
 || '. Add capacity or adjust task requests or agent slots' END
WHERE a.STATUS='QUEUED' AND a.OWNER_ID IS NULL
AND (CAST(:attempt_id AS bigint) IS NULL OR a.ATTEMPT_ID=:attempt_id) AND (CAST(:pool AS varchar) IS NULL OR a.POOL=:pool)
