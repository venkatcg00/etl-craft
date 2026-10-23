UPDATE AUD_TASK_ATTEMPTS AS a SET ERROR_MESSAGE=CASE
WHEN NOT EXISTS (SELECT 1 FROM CFG_POOLS p WHERE p.POOL_NAME='default')
THEN 'CFG_POOLS.default is missing; expected the deployment admission row; restore default in pools.csv and config apply'
WHEN NOT EXISTS (SELECT 1 FROM CFG_POOLS p WHERE p.POOL_NAME=a.POOL AND p.ACTIVE_FLAG='Y')
THEN 'pool ' || a.POOL || ' is missing or ACTIVE_FLAG=N; activate it in pools.csv and config apply'
WHEN (SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x JOIN AUD_TASK_RUN_LOG r ON r.TASK_RUN_ID=x.TASK_RUN_ID
 JOIN CFG_TASKS task ON task.TASK_ID=r.TASK_ID
 WHERE x.STATUS IN ('CLAIMED','RUNNING') AND task.PIPELINE_ID=(SELECT t.PIPELINE_ID FROM CFG_TASKS t JOIN AUD_TASK_RUN_LOG tr ON tr.TASK_ID=t.TASK_ID WHERE tr.TASK_RUN_ID=a.TASK_RUN_ID))
 >= (SELECT p.MAX_PARALLEL_TASKS FROM CFG_PIPELINES p JOIN CFG_TASKS task ON task.PIPELINE_ID=p.PIPELINE_ID JOIN AUD_TASK_RUN_LOG t ON t.TASK_ID=task.TASK_ID WHERE t.TASK_RUN_ID=a.TASK_RUN_ID)
THEN 'pipeline ' || (SELECT p.PIPELINE_CODE FROM CFG_PIPELINES p JOIN CFG_TASKS task ON task.PIPELINE_ID=p.PIPELINE_ID JOIN AUD_TASK_RUN_LOG t ON t.TASK_ID=task.TASK_ID WHERE t.TASK_RUN_ID=a.TASK_RUN_ID)
 || ' at MAX_PARALLEL_TASKS=' || CAST((SELECT p.MAX_PARALLEL_TASKS FROM CFG_PIPELINES p JOIN CFG_TASKS task ON task.PIPELINE_ID=p.PIPELINE_ID JOIN AUD_TASK_RUN_LOG t ON t.TASK_ID=task.TASK_ID WHERE t.TASK_RUN_ID=a.TASK_RUN_ID) AS varchar)
WHEN a.SLOT_KIND='warehouse' AND CAST(:warehouse_limit AS integer) IS NOT NULL AND
 (SELECT COUNT(*) FROM AUD_TASK_ATTEMPTS x WHERE x.STATUS IN ('CLAIMED','RUNNING') AND x.SLOT_KIND='warehouse')>=:warehouse_limit
THEN 'warehouse at MAX_WAREHOUSE_QUERIES=' || CAST(:warehouse_limit AS varchar)
ELSE 'needs ' || COALESCE((SELECT v.PARAMETER_VALUE FROM CFG_TASK_PARAMETERS v JOIN AUD_TASK_RUN_LOG r ON r.TASK_ID=v.TASK_ID WHERE r.TASK_RUN_ID=a.TASK_RUN_ID AND v.PARAMETER_NAME='MEMORY_REQUEST_MB' AND v.ACTIVE_FLAG='Y'),CAST(CASE WHEN a.SLOT_KIND='python' THEN :python_memory_mb ELSE :warehouse_memory_mb END AS varchar)) || ' MB and ' || COALESCE((SELECT v.PARAMETER_VALUE FROM CFG_TASK_PARAMETERS v JOIN AUD_TASK_RUN_LOG r ON r.TASK_ID=v.TASK_ID WHERE r.TASK_RUN_ID=a.TASK_RUN_ID AND v.PARAMETER_NAME='CPU_REQUEST' AND v.ACTIVE_FLAG='Y'),CAST(CASE WHEN a.SLOT_KIND='python' THEN :python_cpu ELSE :warehouse_cpu END AS varchar)) || ' CPU; ' || :capacity_label
 || ' has ' || CAST(:memory_free_mb AS varchar) || ' MB free of ' || CAST(:memory_capacity_mb AS varchar)
 || ' and ' || CAST(:cpu_free AS varchar) || ' CPU free of ' || CAST(:cpu_capacity AS varchar)
 || '; ' || a.SLOT_KIND || ' slots ' || CAST(CASE WHEN a.SLOT_KIND='python' THEN :python_free_slots ELSE :warehouse_free_slots END AS varchar)
 || ' free of ' || CAST(CASE WHEN a.SLOT_KIND='python' THEN :python_capacity_slots ELSE :warehouse_capacity_slots END AS varchar)
 || '. Add capacity or adjust task requests or agent slots' END
WHERE a.STATUS='QUEUED' AND a.OWNER_ID IS NULL
AND (CAST(:attempt_id AS bigint) IS NULL OR a.ATTEMPT_ID=:attempt_id) AND (CAST(:pool AS varchar) IS NULL OR a.POOL=:pool)
