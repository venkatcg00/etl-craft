SELECT PROJECT_ID AS project_id,PROJECT_CODE AS project_code,PROJECT_NAME AS project_name,
       ACTIVE_FLAG AS active_flag,TIMEZONE AS timezone,DEFAULT_POOL AS default_pool,MAX_RUNNING_TASKS AS max_running_tasks
FROM CFG_PROJECTS
WHERE PROJECT_CODE=:code
