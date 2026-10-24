SELECT PROJECT_ID AS row_id, ACTIVE_FLAG AS active_flag, 'Y' AS well_formed,
       PROJECT_CODE AS project_code, PROJECT_NAME AS project_name, TIMEZONE AS timezone,
       DEFAULT_POOL AS default_pool, MAX_RUNNING_TASKS AS max_running_tasks
FROM CFG_PROJECTS WHERE PROJECT_CODE=:project_code ORDER BY 1
