SELECT p.PROJECT_ID AS project_id, r.PIPELINE_RUN_ID AS pipeline_run_id,
r.STARTED_BY_KIND AS started_by_kind, r.TRIGGER_KIND AS trigger_kind,
a.TASK_RUN_ID AS task_run_id, a.FORCE_FLAG AS force_flag,
a.RERUN_FLAG AS rerun_flag, a.STATUS AS status, a.OWNER_ID AS owner_id,
a.SOURCE_COUNT AS source_count, a.TARGET_COUNT AS target_count,
a.INSERT_COUNT AS insert_count, a.UPDATE_COUNT AS update_count, a.DELETE_COUNT AS delete_count,
a.REJECT_COUNT AS reject_count, a.ERROR_MESSAGE AS error_message, a.EXIT_CODE AS exit_code,
a.TASK_LOG AS task_log FROM AUD_TASK_ATTEMPTS a
JOIN AUD_TASK_RUN_LOG t ON t.TASK_RUN_ID=a.TASK_RUN_ID
JOIN AUD_PIPELINES_RUN_LOG r ON r.PIPELINE_RUN_ID=t.PIPELINE_RUN_ID
JOIN CFG_PIPELINES p ON p.PIPELINE_ID=r.PIPELINE_ID WHERE a.ATTEMPT_ID=:attempt_id
