SELECT t.TASK_RUN_ID AS task_run_id, t.TASK_ID AS task_id,
       t.PIPELINE_RUN_ID AS pipeline_run_id, t.TRIGGER_GENERATION AS trigger_generation,
       p.PIPELINE_ID AS pipeline_id, p.RUN_DATE AS run_date,
       p.METADATA_SNAPSHOT AS metadata_snapshot, p.SSO_GROUP_IDS AS sso_group_ids, p.STARTED_BY AS started_by, p.STARTED_BY_KIND AS started_by_kind,
       c.PIPELINE_CODE AS pipeline_code, task.HANDLER AS handler
FROM AUD_TASK_RUN_LOG t JOIN AUD_PIPELINES_RUN_LOG p ON p.PIPELINE_RUN_ID=t.PIPELINE_RUN_ID
JOIN CFG_PIPELINES c ON c.PIPELINE_ID=p.PIPELINE_ID
JOIN CFG_TASKS task ON task.TASK_ID=t.TASK_ID
WHERE t.TASK_RUN_ID=:task_run_id
