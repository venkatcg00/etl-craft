WITH project_pipelines AS not materialized (SELECT PIPELINE_ID FROM CFG_PIPELINES WHERE PROJECT_ID=:project_id),
project_tasks AS not materialized (SELECT TASK_ID FROM CFG_TASKS WHERE PIPELINE_ID IN (SELECT PIPELINE_ID FROM project_pipelines)),
project_runs AS not materialized (SELECT PIPELINE_RUN_ID FROM AUD_PIPELINES_RUN_LOG WHERE PIPELINE_ID IN (SELECT PIPELINE_ID FROM project_pipelines)),
project_task_runs AS not materialized (SELECT TASK_RUN_ID FROM AUD_TASK_RUN_LOG WHERE PIPELINE_RUN_ID IN (SELECT PIPELINE_RUN_ID FROM project_runs)),
project_attempts AS not materialized (SELECT ATTEMPT_ID FROM AUD_TASK_ATTEMPTS WHERE TASK_RUN_ID IN (SELECT TASK_RUN_ID FROM project_task_runs)),
project_rules AS not materialized (SELECT BUSINESS_RULE_ID FROM CFG_BUSINESS_RULES WHERE PIPELINE_ID IN (SELECT PIPELINE_ID FROM project_pipelines)),
project_rule_runs AS not materialized (SELECT BUSINESS_RULE_RUN_ID FROM AUD_BUSINESS_RULES_RUN_LOG WHERE TASK_RUN_ID IN (SELECT TASK_RUN_ID FROM project_task_runs)),
project_lineage AS not materialized (SELECT LINEAGE_VERSION_ID FROM AUD_LINEAGE_VERSIONS WHERE PROJECT_ID=:project_id),
project_tables AS not materialized (SELECT TABLE_ID FROM CFG_TABLES WHERE PROJECT_ID=:project_id)
