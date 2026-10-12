-- etl-craft: check-metadata-identities
-- Retired configuration rows reserve their identity; reactivate them to keep their history.

DROP INDEX ux_pipelines_code_active;

CREATE UNIQUE INDEX ux_pipelines_code_identity
    ON CFG_PIPELINES (PIPELINE_CODE);

DROP INDEX ux_pipedep_edge_active;

CREATE UNIQUE INDEX ux_pipedep_edge_identity
    ON CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE);

DROP INDEX ux_tasks_code_active;

CREATE UNIQUE INDEX ux_tasks_code_identity
    ON CFG_TASKS (PIPELINE_ID, TASK_CODE);

DROP INDEX ux_taskdep_edge_active;

CREATE UNIQUE INDEX ux_taskdep_edge_identity
    ON CFG_TASK_DEPENDENCY (TASK_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE);

DROP INDEX ux_taskparameters_name_active;

CREATE UNIQUE INDEX ux_taskparameters_name_identity
    ON CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME);

DROP INDEX ux_br_name_active;

CREATE UNIQUE INDEX ux_br_name_identity
    ON CFG_BUSINESS_RULES (TASK_ID, BUSINESS_RULE_NAME);
