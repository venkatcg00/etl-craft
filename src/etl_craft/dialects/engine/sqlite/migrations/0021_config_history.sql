-- Typed change history: one append-only AUD_<table>_HISTORY per CFG_ table. The trigger
-- refresh that runs after this migration moves the AUD_METADATA_CHANGES rows into them and
-- replaces that table with a view over them.
-- Every version of a CFG_PIPELINES row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_PIPELINES_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMP NOT NULL,
    VALID_TO     TIMESTAMP NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    PIPELINE_ID BIGINT,
    PIPELINE_CODE VARCHAR,
    PIPELINE_NAME VARCHAR,
    DESCRIPTION VARCHAR,
    RUN_SCHEDULE VARCHAR,
    SLA_IN_HOURS NUMERIC,
    REFRESH_TYPE VARCHAR,
    ACTIVE_FLAG VARCHAR,
    PIPELINE_PARAMETERS TEXT,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMP,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMP,
    SCHEDULE_TIMEZONE VARCHAR,
    CATCHUP VARCHAR(1),
    MAX_CATCHUP_RUNS INT,
    OVERLAP_POLICY VARCHAR,
    SCHEDULE_START_DATE DATE,
    CONSTRAINT ck_aud_cfg_pipelines_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_pipelines_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_pipelines_history_row ON AUD_CFG_PIPELINES_HISTORY (PIPELINE_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_pipelines_history_current ON AUD_CFG_PIPELINES_HISTORY (PIPELINE_ID) WHERE CURRENT_FLAG = 'Y';

-- Every version of a CFG_PIPELINE_DEPENDENCY row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_PIPELINE_DEPENDENCY_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMP NOT NULL,
    VALID_TO     TIMESTAMP NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    PIPELINE_DEPENDENCY_ID BIGINT,
    PIPELINE_ID BIGINT,
    DEPENDS_ON_PIPELINE_ID BIGINT,
    DEPENDENCY_TYPE VARCHAR,
    ACTIVE_FLAG VARCHAR,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMP,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMP,
    CONSUME_REPAIRS VARCHAR(1),
    CONSTRAINT ck_aud_cfg_pipeline_dependency_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_pipeline_dependency_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_pipeline_dependency_history_row ON AUD_CFG_PIPELINE_DEPENDENCY_HISTORY (PIPELINE_DEPENDENCY_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_pipeline_dependency_history_current ON AUD_CFG_PIPELINE_DEPENDENCY_HISTORY (PIPELINE_DEPENDENCY_ID) WHERE CURRENT_FLAG = 'Y';

-- Every version of a CFG_TASKS row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_TASKS_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMP NOT NULL,
    VALID_TO     TIMESTAMP NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    TASK_ID BIGINT,
    TASK_CODE VARCHAR,
    TASK_TYPE VARCHAR,
    PIPELINE_ID BIGINT,
    HANDLER VARCHAR,
    RUN_CONDITION VARCHAR,
    RUN_CONDITION_COUNT INT,
    ACTIVE_FLAG VARCHAR,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMP,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMP,
    CONSTRAINT ck_aud_cfg_tasks_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_tasks_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_tasks_history_row ON AUD_CFG_TASKS_HISTORY (TASK_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_tasks_history_current ON AUD_CFG_TASKS_HISTORY (TASK_ID) WHERE CURRENT_FLAG = 'Y';

-- Every version of a CFG_TASK_DEPENDENCY row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_TASK_DEPENDENCY_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMP NOT NULL,
    VALID_TO     TIMESTAMP NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    TASK_DEPENDENCY_ID BIGINT,
    PIPELINE_ID BIGINT,
    TASK_ID BIGINT,
    DEPENDS_ON_PIPELINE_ID BIGINT,
    DEPENDS_ON_TASK_ID BIGINT,
    DEPENDENCY_TYPE VARCHAR,
    ACTIVE_FLAG VARCHAR,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMP,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMP,
    CONSUME_REPAIRS VARCHAR(1),
    CONSTRAINT ck_aud_cfg_task_dependency_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_task_dependency_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_task_dependency_history_row ON AUD_CFG_TASK_DEPENDENCY_HISTORY (TASK_DEPENDENCY_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_task_dependency_history_current ON AUD_CFG_TASK_DEPENDENCY_HISTORY (TASK_DEPENDENCY_ID) WHERE CURRENT_FLAG = 'Y';

-- Every version of a CFG_TASK_PARAMETERS row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_TASK_PARAMETERS_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMP NOT NULL,
    VALID_TO     TIMESTAMP NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    TASK_PARAMETER_ID BIGINT,
    TASK_ID BIGINT,
    PARAMETER_NAME VARCHAR,
    PARAMETER_VALUE VARCHAR,
    ACTIVE_FLAG VARCHAR,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMP,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMP,
    CONSTRAINT ck_aud_cfg_task_parameters_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_task_parameters_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_task_parameters_history_row ON AUD_CFG_TASK_PARAMETERS_HISTORY (TASK_PARAMETER_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_task_parameters_history_current ON AUD_CFG_TASK_PARAMETERS_HISTORY (TASK_PARAMETER_ID) WHERE CURRENT_FLAG = 'Y';

-- Every version of a CFG_BUSINESS_RULES row, valid from VALID_FROM until VALID_TO (the far future while it
-- is the current one, CURRENT_FLAG 'Y'): its values after each insert and update, and its
-- values when deleted, with who changed it and the project migration that did.
CREATE TABLE AUD_CFG_BUSINESS_RULES_HISTORY (
    CHANGE_ID    BIGINT PRIMARY KEY,
    VALID_FROM   TIMESTAMP NOT NULL,
    VALID_TO     TIMESTAMP NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION    VARCHAR NOT NULL,
    ACTOR        VARCHAR NOT NULL,
    ACTOR_KIND   VARCHAR NOT NULL,
    MIGRATION    VARCHAR,
    BUSINESS_RULE_ID BIGINT,
    BUSINESS_RULE_NAME VARCHAR,
    PIPELINE_ID BIGINT,
    TASK_ID BIGINT,
    BUSINESS_RULE_SQL VARCHAR,
    BUSINESS_RULE_TYPE VARCHAR,
    BUSINESS_RULE_KEY_COLUMN VARCHAR,
    TARGET_TABLE VARCHAR,
    SEQUENCE_NUMBER BIGINT,
    ACTIVE_FLAG VARCHAR,
    CREATED_BY VARCHAR,
    CREATE_DATE TIMESTAMP,
    UPDATED_BY VARCHAR,
    UPDATED_DATE TIMESTAMP,
    CONSTRAINT ck_aud_cfg_business_rules_history_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_aud_cfg_business_rules_history_current CHECK (CURRENT_FLAG IN ('Y','N'))
);
CREATE INDEX ix_aud_cfg_business_rules_history_row ON AUD_CFG_BUSINESS_RULES_HISTORY (BUSINESS_RULE_ID, CHANGE_ID);
CREATE INDEX ix_aud_cfg_business_rules_history_current ON AUD_CFG_BUSINESS_RULES_HISTORY (BUSINESS_RULE_ID) WHERE CURRENT_FLAG = 'Y';

