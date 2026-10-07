-- etl-craft Engine DB schema for PostgreSQL.
--
-- `etl-craft init-db` applies this file to an empty database in one transaction; `migrate`
-- carries an existing database forward with the files in migrations/. The SQLite schema beside
-- it defines the same tables and columns, so every Engine DB query reads the same shape.
--
-- CFG_ tables hold what to run, written by the team and reviewed like code. AUD_ tables are
-- written by the engine as it runs. Every CFG_ row carries ACTIVE_FLAG; rows are retired by
-- setting it to 'N', never deleted, and each code is unique among active rows only.

-- Stamps the audit columns on every CFG_ insert and update. CREATED_BY and CREATE_DATE never
-- change after the insert. The values mean something only when every person and service
-- touching the Engine DB connects as its own role.
CREATE OR REPLACE FUNCTION trg_set_audit_columns()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.CREATED_BY   := current_setting('etl_craft.actor');
        NEW.CREATE_DATE  := now();
        NEW.UPDATED_BY   := current_setting('etl_craft.actor');
        NEW.UPDATED_DATE := now();
    ELSIF TG_OP = 'UPDATE' THEN
        NEW.CREATED_BY   := OLD.CREATED_BY;
        NEW.CREATE_DATE  := OLD.CREATE_DATE;
        NEW.UPDATED_BY   := current_setting('etl_craft.actor');
        NEW.UPDATED_DATE := now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- A task dependency without DEPENDS_ON_PIPELINE_ID is on a task in the same pipeline.
CREATE OR REPLACE FUNCTION trg_default_depends_on_pipeline()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.DEPENDS_ON_PIPELINE_ID IS NULL THEN
        NEW.DEPENDS_ON_PIPELINE_ID := NEW.PIPELINE_ID;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- One row per pipeline: its code, schedule, SLA, refresh type and generated-DAG settings.
CREATE TABLE CFG_PIPELINES (
    PIPELINE_ID          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PIPELINE_CODE        VARCHAR NOT NULL,
    PIPELINE_NAME        VARCHAR NOT NULL,
    DESCRIPTION          VARCHAR,
    RUN_SCHEDULE         VARCHAR,
    SLA_IN_HOURS         NUMERIC,
    REFRESH_TYPE         VARCHAR NOT NULL,
    ACTIVE_FLAG          VARCHAR NOT NULL DEFAULT 'Y',
    PIPELINE_PARAMETERS  JSONB,
    CREATED_BY           VARCHAR,
    CREATE_DATE          TIMESTAMPTZ,
    UPDATED_BY           VARCHAR,
    UPDATED_DATE         TIMESTAMPTZ,
    SCHEDULE_TIMEZONE VARCHAR,
    CATCHUP VARCHAR(1) NOT NULL DEFAULT 'N' CONSTRAINT ck_schedule_catchup CHECK (CATCHUP IN ('Y','N')),
    MAX_CATCHUP_RUNS INT NOT NULL DEFAULT 1 CONSTRAINT ck_schedule_catchup_runs CHECK (MAX_CATCHUP_RUNS > 0),
    OVERLAP_POLICY VARCHAR NOT NULL DEFAULT 'SKIP' CONSTRAINT ck_schedule_overlap CHECK (OVERLAP_POLICY IN ('SKIP','QUEUE')),
    SCHEDULE_START_DATE DATE,
    CONSTRAINT ck_pipelines_code CHECK (PIPELINE_CODE ~ '^[A-Za-z][A-Za-z0-9_]{0,127}$'),
    CONSTRAINT ck_pipelines_refresh_type CHECK (REFRESH_TYPE IN ('FULL', 'INCREMENTAL')),
    CONSTRAINT ck_pipelines_active_flag  CHECK (ACTIVE_FLAG IN ('Y', 'N'))
);
CREATE UNIQUE INDEX ux_pipelines_code_active
    ON CFG_PIPELINES (PIPELINE_CODE) WHERE ACTIVE_FLAG = 'Y';
CREATE TRIGGER trg_audit_cfg_pipelines
    BEFORE INSERT OR UPDATE ON CFG_PIPELINES
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();
COMMENT ON TABLE CFG_PIPELINES IS 'One row per pipeline. PIPELINE_CODE is the key the command line uses; REFRESH_TYPE decides whether $$pipeline_run_id_filter reads the run or all rows.';

-- A pipeline waiting on another: a new run starts only when the upstream's last finished run
-- satisfies DEPENDENCY_TYPE.
CREATE TABLE CFG_PIPELINE_DEPENDENCY (
    PIPELINE_DEPENDENCY_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PIPELINE_ID             BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    DEPENDS_ON_PIPELINE_ID  BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    DEPENDENCY_TYPE         VARCHAR NOT NULL,
    ACTIVE_FLAG             VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY              VARCHAR,
    CREATE_DATE             TIMESTAMPTZ,
    UPDATED_BY              VARCHAR,
    UPDATED_DATE            TIMESTAMPTZ,
    CONSUME_REPAIRS         VARCHAR(1) NOT NULL DEFAULT 'Y' CONSTRAINT ck_pipedep_consume_repairs CHECK (CONSUME_REPAIRS IN ('Y','N')),
    CONSTRAINT ck_pipedep_type        CHECK (DEPENDENCY_TYPE IN ('SUCCESS','FAILURE','ALWAYS','HAS_DATA')),
    CONSTRAINT ck_pipedep_active_flag CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_pipedep_no_self_dep CHECK (PIPELINE_ID <> DEPENDS_ON_PIPELINE_ID)
);
CREATE UNIQUE INDEX ux_pipedep_edge_active
    ON CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE)
    WHERE ACTIVE_FLAG = 'Y';
CREATE INDEX ix_pipedep_depends_on ON CFG_PIPELINE_DEPENDENCY (DEPENDS_ON_PIPELINE_ID);
CREATE TRIGGER trg_audit_cfg_pipeline_dependency
    BEFORE INSERT OR UPDATE ON CFG_PIPELINE_DEPENDENCY
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();
COMMENT ON TABLE CFG_PIPELINE_DEPENDENCY IS 'A pipeline waits for another pipeline''s outcome. Checked against AUD_DEPENDENCY_CONSUMPTION when the pipeline starts.';

-- One row per task: its pipeline, handler and run condition.
CREATE TABLE CFG_TASKS (
    TASK_ID              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    TASK_CODE            VARCHAR NOT NULL,
    TASK_TYPE            VARCHAR NOT NULL,
    PIPELINE_ID          BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    HANDLER              VARCHAR NOT NULL,
    RUN_CONDITION        VARCHAR,
    RUN_CONDITION_COUNT  INT,
    ACTIVE_FLAG          VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY           VARCHAR,
    CREATE_DATE          TIMESTAMPTZ,
    UPDATED_BY           VARCHAR,
    UPDATED_DATE         TIMESTAMPTZ,
    CONSTRAINT ck_tasks_code CHECK (TASK_CODE ~ '^[A-Za-z][A-Za-z0-9_]{0,127}$'),
    CONSTRAINT ck_tasks_task_type       CHECK (TASK_TYPE IN ('INGESTION','ETL')),
    CONSTRAINT ck_tasks_handler         CHECK (HANDLER IN ('PYTHON','SQL','BUSINESS_RULES','EMAIL_ALERT')),
    CONSTRAINT ck_tasks_active_flag     CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_tasks_run_condition   CHECK (RUN_CONDITION IS NULL OR RUN_CONDITION IN ('ALL','ANY','N')),
    CONSTRAINT ck_tasks_run_condition_count CHECK (
        (COALESCE(RUN_CONDITION, '') = 'N' AND RUN_CONDITION_COUNT IS NOT NULL
            AND RUN_CONDITION_COUNT >= 1)
        OR (RUN_CONDITION IS DISTINCT FROM 'N' AND RUN_CONDITION_COUNT IS NULL)
    )
);
CREATE UNIQUE INDEX ux_tasks_code_active
    ON CFG_TASKS (PIPELINE_ID, TASK_CODE) WHERE ACTIVE_FLAG = 'Y';
CREATE TRIGGER trg_audit_cfg_tasks
    BEFORE INSERT OR UPDATE ON CFG_TASKS
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();
COMMENT ON TABLE CFG_TASKS IS 'One row per task. HANDLER runs it; its settings are CFG_TASK_PARAMETERS rows.';
COMMENT ON COLUMN CFG_TASKS.RUN_CONDITION IS 'ALL | ANY | N: how many of this task''s dependencies must be satisfied. NULL means ALL.';
COMMENT ON COLUMN CFG_TASKS.RUN_CONDITION_COUNT IS 'How many dependencies must be satisfied when RUN_CONDITION = ''N''; NULL for every other condition.';

-- A task waiting on another, in its own pipeline or, with DEPENDS_ON_PIPELINE_ID, in another.
CREATE TABLE CFG_TASK_DEPENDENCY (
    TASK_DEPENDENCY_ID      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PIPELINE_ID             BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    TASK_ID                 BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    DEPENDS_ON_PIPELINE_ID  BIGINT REFERENCES CFG_PIPELINES(PIPELINE_ID),
    DEPENDS_ON_TASK_ID      BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    DEPENDENCY_TYPE         VARCHAR NOT NULL,
    ACTIVE_FLAG             VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY              VARCHAR,
    CREATE_DATE             TIMESTAMPTZ,
    UPDATED_BY              VARCHAR,
    UPDATED_DATE            TIMESTAMPTZ,
    CONSUME_REPAIRS         VARCHAR(1) NOT NULL DEFAULT 'Y' CONSTRAINT ck_taskdep_consume_repairs CHECK (CONSUME_REPAIRS IN ('Y','N')),
    CONSTRAINT ck_taskdep_type        CHECK (DEPENDENCY_TYPE IN ('SUCCESS','FAILURE','ALWAYS','HAS_DATA')),
    CONSTRAINT ck_taskdep_active_flag CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_taskdep_no_self_dep CHECK (NOT (TASK_ID = DEPENDS_ON_TASK_ID AND PIPELINE_ID = DEPENDS_ON_PIPELINE_ID))
);
CREATE TRIGGER trg_default_taskdep_pipeline
    BEFORE INSERT OR UPDATE ON CFG_TASK_DEPENDENCY
    FOR EACH ROW EXECUTE FUNCTION trg_default_depends_on_pipeline();
CREATE TRIGGER trg_audit_cfg_task_dependency
    BEFORE INSERT OR UPDATE ON CFG_TASK_DEPENDENCY
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();
CREATE UNIQUE INDEX ux_taskdep_edge_active
    ON CFG_TASK_DEPENDENCY (TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE)
    WHERE ACTIVE_FLAG = 'Y';
CREATE INDEX ix_taskdep_depends_on ON CFG_TASK_DEPENDENCY (DEPENDS_ON_TASK_ID);
COMMENT ON TABLE CFG_TASK_DEPENDENCY IS 'A task waits for another task''s outcome. Same-pipeline dependencies order the pipeline''s waves; one on another pipeline''s task is checked against AUD_DEPENDENCY_CONSUMPTION when the task starts.';

-- A task's settings, as name and value rows; each handler reads its own names.
CREATE TABLE CFG_TASK_PARAMETERS (
    TASK_PARAMETER_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    TASK_ID            BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    PARAMETER_NAME     VARCHAR NOT NULL,
    PARAMETER_VALUE    VARCHAR,
    ACTIVE_FLAG        VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY         VARCHAR,
    CREATE_DATE        TIMESTAMPTZ,
    UPDATED_BY         VARCHAR,
    UPDATED_DATE       TIMESTAMPTZ,
    CONSTRAINT ck_taskparameters_active_flag CHECK (ACTIVE_FLAG IN ('Y','N'))
);
CREATE UNIQUE INDEX ux_taskparameters_name_active
    ON CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME) WHERE ACTIVE_FLAG = 'Y';
CREATE TRIGGER trg_audit_cfg_task_parameters
    BEFORE INSERT OR UPDATE ON CFG_TASK_PARAMETERS
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();
COMMENT ON TABLE CFG_TASK_PARAMETERS IS 'A task''s settings as name and value pairs (SQL_ACTION, TARGET_OBJECT, SOURCE_SQL, ...). Which names each handler reads is documented in the task parameter reference.';

-- A BUSINESS_RULES task's rules: a correlated SELECT that finds the rows of TARGET_TABLE that
-- break it, and what a failing row gets flagged as.
CREATE TABLE CFG_BUSINESS_RULES (
    BUSINESS_RULE_ID          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    BUSINESS_RULE_NAME        VARCHAR NOT NULL,
    PIPELINE_ID               BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    TASK_ID                   BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    BUSINESS_RULE_SQL         VARCHAR NOT NULL,
    BUSINESS_RULE_TYPE        VARCHAR NOT NULL,
    BUSINESS_RULE_KEY_COLUMN  VARCHAR NOT NULL,
    TARGET_TABLE              VARCHAR NOT NULL,
    SEQUENCE_NUMBER           BIGINT NOT NULL,
    ACTIVE_FLAG               VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY                VARCHAR,
    CREATE_DATE               TIMESTAMPTZ,
    UPDATED_BY                VARCHAR,
    UPDATED_DATE              TIMESTAMPTZ,
    CONSTRAINT ck_br_type        CHECK (BUSINESS_RULE_TYPE IN ('INCOMPLETE','REJECT','REPORT')),
    CONSTRAINT ck_br_active_flag CHECK (ACTIVE_FLAG IN ('Y','N'))
);
CREATE UNIQUE INDEX ux_br_name_active
    ON CFG_BUSINESS_RULES (TASK_ID, BUSINESS_RULE_NAME) WHERE ACTIVE_FLAG = 'Y';
CREATE INDEX ix_br_pipeline ON CFG_BUSINESS_RULES (PIPELINE_ID);
CREATE TRIGGER trg_audit_cfg_business_rules
    BEFORE INSERT OR UPDATE ON CFG_BUSINESS_RULES
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();
COMMENT ON TABLE CFG_BUSINESS_RULES IS 'Checks a BUSINESS_RULES task runs against a warehouse table. TARGET_TABLE must have a single-column primary key, named by BUSINESS_RULE_KEY_COLUMN; `validate` checks it in the warehouse.';

-- One row per pipeline run: its status, when it ran, whether it met its SLA, the date it ran
-- as of (RUN_DATE, the SQL tasks' $$run_date), and whether it was part of a backfill.
CREATE TABLE AUD_PIPELINES_RUN_LOG (
    PIPELINE_RUN_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PIPELINE_ID      BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    START_DATE       TIMESTAMPTZ NOT NULL DEFAULT now(),
    END_DATE         TIMESTAMPTZ,
    STATUS           VARCHAR NOT NULL,
    SLA_STATUS       VARCHAR(8),
    RUN_DATE         DATE,
    BACKFILL         VARCHAR(1) NOT NULL DEFAULT 'N',
    RUN_KEY          VARCHAR NOT NULL DEFAULT ('manual:' || gen_random_uuid()::text),
    TRIGGER_KIND     VARCHAR NOT NULL DEFAULT 'MANUAL',
    OWNER_ID         VARCHAR,
    LEASE_EXPIRES_AT TIMESTAMPTZ,
    OUTPUT_REVISION  INT NOT NULL DEFAULT 1,
    CONFIG_SHA256    VARCHAR(64),
    STARTED_BY VARCHAR DEFAULT current_setting('etl_craft.actor', true),
    STARTED_BY_KIND VARCHAR DEFAULT current_setting('etl_craft.actor_kind', true) CONSTRAINT ck_aud_pipelines_run_log_started_by_kind CHECK (STARTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    ENDED_BY VARCHAR,
    ENDED_BY_KIND VARCHAR CONSTRAINT ck_aud_pipelines_run_log_ended_by_kind CHECK (ENDED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    REPAIR_PENDING   VARCHAR NOT NULL DEFAULT 'N' CONSTRAINT ck_run_repair_pending CHECK (REPAIR_PENDING IN ('Y','N')),
    CONSTRAINT ck_pipeline_run_trigger_kind CHECK (TRIGGER_KIND IN ('SCHEDULE','MANUAL','BACKFILL','ORCHESTRATOR','STAND_IN')),
    CONSTRAINT ck_pipeline_run_backfill CHECK (BACKFILL IN ('Y','N')),
    CONSTRAINT ck_pipeline_run_status CHECK (STATUS IN ('QUEUED','IN-PROGRESS','SUCCESS','FAILED','SKIPPED','CANCELLED')),
    CONSTRAINT ck_pipeline_run_sla_status CHECK (SLA_STATUS IN ('MET','BREACHED'))
);
-- At most one IN-PROGRESS run per pipeline: this index is what makes run-id resolution safe
-- when several task processes start at once.
CREATE UNIQUE INDEX ux_pipeline_run_one_active
    ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID) WHERE STATUS = 'IN-PROGRESS';
CREATE INDEX ix_pipeline_run_pipeline ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID);
CREATE UNIQUE INDEX ux_pipeline_run_key ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID, RUN_KEY);

COMMENT ON TABLE AUD_PIPELINES_RUN_LOG IS 'One row per pipeline run. Tasks never receive pipeline_run_id; each resolves the pipeline''s one IN-PROGRESS run here.';

-- One row per task per run, updated by each attempt: the latest attempt's status, counts,
-- message and log tail.
CREATE TABLE AUD_TASK_RUN_LOG (
    TASK_RUN_ID      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    TASK_ID          BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    PIPELINE_RUN_ID  BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    START_DATE       TIMESTAMPTZ NOT NULL DEFAULT now(),
    END_DATE         TIMESTAMPTZ,
    STATUS           VARCHAR NOT NULL,
    SOURCE_COUNT     BIGINT,
    TARGET_COUNT     BIGINT,
    INSERT_COUNT     BIGINT,
    UPDATE_COUNT     BIGINT,
    DELETE_COUNT     BIGINT,
    ERROR_MESSAGE    VARCHAR,
    TASK_LOG         VARCHAR,
    ATTEMPT_COUNT    INT NOT NULL DEFAULT 1,
    ROWS_WRITTEN     BIGINT,
    CONSTRAINT ck_task_run_status CHECK (STATUS IN ('IN-PROGRESS','SUCCESS','FAILED','SKIPPED','CANCELLED'))
);
CREATE UNIQUE INDEX ux_task_run_one_per_pipeline_run
    ON AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID);
CREATE INDEX ix_task_run_pipeline_run ON AUD_TASK_RUN_LOG (PIPELINE_RUN_ID);

-- Every change an operator made to a run: a task or a run marked, a stand-in run recorded, a run
-- cancelled or reopened, a task reset to run again or run again, a task run without its
-- dependencies, and a dependency gate bypassed. TASK_ID is NULL for a change to the run.
-- FROM_STATUS and PREVIOUS_MESSAGE keep what the row held before, so nothing is erased;
-- TO_STATUS is NULL for a task reset to not run yet.
CREATE TABLE AUD_RUN_INTERVENTIONS (
    INTERVENTION_ID   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PIPELINE_ID       BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    PIPELINE_RUN_ID   BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    TASK_ID           BIGINT REFERENCES CFG_TASKS(TASK_ID),
    ACTION            VARCHAR(24) NOT NULL,
    FROM_STATUS       VARCHAR(16),
    TO_STATUS         VARCHAR(16),
    TARGET_COUNT      BIGINT,
    PREVIOUS_MESSAGE  VARCHAR,
    REASON            VARCHAR NOT NULL,
    REQUESTED_BY      VARCHAR NOT NULL,
    REQUESTED_AT      TIMESTAMPTZ NOT NULL DEFAULT now(),
    REQUESTED_BY_KIND VARCHAR DEFAULT current_setting('etl_craft.actor_kind', true) CONSTRAINT ck_aud_run_interventions_requested_by_kind CHECK (REQUESTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    CONSTRAINT ck_intervention_action CHECK (ACTION IN ('MARK','NEW_RUN','CANCEL','REOPEN','RESET',
                                                     'GATE_BYPASS','IGNORE_DEPENDENCIES','RERUN'))
);

CREATE INDEX ix_run_interventions_run ON AUD_RUN_INTERVENTIONS (PIPELINE_RUN_ID);

-- Each time a pipeline was paused: while a pause has no RESUMED_AT, `run` starts nothing of the
-- pipeline, and a run in progress starts no more tasks until the pipeline is resumed.
CREATE TABLE AUD_PIPELINE_PAUSES (
    PIPELINE_PAUSE_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PIPELINE_ID        BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    PAUSED_AT          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PAUSED_BY          VARCHAR NOT NULL,
    REASON             VARCHAR NOT NULL,
    RESUMED_AT         TIMESTAMPTZ,
    RESUMED_BY         VARCHAR,
    RESUME_REASON      VARCHAR,
    PAUSED_BY_KIND VARCHAR DEFAULT current_setting('etl_craft.actor_kind', true) CONSTRAINT ck_aud_pipeline_pauses_paused_by_kind CHECK (PAUSED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    RESUMED_BY_KIND VARCHAR CONSTRAINT ck_aud_pipeline_pauses_resumed_by_kind CHECK (RESUMED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'))
);

-- At most one open pause per pipeline.
CREATE UNIQUE INDEX ux_pipeline_pauses_open
    ON AUD_PIPELINE_PAUSES (PIPELINE_ID) WHERE RESUMED_AT IS NULL;
COMMENT ON TABLE AUD_TASK_RUN_LOG IS 'One row per task per pipeline run. A retry updates the row and counts the attempt in ATTEMPT_COUNT; a task already SUCCESS or SKIPPED is not run again.';

-- One row per business rule per task run: whether the rule ran, and how long it took.
CREATE TABLE AUD_BUSINESS_RULES_RUN_LOG (
    BUSINESS_RULE_RUN_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    BUSINESS_RULE_ID      BIGINT NOT NULL REFERENCES CFG_BUSINESS_RULES(BUSINESS_RULE_ID),
    TASK_RUN_ID           BIGINT NOT NULL REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    START_DATE            TIMESTAMPTZ NOT NULL DEFAULT now(),
    END_DATE              TIMESTAMPTZ,
    STATUS                VARCHAR NOT NULL,
    CONSTRAINT ck_br_run_status CHECK (STATUS IN ('IN-PROGRESS','SUCCESS','FAILED','SKIPPED'))
);
CREATE UNIQUE INDEX ux_br_run_one_per_task_run
    ON AUD_BUSINESS_RULES_RUN_LOG (BUSINESS_RULE_ID, TASK_RUN_ID);
COMMENT ON TABLE AUD_BUSINESS_RULES_RUN_LOG IS 'Whether each business rule ran, per task run. What a rule found is in AUD_BUSINESS_RULES_RESULTS.';

-- The rows each rule flagged, by key; a flag is cleared (ACTIVE_FLAG 'N') once its row passes.
CREATE TABLE AUD_BUSINESS_RULES_RESULTS (
    BUSINESS_RULE_RESULT_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    BUSINESS_RULE_RUN_ID     BIGINT NOT NULL REFERENCES AUD_BUSINESS_RULES_RUN_LOG(BUSINESS_RULE_RUN_ID),
    BUSINESS_RULE_ID         BIGINT NOT NULL REFERENCES CFG_BUSINESS_RULES(BUSINESS_RULE_ID),
    BUSINESS_RULE_KEY        VARCHAR NOT NULL,
    TARGET_TABLE             VARCHAR NOT NULL,
    STATUS                   VARCHAR NOT NULL,
    ACTIVE_FLAG              VARCHAR NOT NULL DEFAULT 'Y',
    START_DATE               TIMESTAMPTZ NOT NULL DEFAULT now(),
    END_DATE                 TIMESTAMPTZ,
    CONSTRAINT ck_brresults_status      CHECK (STATUS IN ('INCOMPLETE','REJECT','REPORT')),
    CONSTRAINT ck_brresults_active_flag CHECK (ACTIVE_FLAG IN ('Y','N'))
);
CREATE INDEX ix_brresults_run ON AUD_BUSINESS_RULES_RESULTS (BUSINESS_RULE_RUN_ID);
CREATE INDEX ix_brresults_rule ON AUD_BUSINESS_RULES_RESULTS (BUSINESS_RULE_ID);
COMMENT ON TABLE AUD_BUSINESS_RULES_RESULTS IS 'One row per record a business rule flagged. A record that passes on a later run is cleared: ACTIVE_FLAG = ''N'' and END_DATE set.';

-- Where each incremental ingestion script got to: the offset it reads from next.
CREATE TABLE AUD_TASK_OFFSET_TRACKER (
    TASK_ID                 BIGINT PRIMARY KEY REFERENCES CFG_TASKS(TASK_ID),
    OFFSET_TYPE             VARCHAR NOT NULL,
    OFFSET_VALUE            VARCHAR,
    LAST_UPDATED_TIMESTAMP  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_offset_type CHECK (OFFSET_TYPE IN ('NUMBER','TEXT','TIMESTAMP'))
);
COMMENT ON TABLE AUD_TASK_OFFSET_TRACKER IS 'The watermark an incremental ingestion script reads and advances.';

-- Column lineage traced from each SQL task's SELECT, stored by a hash of what it depends on.
CREATE TABLE AUD_COLUMN_LINEAGE (
    COLUMN_LINEAGE_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    TASK_ID            BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    SOURCE_SQL_HASH    VARCHAR NOT NULL,
    TARGET_OBJECT      VARCHAR NOT NULL,
    TARGET_COLUMN      VARCHAR NOT NULL,
    SOURCE_OBJECT      VARCHAR,
    SOURCE_COLUMN      VARCHAR,
    TRANSFORMATION     VARCHAR,
    COMPUTED_AT        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_column_lineage_task ON AUD_COLUMN_LINEAGE (TASK_ID, SOURCE_SQL_HASH);
CREATE INDEX ix_column_lineage_target ON AUD_COLUMN_LINEAGE (TARGET_OBJECT, TARGET_COLUMN);
CREATE INDEX ix_column_lineage_source ON AUD_COLUMN_LINEAGE (SOURCE_OBJECT, SOURCE_COLUMN);
COMMENT ON TABLE AUD_COLUMN_LINEAGE IS 'Column lineage parsed from each SQL task''s SOURCE_SQL, keyed by a hash of what it was parsed from. A row whose hash no longer matches is replaced, never read.';

-- The URL the catalog site is published at, one row per URL: the latest is current. A publish
-- that is given another URL fails unless told to accept it, so shared links keep working.
CREATE TABLE AUD_DOCS_PUBLICATION (
    DOCS_PUBLICATION_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PUBLISHED_URL        VARCHAR NOT NULL,
    FIRST_PUBLISHED      TIMESTAMPTZ NOT NULL DEFAULT now(),
    LAST_PUBLISHED       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The versions of each task's DOCUMENTATION, one row per change.
CREATE TABLE AUD_TASK_DOCUMENTATION (
    TASK_DOCUMENTATION_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    TASK_ID                BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    VERSION                INT NOT NULL,
    DOCUMENTATION_HASH     VARCHAR NOT NULL,
    DOCUMENTATION          VARCHAR NOT NULL,
    RECORDED_AT            TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ux_task_documentation_version UNIQUE (TASK_ID, VERSION)
);
CREATE INDEX ix_task_documentation_task ON AUD_TASK_DOCUMENTATION (TASK_ID, VERSION DESC);
COMMENT ON TABLE AUD_TASK_DOCUMENTATION IS 'Each version of a task''s DOCUMENTATION parameter. A new version is recorded only when the text changes.';

-- Every upstream run a downstream consumed, one row each, never updated: for a dependency on a
-- pipeline, the downstream run and the upstream run; for a dependency on another pipeline's task,
-- the downstream task and the upstream task run. A dependency's latest row is what it last
-- consumed: an upstream run satisfies it only when it is newer than that one. PIPELINE_RUN_ID is
-- NULL on the rows carried over from the trackers this table replaced.
CREATE TABLE AUD_DEPENDENCY_CONSUMPTION (
    CONSUMPTION_ID            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PIPELINE_DEPENDENCY_ID    BIGINT REFERENCES CFG_PIPELINE_DEPENDENCY(PIPELINE_DEPENDENCY_ID),
    TASK_DEPENDENCY_ID        BIGINT REFERENCES CFG_TASK_DEPENDENCY(TASK_DEPENDENCY_ID),
    PIPELINE_ID               BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    PIPELINE_RUN_ID           BIGINT REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    TASK_ID                   BIGINT REFERENCES CFG_TASKS(TASK_ID),
    DEPENDS_ON_PIPELINE_ID    BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    CONSUMED_PIPELINE_RUN_ID  BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    CONSUMED_TASK_RUN_ID      BIGINT REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    CONSUMED_AT               TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSUMED_REVISION INT NOT NULL DEFAULT 1,
    CONSTRAINT ck_consumption_one_dependency CHECK (
        (PIPELINE_DEPENDENCY_ID IS NULL) <> (TASK_DEPENDENCY_ID IS NULL)
    )
);

CREATE INDEX ix_consumption_pipeline_dependency
    ON AUD_DEPENDENCY_CONSUMPTION (PIPELINE_DEPENDENCY_ID, CONSUMPTION_ID);
CREATE INDEX ix_consumption_task_dependency
    ON AUD_DEPENDENCY_CONSUMPTION (TASK_DEPENDENCY_ID, CONSUMPTION_ID);
CREATE INDEX ix_consumption_run ON AUD_DEPENDENCY_CONSUMPTION (PIPELINE_RUN_ID);

-- Every migration applied, from etl-craft (ENGINE) and from the project (PROJECT), with its
-- checksum.
CREATE TABLE SCHEMA_MIGRATIONS (
    SOURCE      VARCHAR NOT NULL,
    VERSION     VARCHAR NOT NULL,
    CHECKSUM    VARCHAR(64) NOT NULL,
    APPLIED_AT  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_schema_migrations PRIMARY KEY (SOURCE, VERSION),
    CONSTRAINT ck_schema_migrations_source CHECK (SOURCE IN ('ENGINE', 'PROJECT')),
    CONSTRAINT ck_schema_migrations_checksum CHECK (CHECKSUM ~ '^[0-9a-f]{64}$')
);
COMMENT ON TABLE SCHEMA_MIGRATIONS IS 'Every migration file applied: the packaged ENGINE stream and the team''s own PROJECT stream, each with the SHA-256 of the file as applied.';

-- Attempt history and the dependencies judged at admission.
CREATE TABLE AUD_TASK_ATTEMPTS (
    ATTEMPT_ID       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    TASK_RUN_ID      BIGINT NOT NULL REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    ATTEMPT_NUMBER   INT NOT NULL,
    STATUS           VARCHAR NOT NULL,
    OWNER_ID         VARCHAR,
    LEASE_EXPIRES_AT TIMESTAMPTZ,
    HEARTBEAT_AT     TIMESTAMPTZ,
    QUEUED_AT        TIMESTAMPTZ,
    CLAIMED_AT       TIMESTAMPTZ,
    STARTED_AT       TIMESTAMPTZ,
    ENDED_AT         TIMESTAMPTZ,
    HOST             VARCHAR,
    PID              INT,
    PROCESS_START    VARCHAR,
    EXIT_CODE        INT,
    SOURCE_COUNT     BIGINT,
    TARGET_COUNT     BIGINT,
    INSERT_COUNT     BIGINT,
    UPDATE_COUNT     BIGINT,
    DELETE_COUNT     BIGINT,
    ROWS_WRITTEN     BIGINT,
    ERROR_MESSAGE    VARCHAR,
    TASK_LOG         VARCHAR,
    LOG_PATH         VARCHAR,
    REQUESTED_BY     VARCHAR DEFAULT current_setting('etl_craft.actor', true),
    REQUESTED_BY_KIND VARCHAR DEFAULT current_setting('etl_craft.actor_kind', true) CONSTRAINT ck_aud_task_attempts_requested_by_kind CHECK (REQUESTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    CONSTRAINT ck_attempt_status CHECK (STATUS IN ('QUEUED','CLAIMED','RUNNING','SUCCESS','FAILED','TIMED_OUT','CANCELLED','LOST'))
);
CREATE UNIQUE INDEX ux_attempt_number ON AUD_TASK_ATTEMPTS (TASK_RUN_ID, ATTEMPT_NUMBER);
CREATE UNIQUE INDEX ux_attempt_one_active ON AUD_TASK_ATTEMPTS (TASK_RUN_ID)
    WHERE STATUS IN ('QUEUED','CLAIMED','RUNNING');
CREATE INDEX ix_attempt_status_lease ON AUD_TASK_ATTEMPTS (STATUS, LEASE_EXPIRES_AT);

-- Each dependency judged at admission, with the selected upstream revision and reason.
CREATE TABLE AUD_GATE_DECISIONS (
    DECISION_ID              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PIPELINE_RUN_ID          BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    ATTEMPT_ID               BIGINT REFERENCES AUD_TASK_ATTEMPTS(ATTEMPT_ID),
    PIPELINE_DEPENDENCY_ID   BIGINT REFERENCES CFG_PIPELINE_DEPENDENCY(PIPELINE_DEPENDENCY_ID),
    TASK_DEPENDENCY_ID       BIGINT REFERENCES CFG_TASK_DEPENDENCY(TASK_DEPENDENCY_ID),
    SELECTED_PIPELINE_RUN_ID BIGINT REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    SELECTED_TASK_RUN_ID     BIGINT REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    SELECTED_REVISION        INT,
    RESULT                   VARCHAR NOT NULL,
    REASON                   VARCHAR NOT NULL,
    DECIDED_AT               TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_gate_one_dependency CHECK (
        (PIPELINE_DEPENDENCY_ID IS NOT NULL AND TASK_DEPENDENCY_ID IS NULL)
        OR (PIPELINE_DEPENDENCY_ID IS NULL AND TASK_DEPENDENCY_ID IS NOT NULL)
    ),
    CONSTRAINT ck_gate_result CHECK (RESULT IN ('SATISFIED','UNSATISFIED','BYPASSED'))
);

-- An immutable record of a command request; run and attempt outcomes live in their own logs.
CREATE TABLE AUD_ACTIONS (
    ACTION_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    STARTED_AT TIMESTAMPTZ NOT NULL DEFAULT now(),
    ENDED_AT TIMESTAMPTZ NOT NULL DEFAULT now(),
    ACTOR VARCHAR NOT NULL,
    ACTOR_KIND VARCHAR NOT NULL,
    HOST VARCHAR NOT NULL,
    COMMAND VARCHAR NOT NULL,
    ARGUMENTS JSONB NOT NULL,
    PIPELINE_ID BIGINT REFERENCES CFG_PIPELINES(PIPELINE_ID),
    PIPELINE_RUN_ID BIGINT REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    TASK_ID BIGINT REFERENCES CFG_TASKS(TASK_ID),
    OUTCOME VARCHAR NOT NULL,
    EXIT_CODE INT,
    CONSTRAINT ck_actions_actor_kind CHECK (ACTOR_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'))
);
CREATE INDEX ix_actions_pipeline ON AUD_ACTIONS (PIPELINE_ID, STARTED_AT);

-- Every metadata row change with its before/after values and the project migration that made it.
CREATE TABLE AUD_METADATA_CHANGES (
    CHANGE_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    CHANGED_AT TIMESTAMPTZ NOT NULL DEFAULT now(),
    ACTOR VARCHAR NOT NULL,
    ACTOR_KIND VARCHAR NOT NULL,
    TABLE_NAME VARCHAR NOT NULL,
    ROW_KEY VARCHAR NOT NULL,
    OPERATION VARCHAR NOT NULL,
    BEFORE_JSON JSONB,
    AFTER_JSON JSONB,
    MIGRATION VARCHAR,
    CONSTRAINT ck_metadata_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_metadata_actor_kind CHECK (ACTOR_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'))
);
CREATE INDEX ix_metadata_changes_table ON AUD_METADATA_CHANGES (TABLE_NAME, CHANGED_AT);

-- The canonical hash version published after a target warehouse update commits.
CREATE TABLE AUD_TARGET_HASH_VERSION (
    TARGET_OBJECT VARCHAR PRIMARY KEY,
    HASH_VERSION INTEGER NOT NULL CONSTRAINT ck_target_hash_version CHECK (HASH_VERSION IN (1,2)),
    RECOMPUTED_AT TIMESTAMPTZ NOT NULL
);

-- Require the transaction marker before changing protected rows or truncating a table.
CREATE OR REPLACE FUNCTION etl_craft_guard() RETURNS trigger AS $$
BEGIN
    IF COALESCE(current_setting('etl_craft.actor', true), '') = '' THEN
        RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is written only by etl-craft; change runs with etl-craft mark, cancel or run, and metadata with a project migration (etl-craft migrate)';
    END IF;
    IF COALESCE(current_setting('etl_craft.purpose', true), '') <> 'retention' AND TG_OP IN ('UPDATE','DELETE','TRUNCATE') THEN
        IF TG_TABLE_NAME IN ('aud_run_interventions','aud_actions','aud_metadata_changes','aud_dependency_consumption','aud_gate_decisions') THEN
            RAISE EXCEPTION USING MESSAGE = upper(TG_TABLE_NAME) || ' is append-only; use etl-craft retention';
        END IF;
        IF TG_TABLE_NAME = 'aud_task_attempts' THEN
            IF TG_OP = 'TRUNCATE' THEN
                RAISE EXCEPTION 'AUD_TASK_ATTEMPTS terminal rows are immutable; queue a new attempt';
            ELSIF OLD.STATUS NOT IN ('QUEUED','CLAIMED','RUNNING') THEN
                RAISE EXCEPTION 'AUD_TASK_ATTEMPTS terminal rows are immutable; queue a new attempt';
            END IF;
        END IF;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION etl_craft_metadata_change() RETURNS trigger AS $$
DECLARE
    before_row jsonb;
    after_row jsonb;
    key_value text;
BEGIN
    IF TG_OP <> 'INSERT' THEN before_row := to_jsonb(OLD); END IF;
    IF TG_OP <> 'DELETE' THEN after_row := to_jsonb(NEW); END IF;
    IF TG_OP = 'UPDATE' AND before_row = after_row THEN RETURN NEW; END IF;
    key_value := COALESCE(after_row, before_row)->>TG_ARGV[0];
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION)
    VALUES (current_setting('etl_craft.actor'), current_setting('etl_craft.actor_kind'), upper(TG_TABLE_NAME), key_value, TG_OP, before_row, after_row, NULLIF(current_setting('etl_craft.migration', true), ''));
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_actor_guard_cfg_pipelines BEFORE INSERT OR UPDATE OR DELETE ON CFG_PIPELINES FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_pipelines BEFORE TRUNCATE ON CFG_PIPELINES FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_pipelines AFTER INSERT OR UPDATE OR DELETE ON CFG_PIPELINES FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('pipeline_id');
CREATE TRIGGER trg_actor_guard_cfg_pipeline_dependency BEFORE INSERT OR UPDATE OR DELETE ON CFG_PIPELINE_DEPENDENCY FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_pipeline_dependency BEFORE TRUNCATE ON CFG_PIPELINE_DEPENDENCY FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_pipeline_dependency AFTER INSERT OR UPDATE OR DELETE ON CFG_PIPELINE_DEPENDENCY FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('pipeline_dependency_id');
CREATE TRIGGER trg_actor_guard_cfg_tasks BEFORE INSERT OR UPDATE OR DELETE ON CFG_TASKS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_tasks BEFORE TRUNCATE ON CFG_TASKS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_tasks AFTER INSERT OR UPDATE OR DELETE ON CFG_TASKS FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('task_id');
CREATE TRIGGER trg_actor_guard_cfg_task_dependency BEFORE INSERT OR UPDATE OR DELETE ON CFG_TASK_DEPENDENCY FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_task_dependency BEFORE TRUNCATE ON CFG_TASK_DEPENDENCY FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_task_dependency AFTER INSERT OR UPDATE OR DELETE ON CFG_TASK_DEPENDENCY FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('task_dependency_id');
CREATE TRIGGER trg_actor_guard_cfg_task_parameters BEFORE INSERT OR UPDATE OR DELETE ON CFG_TASK_PARAMETERS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_task_parameters BEFORE TRUNCATE ON CFG_TASK_PARAMETERS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_task_parameters AFTER INSERT OR UPDATE OR DELETE ON CFG_TASK_PARAMETERS FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('task_parameter_id');
CREATE TRIGGER trg_actor_guard_cfg_business_rules BEFORE INSERT OR UPDATE OR DELETE ON CFG_BUSINESS_RULES FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_cfg_business_rules BEFORE TRUNCATE ON CFG_BUSINESS_RULES FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_changes_cfg_business_rules AFTER INSERT OR UPDATE OR DELETE ON CFG_BUSINESS_RULES FOR EACH ROW EXECUTE FUNCTION etl_craft_metadata_change('business_rule_id');
CREATE TRIGGER trg_actor_guard_aud_pipelines_run_log BEFORE INSERT OR UPDATE OR DELETE ON AUD_PIPELINES_RUN_LOG FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_pipelines_run_log BEFORE TRUNCATE ON AUD_PIPELINES_RUN_LOG FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_task_run_log BEFORE INSERT OR UPDATE OR DELETE ON AUD_TASK_RUN_LOG FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_task_run_log BEFORE TRUNCATE ON AUD_TASK_RUN_LOG FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_run_interventions BEFORE INSERT OR UPDATE OR DELETE ON AUD_RUN_INTERVENTIONS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_run_interventions BEFORE TRUNCATE ON AUD_RUN_INTERVENTIONS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_pipeline_pauses BEFORE INSERT OR UPDATE OR DELETE ON AUD_PIPELINE_PAUSES FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_pipeline_pauses BEFORE TRUNCATE ON AUD_PIPELINE_PAUSES FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_business_rules_run_log BEFORE INSERT OR UPDATE OR DELETE ON AUD_BUSINESS_RULES_RUN_LOG FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_business_rules_run_log BEFORE TRUNCATE ON AUD_BUSINESS_RULES_RUN_LOG FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_business_rules_results BEFORE INSERT OR UPDATE OR DELETE ON AUD_BUSINESS_RULES_RESULTS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_business_rules_results BEFORE TRUNCATE ON AUD_BUSINESS_RULES_RESULTS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_task_offset_tracker BEFORE INSERT OR UPDATE OR DELETE ON AUD_TASK_OFFSET_TRACKER FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_task_offset_tracker BEFORE TRUNCATE ON AUD_TASK_OFFSET_TRACKER FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_column_lineage BEFORE INSERT OR UPDATE OR DELETE ON AUD_COLUMN_LINEAGE FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_column_lineage BEFORE TRUNCATE ON AUD_COLUMN_LINEAGE FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_docs_publication BEFORE INSERT OR UPDATE OR DELETE ON AUD_DOCS_PUBLICATION FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_docs_publication BEFORE TRUNCATE ON AUD_DOCS_PUBLICATION FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_task_documentation BEFORE INSERT OR UPDATE OR DELETE ON AUD_TASK_DOCUMENTATION FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_task_documentation BEFORE TRUNCATE ON AUD_TASK_DOCUMENTATION FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_dependency_consumption BEFORE INSERT OR UPDATE OR DELETE ON AUD_DEPENDENCY_CONSUMPTION FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_dependency_consumption BEFORE TRUNCATE ON AUD_DEPENDENCY_CONSUMPTION FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_task_attempts BEFORE INSERT OR UPDATE OR DELETE ON AUD_TASK_ATTEMPTS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_task_attempts BEFORE TRUNCATE ON AUD_TASK_ATTEMPTS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_gate_decisions BEFORE INSERT OR UPDATE OR DELETE ON AUD_GATE_DECISIONS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_gate_decisions BEFORE TRUNCATE ON AUD_GATE_DECISIONS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_actions BEFORE INSERT OR UPDATE OR DELETE ON AUD_ACTIONS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_actions BEFORE TRUNCATE ON AUD_ACTIONS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_actor_guard_aud_metadata_changes BEFORE INSERT OR UPDATE OR DELETE ON AUD_METADATA_CHANGES FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_metadata_changes BEFORE TRUNCATE ON AUD_METADATA_CHANGES FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();

-- Fill attribution when the engine inserts an action-bearing row.
CREATE OR REPLACE FUNCTION etl_craft_attribute() RETURNS trigger AS $$
BEGIN
    IF TG_TABLE_NAME = 'aud_pipelines_run_log' THEN
        IF TG_OP = 'INSERT' THEN
            NEW.STARTED_BY := COALESCE(NEW.STARTED_BY, current_setting('etl_craft.actor'));
            NEW.STARTED_BY_KIND := COALESCE(NEW.STARTED_BY_KIND, current_setting('etl_craft.actor_kind'));
        END IF;
        IF NEW.END_DATE IS NOT NULL AND (TG_OP = 'INSERT' OR OLD.END_DATE IS NULL) THEN
            NEW.ENDED_BY := COALESCE(NEW.ENDED_BY, current_setting('etl_craft.actor'));
            NEW.ENDED_BY_KIND := COALESCE(NEW.ENDED_BY_KIND, current_setting('etl_craft.actor_kind'));
        END IF;
    ELSIF TG_TABLE_NAME IN ('aud_task_attempts','aud_run_interventions') AND TG_OP = 'INSERT' THEN
        NEW.REQUESTED_BY := COALESCE(NEW.REQUESTED_BY, current_setting('etl_craft.actor'));
        NEW.REQUESTED_BY_KIND := COALESCE(NEW.REQUESTED_BY_KIND, current_setting('etl_craft.actor_kind'));
    ELSIF TG_TABLE_NAME = 'aud_pipeline_pauses' THEN
        IF TG_OP = 'INSERT' THEN
            NEW.PAUSED_BY_KIND := COALESCE(NEW.PAUSED_BY_KIND, current_setting('etl_craft.actor_kind'));
        END IF;
        IF NEW.RESUMED_AT IS NOT NULL AND (TG_OP = 'INSERT' OR OLD.RESUMED_AT IS NULL) THEN
            NEW.RESUMED_BY_KIND := COALESCE(NEW.RESUMED_BY_KIND, current_setting('etl_craft.actor_kind'));
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_attribute_aud_pipelines_run_log BEFORE INSERT OR UPDATE ON AUD_PIPELINES_RUN_LOG FOR EACH ROW EXECUTE FUNCTION etl_craft_attribute();
CREATE TRIGGER trg_attribute_aud_task_attempts BEFORE INSERT OR UPDATE ON AUD_TASK_ATTEMPTS FOR EACH ROW EXECUTE FUNCTION etl_craft_attribute();
CREATE TRIGGER trg_attribute_aud_run_interventions BEFORE INSERT OR UPDATE ON AUD_RUN_INTERVENTIONS FOR EACH ROW EXECUTE FUNCTION etl_craft_attribute();
CREATE TRIGGER trg_attribute_aud_pipeline_pauses BEFORE INSERT OR UPDATE ON AUD_PIPELINE_PAUSES FOR EACH ROW EXECUTE FUNCTION etl_craft_attribute();

CREATE TRIGGER trg_actor_guard_aud_target_hash_version BEFORE INSERT OR UPDATE OR DELETE ON AUD_TARGET_HASH_VERSION FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_target_hash_version BEFORE TRUNCATE ON AUD_TARGET_HASH_VERSION FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();

-- Overseer process history; the session lock is the source of leadership.
CREATE TABLE AUD_OVERSEERS (
    OVERSEER_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    HOST VARCHAR NOT NULL,
    PID INTEGER NOT NULL CHECK (PID > 0),
    VERSION VARCHAR NOT NULL,
    STARTED_AT TIMESTAMPTZ NOT NULL,
    HEARTBEAT_AT TIMESTAMPTZ NOT NULL,
    STOPPED_AT TIMESTAMPTZ,
    STARTED_BY VARCHAR NOT NULL,
    STARTED_BY_KIND VARCHAR NOT NULL,
    STOPPED_BY VARCHAR,
    STOPPED_BY_KIND VARCHAR
);
COMMENT ON TABLE AUD_OVERSEERS IS 'Overseer process history; the session lock owns leadership and unclosed history does not block a replacement.';
CREATE INDEX ix_overseer_unclosed ON AUD_OVERSEERS (OVERSEER_ID) WHERE STOPPED_AT IS NULL;
CREATE TRIGGER trg_actor_guard_aud_overseers BEFORE INSERT OR UPDATE OR DELETE ON AUD_OVERSEERS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_overseers BEFORE TRUNCATE ON AUD_OVERSEERS FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
CREATE OR REPLACE FUNCTION etl_craft_notify_execution() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('etl_craft_events', current_schema());
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_notify_aud_pipelines_run_log AFTER INSERT OR UPDATE OR DELETE ON aud_pipelines_run_log FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_notify_execution();
CREATE TRIGGER trg_notify_aud_task_attempts AFTER INSERT OR UPDATE OR DELETE ON aud_task_attempts FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_notify_execution();
CREATE TRIGGER trg_notify_aud_pipeline_pauses AFTER INSERT OR UPDATE OR DELETE ON aud_pipeline_pauses FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_notify_execution();

CREATE INDEX ix_pipeline_schedule_key ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID, RUN_KEY) WHERE TRIGGER_KIND = 'SCHEDULE';

-- Durable gate polling budget; TASK_ID is NULL for pipeline admission and NEXT_CHECK_AT is NULL when settled.
CREATE TABLE AUD_GATE_WAITS (
    PIPELINE_RUN_ID BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    TASK_ID BIGINT REFERENCES CFG_TASKS(TASK_ID),
    FIRST_CHECK_AT TIMESTAMPTZ NOT NULL,
    NEXT_CHECK_AT TIMESTAMPTZ,
    LOOKS INT NOT NULL,
    WAIT_UNTIL TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_gate_wait_task CHECK (TASK_ID IS NULL OR TASK_ID > 0),
    CONSTRAINT ck_gate_wait_looks CHECK (LOOKS >= 0 AND LOOKS <= 30)
);
CREATE UNIQUE INDEX ux_gate_wait ON AUD_GATE_WAITS (PIPELINE_RUN_ID, COALESCE(TASK_ID,0));
CREATE TRIGGER trg_actor_guard_aud_gate_waits BEFORE INSERT OR UPDATE OR DELETE
ON AUD_GATE_WAITS FOR EACH ROW EXECUTE FUNCTION etl_craft_guard();
CREATE TRIGGER trg_truncate_aud_gate_waits BEFORE TRUNCATE ON AUD_GATE_WAITS
FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_guard();
