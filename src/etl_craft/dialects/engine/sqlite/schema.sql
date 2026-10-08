-- etl-craft Engine DB schema for SQLite.
--
-- The same tables and columns as the PostgreSQL schema, so every Engine DB query reads the same
-- shape from either. `etl-craft init-db` applies it to an empty database in one transaction.
-- Where SQLite differs:
--
--   * Identity columns are INTEGER PRIMARY KEY AUTOINCREMENT, so an id is never reused: the
--     dependency trackers compare runs by id.
--   * Timestamps are TIMESTAMP holding UTC text in one fixed format
--     ('YYYY-MM-DD HH:MM:SS.ffffff+00:00'), so text comparison orders them. The connection
--     writes and reads that format; the column defaults produce it too.
--   * PIPELINE_PARAMETERS is TEXT holding JSON, checked with json_valid.
--   * SQLite actor functions supply the identity; triggers stamp CREATED_BY and UPDATED_BY.
--     AFTER UPDATE triggers keep CREATED_BY and CREATE_DATE unchanged and stamp UPDATED_DATE,
--     since SQLite triggers cannot assign to NEW; their own UPDATE does not fire them again.
--   * A task dependency without DEPENDS_ON_PIPELINE_ID is filled in by an AFTER trigger, and
--     ck_taskdep_no_self_dep is checked again when it does.
--   * There is no COMMENT ON; the PostgreSQL schema carries the table comments.
--
-- Trigger bodies contain semicolons; the dialect splits this file with SQLite's own
-- complete_statement, which keeps each BEGIN ... END; whole.

CREATE TABLE CFG_PIPELINES (
    PIPELINE_ID          INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_CODE        VARCHAR NOT NULL,
    PIPELINE_NAME        VARCHAR NOT NULL,
    DESCRIPTION          VARCHAR,
    RUN_SCHEDULE         VARCHAR,
    SLA_IN_HOURS         NUMERIC,
    REFRESH_TYPE         VARCHAR NOT NULL,
    ACTIVE_FLAG          VARCHAR NOT NULL DEFAULT 'Y',
    PIPELINE_PARAMETERS  TEXT,
    CREATED_BY           VARCHAR DEFAULT 'etl-craft',
    CREATE_DATE          TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    UPDATED_BY           VARCHAR DEFAULT 'etl-craft',
    UPDATED_DATE         TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    SCHEDULE_TIMEZONE VARCHAR,
    CATCHUP VARCHAR(1) NOT NULL DEFAULT 'N' CONSTRAINT ck_schedule_catchup CHECK (CATCHUP IN ('Y','N')),
    MAX_CATCHUP_RUNS INT NOT NULL DEFAULT 1 CONSTRAINT ck_schedule_catchup_runs CHECK (MAX_CATCHUP_RUNS > 0 AND typeof(MAX_CATCHUP_RUNS) = 'integer'),
    OVERLAP_POLICY VARCHAR NOT NULL DEFAULT 'SKIP' CONSTRAINT ck_schedule_overlap CHECK (OVERLAP_POLICY IN ('SKIP','QUEUE')),
    SCHEDULE_START_DATE DATE,
    CONSTRAINT ck_pipelines_code CHECK (PIPELINE_CODE GLOB '[A-Za-z]*' AND PIPELINE_CODE NOT GLOB '*[^A-Za-z0-9_]*' AND length(PIPELINE_CODE) <= 128 AND instr(PIPELINE_CODE, char(0)) = 0),
    CONSTRAINT ck_pipelines_refresh_type CHECK (REFRESH_TYPE IN ('FULL', 'INCREMENTAL')),
    CONSTRAINT ck_pipelines_active_flag  CHECK (ACTIVE_FLAG IN ('Y', 'N')),
    CONSTRAINT ck_pipelines_parameters_json
        CHECK (PIPELINE_PARAMETERS IS NULL OR json_valid(PIPELINE_PARAMETERS))
);

CREATE UNIQUE INDEX ux_pipelines_code_active
    ON CFG_PIPELINES (PIPELINE_CODE) WHERE ACTIVE_FLAG = 'Y';



-- A pipeline waiting on another: a new run starts only when the upstream's last finished run
-- satisfies DEPENDENCY_TYPE.
CREATE TABLE CFG_PIPELINE_DEPENDENCY (
    PIPELINE_DEPENDENCY_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID             BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    DEPENDS_ON_PIPELINE_ID  BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    DEPENDENCY_TYPE         VARCHAR NOT NULL,
    ACTIVE_FLAG             VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY              VARCHAR DEFAULT 'etl-craft',
    CREATE_DATE             TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    UPDATED_BY              VARCHAR DEFAULT 'etl-craft',
    UPDATED_DATE            TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSUME_REPAIRS         VARCHAR(1) NOT NULL DEFAULT 'Y' CONSTRAINT ck_pipedep_consume_repairs CHECK (CONSUME_REPAIRS IN ('Y','N')),
    CONSTRAINT ck_pipedep_type        CHECK (DEPENDENCY_TYPE IN ('SUCCESS','FAILURE','ALWAYS','HAS_DATA')),
    CONSTRAINT ck_pipedep_active_flag CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_pipedep_no_self_dep CHECK (PIPELINE_ID <> DEPENDS_ON_PIPELINE_ID)
);

CREATE UNIQUE INDEX ux_pipedep_edge_active
    ON CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE)
    WHERE ACTIVE_FLAG = 'Y';
CREATE INDEX ix_pipedep_depends_on ON CFG_PIPELINE_DEPENDENCY (DEPENDS_ON_PIPELINE_ID);



-- One row per task: its pipeline, handler and run condition.
CREATE TABLE CFG_TASKS (
    TASK_ID              INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_CODE            VARCHAR NOT NULL,
    TASK_TYPE            VARCHAR NOT NULL,
    PIPELINE_ID          BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    HANDLER              VARCHAR NOT NULL,
    RUN_CONDITION        VARCHAR,
    RUN_CONDITION_COUNT  INT,
    ACTIVE_FLAG          VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY           VARCHAR DEFAULT 'etl-craft',
    CREATE_DATE          TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    UPDATED_BY           VARCHAR DEFAULT 'etl-craft',
    UPDATED_DATE         TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSTRAINT ck_tasks_code CHECK (TASK_CODE GLOB '[A-Za-z]*' AND TASK_CODE NOT GLOB '*[^A-Za-z0-9_]*' AND length(TASK_CODE) <= 128 AND instr(TASK_CODE, char(0)) = 0),
    CONSTRAINT ck_tasks_task_type     CHECK (TASK_TYPE IN ('INGESTION','ETL')),
    CONSTRAINT ck_tasks_handler       CHECK (HANDLER IN ('PYTHON','SQL','BUSINESS_RULES','EMAIL_ALERT')),
    CONSTRAINT ck_tasks_active_flag   CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_tasks_run_condition CHECK (RUN_CONDITION IS NULL OR RUN_CONDITION IN ('ALL','ANY','N')),
    CONSTRAINT ck_tasks_run_condition_count CHECK (
        (COALESCE(RUN_CONDITION, '') = 'N' AND RUN_CONDITION_COUNT IS NOT NULL
            AND RUN_CONDITION_COUNT >= 1)
        OR (RUN_CONDITION IS NOT 'N' AND RUN_CONDITION_COUNT IS NULL)
    )
);

CREATE UNIQUE INDEX ux_tasks_code_active
    ON CFG_TASKS (PIPELINE_ID, TASK_CODE) WHERE ACTIVE_FLAG = 'Y';



-- A task waiting on another, in its own pipeline or, with DEPENDS_ON_PIPELINE_ID, in another.
CREATE TABLE CFG_TASK_DEPENDENCY (
    TASK_DEPENDENCY_ID      INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID             BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    TASK_ID                 BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    DEPENDS_ON_PIPELINE_ID  BIGINT REFERENCES CFG_PIPELINES(PIPELINE_ID),
    DEPENDS_ON_TASK_ID      BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    DEPENDENCY_TYPE         VARCHAR NOT NULL,
    ACTIVE_FLAG             VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY              VARCHAR DEFAULT 'etl-craft',
    CREATE_DATE             TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    UPDATED_BY              VARCHAR DEFAULT 'etl-craft',
    UPDATED_DATE            TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSUME_REPAIRS         VARCHAR(1) NOT NULL DEFAULT 'Y' CONSTRAINT ck_taskdep_consume_repairs CHECK (CONSUME_REPAIRS IN ('Y','N')),
    CONSTRAINT ck_taskdep_type        CHECK (DEPENDENCY_TYPE IN ('SUCCESS','FAILURE','ALWAYS','HAS_DATA')),
    CONSTRAINT ck_taskdep_active_flag CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_taskdep_no_self_dep CHECK (NOT (TASK_ID = DEPENDS_ON_TASK_ID AND PIPELINE_ID = DEPENDS_ON_PIPELINE_ID))
);

CREATE TRIGGER trg_default_taskdep_pipeline_insert AFTER INSERT ON CFG_TASK_DEPENDENCY
WHEN NEW.DEPENDS_ON_PIPELINE_ID IS NULL
BEGIN
    UPDATE CFG_TASK_DEPENDENCY SET DEPENDS_ON_PIPELINE_ID = NEW.PIPELINE_ID
    WHERE TASK_DEPENDENCY_ID = NEW.TASK_DEPENDENCY_ID;
END;



CREATE UNIQUE INDEX ux_taskdep_edge_active
    ON CFG_TASK_DEPENDENCY (TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE)
    WHERE ACTIVE_FLAG = 'Y';
CREATE INDEX ix_taskdep_depends_on ON CFG_TASK_DEPENDENCY (DEPENDS_ON_TASK_ID);

-- A task's settings, as name and value rows; each handler reads its own names.
CREATE TABLE CFG_TASK_PARAMETERS (
    TASK_PARAMETER_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_ID            BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    PARAMETER_NAME     VARCHAR NOT NULL,
    PARAMETER_VALUE    VARCHAR,
    ACTIVE_FLAG        VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY         VARCHAR DEFAULT 'etl-craft',
    CREATE_DATE        TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    UPDATED_BY         VARCHAR DEFAULT 'etl-craft',
    UPDATED_DATE       TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSTRAINT ck_taskparameters_active_flag CHECK (ACTIVE_FLAG IN ('Y','N'))
);

CREATE UNIQUE INDEX ux_taskparameters_name_active
    ON CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME) WHERE ACTIVE_FLAG = 'Y';



-- A BUSINESS_RULES task's rules: a correlated SELECT that finds the rows of TARGET_TABLE that
-- break it, and what a failing row gets flagged as.
CREATE TABLE CFG_BUSINESS_RULES (
    BUSINESS_RULE_ID          INTEGER PRIMARY KEY AUTOINCREMENT,
    BUSINESS_RULE_NAME        VARCHAR NOT NULL,
    PIPELINE_ID               BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    TASK_ID                   BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    BUSINESS_RULE_SQL         VARCHAR NOT NULL,
    BUSINESS_RULE_TYPE        VARCHAR NOT NULL,
    BUSINESS_RULE_KEY_COLUMN  VARCHAR NOT NULL,
    TARGET_TABLE              VARCHAR NOT NULL,
    SEQUENCE_NUMBER           BIGINT NOT NULL,
    ACTIVE_FLAG               VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY                VARCHAR DEFAULT 'etl-craft',
    CREATE_DATE               TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    UPDATED_BY                VARCHAR DEFAULT 'etl-craft',
    UPDATED_DATE              TIMESTAMP DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSTRAINT ck_br_type        CHECK (BUSINESS_RULE_TYPE IN ('INCOMPLETE','REJECT','REPORT')),
    CONSTRAINT ck_br_active_flag CHECK (ACTIVE_FLAG IN ('Y','N'))
);

CREATE UNIQUE INDEX ux_br_name_active
    ON CFG_BUSINESS_RULES (TASK_ID, BUSINESS_RULE_NAME) WHERE ACTIVE_FLAG = 'Y';
CREATE INDEX ix_br_pipeline ON CFG_BUSINESS_RULES (PIPELINE_ID);



-- One row per pipeline run: its status, when it ran, whether it met its SLA, the date it ran
-- as of (RUN_DATE, the SQL tasks' $$run_date), and whether it was part of a backfill.
CREATE TABLE AUD_PIPELINES_RUN_LOG (
    PIPELINE_RUN_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID      BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    START_DATE       TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    END_DATE         TIMESTAMP,
    STATUS           VARCHAR NOT NULL,
    SLA_STATUS       VARCHAR(8),
    RUN_DATE         DATE,
    BACKFILL         VARCHAR(1) NOT NULL DEFAULT 'N',
    RUN_KEY          VARCHAR NOT NULL DEFAULT ('manual:' || lower(hex(randomblob(16)))),
    TRIGGER_KIND     VARCHAR NOT NULL DEFAULT 'MANUAL',
    OWNER_ID         VARCHAR,
    LEASE_EXPIRES_AT TIMESTAMP,
    OUTPUT_REVISION  INT NOT NULL DEFAULT 1,
    CONFIG_SHA256    VARCHAR(64),
    STARTED_BY VARCHAR DEFAULT (etl_craft_actor()),
    STARTED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_pipelines_run_log_started_by_kind CHECK (STARTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
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


-- One row per task per run, updated by each attempt: the latest attempt's status, counts,
-- message and log tail.
CREATE TABLE AUD_TASK_RUN_LOG (
    TASK_RUN_ID      INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_ID          BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    PIPELINE_RUN_ID  BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    START_DATE       TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    END_DATE         TIMESTAMP,
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
    INTERVENTION_ID   INTEGER PRIMARY KEY AUTOINCREMENT,
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
    REQUESTED_AT      TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    REQUESTED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_run_interventions_requested_by_kind CHECK (REQUESTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    CONSTRAINT ck_intervention_action CHECK (ACTION IN ('MARK','NEW_RUN','CANCEL','REOPEN','RESET',
                                                     'GATE_BYPASS','IGNORE_DEPENDENCIES','RERUN'))
);

CREATE INDEX ix_run_interventions_run ON AUD_RUN_INTERVENTIONS (PIPELINE_RUN_ID);

-- Each time a pipeline was paused: while a pause has no RESUMED_AT, `run` starts nothing of the
-- pipeline, and a run in progress starts no more tasks until the pipeline is resumed.
CREATE TABLE AUD_PIPELINE_PAUSES (
    PIPELINE_PAUSE_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID        BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    PAUSED_AT          TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    PAUSED_BY          VARCHAR NOT NULL,
    REASON             VARCHAR NOT NULL,
    RESUMED_AT         TIMESTAMP,
    RESUMED_BY         VARCHAR,
    RESUME_REASON      VARCHAR,
    PAUSED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_pipeline_pauses_paused_by_kind CHECK (PAUSED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    RESUMED_BY_KIND VARCHAR CONSTRAINT ck_aud_pipeline_pauses_resumed_by_kind CHECK (RESUMED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'))
);

-- At most one open pause per pipeline.
CREATE UNIQUE INDEX ux_pipeline_pauses_open
    ON AUD_PIPELINE_PAUSES (PIPELINE_ID) WHERE RESUMED_AT IS NULL;

-- One row per business rule per task run: whether the rule ran, and how long it took.
CREATE TABLE AUD_BUSINESS_RULES_RUN_LOG (
    BUSINESS_RULE_RUN_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    BUSINESS_RULE_ID      BIGINT NOT NULL REFERENCES CFG_BUSINESS_RULES(BUSINESS_RULE_ID),
    TASK_RUN_ID           BIGINT NOT NULL REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    START_DATE            TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    END_DATE              TIMESTAMP,
    STATUS                VARCHAR NOT NULL,
    CONSTRAINT ck_br_run_status CHECK (STATUS IN ('IN-PROGRESS','SUCCESS','FAILED','SKIPPED'))
);

CREATE UNIQUE INDEX ux_br_run_one_per_task_run
    ON AUD_BUSINESS_RULES_RUN_LOG (BUSINESS_RULE_ID, TASK_RUN_ID);

-- The rows each rule flagged, by key; a flag is cleared (ACTIVE_FLAG 'N') once its row passes.
CREATE TABLE AUD_BUSINESS_RULES_RESULTS (
    BUSINESS_RULE_RESULT_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    BUSINESS_RULE_RUN_ID     BIGINT NOT NULL REFERENCES AUD_BUSINESS_RULES_RUN_LOG(BUSINESS_RULE_RUN_ID),
    BUSINESS_RULE_ID         BIGINT NOT NULL REFERENCES CFG_BUSINESS_RULES(BUSINESS_RULE_ID),
    BUSINESS_RULE_KEY        VARCHAR NOT NULL,
    TARGET_TABLE             VARCHAR NOT NULL,
    STATUS                   VARCHAR NOT NULL,
    ACTIVE_FLAG              VARCHAR NOT NULL DEFAULT 'Y',
    START_DATE               TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    END_DATE                 TIMESTAMP,
    CONSTRAINT ck_brresults_status      CHECK (STATUS IN ('INCOMPLETE','REJECT','REPORT')),
    CONSTRAINT ck_brresults_active_flag CHECK (ACTIVE_FLAG IN ('Y','N'))
);

CREATE INDEX ix_brresults_run ON AUD_BUSINESS_RULES_RESULTS (BUSINESS_RULE_RUN_ID);
CREATE INDEX ix_brresults_rule ON AUD_BUSINESS_RULES_RESULTS (BUSINESS_RULE_ID);

-- Where each incremental ingestion script got to: the offset it reads from next.
CREATE TABLE AUD_TASK_OFFSET_TRACKER (
    TASK_ID                 BIGINT PRIMARY KEY REFERENCES CFG_TASKS(TASK_ID),
    OFFSET_TYPE             VARCHAR NOT NULL,
    OFFSET_VALUE            VARCHAR,
    LAST_UPDATED_TIMESTAMP  TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSTRAINT ck_offset_type CHECK (OFFSET_TYPE IN ('NUMBER','TEXT','TIMESTAMP'))
);

-- Column lineage traced from each SQL task's SELECT, stored by a hash of what it depends on.
CREATE TABLE AUD_COLUMN_LINEAGE (
    COLUMN_LINEAGE_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_ID            BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    SOURCE_SQL_HASH    VARCHAR NOT NULL,
    TARGET_OBJECT      VARCHAR NOT NULL,
    TARGET_COLUMN      VARCHAR NOT NULL,
    SOURCE_OBJECT      VARCHAR,
    SOURCE_COLUMN      VARCHAR,
    TRANSFORMATION     VARCHAR,
    COMPUTED_AT        TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')
);

CREATE INDEX ix_column_lineage_task ON AUD_COLUMN_LINEAGE (TASK_ID, SOURCE_SQL_HASH);
CREATE INDEX ix_column_lineage_target ON AUD_COLUMN_LINEAGE (TARGET_OBJECT, TARGET_COLUMN);
CREATE INDEX ix_column_lineage_source ON AUD_COLUMN_LINEAGE (SOURCE_OBJECT, SOURCE_COLUMN);

-- The URL the catalog site is published at, one row per URL: the latest is current. A publish
-- that is given another URL fails unless told to accept it, so shared links keep working.
CREATE TABLE AUD_DOCS_PUBLICATION (
    DOCS_PUBLICATION_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    PUBLISHED_URL        VARCHAR NOT NULL,
    FIRST_PUBLISHED      TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    LAST_PUBLISHED       TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')
);

-- The versions of each task's DOCUMENTATION, one row per change.
CREATE TABLE AUD_TASK_DOCUMENTATION (
    TASK_DOCUMENTATION_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_ID                BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    VERSION                INT NOT NULL,
    DOCUMENTATION_HASH     VARCHAR NOT NULL,
    DOCUMENTATION          VARCHAR NOT NULL,
    RECORDED_AT            TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSTRAINT ux_task_documentation_version UNIQUE (TASK_ID, VERSION)
);

CREATE INDEX ix_task_documentation_task ON AUD_TASK_DOCUMENTATION (TASK_ID, VERSION DESC);

-- Every upstream run a downstream consumed, one row each, never updated: for a dependency on a
-- pipeline, the downstream run and the upstream run; for a dependency on another pipeline's task,
-- the downstream task and the upstream task run. A dependency's latest row is what it last
-- consumed: an upstream run satisfies it only when it is newer than that one. PIPELINE_RUN_ID is
-- NULL on the rows carried over from the trackers this table replaced.
CREATE TABLE AUD_DEPENDENCY_CONSUMPTION (
    CONSUMPTION_ID            INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_DEPENDENCY_ID    BIGINT REFERENCES CFG_PIPELINE_DEPENDENCY(PIPELINE_DEPENDENCY_ID),
    TASK_DEPENDENCY_ID        BIGINT REFERENCES CFG_TASK_DEPENDENCY(TASK_DEPENDENCY_ID),
    PIPELINE_ID               BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    PIPELINE_RUN_ID           BIGINT REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    TASK_ID                   BIGINT REFERENCES CFG_TASKS(TASK_ID),
    DEPENDS_ON_PIPELINE_ID    BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    CONSUMED_PIPELINE_RUN_ID  BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    CONSUMED_TASK_RUN_ID      BIGINT REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    CONSUMED_AT               TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
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
    APPLIED_AT  TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSTRAINT pk_schema_migrations PRIMARY KEY (SOURCE, VERSION),
    CONSTRAINT ck_schema_migrations_source CHECK (SOURCE IN ('ENGINE', 'PROJECT')),
    CONSTRAINT ck_schema_migrations_checksum
        CHECK (length(CHECKSUM) = 64 AND CHECKSUM NOT GLOB '*[^0-9a-f]*')
);

-- Attempt history and the dependencies judged at admission.
CREATE TABLE AUD_TASK_ATTEMPTS (
    ATTEMPT_ID       INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_RUN_ID      BIGINT NOT NULL REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    ATTEMPT_NUMBER   INT NOT NULL,
    STATUS           VARCHAR NOT NULL,
    OWNER_ID         VARCHAR,
    LEASE_EXPIRES_AT TIMESTAMP,
    HEARTBEAT_AT     TIMESTAMP,
    QUEUED_AT        TIMESTAMP,
    CLAIMED_AT       TIMESTAMP,
    STARTED_AT       TIMESTAMP,
    ENDED_AT         TIMESTAMP,
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
    REQUESTED_BY     VARCHAR DEFAULT (etl_craft_actor()),
    REQUESTED_BY_KIND VARCHAR DEFAULT (etl_craft_actor_kind()) CONSTRAINT ck_aud_task_attempts_requested_by_kind CHECK (REQUESTED_BY_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM')),
    NOT_BEFORE       TIMESTAMP,
    RETRYABLE        BOOLEAN NOT NULL DEFAULT TRUE CHECK (RETRYABLE IN (TRUE, FALSE)),
    CONSTRAINT ck_attempt_status CHECK (STATUS IN ('QUEUED','CLAIMED','RUNNING','SUCCESS','FAILED','TIMED_OUT','CANCELLED','LOST'))
);
CREATE UNIQUE INDEX ux_attempt_number ON AUD_TASK_ATTEMPTS (TASK_RUN_ID, ATTEMPT_NUMBER);
CREATE UNIQUE INDEX ux_attempt_one_active ON AUD_TASK_ATTEMPTS (TASK_RUN_ID)
    WHERE STATUS IN ('QUEUED','CLAIMED','RUNNING');
CREATE INDEX ix_attempt_status_lease ON AUD_TASK_ATTEMPTS (STATUS, LEASE_EXPIRES_AT);

-- Each dependency judged at admission, with the selected upstream revision and reason.
CREATE TABLE AUD_GATE_DECISIONS (
    DECISION_ID              INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_RUN_ID          BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    ATTEMPT_ID               BIGINT REFERENCES AUD_TASK_ATTEMPTS(ATTEMPT_ID),
    PIPELINE_DEPENDENCY_ID   BIGINT REFERENCES CFG_PIPELINE_DEPENDENCY(PIPELINE_DEPENDENCY_ID),
    TASK_DEPENDENCY_ID       BIGINT REFERENCES CFG_TASK_DEPENDENCY(TASK_DEPENDENCY_ID),
    SELECTED_PIPELINE_RUN_ID BIGINT REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    SELECTED_TASK_RUN_ID     BIGINT REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    SELECTED_REVISION        INT,
    RESULT                   VARCHAR NOT NULL,
    REASON                   VARCHAR NOT NULL,
    DECIDED_AT               TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSTRAINT ck_gate_one_dependency CHECK (
        (PIPELINE_DEPENDENCY_ID IS NOT NULL AND TASK_DEPENDENCY_ID IS NULL)
        OR (PIPELINE_DEPENDENCY_ID IS NULL AND TASK_DEPENDENCY_ID IS NOT NULL)
    ),
    CONSTRAINT ck_gate_result CHECK (RESULT IN ('SATISFIED','UNSATISFIED','BYPASSED'))
);

-- An immutable record of a command request; run and attempt outcomes live in their own logs.
CREATE TABLE AUD_ACTIONS (
    ACTION_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    STARTED_AT TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    ENDED_AT TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    ACTOR VARCHAR NOT NULL,
    ACTOR_KIND VARCHAR NOT NULL,
    HOST VARCHAR NOT NULL,
    COMMAND VARCHAR NOT NULL,
    ARGUMENTS TEXT NOT NULL,
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
    CHANGE_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    CHANGED_AT TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    ACTOR VARCHAR NOT NULL,
    ACTOR_KIND VARCHAR NOT NULL,
    TABLE_NAME VARCHAR NOT NULL,
    ROW_KEY VARCHAR NOT NULL,
    OPERATION VARCHAR NOT NULL,
    BEFORE_JSON TEXT,
    AFTER_JSON TEXT,
    MIGRATION VARCHAR,
    CONSTRAINT ck_metadata_operation CHECK (OPERATION IN ('INSERT','UPDATE','DELETE')),
    CONSTRAINT ck_metadata_actor_kind CHECK (ACTOR_KIND IN ('HUMAN','SCHEDULE','ORCHESTRATOR','WORKER','SYSTEM'))
);
CREATE INDEX ix_metadata_changes_table ON AUD_METADATA_CHANGES (TABLE_NAME, CHANGED_AT);
-- The canonical hash version published after a target warehouse update commits.
CREATE TABLE AUD_TARGET_HASH_VERSION (
    TARGET_OBJECT VARCHAR PRIMARY KEY,
    HASH_VERSION INTEGER NOT NULL CONSTRAINT ck_target_hash_version CHECK (HASH_VERSION IN (1,2)),
    RECOMPUTED_AT TIMESTAMP NOT NULL
);

CREATE TRIGGER trg_actor_guard_cfg_pipelines_insert BEFORE INSERT ON CFG_PIPELINES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_PIPELINES');
END;
CREATE TRIGGER trg_actor_guard_cfg_pipelines_update BEFORE UPDATE ON CFG_PIPELINES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_pipelines_delete BEFORE DELETE ON CFG_PIPELINES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_pipeline_dependency_insert BEFORE INSERT ON CFG_PIPELINE_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINE_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_PIPELINE_DEPENDENCY');
END;
CREATE TRIGGER trg_actor_guard_cfg_pipeline_dependency_update BEFORE UPDATE ON CFG_PIPELINE_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINE_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_pipeline_dependency_delete BEFORE DELETE ON CFG_PIPELINE_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_PIPELINE_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_tasks_insert BEFORE INSERT ON CFG_TASKS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASKS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_TASKS');
END;
CREATE TRIGGER trg_actor_guard_cfg_tasks_update BEFORE UPDATE ON CFG_TASKS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASKS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_tasks_delete BEFORE DELETE ON CFG_TASKS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASKS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_task_dependency_insert BEFORE INSERT ON CFG_TASK_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_TASK_DEPENDENCY');
END;
CREATE TRIGGER trg_actor_guard_cfg_task_dependency_update BEFORE UPDATE ON CFG_TASK_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_task_dependency_delete BEFORE DELETE ON CFG_TASK_DEPENDENCY
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_DEPENDENCY is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_task_parameters_insert BEFORE INSERT ON CFG_TASK_PARAMETERS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_PARAMETERS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_TASK_PARAMETERS');
END;
CREATE TRIGGER trg_actor_guard_cfg_task_parameters_update BEFORE UPDATE ON CFG_TASK_PARAMETERS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_PARAMETERS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_task_parameters_delete BEFORE DELETE ON CFG_TASK_PARAMETERS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_TASK_PARAMETERS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_business_rules_insert BEFORE INSERT ON CFG_BUSINESS_RULES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_BUSINESS_RULES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT etl_craft_enter_insert('CFG_BUSINESS_RULES');
END;
CREATE TRIGGER trg_actor_guard_cfg_business_rules_update BEFORE UPDATE ON CFG_BUSINESS_RULES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_BUSINESS_RULES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_cfg_business_rules_delete BEFORE DELETE ON CFG_BUSINESS_RULES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'CFG_BUSINESS_RULES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipelines_run_log_insert BEFORE INSERT ON AUD_PIPELINES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipelines_run_log_update BEFORE UPDATE ON AUD_PIPELINES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipelines_run_log_delete BEFORE DELETE ON AUD_PIPELINES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_run_log_insert BEFORE INSERT ON AUD_TASK_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_run_log_update BEFORE UPDATE ON AUD_TASK_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_run_log_delete BEFORE DELETE ON AUD_TASK_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_run_interventions_insert BEFORE INSERT ON AUD_RUN_INTERVENTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RUN_INTERVENTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_run_interventions_update BEFORE UPDATE ON AUD_RUN_INTERVENTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RUN_INTERVENTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_RUN_INTERVENTIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_run_interventions_delete BEFORE DELETE ON AUD_RUN_INTERVENTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_RUN_INTERVENTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_RUN_INTERVENTIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipeline_pauses_insert BEFORE INSERT ON AUD_PIPELINE_PAUSES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINE_PAUSES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipeline_pauses_update BEFORE UPDATE ON AUD_PIPELINE_PAUSES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINE_PAUSES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_pipeline_pauses_delete BEFORE DELETE ON AUD_PIPELINE_PAUSES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_PIPELINE_PAUSES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_run_log_insert BEFORE INSERT ON AUD_BUSINESS_RULES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_run_log_update BEFORE UPDATE ON AUD_BUSINESS_RULES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_run_log_delete BEFORE DELETE ON AUD_BUSINESS_RULES_RUN_LOG
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RUN_LOG is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_results_insert BEFORE INSERT ON AUD_BUSINESS_RULES_RESULTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RESULTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_results_update BEFORE UPDATE ON AUD_BUSINESS_RULES_RESULTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RESULTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_business_rules_results_delete BEFORE DELETE ON AUD_BUSINESS_RULES_RESULTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_BUSINESS_RULES_RESULTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_offset_tracker_insert BEFORE INSERT ON AUD_TASK_OFFSET_TRACKER
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_OFFSET_TRACKER is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_offset_tracker_update BEFORE UPDATE ON AUD_TASK_OFFSET_TRACKER
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_OFFSET_TRACKER is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_offset_tracker_delete BEFORE DELETE ON AUD_TASK_OFFSET_TRACKER
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_OFFSET_TRACKER is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_column_lineage_insert BEFORE INSERT ON AUD_COLUMN_LINEAGE
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_COLUMN_LINEAGE is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_column_lineage_update BEFORE UPDATE ON AUD_COLUMN_LINEAGE
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_COLUMN_LINEAGE is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_column_lineage_delete BEFORE DELETE ON AUD_COLUMN_LINEAGE
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_COLUMN_LINEAGE is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_docs_publication_insert BEFORE INSERT ON AUD_DOCS_PUBLICATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DOCS_PUBLICATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_docs_publication_update BEFORE UPDATE ON AUD_DOCS_PUBLICATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DOCS_PUBLICATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_docs_publication_delete BEFORE DELETE ON AUD_DOCS_PUBLICATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DOCS_PUBLICATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_documentation_insert BEFORE INSERT ON AUD_TASK_DOCUMENTATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_DOCUMENTATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_documentation_update BEFORE UPDATE ON AUD_TASK_DOCUMENTATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_DOCUMENTATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_documentation_delete BEFORE DELETE ON AUD_TASK_DOCUMENTATION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_DOCUMENTATION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_dependency_consumption_insert BEFORE INSERT ON AUD_DEPENDENCY_CONSUMPTION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DEPENDENCY_CONSUMPTION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_dependency_consumption_update BEFORE UPDATE ON AUD_DEPENDENCY_CONSUMPTION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DEPENDENCY_CONSUMPTION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_DEPENDENCY_CONSUMPTION immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_dependency_consumption_delete BEFORE DELETE ON AUD_DEPENDENCY_CONSUMPTION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_DEPENDENCY_CONSUMPTION is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_DEPENDENCY_CONSUMPTION immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_attempts_insert BEFORE INSERT ON AUD_TASK_ATTEMPTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_ATTEMPTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_attempts_update BEFORE UPDATE ON AUD_TASK_ATTEMPTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_ATTEMPTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' AND OLD.STATUS NOT IN ('QUEUED','CLAIMED','RUNNING') THEN RAISE(ABORT, 'AUD_TASK_ATTEMPTS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_attempts_delete BEFORE DELETE ON AUD_TASK_ATTEMPTS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_ATTEMPTS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' AND OLD.STATUS NOT IN ('QUEUED','CLAIMED','RUNNING') THEN RAISE(ABORT, 'AUD_TASK_ATTEMPTS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_gate_decisions_insert BEFORE INSERT ON AUD_GATE_DECISIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_GATE_DECISIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_gate_decisions_update BEFORE UPDATE ON AUD_GATE_DECISIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_GATE_DECISIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_GATE_DECISIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_gate_decisions_delete BEFORE DELETE ON AUD_GATE_DECISIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_GATE_DECISIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_GATE_DECISIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_actions_insert BEFORE INSERT ON AUD_ACTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_ACTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_actions_update BEFORE UPDATE ON AUD_ACTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_ACTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_ACTIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_actions_delete BEFORE DELETE ON AUD_ACTIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_ACTIONS is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_ACTIONS immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_metadata_changes_insert BEFORE INSERT ON AUD_METADATA_CHANGES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_METADATA_CHANGES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_metadata_changes_update BEFORE UPDATE ON AUD_METADATA_CHANGES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_METADATA_CHANGES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_METADATA_CHANGES immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_actor_guard_aud_metadata_changes_delete BEFORE DELETE ON AUD_METADATA_CHANGES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_METADATA_CHANGES is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
    SELECT CASE WHEN etl_craft_purpose() <> 'retention' THEN RAISE(ABORT, 'AUD_METADATA_CHANGES immutable history; use a new action or etl-craft retention') END;
END;
CREATE TRIGGER trg_audit_cfg_pipelines_insert AFTER INSERT ON CFG_PIPELINES
BEGIN
    UPDATE CFG_PIPELINES SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE PIPELINE_ID=NEW.PIPELINE_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINES', CAST(NEW.PIPELINE_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('pipeline_id', row.PIPELINE_ID, 'pipeline_code', row.PIPELINE_CODE, 'pipeline_name', row.PIPELINE_NAME, 'description', row.DESCRIPTION, 'run_schedule', row.RUN_SCHEDULE, 'sla_in_hours', row.SLA_IN_HOURS, 'refresh_type', row.REFRESH_TYPE, 'active_flag', row.ACTIVE_FLAG, 'pipeline_parameters', row.PIPELINE_PARAMETERS, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_PIPELINES row WHERE row.PIPELINE_ID=NEW.PIPELINE_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_PIPELINES');
END;
CREATE TRIGGER trg_audit_cfg_pipelines_update AFTER UPDATE ON CFG_PIPELINES
BEGIN
    UPDATE CFG_PIPELINES SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE PIPELINE_ID=NEW.PIPELINE_ID AND NOT etl_craft_inserting('CFG_PIPELINES');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINES', CAST(NEW.PIPELINE_ID AS TEXT), 'UPDATE', json_object('pipeline_id', OLD.PIPELINE_ID, 'pipeline_code', OLD.PIPELINE_CODE, 'pipeline_name', OLD.PIPELINE_NAME, 'description', OLD.DESCRIPTION, 'run_schedule', OLD.RUN_SCHEDULE, 'sla_in_hours', OLD.SLA_IN_HOURS, 'refresh_type', OLD.REFRESH_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'pipeline_parameters', OLD.PIPELINE_PARAMETERS, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), (SELECT json_object('pipeline_id', row.PIPELINE_ID, 'pipeline_code', row.PIPELINE_CODE, 'pipeline_name', row.PIPELINE_NAME, 'description', row.DESCRIPTION, 'run_schedule', row.RUN_SCHEDULE, 'sla_in_hours', row.SLA_IN_HOURS, 'refresh_type', row.REFRESH_TYPE, 'active_flag', row.ACTIVE_FLAG, 'pipeline_parameters', row.PIPELINE_PARAMETERS, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_PIPELINES row WHERE row.PIPELINE_ID=NEW.PIPELINE_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.PIPELINE_ID IS NOT OLD.PIPELINE_ID OR NEW.PIPELINE_CODE IS NOT OLD.PIPELINE_CODE OR NEW.PIPELINE_NAME IS NOT OLD.PIPELINE_NAME OR NEW.DESCRIPTION IS NOT OLD.DESCRIPTION OR NEW.RUN_SCHEDULE IS NOT OLD.RUN_SCHEDULE OR NEW.SLA_IN_HOURS IS NOT OLD.SLA_IN_HOURS OR NEW.REFRESH_TYPE IS NOT OLD.REFRESH_TYPE OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG OR NEW.PIPELINE_PARAMETERS IS NOT OLD.PIPELINE_PARAMETERS) AND NOT etl_craft_inserting('CFG_PIPELINES');
END;
CREATE TRIGGER trg_audit_cfg_pipelines_delete AFTER DELETE ON CFG_PIPELINES
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINES', CAST(OLD.PIPELINE_ID AS TEXT), 'DELETE', json_object('pipeline_id', OLD.PIPELINE_ID, 'pipeline_code', OLD.PIPELINE_CODE, 'pipeline_name', OLD.PIPELINE_NAME, 'description', OLD.DESCRIPTION, 'run_schedule', OLD.RUN_SCHEDULE, 'sla_in_hours', OLD.SLA_IN_HOURS, 'refresh_type', OLD.REFRESH_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'pipeline_parameters', OLD.PIPELINE_PARAMETERS, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), NULL, NULLIF(etl_craft_migration(), ''));
END;
CREATE TRIGGER trg_audit_cfg_pipeline_dependency_insert AFTER INSERT ON CFG_PIPELINE_DEPENDENCY
BEGIN
    UPDATE CFG_PIPELINE_DEPENDENCY SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE PIPELINE_DEPENDENCY_ID=NEW.PIPELINE_DEPENDENCY_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINE_DEPENDENCY', CAST(NEW.PIPELINE_DEPENDENCY_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('pipeline_dependency_id', row.PIPELINE_DEPENDENCY_ID, 'pipeline_id', row.PIPELINE_ID, 'depends_on_pipeline_id', row.DEPENDS_ON_PIPELINE_ID, 'dependency_type', row.DEPENDENCY_TYPE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE, 'consume_repairs', row.CONSUME_REPAIRS) FROM CFG_PIPELINE_DEPENDENCY row WHERE row.PIPELINE_DEPENDENCY_ID=NEW.PIPELINE_DEPENDENCY_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_PIPELINE_DEPENDENCY');
END;
CREATE TRIGGER trg_audit_cfg_pipeline_dependency_update AFTER UPDATE ON CFG_PIPELINE_DEPENDENCY
BEGIN
    UPDATE CFG_PIPELINE_DEPENDENCY SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE PIPELINE_DEPENDENCY_ID=NEW.PIPELINE_DEPENDENCY_ID AND NOT etl_craft_inserting('CFG_PIPELINE_DEPENDENCY');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINE_DEPENDENCY', CAST(NEW.PIPELINE_DEPENDENCY_ID AS TEXT), 'UPDATE', json_object('pipeline_dependency_id', OLD.PIPELINE_DEPENDENCY_ID, 'pipeline_id', OLD.PIPELINE_ID, 'depends_on_pipeline_id', OLD.DEPENDS_ON_PIPELINE_ID, 'dependency_type', OLD.DEPENDENCY_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE, 'consume_repairs', OLD.CONSUME_REPAIRS), (SELECT json_object('pipeline_dependency_id', row.PIPELINE_DEPENDENCY_ID, 'pipeline_id', row.PIPELINE_ID, 'depends_on_pipeline_id', row.DEPENDS_ON_PIPELINE_ID, 'dependency_type', row.DEPENDENCY_TYPE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE, 'consume_repairs', row.CONSUME_REPAIRS) FROM CFG_PIPELINE_DEPENDENCY row WHERE row.PIPELINE_DEPENDENCY_ID=NEW.PIPELINE_DEPENDENCY_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.PIPELINE_DEPENDENCY_ID IS NOT OLD.PIPELINE_DEPENDENCY_ID OR NEW.PIPELINE_ID IS NOT OLD.PIPELINE_ID OR NEW.DEPENDS_ON_PIPELINE_ID IS NOT OLD.DEPENDS_ON_PIPELINE_ID OR NEW.DEPENDENCY_TYPE IS NOT OLD.DEPENDENCY_TYPE OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG OR NEW.CONSUME_REPAIRS IS NOT OLD.CONSUME_REPAIRS) AND NOT etl_craft_inserting('CFG_PIPELINE_DEPENDENCY');
END;
CREATE TRIGGER trg_audit_cfg_pipeline_dependency_delete AFTER DELETE ON CFG_PIPELINE_DEPENDENCY
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_PIPELINE_DEPENDENCY', CAST(OLD.PIPELINE_DEPENDENCY_ID AS TEXT), 'DELETE', json_object('pipeline_dependency_id', OLD.PIPELINE_DEPENDENCY_ID, 'pipeline_id', OLD.PIPELINE_ID, 'depends_on_pipeline_id', OLD.DEPENDS_ON_PIPELINE_ID, 'dependency_type', OLD.DEPENDENCY_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE, 'consume_repairs', OLD.CONSUME_REPAIRS), NULL, NULLIF(etl_craft_migration(), ''));
END;
CREATE TRIGGER trg_audit_cfg_tasks_insert AFTER INSERT ON CFG_TASKS
BEGIN
    UPDATE CFG_TASKS SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE TASK_ID=NEW.TASK_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASKS', CAST(NEW.TASK_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('task_id', row.TASK_ID, 'task_code', row.TASK_CODE, 'task_type', row.TASK_TYPE, 'pipeline_id', row.PIPELINE_ID, 'handler', row.HANDLER, 'run_condition', row.RUN_CONDITION, 'run_condition_count', row.RUN_CONDITION_COUNT, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_TASKS row WHERE row.TASK_ID=NEW.TASK_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_TASKS');
END;
CREATE TRIGGER trg_audit_cfg_tasks_update AFTER UPDATE ON CFG_TASKS
BEGIN
    UPDATE CFG_TASKS SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE TASK_ID=NEW.TASK_ID AND NOT etl_craft_inserting('CFG_TASKS');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASKS', CAST(NEW.TASK_ID AS TEXT), 'UPDATE', json_object('task_id', OLD.TASK_ID, 'task_code', OLD.TASK_CODE, 'task_type', OLD.TASK_TYPE, 'pipeline_id', OLD.PIPELINE_ID, 'handler', OLD.HANDLER, 'run_condition', OLD.RUN_CONDITION, 'run_condition_count', OLD.RUN_CONDITION_COUNT, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), (SELECT json_object('task_id', row.TASK_ID, 'task_code', row.TASK_CODE, 'task_type', row.TASK_TYPE, 'pipeline_id', row.PIPELINE_ID, 'handler', row.HANDLER, 'run_condition', row.RUN_CONDITION, 'run_condition_count', row.RUN_CONDITION_COUNT, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_TASKS row WHERE row.TASK_ID=NEW.TASK_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.TASK_ID IS NOT OLD.TASK_ID OR NEW.TASK_CODE IS NOT OLD.TASK_CODE OR NEW.TASK_TYPE IS NOT OLD.TASK_TYPE OR NEW.PIPELINE_ID IS NOT OLD.PIPELINE_ID OR NEW.HANDLER IS NOT OLD.HANDLER OR NEW.RUN_CONDITION IS NOT OLD.RUN_CONDITION OR NEW.RUN_CONDITION_COUNT IS NOT OLD.RUN_CONDITION_COUNT OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG) AND NOT etl_craft_inserting('CFG_TASKS');
END;
CREATE TRIGGER trg_audit_cfg_tasks_delete AFTER DELETE ON CFG_TASKS
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASKS', CAST(OLD.TASK_ID AS TEXT), 'DELETE', json_object('task_id', OLD.TASK_ID, 'task_code', OLD.TASK_CODE, 'task_type', OLD.TASK_TYPE, 'pipeline_id', OLD.PIPELINE_ID, 'handler', OLD.HANDLER, 'run_condition', OLD.RUN_CONDITION, 'run_condition_count', OLD.RUN_CONDITION_COUNT, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), NULL, NULLIF(etl_craft_migration(), ''));
END;
CREATE TRIGGER trg_audit_cfg_task_dependency_insert AFTER INSERT ON CFG_TASK_DEPENDENCY
BEGIN
    UPDATE CFG_TASK_DEPENDENCY SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), DEPENDS_ON_PIPELINE_ID=COALESCE(NEW.DEPENDS_ON_PIPELINE_ID, NEW.PIPELINE_ID) WHERE TASK_DEPENDENCY_ID=NEW.TASK_DEPENDENCY_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_DEPENDENCY', CAST(NEW.TASK_DEPENDENCY_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('task_dependency_id', row.TASK_DEPENDENCY_ID, 'pipeline_id', row.PIPELINE_ID, 'task_id', row.TASK_ID, 'depends_on_pipeline_id', row.DEPENDS_ON_PIPELINE_ID, 'depends_on_task_id', row.DEPENDS_ON_TASK_ID, 'dependency_type', row.DEPENDENCY_TYPE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE, 'consume_repairs', row.CONSUME_REPAIRS) FROM CFG_TASK_DEPENDENCY row WHERE row.TASK_DEPENDENCY_ID=NEW.TASK_DEPENDENCY_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_TASK_DEPENDENCY');
END;
CREATE TRIGGER trg_audit_cfg_task_dependency_update AFTER UPDATE ON CFG_TASK_DEPENDENCY
BEGIN
    UPDATE CFG_TASK_DEPENDENCY SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), DEPENDS_ON_PIPELINE_ID=COALESCE(NEW.DEPENDS_ON_PIPELINE_ID, NEW.PIPELINE_ID) WHERE TASK_DEPENDENCY_ID=NEW.TASK_DEPENDENCY_ID AND NOT etl_craft_inserting('CFG_TASK_DEPENDENCY');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_DEPENDENCY', CAST(NEW.TASK_DEPENDENCY_ID AS TEXT), 'UPDATE', json_object('task_dependency_id', OLD.TASK_DEPENDENCY_ID, 'pipeline_id', OLD.PIPELINE_ID, 'task_id', OLD.TASK_ID, 'depends_on_pipeline_id', OLD.DEPENDS_ON_PIPELINE_ID, 'depends_on_task_id', OLD.DEPENDS_ON_TASK_ID, 'dependency_type', OLD.DEPENDENCY_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE, 'consume_repairs', OLD.CONSUME_REPAIRS), (SELECT json_object('task_dependency_id', row.TASK_DEPENDENCY_ID, 'pipeline_id', row.PIPELINE_ID, 'task_id', row.TASK_ID, 'depends_on_pipeline_id', row.DEPENDS_ON_PIPELINE_ID, 'depends_on_task_id', row.DEPENDS_ON_TASK_ID, 'dependency_type', row.DEPENDENCY_TYPE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE, 'consume_repairs', row.CONSUME_REPAIRS) FROM CFG_TASK_DEPENDENCY row WHERE row.TASK_DEPENDENCY_ID=NEW.TASK_DEPENDENCY_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.TASK_DEPENDENCY_ID IS NOT OLD.TASK_DEPENDENCY_ID OR NEW.PIPELINE_ID IS NOT OLD.PIPELINE_ID OR NEW.TASK_ID IS NOT OLD.TASK_ID OR NEW.DEPENDS_ON_PIPELINE_ID IS NOT OLD.DEPENDS_ON_PIPELINE_ID OR NEW.DEPENDS_ON_TASK_ID IS NOT OLD.DEPENDS_ON_TASK_ID OR NEW.DEPENDENCY_TYPE IS NOT OLD.DEPENDENCY_TYPE OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG OR NEW.CONSUME_REPAIRS IS NOT OLD.CONSUME_REPAIRS) AND NOT etl_craft_inserting('CFG_TASK_DEPENDENCY');
END;
CREATE TRIGGER trg_audit_cfg_task_dependency_delete AFTER DELETE ON CFG_TASK_DEPENDENCY
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_DEPENDENCY', CAST(OLD.TASK_DEPENDENCY_ID AS TEXT), 'DELETE', json_object('task_dependency_id', OLD.TASK_DEPENDENCY_ID, 'pipeline_id', OLD.PIPELINE_ID, 'task_id', OLD.TASK_ID, 'depends_on_pipeline_id', OLD.DEPENDS_ON_PIPELINE_ID, 'depends_on_task_id', OLD.DEPENDS_ON_TASK_ID, 'dependency_type', OLD.DEPENDENCY_TYPE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE, 'consume_repairs', OLD.CONSUME_REPAIRS), NULL, NULLIF(etl_craft_migration(), ''));
END;
CREATE TRIGGER trg_audit_cfg_task_parameters_insert AFTER INSERT ON CFG_TASK_PARAMETERS
BEGIN
    UPDATE CFG_TASK_PARAMETERS SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE TASK_PARAMETER_ID=NEW.TASK_PARAMETER_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_PARAMETERS', CAST(NEW.TASK_PARAMETER_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('task_parameter_id', row.TASK_PARAMETER_ID, 'task_id', row.TASK_ID, 'parameter_name', row.PARAMETER_NAME, 'parameter_value', row.PARAMETER_VALUE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_TASK_PARAMETERS row WHERE row.TASK_PARAMETER_ID=NEW.TASK_PARAMETER_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_TASK_PARAMETERS');
END;
CREATE TRIGGER trg_audit_cfg_task_parameters_update AFTER UPDATE ON CFG_TASK_PARAMETERS
BEGIN
    UPDATE CFG_TASK_PARAMETERS SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE TASK_PARAMETER_ID=NEW.TASK_PARAMETER_ID AND NOT etl_craft_inserting('CFG_TASK_PARAMETERS');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_PARAMETERS', CAST(NEW.TASK_PARAMETER_ID AS TEXT), 'UPDATE', json_object('task_parameter_id', OLD.TASK_PARAMETER_ID, 'task_id', OLD.TASK_ID, 'parameter_name', OLD.PARAMETER_NAME, 'parameter_value', OLD.PARAMETER_VALUE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), (SELECT json_object('task_parameter_id', row.TASK_PARAMETER_ID, 'task_id', row.TASK_ID, 'parameter_name', row.PARAMETER_NAME, 'parameter_value', row.PARAMETER_VALUE, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_TASK_PARAMETERS row WHERE row.TASK_PARAMETER_ID=NEW.TASK_PARAMETER_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.TASK_PARAMETER_ID IS NOT OLD.TASK_PARAMETER_ID OR NEW.TASK_ID IS NOT OLD.TASK_ID OR NEW.PARAMETER_NAME IS NOT OLD.PARAMETER_NAME OR NEW.PARAMETER_VALUE IS NOT OLD.PARAMETER_VALUE OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG) AND NOT etl_craft_inserting('CFG_TASK_PARAMETERS');
END;
CREATE TRIGGER trg_audit_cfg_task_parameters_delete AFTER DELETE ON CFG_TASK_PARAMETERS
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_TASK_PARAMETERS', CAST(OLD.TASK_PARAMETER_ID AS TEXT), 'DELETE', json_object('task_parameter_id', OLD.TASK_PARAMETER_ID, 'task_id', OLD.TASK_ID, 'parameter_name', OLD.PARAMETER_NAME, 'parameter_value', OLD.PARAMETER_VALUE, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), NULL, NULLIF(etl_craft_migration(), ''));
END;
CREATE TRIGGER trg_audit_cfg_business_rules_insert AFTER INSERT ON CFG_BUSINESS_RULES
BEGIN
    UPDATE CFG_BUSINESS_RULES SET CREATED_BY=etl_craft_actor(), CREATE_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE BUSINESS_RULE_ID=NEW.BUSINESS_RULE_ID;
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_BUSINESS_RULES', CAST(NEW.BUSINESS_RULE_ID AS TEXT), 'INSERT', NULL, (SELECT json_object('business_rule_id', row.BUSINESS_RULE_ID, 'business_rule_name', row.BUSINESS_RULE_NAME, 'pipeline_id', row.PIPELINE_ID, 'task_id', row.TASK_ID, 'business_rule_sql', row.BUSINESS_RULE_SQL, 'business_rule_type', row.BUSINESS_RULE_TYPE, 'business_rule_key_column', row.BUSINESS_RULE_KEY_COLUMN, 'target_table', row.TARGET_TABLE, 'sequence_number', row.SEQUENCE_NUMBER, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_BUSINESS_RULES row WHERE row.BUSINESS_RULE_ID=NEW.BUSINESS_RULE_ID), NULLIF(etl_craft_migration(), ''));
    SELECT etl_craft_exit_insert('CFG_BUSINESS_RULES');
END;
CREATE TRIGGER trg_audit_cfg_business_rules_update AFTER UPDATE ON CFG_BUSINESS_RULES
BEGIN
    UPDATE CFG_BUSINESS_RULES SET CREATED_BY=OLD.CREATED_BY, CREATE_DATE=OLD.CREATE_DATE, UPDATED_BY=etl_craft_actor(), UPDATED_DATE=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') WHERE BUSINESS_RULE_ID=NEW.BUSINESS_RULE_ID AND NOT etl_craft_inserting('CFG_BUSINESS_RULES');
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) SELECT etl_craft_actor(), etl_craft_actor_kind(), 'CFG_BUSINESS_RULES', CAST(NEW.BUSINESS_RULE_ID AS TEXT), 'UPDATE', json_object('business_rule_id', OLD.BUSINESS_RULE_ID, 'business_rule_name', OLD.BUSINESS_RULE_NAME, 'pipeline_id', OLD.PIPELINE_ID, 'task_id', OLD.TASK_ID, 'business_rule_sql', OLD.BUSINESS_RULE_SQL, 'business_rule_type', OLD.BUSINESS_RULE_TYPE, 'business_rule_key_column', OLD.BUSINESS_RULE_KEY_COLUMN, 'target_table', OLD.TARGET_TABLE, 'sequence_number', OLD.SEQUENCE_NUMBER, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), (SELECT json_object('business_rule_id', row.BUSINESS_RULE_ID, 'business_rule_name', row.BUSINESS_RULE_NAME, 'pipeline_id', row.PIPELINE_ID, 'task_id', row.TASK_ID, 'business_rule_sql', row.BUSINESS_RULE_SQL, 'business_rule_type', row.BUSINESS_RULE_TYPE, 'business_rule_key_column', row.BUSINESS_RULE_KEY_COLUMN, 'target_table', row.TARGET_TABLE, 'sequence_number', row.SEQUENCE_NUMBER, 'active_flag', row.ACTIVE_FLAG, 'created_by', row.CREATED_BY, 'create_date', row.CREATE_DATE, 'updated_by', row.UPDATED_BY, 'updated_date', row.UPDATED_DATE) FROM CFG_BUSINESS_RULES row WHERE row.BUSINESS_RULE_ID=NEW.BUSINESS_RULE_ID), NULLIF(etl_craft_migration(), '') WHERE (NEW.BUSINESS_RULE_ID IS NOT OLD.BUSINESS_RULE_ID OR NEW.BUSINESS_RULE_NAME IS NOT OLD.BUSINESS_RULE_NAME OR NEW.PIPELINE_ID IS NOT OLD.PIPELINE_ID OR NEW.TASK_ID IS NOT OLD.TASK_ID OR NEW.BUSINESS_RULE_SQL IS NOT OLD.BUSINESS_RULE_SQL OR NEW.BUSINESS_RULE_TYPE IS NOT OLD.BUSINESS_RULE_TYPE OR NEW.BUSINESS_RULE_KEY_COLUMN IS NOT OLD.BUSINESS_RULE_KEY_COLUMN OR NEW.TARGET_TABLE IS NOT OLD.TARGET_TABLE OR NEW.SEQUENCE_NUMBER IS NOT OLD.SEQUENCE_NUMBER OR NEW.ACTIVE_FLAG IS NOT OLD.ACTIVE_FLAG) AND NOT etl_craft_inserting('CFG_BUSINESS_RULES');
END;
CREATE TRIGGER trg_audit_cfg_business_rules_delete AFTER DELETE ON CFG_BUSINESS_RULES
BEGIN
    INSERT INTO AUD_METADATA_CHANGES (ACTOR, ACTOR_KIND, TABLE_NAME, ROW_KEY, OPERATION, BEFORE_JSON, AFTER_JSON, MIGRATION) VALUES (etl_craft_actor(), etl_craft_actor_kind(), 'CFG_BUSINESS_RULES', CAST(OLD.BUSINESS_RULE_ID AS TEXT), 'DELETE', json_object('business_rule_id', OLD.BUSINESS_RULE_ID, 'business_rule_name', OLD.BUSINESS_RULE_NAME, 'pipeline_id', OLD.PIPELINE_ID, 'task_id', OLD.TASK_ID, 'business_rule_sql', OLD.BUSINESS_RULE_SQL, 'business_rule_type', OLD.BUSINESS_RULE_TYPE, 'business_rule_key_column', OLD.BUSINESS_RULE_KEY_COLUMN, 'target_table', OLD.TARGET_TABLE, 'sequence_number', OLD.SEQUENCE_NUMBER, 'active_flag', OLD.ACTIVE_FLAG, 'created_by', OLD.CREATED_BY, 'create_date', OLD.CREATE_DATE, 'updated_by', OLD.UPDATED_BY, 'updated_date', OLD.UPDATED_DATE), NULL, NULLIF(etl_craft_migration(), ''));
END;

-- The run log is mutable; stamp its inserting actor before the transaction commits.
CREATE TRIGGER trg_attribute_run_insert AFTER INSERT ON AUD_PIPELINES_RUN_LOG
BEGIN
    UPDATE AUD_PIPELINES_RUN_LOG SET STARTED_BY=COALESCE(NEW.STARTED_BY, etl_craft_actor()), STARTED_BY_KIND=COALESCE(NEW.STARTED_BY_KIND, etl_craft_actor_kind()), ENDED_BY=CASE WHEN NEW.END_DATE IS NOT NULL THEN COALESCE(NEW.ENDED_BY, etl_craft_actor()) ELSE NEW.ENDED_BY END, ENDED_BY_KIND=CASE WHEN NEW.END_DATE IS NOT NULL THEN COALESCE(NEW.ENDED_BY_KIND, etl_craft_actor_kind()) ELSE NEW.ENDED_BY_KIND END WHERE PIPELINE_RUN_ID=NEW.PIPELINE_RUN_ID;
END;

CREATE TRIGGER trg_actor_guard_aud_target_hash_version_insert BEFORE INSERT ON AUD_TARGET_HASH_VERSION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TARGET_HASH_VERSION is written only by etl-craft; use etl-craft rehash') END;
END;

CREATE TRIGGER trg_actor_guard_aud_target_hash_version_update BEFORE UPDATE ON AUD_TARGET_HASH_VERSION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TARGET_HASH_VERSION is written only by etl-craft; use etl-craft rehash') END;
END;

CREATE TRIGGER trg_actor_guard_aud_target_hash_version_delete BEFORE DELETE ON AUD_TARGET_HASH_VERSION
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TARGET_HASH_VERSION is written only by etl-craft; use etl-craft rehash') END;
END;

-- Overseer process history; the session lock is the source of leadership.
CREATE TABLE AUD_OVERSEERS (
    OVERSEER_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    HOST VARCHAR NOT NULL,
    PID INTEGER NOT NULL CHECK (PID > 0),
    VERSION VARCHAR NOT NULL,
    STARTED_AT TIMESTAMP NOT NULL,
    HEARTBEAT_AT TIMESTAMP NOT NULL,
    STOPPED_AT TIMESTAMP,
    STARTED_BY VARCHAR NOT NULL,
    STARTED_BY_KIND VARCHAR NOT NULL,
    STOPPED_BY VARCHAR,
    STOPPED_BY_KIND VARCHAR
);
CREATE INDEX ix_overseer_unclosed ON AUD_OVERSEERS (OVERSEER_ID) WHERE STOPPED_AT IS NULL;
CREATE TRIGGER trg_actor_guard_aud_overseers_insert BEFORE INSERT ON AUD_OVERSEERS
WHEN etl_craft_actor() IS NULL OR etl_craft_actor() = ''
BEGIN
    SELECT RAISE(ABORT, 'AUD_OVERSEERS is written only by etl-craft');
END;
CREATE TRIGGER trg_actor_guard_aud_overseers_update BEFORE UPDATE ON AUD_OVERSEERS
WHEN etl_craft_actor() IS NULL OR etl_craft_actor() = ''
BEGIN
    SELECT RAISE(ABORT, 'AUD_OVERSEERS is written only by etl-craft');
END;
CREATE TRIGGER trg_actor_guard_aud_overseers_delete BEFORE DELETE ON AUD_OVERSEERS
WHEN etl_craft_actor() IS NULL OR etl_craft_actor() = ''
BEGIN
    SELECT RAISE(ABORT, 'AUD_OVERSEERS is written only by etl-craft');
END;

CREATE INDEX ix_pipeline_schedule_key ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID, RUN_KEY) WHERE TRIGGER_KIND = 'SCHEDULE';

-- Durable gate polling budget; TASK_ID is NULL for pipeline admission and NEXT_CHECK_AT is NULL when settled.
CREATE TABLE AUD_GATE_WAITS (
    PIPELINE_RUN_ID BIGINT NOT NULL REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    TASK_ID BIGINT REFERENCES CFG_TASKS(TASK_ID),
    FIRST_CHECK_AT TIMESTAMP NOT NULL,
    NEXT_CHECK_AT TIMESTAMP,
    LOOKS INT NOT NULL,
    WAIT_UNTIL TIMESTAMP NOT NULL,
    CONSTRAINT ck_gate_wait_task CHECK (TASK_ID IS NULL OR TASK_ID > 0),
    CONSTRAINT ck_gate_wait_looks CHECK (LOOKS >= 0 AND LOOKS <= 30)
);
CREATE UNIQUE INDEX ux_gate_wait ON AUD_GATE_WAITS (PIPELINE_RUN_ID, COALESCE(TASK_ID,0));
CREATE TRIGGER trg_actor_guard_aud_gate_waits_insert BEFORE INSERT ON AUD_GATE_WAITS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'audit rows are written only by etl-craft; use etl-craft run, mark or cancel') END;
END;
CREATE TRIGGER trg_actor_guard_aud_gate_waits_update BEFORE UPDATE ON AUD_GATE_WAITS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'audit rows are written only by etl-craft; use etl-craft run, mark or cancel') END;
END;
CREATE TRIGGER trg_actor_guard_aud_gate_waits_delete BEFORE DELETE ON AUD_GATE_WAITS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'audit rows are written only by etl-craft; use etl-craft run, mark or cancel') END;
END;
