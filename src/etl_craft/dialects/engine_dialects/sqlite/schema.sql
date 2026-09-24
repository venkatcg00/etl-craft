-- etl-craft Engine DB schema — SQLite edition.
--
-- [ADDITION, 2026-09-24] Per explicit instruction ("implement sqlite as default
-- engine and postgres as recommended for production"). schema.sql remains the
-- authoritative definition, and the Postgres one is the recommended production
-- Engine DB; this file is its SQLite translation, table for table and column
-- for column, so every engine query reads the same shape from either one.
-- Apply it with `etl-craft init-db` (or `etl-craft setup`), which picks this
-- file automatically for a jdbc:sqlite: Engine DB.
--
-- What changes in translation, and why nothing load-bearing is lost:
--
--   * IDENTITY -> INTEGER PRIMARY KEY AUTOINCREMENT. AUTOINCREMENT, not the
--     plain rowid alias, because ids must never be reused: the dependency
--     trackers compare "newer than the run last consumed" by id.
--   * ux_pipeline_run_one_active, the partial unique index that makes run-id
--     minting race-safe, is carried over unchanged — SQLite supports partial
--     indexes, and it serializes writers besides.
--   * TIMESTAMPTZ -> TIMESTAMP, stored as UTC text in one fixed format
--     ('YYYY-MM-DD HH:MM:SS.ffffff+00:00') so plain text comparison orders
--     correctly. db.py's adapter writes that format and its converter reads it
--     back as an aware datetime. DEFAULT_NOW below produces the same format.
--   * JSONB -> TEXT holding JSON; cfg.py decodes it.
--   * [DEVIATION] trg_set_audit_columns stamped CREATED_BY/UPDATED_BY from
--     Postgres's current_user. SQLite has no users at all, so here they default
--     to 'etl-craft' and otherwise hold whatever the inserting script supplies.
--     CREATED_BY and CREATE_DATE are still kept immutable on UPDATE, and
--     UPDATED_DATE is still stamped on every UPDATE, by AFTER UPDATE triggers
--     (SQLite triggers cannot assign to NEW). Those triggers' own UPDATE does
--     not re-fire them, because recursive_triggers is off by default.
--   * trg_default_depends_on_pipeline becomes an AFTER INSERT/UPDATE trigger.
--     ck_taskdep_no_self_dep is re-checked when it fills the column in, so a
--     self-dependency is still rejected.
--   * IS DISTINCT FROM -> IS NOT; the CHECKSUM regex -> GLOB.
--   * COMMENT ON is not SQL SQLite has; the comments live in schema.sql.
--
-- Statements are split with sqlite3.complete_statement (see migrate.py), so the
-- BEGIN ... END; trigger bodies below are safe.

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
    CONSTRAINT ck_pipelines_refresh_type CHECK (REFRESH_TYPE IN ('FULL', 'INCREMENTAL')),
    CONSTRAINT ck_pipelines_active_flag  CHECK (ACTIVE_FLAG IN ('Y', 'N')),
    CONSTRAINT ck_pipelines_parameters_json
        CHECK (PIPELINE_PARAMETERS IS NULL OR json_valid(PIPELINE_PARAMETERS))
);

CREATE UNIQUE INDEX ux_pipelines_code_active
    ON CFG_PIPELINES (PIPELINE_CODE) WHERE ACTIVE_FLAG = 'Y';

CREATE TRIGGER trg_audit_cfg_pipelines AFTER UPDATE ON CFG_PIPELINES
BEGIN
    UPDATE CFG_PIPELINES
    SET CREATED_BY = OLD.CREATED_BY,
        CREATE_DATE = OLD.CREATE_DATE,
        UPDATED_DATE = strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'
    WHERE PIPELINE_ID = NEW.PIPELINE_ID;
END;

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
    CONSTRAINT ck_pipedep_type        CHECK (DEPENDENCY_TYPE IN ('SUCCESS','FAILURE','ALWAYS','HAS_DATA')),
    CONSTRAINT ck_pipedep_active_flag CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_pipedep_no_self_dep CHECK (PIPELINE_ID <> DEPENDS_ON_PIPELINE_ID)
);

CREATE UNIQUE INDEX ux_pipedep_edge_active
    ON CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE)
    WHERE ACTIVE_FLAG = 'Y';
CREATE INDEX ix_pipedep_depends_on ON CFG_PIPELINE_DEPENDENCY (DEPENDS_ON_PIPELINE_ID);

CREATE TRIGGER trg_audit_cfg_pipeline_dependency AFTER UPDATE ON CFG_PIPELINE_DEPENDENCY
BEGIN
    UPDATE CFG_PIPELINE_DEPENDENCY
    SET CREATED_BY = OLD.CREATED_BY,
        CREATE_DATE = OLD.CREATE_DATE,
        UPDATED_DATE = strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'
    WHERE PIPELINE_DEPENDENCY_ID = NEW.PIPELINE_DEPENDENCY_ID;
END;

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
    CONSTRAINT ck_tasks_task_type     CHECK (TASK_TYPE IN ('INGESTION','ETL')),
    CONSTRAINT ck_tasks_handler       CHECK (HANDLER IN ('PYTHON','SQL','BUSINESS_RULES','EMAIL_ALERT')),
    CONSTRAINT ck_tasks_active_flag   CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_tasks_run_condition CHECK (RUN_CONDITION IS NULL OR RUN_CONDITION IN ('ALL','ANY','N')),
    CONSTRAINT ck_tasks_run_condition_count CHECK (
        (RUN_CONDITION = 'N' AND RUN_CONDITION_COUNT IS NOT NULL AND RUN_CONDITION_COUNT >= 1)
        OR (RUN_CONDITION IS NOT 'N' AND RUN_CONDITION_COUNT IS NULL)
    )
);

CREATE UNIQUE INDEX ux_tasks_code_active
    ON CFG_TASKS (PIPELINE_ID, TASK_CODE) WHERE ACTIVE_FLAG = 'Y';

CREATE TRIGGER trg_audit_cfg_tasks AFTER UPDATE ON CFG_TASKS
BEGIN
    UPDATE CFG_TASKS
    SET CREATED_BY = OLD.CREATED_BY,
        CREATE_DATE = OLD.CREATE_DATE,
        UPDATED_DATE = strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'
    WHERE TASK_ID = NEW.TASK_ID;
END;

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

CREATE TRIGGER trg_audit_cfg_task_dependency AFTER UPDATE ON CFG_TASK_DEPENDENCY
BEGIN
    UPDATE CFG_TASK_DEPENDENCY
    SET CREATED_BY = OLD.CREATED_BY,
        CREATE_DATE = OLD.CREATE_DATE,
        UPDATED_DATE = strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00',
        DEPENDS_ON_PIPELINE_ID = COALESCE(NEW.DEPENDS_ON_PIPELINE_ID, NEW.PIPELINE_ID)
    WHERE TASK_DEPENDENCY_ID = NEW.TASK_DEPENDENCY_ID;
END;

CREATE UNIQUE INDEX ux_taskdep_edge_active
    ON CFG_TASK_DEPENDENCY (TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE)
    WHERE ACTIVE_FLAG = 'Y';
CREATE INDEX ix_taskdep_depends_on ON CFG_TASK_DEPENDENCY (DEPENDS_ON_TASK_ID);

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

CREATE TRIGGER trg_audit_cfg_task_parameters AFTER UPDATE ON CFG_TASK_PARAMETERS
BEGIN
    UPDATE CFG_TASK_PARAMETERS
    SET CREATED_BY = OLD.CREATED_BY,
        CREATE_DATE = OLD.CREATE_DATE,
        UPDATED_DATE = strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'
    WHERE TASK_PARAMETER_ID = NEW.TASK_PARAMETER_ID;
END;

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

CREATE TRIGGER trg_audit_cfg_business_rules AFTER UPDATE ON CFG_BUSINESS_RULES
BEGIN
    UPDATE CFG_BUSINESS_RULES
    SET CREATED_BY = OLD.CREATED_BY,
        CREATE_DATE = OLD.CREATE_DATE,
        UPDATED_DATE = strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'
    WHERE BUSINESS_RULE_ID = NEW.BUSINESS_RULE_ID;
END;

CREATE TABLE AUD_PIPELINES_RUN_LOG (
    PIPELINE_RUN_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID      BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    START_DATE       TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    END_DATE         TIMESTAMP,
    STATUS           VARCHAR NOT NULL,
    -- MET/BREACHED against SLA_IN_HOURS when Enforce_sla is on (migration 0005).
    SLA_STATUS       VARCHAR(8),
    CONSTRAINT ck_pipeline_run_status CHECK (STATUS IN ('IN-PROGRESS','SUCCESS','FAILED','SKIPPED')),
    CONSTRAINT ck_pipeline_run_sla_status CHECK (SLA_STATUS IN ('MET','BREACHED'))
);

-- The one that matters most: at most one IN-PROGRESS run per pipeline.
CREATE UNIQUE INDEX ux_pipeline_run_one_active
    ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID) WHERE STATUS = 'IN-PROGRESS';
CREATE INDEX ix_pipeline_run_pipeline ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID);

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
    CONSTRAINT ck_task_run_status CHECK (STATUS IN ('IN-PROGRESS','SUCCESS','FAILED','SKIPPED'))
);

CREATE UNIQUE INDEX ux_task_run_one_per_pipeline_run
    ON AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID);
CREATE INDEX ix_task_run_pipeline_run ON AUD_TASK_RUN_LOG (PIPELINE_RUN_ID);

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

CREATE TABLE AUD_TASK_OFFSET_TRACKER (
    TASK_ID                 BIGINT PRIMARY KEY REFERENCES CFG_TASKS(TASK_ID),
    OFFSET_TYPE             VARCHAR NOT NULL,
    OFFSET_VALUE            VARCHAR,
    LAST_UPDATED_TIMESTAMP  TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSTRAINT ck_offset_type CHECK (OFFSET_TYPE IN ('NUMBER','TEXT','TIMESTAMP'))
);

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

CREATE TABLE AUD_PIPELINE_DEPENDENCY_TRACKER (
    PIPELINE_DEPENDENCY_ID         BIGINT PRIMARY KEY REFERENCES CFG_PIPELINE_DEPENDENCY(PIPELINE_DEPENDENCY_ID),
    PIPELINE_ID                    BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    DEPENDS_ON_PIPELINE_ID         BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    LAST_CONSUMED_PIPELINE_RUN_ID  BIGINT REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    LAST_CONSUMED_END_DATE         TIMESTAMP,
    LAST_UPDATED_TIMESTAMP         TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')
);

CREATE TABLE AUD_TASK_DEPENDENCY_TRACKER (
    TASK_DEPENDENCY_ID         BIGINT PRIMARY KEY REFERENCES CFG_TASK_DEPENDENCY(TASK_DEPENDENCY_ID),
    TASK_ID                    BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    PIPELINE_ID                BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    DEPENDS_ON_TASK_ID         BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    DEPENDS_ON_PIPELINE_ID     BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    LAST_CONSUMED_TASK_RUN_ID  BIGINT REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    LAST_CONSUMED_END_DATE     TIMESTAMP,
    LAST_UPDATED_TIMESTAMP     TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')
);

CREATE TABLE SCHEMA_MIGRATIONS (
    SOURCE       VARCHAR NOT NULL DEFAULT 'LEGACY',
    VERSION      VARCHAR NOT NULL,
    CHECKSUM     VARCHAR(64),
    APPLIED_AT   TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSTRAINT pk_schema_migrations PRIMARY KEY (SOURCE, VERSION),
    CONSTRAINT ck_schema_migrations_source
        CHECK (SOURCE IN ('ENGINE', 'PROJECT', 'LEGACY')),
    CONSTRAINT ck_schema_migrations_checksum
        CHECK (CHECKSUM IS NULL OR (length(CHECKSUM) = 64 AND CHECKSUM NOT GLOB '*[^0-9a-f]*'))
);
