-- ============================================================================
-- etl-craft — Engine DB schema (SIGNED OFF 2026-09-19, see CLAUDE.md)
-- Target: PostgreSQL 14+ (uses GENERATED ALWAYS AS IDENTITY, partial unique
-- indexes, and CHECK constraints referencing sibling columns of the same row)
-- ============================================================================
--
-- STATUS: reconciled against two pasted schema drafts (an earlier and a
-- later revision) plus every correction made verbally after them, then
-- reviewed and signed off on 2026-09-19 — every [DEVIATION]/[ADDITION]/
-- [CHOICE] flag below was walked through individually and confirmed as
-- written. Resolver/CLI/DAG-gen code may now be built against this.
--
-- Assumes it runs inside an already-provisioned, empty Postgres database/
-- schema dedicated to the engine. Does not create the database itself —
-- that's a deployment concern (Docker Compose init, `configure`, etc.).
--
-- HOW TO READ THE FLAGS IN THIS FILE:
--   [DEVIATION] — this table/column differs from what was literally pasted
--                 in the schema drafts, because a later verbal decision
--                 superseded it. Flagged so it's easy to spot and challenge.
--   [ADDITION]  — a column/constraint/index not present in either pasted
--                 draft at all — either because something else already
--                 agreed on (usually the CLI) requires it to exist
--                 somewhere, or because it's a straightforward consequence
--                 of a mechanism discussed elsewhere (e.g. "resume, not
--                 restart" implying one log row per attempt, not many).
--   [CHOICE]    — the pasted notes said "(WILL BE ENFORCED BY THE ENGINE
--                 SETUP AS TRIGGER)" for this column; implemented here as a
--                 CHECK constraint (or an IDENTITY column) instead, because
--                 Postgres CHECK constraints can already reference sibling
--                 columns of the same row and are the cheaper, more
--                 idiomatic tool for pure value/row validation, and IDENTITY
--                 is the standard mechanism for auto-numbered keys. A real
--                 trigger is used only in the two places below where a
--                 CHECK/DEFAULT genuinely cannot do the job. Flagged in case
--                 a literal hand-written trigger was actually wanted instead
--                 (e.g. to centralize validation alongside audit-column
--                 logic, or to log rejected attempts somewhere).
--
-- See the summary block at the very end of this file for the full,
-- consolidated list of every flag below.
-- ============================================================================

BEGIN;

-- ============================================================================
-- SHARED TRIGGER FUNCTIONS
-- ============================================================================

-- CREATED_BY / UPDATED_BY must resolve to Postgres's own current_user, and
-- UPDATED_BY must change on every UPDATE — a plain column DEFAULT can't do
-- that (DEFAULT only ever fires at INSERT). This one is a trigger out of
-- genuine necessity, reused as BEFORE INSERT OR UPDATE on every CFG_ table.
CREATE OR REPLACE FUNCTION trg_set_audit_columns()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.CREATED_BY   := current_user;
        NEW.CREATE_DATE  := now();
        NEW.UPDATED_BY   := current_user;
        NEW.UPDATED_DATE := now();
    ELSIF TG_OP = 'UPDATE' THEN
        NEW.CREATED_BY   := OLD.CREATED_BY;    -- immutable once set
        NEW.CREATE_DATE  := OLD.CREATE_DATE;   -- immutable once set
        NEW.UPDATED_BY   := current_user;
        NEW.UPDATED_DATE := now();
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION trg_set_audit_columns() IS
    'Stamps CREATED_BY/CREATE_DATE/UPDATED_BY/UPDATED_DATE from current_user and now(). '
    'For these to mean anything, every human or service account touching the Engine DB '
    'needs its own distinct Postgres role — not one shared role for everyone.';

-- CFG_TASK_DEPENDENCY.DEPENDS_ON_PIPELINE_ID defaults to the task's own
-- PIPELINE_ID when left NULL ("defaults to pipeline_id if null"). A column
-- DEFAULT cannot reference a sibling column of the same row being inserted,
-- so this is also a trigger out of genuine necessity, not a [CHOICE].
CREATE OR REPLACE FUNCTION trg_default_depends_on_pipeline()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.DEPENDS_ON_PIPELINE_ID IS NULL THEN
        NEW.DEPENDS_ON_PIPELINE_ID := NEW.PIPELINE_ID;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- CONFIG TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- CFG_PIPELINES
-- ----------------------------------------------------------------------------
CREATE TABLE CFG_PIPELINES (
    PIPELINE_ID    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,   -- [CHOICE] IDENTITY, not a hand-written trigger
    PIPELINE_CODE  VARCHAR NOT NULL,                                  -- [ADDITION] not in either pasted draft — the CLI
                                                                        -- (`run --pipeline_code X`) needs a stable lookup
                                                                        -- key distinct from PIPELINE_ID, the same way
                                                                        -- CFG_TASKS.TASK_CODE works for tasks. Confirm
                                                                        -- name/existence before code depends on it.
    PIPELINE_NAME  VARCHAR NOT NULL,
    DESCRIPTION    VARCHAR,
    RUN_SCHEDULE   VARCHAR,                                            -- Airflow-standard cron expression
    SLA_IN_HOURS   NUMERIC,                                            -- fractional hours allowed (e.g. 1.5)
    REFRESH_TYPE   VARCHAR NOT NULL,                                   -- [DEVIATION] pasted drafts have this on CFG_TASKS;
                                                                        -- moved here per explicit later decision ("Make
                                                                        -- the refresh_type as pipeline value, not task
                                                                        -- value") — every task in one pipeline run
                                                                        -- shares one mode.
    ACTIVE_FLAG    VARCHAR NOT NULL DEFAULT 'Y',
    -- [ADDITION, post-signoff 2026-09-19] Per-pipeline overrides for
    -- generate-yml's Airflow-facing DAG fields (default_args/catchup/
    -- tags). All nullable by design: NULL means "not set at the pipeline
    -- level" and generate-yml falls back to craft-connector.yml's
    -- [Orchestrator] section, then to a final hardcoded default if that
    -- isn't set either — never silently invented, always resolvable back
    -- to either a real CFG_ row or a real config file setting. See
    -- CLAUDE.md's "Where things stand" for the full three-tier resolution
    -- and why EMAIL_RECIPIENTS exists (EMAIL_ON_FAILURE alone is inert in
    -- real Airflow without addresses to send to).
    CATCHUP              BOOLEAN,
    TAGS                 VARCHAR[],
    RETRIES              INTEGER,
    RETRY_DELAY_MINUTES  INTEGER,
    DEPENDS_ON_PAST      BOOLEAN,
    EMAIL_ON_FAILURE     BOOLEAN,
    EMAIL_RECIPIENTS     VARCHAR[],
    CREATED_BY     VARCHAR,
    CREATE_DATE    TIMESTAMPTZ,
    UPDATED_BY     VARCHAR,
    UPDATED_DATE   TIMESTAMPTZ,
    CONSTRAINT ck_pipelines_refresh_type CHECK (REFRESH_TYPE IN ('FULL', 'INCREMENTAL')),  -- [CHOICE]
    CONSTRAINT ck_pipelines_active_flag  CHECK (ACTIVE_FLAG IN ('Y', 'N')),
    CONSTRAINT ck_pipelines_retries_non_negative CHECK (RETRIES IS NULL OR RETRIES >= 0),  -- [ADDITION]
    CONSTRAINT ck_pipelines_retry_delay_non_negative
        CHECK (RETRY_DELAY_MINUTES IS NULL OR RETRY_DELAY_MINUTES >= 0)  -- [ADDITION]
);

CREATE UNIQUE INDEX ux_pipelines_code_active
    ON CFG_PIPELINES (PIPELINE_CODE) WHERE ACTIVE_FLAG = 'Y';         -- [ADDITION]

CREATE TRIGGER trg_audit_cfg_pipelines
    BEFORE INSERT OR UPDATE ON CFG_PIPELINES
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();

COMMENT ON TABLE CFG_PIPELINES IS 'Top-level pipeline definitions. Pipeline creation is always manual (git-managed migrations/inserts) — never a CLI verb.';

-- ----------------------------------------------------------------------------
-- CFG_PIPELINE_DEPENDENCY
-- ----------------------------------------------------------------------------
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
    CONSTRAINT ck_pipedep_type        CHECK (DEPENDENCY_TYPE IN ('SUCCESS','FAILURE','ALWAYS','HAS_DATA')),  -- [CHOICE]
    CONSTRAINT ck_pipedep_active_flag CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_pipedep_no_self_dep CHECK (PIPELINE_ID <> DEPENDS_ON_PIPELINE_ID)  -- [ADDITION] safety guard, not stated explicitly
);

CREATE UNIQUE INDEX ux_pipedep_edge_active
    ON CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE)
    WHERE ACTIVE_FLAG = 'Y';                                          -- [ADDITION] prevents duplicate identical edges

CREATE INDEX ix_pipedep_depends_on ON CFG_PIPELINE_DEPENDENCY (DEPENDS_ON_PIPELINE_ID);  -- [ADDITION] "what depends on X" lookups

CREATE TRIGGER trg_audit_cfg_pipeline_dependency
    BEFORE INSERT OR UPDATE ON CFG_PIPELINE_DEPENDENCY
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();

COMMENT ON TABLE CFG_PIPELINE_DEPENDENCY IS 'Cross-pipeline dependency edges. Always resolved by polling + AUD_PIPELINE_DEPENDENCY_TRACKER at runtime — no orchestrator has a native way to express one DAG depending on another.';

-- ----------------------------------------------------------------------------
-- CFG_TASKS
-- ----------------------------------------------------------------------------
CREATE TABLE CFG_TASKS (
    TASK_ID        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    TASK_CODE      VARCHAR NOT NULL,
    TASK_TYPE      VARCHAR NOT NULL,
    PIPELINE_ID    BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    HANDLER        VARCHAR NOT NULL,
    SCRIPT_NAME    VARCHAR,
    RETURN_VALUES  VARCHAR,
    ACTIVE_FLAG    VARCHAR NOT NULL DEFAULT 'Y',
    -- [ADDITION, post-signoff 2026-09-19] Per-task opt-in to the SQL
    -- execution engine's schema-evolution path: when a HANDLER=SQL task's
    -- staged SELECT shape gains a column the target table doesn't have yet,
    -- FALSE (the default) fails the task with a clear reason instead of
    -- silently writing a mismatched shape; TRUE rebuilds the target with the
    -- new column added at the position the SELECT puts it. See
    -- sql_actions.py for the full mechanism (information_schema-driven
    -- comparison, portable rebuild-and-swap in place of vendor-specific
    -- CREATE OR REPLACE TABLE, which Postgres itself doesn't support).
    SCHEMA_EVOLUTION BOOLEAN NOT NULL DEFAULT FALSE,
    CREATED_BY     VARCHAR,
    CREATE_DATE    TIMESTAMPTZ,
    UPDATED_BY     VARCHAR,
    UPDATED_DATE   TIMESTAMPTZ,
    CONSTRAINT ck_tasks_task_type       CHECK (TASK_TYPE IN ('INGESTION','ETL')),  -- [CHOICE]
    CONSTRAINT ck_tasks_handler         CHECK (HANDLER IN ('PYTHON','SQL','BUSINESS_RULES','EMAIL_ALERT')),  -- [DEVIATION] EMAIL_ALERT added per later decision; neither pasted draft has it
    CONSTRAINT ck_tasks_script_required CHECK (HANDLER <> 'PYTHON' OR SCRIPT_NAME IS NOT NULL),  -- [CHOICE] "required for python scripts"
    CONSTRAINT ck_tasks_return_values   CHECK (                                    -- [CHOICE]
        RETURN_VALUES IS NULL OR
        RETURN_VALUES ~ '^(INGESTION_COUNT|LATEST_OFFSET_UPDATE)(,(INGESTION_COUNT|LATEST_OFFSET_UPDATE))*$'
    ),
    CONSTRAINT ck_tasks_active_flag     CHECK (ACTIVE_FLAG IN ('Y','N'))
);

CREATE UNIQUE INDEX ux_tasks_code_active
    ON CFG_TASKS (PIPELINE_ID, TASK_CODE) WHERE ACTIVE_FLAG = 'Y';    -- [ADDITION] scoped per-pipeline, not global

CREATE TRIGGER trg_audit_cfg_tasks
    BEFORE INSERT OR UPDATE ON CFG_TASKS
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();

COMMENT ON TABLE CFG_TASKS IS 'REFRESH_TYPE intentionally absent here — moved to CFG_PIPELINES, see comment there. Do not re-add it here out of habit when comparing against the pasted schema notes.';

-- ----------------------------------------------------------------------------
-- CFG_TASK_DEPENDENCY
-- ----------------------------------------------------------------------------
CREATE TABLE CFG_TASK_DEPENDENCY (
    TASK_DEPENDENCY_ID      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PIPELINE_ID             BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    TASK_ID                 BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    DEPENDS_ON_PIPELINE_ID  BIGINT REFERENCES CFG_PIPELINES(PIPELINE_ID),  -- nullable on input; trg_default_depends_on_pipeline fills it
    DEPENDS_ON_TASK_ID      BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    DEPENDENCY_TYPE         VARCHAR NOT NULL,
    ACTIVE_FLAG             VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY              VARCHAR,
    CREATE_DATE             TIMESTAMPTZ,
    UPDATED_BY              VARCHAR,
    UPDATED_DATE            TIMESTAMPTZ,
    CONSTRAINT ck_taskdep_type        CHECK (DEPENDENCY_TYPE IN ('SUCCESS','FAILURE','ALWAYS','HAS_DATA')),  -- [CHOICE]
    CONSTRAINT ck_taskdep_active_flag CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_taskdep_no_self_dep CHECK (NOT (TASK_ID = DEPENDS_ON_TASK_ID AND PIPELINE_ID = DEPENDS_ON_PIPELINE_ID))  -- [ADDITION]
);

-- Postgres evaluates CHECK constraints only after ALL BEFORE ROW triggers
-- have finished modifying NEW, so ck_taskdep_no_self_dep above always sees
-- the fully-resolved DEPENDS_ON_PIPELINE_ID regardless of which of these two
-- BEFORE triggers happens to fire first (they don't interact either way).
CREATE TRIGGER trg_default_taskdep_pipeline
    BEFORE INSERT OR UPDATE ON CFG_TASK_DEPENDENCY
    FOR EACH ROW EXECUTE FUNCTION trg_default_depends_on_pipeline();

CREATE TRIGGER trg_audit_cfg_task_dependency
    BEFORE INSERT OR UPDATE ON CFG_TASK_DEPENDENCY
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();

CREATE UNIQUE INDEX ux_taskdep_edge_active
    ON CFG_TASK_DEPENDENCY (TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE)
    WHERE ACTIVE_FLAG = 'Y';                                          -- [ADDITION]

CREATE INDEX ix_taskdep_depends_on ON CFG_TASK_DEPENDENCY (DEPENDS_ON_TASK_ID);  -- [ADDITION]

COMMENT ON TABLE CFG_TASK_DEPENDENCY IS 'Same-pipeline edges compile into native Airflow task chaining when generating YAML; cross-pipeline edges (DEPENDS_ON_PIPELINE_ID <> PIPELINE_ID) resolve via AUD_TASK_DEPENDENCY_TRACKER at runtime instead.';

-- ----------------------------------------------------------------------------
-- CFG_TASK_PARAMETERS
-- ----------------------------------------------------------------------------
-- [DEVIATION, post-signoff 2026-09-19] Renamed from CFG_TASK_PARAMS, with
-- VARIABLE_NAME/VARIABLE_VALUE -> PARAMETER_NAME/PARAMETER_VALUE (and
-- TASK_PARAM_ID -> TASK_PARAMETER_ID to match) — "variable" read wrong for
-- what these rows actually are: fixed, per-task configuration values the
-- SQL execution engine reads by a closed key vocabulary, not variables in
-- any programming sense. Corrected per explicit instruction before any
-- external tooling could come to depend on the old names.
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
    ON CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME) WHERE ACTIVE_FLAG = 'Y';  -- [ADDITION] "one step per task enforced"

CREATE TRIGGER trg_audit_cfg_task_parameters
    BEFORE INSERT OR UPDATE ON CFG_TASK_PARAMETERS
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();

COMMENT ON TABLE CFG_TASK_PARAMETERS IS
    'Flexible key-value store. PARAMETER_NAME conventions relied on by the engine at read time, not enforced at the schema level (see sql_actions.py''s own module docstring for the authoritative, current list): '
    'SQL_ACTION (one of CREATE_TABLE | SETUP_TABLE | OVERWRITE_TABLE | SCD1_MERGE | SCD2_MERGE | DROP_TABLE | DELETE_ROWS), '
    'TARGET_OBJECT ("schema.table", database name always supplied at runtime from the active [Warehouse] profile — never stored here), '
    'SOURCE_SQL (the bare read-only SELECT; required for every SQL_ACTION except DROP_TABLE), '
    'MERGE_KEY / MERGE_COMPARE_COLUMNS (pipe-separated column lists; required for SCD1_MERGE/SCD2_MERGE, MERGE_KEY alone also required for DELETE_ROWS), '
    'HARD_DELETE (DELETE_ROWS only; "true" deletes for real, anything else soft-deletes via DELETE_FLAG).';

-- ----------------------------------------------------------------------------
-- CFG_BUSINESS_RULES
-- ----------------------------------------------------------------------------
CREATE TABLE CFG_BUSINESS_RULES (
    BUSINESS_RULE_ID          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    BUSINESS_RULE_NAME        VARCHAR NOT NULL,
    PIPELINE_ID               BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    TASK_ID                   BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    BUSINESS_RULE_SQL         VARCHAR NOT NULL,
    BUSINESS_RULE_TYPE        VARCHAR NOT NULL,
    BUSINESS_RULE_KEY_COLUMN  VARCHAR NOT NULL,
    TARGET_TABLE              VARCHAR NOT NULL,   -- schema.table in the Data DB
    SEQUENCE_NUMBER           BIGINT NOT NULL,    -- dense rank per TASK_ID; same rank = parallel, different rank = sequential
    ACTIVE_FLAG               VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY                VARCHAR,
    CREATE_DATE               TIMESTAMPTZ,
    UPDATED_BY                VARCHAR,
    UPDATED_DATE              TIMESTAMPTZ,
    CONSTRAINT ck_br_type        CHECK (BUSINESS_RULE_TYPE IN ('INCOMPLETE','REJECT','REPORT')),  -- [CHOICE] — note: neither
                                                                                           -- pasted draft annotated this one
                                                                                           -- as trigger-enforced at all,
                                                                                           -- unlike every other enum column;
                                                                                           -- CHECK used anyway for
                                                                                           -- consistency. REPORT added
                                                                                           -- post-signoff 2026-09-19 — a
                                                                                           -- third classification bucket
                                                                                           -- for teams that want pure
                                                                                           -- reportability (flagged +
                                                                                           -- tracked in
                                                                                           -- AUD_BUSINESS_RULES_RESULTS)
                                                                                           -- without INCOMPLETE/REJECT's
                                                                                           -- connotations. Execution is
                                                                                           -- identical for all three types
                                                                                           -- — this only widens the
                                                                                           -- vocabulary, see
                                                                                           -- business_rules.py
    CONSTRAINT ck_br_active_flag CHECK (ACTIVE_FLAG IN ('Y','N'))
);

CREATE UNIQUE INDEX ux_br_name_active
    ON CFG_BUSINESS_RULES (TASK_ID, BUSINESS_RULE_NAME) WHERE ACTIVE_FLAG = 'Y';  -- [ADDITION]

CREATE INDEX ix_br_pipeline ON CFG_BUSINESS_RULES (PIPELINE_ID);  -- [ADDITION]

CREATE TRIGGER trg_audit_cfg_business_rules
    BEFORE INSERT OR UPDATE ON CFG_BUSINESS_RULES
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();

COMMENT ON TABLE CFG_BUSINESS_RULES IS
    'BUSINESS_RULE_KEY_COLUMN can safely stay a single column because every TARGET_TABLE is required to have a single-column primary key — an enforced framework convention this schema cannot itself check, since TARGET_TABLE lives in the Data DB, a separate connection and possibly a separate database engine entirely. Enforce it at `validate` time via introspection, not here.';


-- ============================================================================
-- AUDIT TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- AUD_PIPELINES_RUN_LOG
-- ----------------------------------------------------------------------------
CREATE TABLE AUD_PIPELINES_RUN_LOG (
    PIPELINE_RUN_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    PIPELINE_ID      BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    START_DATE       TIMESTAMPTZ NOT NULL DEFAULT now(),
    END_DATE         TIMESTAMPTZ,
    STATUS           VARCHAR NOT NULL,
    CONSTRAINT ck_pipeline_run_status CHECK (STATUS IN ('IN-PROGRESS','SUCCESS','FAILED','SKIPPED'))  -- [CHOICE]
);

-- THE single most load-bearing constraint in this entire schema. Every task
-- resolves the "currently active" pipeline_run_id by querying this table
-- rather than being handed one (see CLAUDE.md "Run-id resolution") — this
-- index is what makes that a find-or-create instead of a race. A trigger or
-- an application-level check-then-insert CANNOT close this race; only a
-- DB-enforced constraint like this can, because Postgres evaluates it
-- atomically inside the storage engine at insert time.
CREATE UNIQUE INDEX ux_pipeline_run_one_active
    ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID) WHERE STATUS = 'IN-PROGRESS';

CREATE INDEX ix_pipeline_run_pipeline ON AUD_PIPELINES_RUN_LOG (PIPELINE_ID);  -- [ADDITION] general run-history lookups;
                                                                                 -- the partial index above only covers
                                                                                 -- IN-PROGRESS rows

COMMENT ON TABLE AUD_PIPELINES_RUN_LOG IS 'One row per pipeline run. pipeline_run_id is never passed between tasks — every task resolves the active run by querying this table. See CLAUDE.md "Run-id resolution".';

-- ----------------------------------------------------------------------------
-- AUD_TASK_RUN_LOG
-- ----------------------------------------------------------------------------
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
    ERROR_MESSAGE    VARCHAR,   -- [DEVIATION] fixed from ERROR_MESSGAE typo in the earlier pasted draft (already fixed in the later one too)
    TASK_LOG         VARCHAR,
    CONSTRAINT ck_task_run_status CHECK (STATUS IN ('IN-PROGRESS','SUCCESS','FAILED','SKIPPED'))  -- [CHOICE]
);

-- [ADDITION] Not stated as an explicit constraint anywhere in either pasted
-- draft, but implied by "resume, not restart": a task should have exactly
-- one log row per pipeline run, updated in place across retries
-- (IN-PROGRESS -> terminal), never a new row per attempt. Flag this if
-- multiple attempt-rows per run was actually intended.
CREATE UNIQUE INDEX ux_task_run_one_per_pipeline_run
    ON AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID);

CREATE INDEX ix_task_run_pipeline_run ON AUD_TASK_RUN_LOG (PIPELINE_RUN_ID);  -- [ADDITION] "all tasks in this run" lookups

COMMENT ON TABLE AUD_TASK_RUN_LOG IS 'Task-level execution record per run. A task with an existing SUCCESS binding under the active pipeline_run_id short-circuits to success without re-running.';

-- ----------------------------------------------------------------------------
-- AUD_BUSINESS_RULES_RUN_LOG
-- ----------------------------------------------------------------------------
CREATE TABLE AUD_BUSINESS_RULES_RUN_LOG (
    BUSINESS_RULE_RUN_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    BUSINESS_RULE_ID      BIGINT NOT NULL REFERENCES CFG_BUSINESS_RULES(BUSINESS_RULE_ID),
    TASK_RUN_ID           BIGINT NOT NULL REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    START_DATE            TIMESTAMPTZ NOT NULL DEFAULT now(),   -- [DEVIATION] was NUMBER in both pasted drafts; fixed to
                                                                  -- TIMESTAMPTZ per explicit "fix it" instruction
    END_DATE              TIMESTAMPTZ,
    STATUS                VARCHAR NOT NULL,                     -- [DEVIATION] was TIMESTAMP in the later pasted draft
                                                                  -- (the fix landed on the wrong pair of columns there);
                                                                  -- corrected to VARCHAR per the same instruction
    CONSTRAINT ck_br_run_status CHECK (STATUS IN ('IN-PROGRESS','SUCCESS','FAILED','SKIPPED'))  -- [CHOICE]
);

CREATE UNIQUE INDEX ux_br_run_one_per_task_run
    ON AUD_BUSINESS_RULES_RUN_LOG (BUSINESS_RULE_ID, TASK_RUN_ID);  -- [ADDITION] same resume-not-restart reasoning as AUD_TASK_RUN_LOG

COMMENT ON TABLE AUD_BUSINESS_RULES_RUN_LOG IS 'STATUS here is whether the rule *ran* successfully, not what it found — a rule that flags every row it checks is still STATUS = SUCCESS. See CFG_BUSINESS_RULES.BUSINESS_RULE_TYPE for row classification, and AUD_BUSINESS_RULES_RESULTS for what got flagged.';

-- ----------------------------------------------------------------------------
-- AUD_BUSINESS_RULES_RESULTS
-- ----------------------------------------------------------------------------
CREATE TABLE AUD_BUSINESS_RULES_RESULTS (
    BUSINESS_RULE_RESULT_ID  BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    BUSINESS_RULE_RUN_ID     BIGINT NOT NULL REFERENCES AUD_BUSINESS_RULES_RUN_LOG(BUSINESS_RULE_RUN_ID),
    BUSINESS_RULE_ID         BIGINT NOT NULL REFERENCES CFG_BUSINESS_RULES(BUSINESS_RULE_ID),
    BUSINESS_RULE_KEY        VARCHAR NOT NULL,   -- [DEVIATION] was a numeric hash in the earlier pasted draft (collision
                                                   -- risk flagged and dropped); later draft stores the raw key value as
                                                   -- text regardless of the source column's real type, kept here
    TARGET_TABLE             VARCHAR NOT NULL,
    STATUS                   VARCHAR NOT NULL,
    ACTIVE_FLAG              VARCHAR NOT NULL DEFAULT 'Y',
    START_DATE               TIMESTAMPTZ NOT NULL DEFAULT now(),
    END_DATE                 TIMESTAMPTZ,
    CONSTRAINT ck_brresults_status      CHECK (STATUS IN ('INCOMPLETE','REJECT','REPORT')),  -- [CHOICE] mirrors
                                                                                                -- CFG_BUSINESS_RULES.BUSINESS_RULE_TYPE
                                                                                                -- above — STATUS here is that
                                                                                                -- rule's type, copied down onto
                                                                                                -- each flagged row
    CONSTRAINT ck_brresults_active_flag CHECK (ACTIVE_FLAG IN ('Y','N'))
);

CREATE INDEX ix_brresults_run ON AUD_BUSINESS_RULES_RESULTS (BUSINESS_RULE_RUN_ID);   -- [ADDITION]
CREATE INDEX ix_brresults_rule ON AUD_BUSINESS_RULES_RESULTS (BUSINESS_RULE_ID);      -- [ADDITION]

COMMENT ON TABLE AUD_BUSINESS_RULES_RESULTS IS 'One row per flagged record. Cloned to the Data DB after pipeline completion when [Cloning] is enabled in craft-connector.yml and a BR step ran that pipeline.';

-- ----------------------------------------------------------------------------
-- AUD_TASK_OFFSET_TRACKER
-- ----------------------------------------------------------------------------
CREATE TABLE AUD_TASK_OFFSET_TRACKER (
    TASK_ID                 BIGINT PRIMARY KEY REFERENCES CFG_TASKS(TASK_ID),
    OFFSET_TYPE             VARCHAR NOT NULL,
    OFFSET_VALUE            VARCHAR,
    LAST_UPDATED_TIMESTAMP  TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_offset_type CHECK (OFFSET_TYPE IN ('NUMBER','TEXT','TIMESTAMP'))  -- [CHOICE]
);

COMMENT ON TABLE AUD_TASK_OFFSET_TRACKER IS 'Watermark for incremental ingestion. Only ever consulted at read time; everything downstream of ingestion filters on pipeline_run_id instead — see CLAUDE.md "Incremental / full-refresh mechanics".';

-- ----------------------------------------------------------------------------
-- AUD_PIPELINE_DEPENDENCY_TRACKER   [ADDITION] — not in either pasted draft
-- ----------------------------------------------------------------------------
CREATE TABLE AUD_PIPELINE_DEPENDENCY_TRACKER (
    PIPELINE_DEPENDENCY_ID         BIGINT PRIMARY KEY REFERENCES CFG_PIPELINE_DEPENDENCY(PIPELINE_DEPENDENCY_ID),
    PIPELINE_ID                    BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    DEPENDS_ON_PIPELINE_ID         BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    LAST_CONSUMED_PIPELINE_RUN_ID  BIGINT REFERENCES AUD_PIPELINES_RUN_LOG(PIPELINE_RUN_ID),
    LAST_CONSUMED_END_DATE         TIMESTAMPTZ,
    LAST_UPDATED_TIMESTAMP         TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE AUD_PIPELINE_DEPENDENCY_TRACKER IS
    '[ADDITION] Designed after both pasted schema drafts, to fix a real bug: comparing only against a dependency''s "last logged run" breaks the moment two pipelines run on different schedules (e.g. weekly vs. daily) — most days the dependency isn''t IN-PROGRESS, so the naive check falls through to a stale run from days ago and wrongly proceeds. '
    'One row per CFG_PIPELINE_DEPENDENCY edge; the condition to track is implied by that edge''s own DEPENDENCY_TYPE (not a separate column here). A candidate run only satisfies the edge if it is newer than LAST_CONSUMED_* *and* matches that condition — SUCCESS tracks the last SUCCESS run, FAILURE the last FAILED run, HAS_DATA the last run with TARGET_COUNT > 0. Updated only after the gated pipeline completes.';

-- ----------------------------------------------------------------------------
-- AUD_TASK_DEPENDENCY_TRACKER   [ADDITION] — not in either pasted draft
-- ----------------------------------------------------------------------------
CREATE TABLE AUD_TASK_DEPENDENCY_TRACKER (
    TASK_DEPENDENCY_ID         BIGINT PRIMARY KEY REFERENCES CFG_TASK_DEPENDENCY(TASK_DEPENDENCY_ID),
    TASK_ID                    BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    PIPELINE_ID                BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    DEPENDS_ON_TASK_ID         BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    DEPENDS_ON_PIPELINE_ID     BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    LAST_CONSUMED_TASK_RUN_ID  BIGINT REFERENCES AUD_TASK_RUN_LOG(TASK_RUN_ID),
    LAST_CONSUMED_END_DATE     TIMESTAMPTZ,
    LAST_UPDATED_TIMESTAMP     TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE AUD_TASK_DEPENDENCY_TRACKER IS
    '[ADDITION] Same fix as AUD_PIPELINE_DEPENDENCY_TRACKER, at task grain. Only meaningful for genuinely cross-pipeline task edges (DEPENDS_ON_PIPELINE_ID <> PIPELINE_ID on the CFG row) — a same-pipeline task dependency is checked live against AUD_TASK_RUN_LOG scoped by the shared pipeline_run_id instead, and never touches this table.';

COMMIT;

-- ============================================================================
-- SUMMARY OF FLAGS — read this before signing off on the schema
-- ============================================================================
--
-- [DEVIATION] from the literally pasted schema text (later verbal decisions
-- that supersede it):
--   1. REFRESH_TYPE moved from CFG_TASKS to CFG_PIPELINES (pipeline-level mode).
--   2. HANDLER gains a 4th value, EMAIL_ALERT, alongside PYTHON/SQL/BUSINESS_RULES.
--   3. AUD_BUSINESS_RULES_RUN_LOG.START_DATE: NUMBER -> TIMESTAMPTZ.
--   4. AUD_BUSINESS_RULES_RUN_LOG.STATUS: TIMESTAMP -> VARCHAR.
--   5. AUD_BUSINESS_RULES_RESULTS.BUSINESS_RULE_KEY: numeric hash -> plain
--      VARCHAR text of the key value (collision risk was the reason the hash
--      was dropped between the two pasted drafts; kept as VARCHAR here).
--   6. ERROR_MESSAGE spelling fixed (was ERROR_MESSGAE in the earlier draft).
--
-- [ADDITION] — not present in either pasted draft at all:
--   1. CFG_PIPELINES.PIPELINE_CODE — needed for `--pipeline_code` to resolve
--      against something; confirm this is the right fix.
--   2. Two full tables: AUD_PIPELINE_DEPENDENCY_TRACKER and
--      AUD_TASK_DEPENDENCY_TRACKER — designed later in the conversation to
--      fix the mismatched-schedule staleness bug described on both tables'
--      COMMENT ON TABLE text above.
--   3. Every partial-unique / plain index in this file beyond the one the
--      pasted notes call out by name (the pipeline-run one). All either
--      protect a natural key (scoped to ACTIVE_FLAG = 'Y' so deactivated
--      rows never block re-registration) or support an "on retry, update
--      in place" invariant implied by the resume-not-restart design.
--   4. Several self-dependency CHECK constraints (a pipeline/task cannot
--      depend on itself) — cheap safety nets, not explicitly requested.
--   5. CFG_BUSINESS_RULES gets a (TASK_ID, BUSINESS_RULE_NAME) uniqueness
--      constraint, not stated explicitly.
--
-- [CHOICE] — pasted notes say "(WILL BE ENFORCED BY THE ENGINE SETUP AS
-- TRIGGER)"; implemented instead as a CHECK constraint or IDENTITY column:
--   Every enum-style value list (TASK_TYPE, HANDLER, DEPENDENCY_TYPE,
--   REFRESH_TYPE, all the STATUS columns, OFFSET_TYPE, RETURN_VALUES'
--   allowed-token list, the PYTHON-requires-SCRIPT_NAME rule) and every
--   surrogate primary key. Only two things in this file are actual
--   hand-written trigger functions, because only these two genuinely cannot
--   be expressed as a CHECK or DEFAULT: (a) CREATED_BY/UPDATED_BY capturing
--   current_user, since UPDATED_BY must change on every UPDATE and DEFAULT
--   only fires at INSERT; (b) CFG_TASK_DEPENDENCY.DEPENDS_ON_PIPELINE_ID
--   defaulting from a sibling column, since a plain DEFAULT cannot reference
--   another column of the same row.
--
-- NOT enforceable at this level, by construction — flagged as `validate`
-- CLI responsibilities instead of schema constraints:
--   - CFG_BUSINESS_RULES.TARGET_TABLE having a single-column primary key.
--   - Any check that a CFG_TASK_PARAMETERS row actually exists for required
--     conventions (SQL_ACTION, TARGET_OBJECT, etc.) — this
--     schema has no way to make a key-value table enforce "this key must be
--     present for this other row," short of a trigger that hardcodes every
--     convention by name. Left as an application-level validation instead.
-- ============================================================================

-- ============================================================================
-- POST-SIGNOFF CHANGES — additions made after the 2026-09-19 sign-off above,
-- flagged separately rather than silently folded into the original block.
-- ============================================================================
--
-- [ADDITION, 2026-09-19, explicitly requested] CFG_PIPELINES gains 7 nullable
-- columns — CATCHUP, TAGS, RETRIES, RETRY_DELAY_MINUTES, DEPENDS_ON_PAST,
-- EMAIL_ON_FAILURE, EMAIL_RECIPIENTS — per-pipeline overrides for
-- generate-yml's Airflow-facing fields. NULL means "not set here"; `generate-
-- yml` resolves each in three tiers: this row, then craft-connector.yml's new
-- [Orchestrator] section, then a final hardcoded default. All nullable by
-- design (a fresh pipeline needs to set none of these to get a working DAG).
-- EMAIL_RECIPIENTS exists specifically because Airflow's own email_on_failure
-- does nothing without a recipient list to send to. Two CHECK constraints
-- (RETRIES/RETRY_DELAY_MINUTES >= 0 when set) are the only new validation;
-- everything else is a plain nullable column, no enum-style CHECK needed.
--
-- [ADDITION, 2026-09-19, explicitly requested] CFG_TASKS gains
-- SCHEMA_EVOLUTION BOOLEAN NOT NULL DEFAULT FALSE — per-task opt-in to the
-- SQL execution engine's schema-evolution path (sql_actions.py). Defaults
-- false per explicit instruction: an unexpected new column in a task's
-- staged SELECT fails the task with a clear reason unless a team
-- deliberately opts a task into automatic evolution.
--
-- [ADDITION, 2026-09-19, explicitly requested] CFG_BUSINESS_RULES gains a
-- third BUSINESS_RULE_TYPE value, REPORT, alongside INCOMPLETE/REJECT — for
-- teams that want a rule that flags/reports without either of those two
-- connotations. AUD_BUSINESS_RULES_RESULTS.STATUS gets the same third value
-- (it mirrors the flagged row's rule type). No behavioral difference in how
-- the engine executes a REPORT-typed rule versus the other two — see
-- business_rules.py.
--
-- No schema registry / expected-shape tables were added for the SQL
-- execution engine's schema-check-before-write step: per explicit
-- direction, that comparison is done live via each dialect's own
-- information_schema.columns (both the target table and a materialized
-- temp-table staging of the task's own SELECT), not a separate persisted
-- "what shape should this table be" table.
-- ============================================================================
