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
    -- [DEVIATION, post-signoff 2026-09-20] Was 7 separate nullable columns
    -- (CATCHUP, TAGS, RETRIES, RETRY_DELAY_MINUTES, DEPENDS_ON_PAST,
    -- EMAIL_ON_FAILURE, EMAIL_RECIPIENTS), added 2026-09-19 for
    -- generate-yml's Airflow-facing DAG fields. Collapsed into one JSONB
    -- column per explicit instruction ("these all as one pipeline_parameters
    -- column... think like an data modelling architect") — a cohesive bag
    -- of optional, generate-yml-specific settings doesn't need a dedicated
    -- typed column per key, and JSONB lets this set grow (a new
    -- Airflow-facing field) without another migration. Same three-tier
    -- resolution as before, unchanged in spirit: a key absent from this
    -- JSON (or the whole column NULL) means "not set at the pipeline
    -- level," and generate-yml falls back to craft-connector.yml's
    -- [Orchestrator] section, then a final hardcoded default. Keys are
    -- the same UPPERCASE names the removed columns used
    -- (CATCHUP/TAGS/RETRIES/RETRY_DELAY_MINUTES/DEPENDS_ON_PAST/
    -- EMAIL_ON_FAILURE/EMAIL_RECIPIENTS), matching CFG_TASK_PARAMETERS'
    -- own PARAMETER_NAME convention rather than inventing a second casing
    -- style. No CHECK constraint on shape/value ranges (RETRIES >= 0 etc.)
    -- — same "not enforceable at this level, enforce at the application
    -- layer" reasoning already used for CFG_TASK_PARAMETERS' own
    -- conventions; cfg.py/generate_yml.py validate what they read.
    PIPELINE_PARAMETERS  JSONB,
    CREATED_BY     VARCHAR,
    CREATE_DATE    TIMESTAMPTZ,
    UPDATED_BY     VARCHAR,
    UPDATED_DATE   TIMESTAMPTZ,
    CONSTRAINT ck_pipelines_refresh_type CHECK (REFRESH_TYPE IN ('FULL', 'INCREMENTAL')),  -- [CHOICE]
    CONSTRAINT ck_pipelines_active_flag  CHECK (ACTIVE_FLAG IN ('Y', 'N'))
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
    RUN_CONDITION        VARCHAR,   -- [ADDITION, post-signoff 2026-09-20] see COMMENT ON COLUMN below
    RUN_CONDITION_COUNT  INT,       -- [ADDITION, post-signoff 2026-09-20] only meaningful when RUN_CONDITION = 'N'
    ACTIVE_FLAG    VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY     VARCHAR,
    CREATE_DATE    TIMESTAMPTZ,
    UPDATED_BY     VARCHAR,
    UPDATED_DATE   TIMESTAMPTZ,
    CONSTRAINT ck_tasks_task_type       CHECK (TASK_TYPE IN ('INGESTION','ETL')),  -- [CHOICE]
    CONSTRAINT ck_tasks_handler         CHECK (HANDLER IN ('PYTHON','SQL','BUSINESS_RULES','EMAIL_ALERT')),  -- [DEVIATION] EMAIL_ALERT added per later decision; neither pasted draft has it
    CONSTRAINT ck_tasks_active_flag     CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_tasks_run_condition   CHECK (RUN_CONDITION IS NULL OR RUN_CONDITION IN ('ALL','ANY','N')),
    -- RUN_CONDITION_COUNT is required by, and only meaningful for, mode 'N'.
    -- Both halves are enforced: 'N' without a count is as broken as a count
    -- on a mode that ignores it.
    CONSTRAINT ck_tasks_run_condition_count CHECK (
        (RUN_CONDITION = 'N' AND RUN_CONDITION_COUNT IS NOT NULL AND RUN_CONDITION_COUNT >= 1)
        OR (RUN_CONDITION IS DISTINCT FROM 'N' AND RUN_CONDITION_COUNT IS NULL)
    )
);
-- [DEVIATION, post-signoff 2026-09-20] SCRIPT_NAME, RETURN_VALUES (added
-- with CFG_TASKS originally/post-signoff) and SCHEMA_EVOLUTION (added
-- post-signoff 2026-09-19) all removed from this table and moved into
-- CFG_TASK_PARAMETERS as ordinary PARAMETER_NAME/PARAMETER_VALUE rows
-- ('SCRIPT_NAME', 'RETURN_VALUES', 'SCHEMA_EVOLUTION') — per explicit
-- instruction ("why special treatment for ingestion task alone... make
-- these as something we give as parameter values"). All three are only
-- ever meaningful for a subset of rows (SCRIPT_NAME/RETURN_VALUES for
-- HANDLER='PYTHON' alone, SCHEMA_EVOLUTION for HANDLER='SQL' alone) —
-- exactly the class of column CFG_TASK_PARAMETERS' own flexible
-- key-value design already exists to hold, rather than every CFG_TASKS
-- row carrying columns most handlers never use. CFG_TASKS itself now
-- holds only what's true of *every* task regardless of HANDLER. Their
-- old CHECK constraints (ck_tasks_script_required, ck_tasks_return_values)
-- are gone with them — same "not enforceable as a schema constraint once
-- it's a CFG_TASK_PARAMETERS convention, check at the application layer"
-- reasoning already used for every other PARAMETER_NAME. See
-- sql_actions.py's/scripts.py's own module docstrings for the current,
-- authoritative parameter vocabulary.

CREATE UNIQUE INDEX ux_tasks_code_active
    ON CFG_TASKS (PIPELINE_ID, TASK_CODE) WHERE ACTIVE_FLAG = 'Y';    -- [ADDITION] scoped per-pipeline, not global

CREATE TRIGGER trg_audit_cfg_tasks
    BEFORE INSERT OR UPDATE ON CFG_TASKS
    FOR EACH ROW EXECUTE FUNCTION trg_set_audit_columns();

COMMENT ON TABLE CFG_TASKS IS 'REFRESH_TYPE intentionally absent here — moved to CFG_PIPELINES, see comment there. Do not re-add it here out of habit when comparing against the pasted schema notes.';

-- [ADDITION, post-signoff 2026-09-20] RUN_CONDITION / RUN_CONDITION_COUNT.
-- How many of a task's own CFG_TASK_DEPENDENCY edges must be satisfied for
-- it to become ready: 'ALL' (every edge — the historical behaviour, and what
-- NULL means), 'ANY' (at least one), or 'N' (at least RUN_CONDITION_COUNT).
-- Per explicit instruction: "a task is dependent on 10 tasks but it can run
-- at least one meets the condition, it should be possible".
--
-- [CHOICE] It lives on CFG_TASKS, per explicit instruction ("it should be in
-- cfg_tasks ... because these should be resolved while dag chain generation
-- itself"), and applies uniformly to all of that task's edges rather than
-- being an OR-group on CFG_TASK_DEPENDENCY. That placement is what lets
-- generate-yml map (RUN_CONDITION, DEPENDENCY_TYPE) onto a single Airflow
-- trigger_rule (all_success / one_success / all_failed / one_failed /
-- all_done / one_done) at DAG-generation time — OR-groups have no Airflow
-- equivalent and could only ever be gated engine-side. The cost is that
-- "all of A,B plus any of C,D" cannot be expressed; flag it if that shape
-- is ever needed rather than bolting a second mechanism on.
--
-- [CHOICE] Named RUN_CONDITION rather than RUN_TYPE (both were offered):
-- CFG_PIPELINES.REFRESH_TYPE already owns the *_TYPE shape in this schema,
-- and "run type" would read as a sibling of it.
--
-- 'N' and HAS_DATA have no Airflow trigger_rule equivalent — see
-- generate_yml.py's own docstring for what it emits for those and why.
COMMENT ON COLUMN CFG_TASKS.RUN_CONDITION IS 'ALL | ANY | N — how many of this task''s dependency edges must be satisfied. NULL means ALL.';
COMMENT ON COLUMN CFG_TASKS.RUN_CONDITION_COUNT IS 'How many edges must be satisfied when RUN_CONDITION = ''N''. Must be NULL for every other mode.';

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
    'HARD_DELETE (DELETE_ROWS only; "true" deletes for real, anything else soft-deletes via DELETE_FLAG), '
    'SCHEMA_EVOLUTION (SQL actions only; "true" opts a task into the schema-evolution rebuild path, default/absent is false), '
    'SCRIPT_NAME / RETURN_VALUES (HANDLER=PYTHON only; the script path, and its pipe-separated declared return-variable names — see scripts.py''s own module docstring), '
    'SOURCE_OBJECT / TARGET_OBJECT (every task, any HANDLER; pipe-separated "schema.table" lists for lineage — see cfg.py''s own LINEAGE_SOURCE_PARAM/LINEAGE_TARGET_PARAM comment).';

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

-- ----------------------------------------------------------------------------
-- SCHEMA_MIGRATIONS   [ADDITION, 2026-09-20] — closes CLAUDE.md open question
-- #7 ("No migration tooling... has been discussed"), per explicit permission
-- ("you may implement the migration mechanism as well").
-- ----------------------------------------------------------------------------
-- [CHOICE] This file (schema.sql) stays the single authoritative *full*
-- definition for a brand-new install — that role is unchanged. Migration
-- files under sql/migrations/ are for carrying an *already-deployed*
-- database forward incrementally from here on; nothing already baked into
-- schema.sql above gets a retroactive migration file (that would misstate
-- history — every one of those changes already happened as a direct edit
-- to this file, flagged in its own POST-SIGNOFF CHANGES block). Zero
-- migration files exist yet as of this table's own creation, so a fresh
-- install (via this file) and this table starting empty are consistent by
-- construction — nothing here is "pending" that a fresh database is
-- missing. See migrate.py for the runner this table backs.
CREATE TABLE SCHEMA_MIGRATIONS (
    VERSION      VARCHAR PRIMARY KEY,   -- migration filename, e.g. '0001_add_thing.sql'
    APPLIED_AT   TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE SCHEMA_MIGRATIONS IS 'Bookkeeping for migrate.py: one row per sql/migrations/*.sql file already applied to this database. schema.sql itself is never re-run against an existing database — this table is only ever consulted/written by `etl-craft migrate`.';

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
--
-- [DEVIATION, 2026-09-19, explicitly requested] CFG_TASKS.RETURN_VALUES'
-- CHECK constraint relaxed from a closed two-token allow-list
-- (INGESTION_COUNT/LATEST_OFFSET_UPDATE only) to any pipe-separated list of
-- uppercase identifiers — a HANDLER=PYTHON script can report an arbitrary,
-- per-task-declared set of variables, not just those two fixed ones. See the
-- ck_tasks_return_values constraint's own comment above and scripts.py for
-- the runtime rule this shifts onto the application layer: both mandatory
-- names must still be present in whatever a task actually declares, checked
-- there rather than by a CHECK constraint this table alone can't express.
--
-- [DEVIATION, 2026-09-20, explicitly requested] "any column that has a need
-- to store more than one value must use | as separator" — RETURN_VALUES was
-- comma-separated when first relaxed (above); switched to pipe-separated
-- for consistency with every other multi-value CFG_TASK_PARAMETERS
-- convention (MERGE_KEY, MERGE_COMPARE_COLUMNS, SOURCE_OBJECT,
-- TARGET_OBJECT), all pipe-separated from the start.
--
-- [DEVIATION, 2026-09-20, explicitly requested, supersedes three entries
-- above] The three CFG_TASKS.* additions above this point in the file
-- (SCRIPT_NAME/RETURN_VALUES, SCHEMA_EVOLUTION) are removed from that table
-- entirely and now live as CFG_TASK_PARAMETERS rows instead — see
-- CFG_TASKS' own comment, right where those columns used to be defined,
-- for the full reasoning ("why special treatment for ingestion task
-- alone"). Likewise, CFG_PIPELINES' 7-column addition above (CATCHUP
-- through EMAIL_RECIPIENTS) is collapsed into one PIPELINE_PARAMETERS
-- JSONB column — see CFG_PIPELINES' own comment. Kept as separate entries
-- here, not edited away, so this block still reads as an accurate history
-- of what actually happened and when, per this file's own established
-- practice for post-signoff changes.
-- ============================================================================
--
-- [ADDITION, 2026-09-20, explicitly requested — iteration 2, E2-41]
-- CFG_TASKS gains RUN_CONDITION (VARCHAR, NULL) and RUN_CONDITION_COUNT
-- (INT, NULL), with ck_tasks_run_condition and
-- ck_tasks_run_condition_count. They say how many of a task's own
-- CFG_TASK_DEPENDENCY edges must be satisfied before it becomes ready:
-- 'ALL' (every edge — the historical behaviour, and what NULL means),
-- 'ANY' (at least one), or 'N' (at least RUN_CONDITION_COUNT of them).
-- Per explicit instruction: "the dependency should have something like
-- all, one, some etc to have conditional dependency. lets say a task is
-- dependent on 10 tasks but it can run at least one meets the condition".
-- Fully backward compatible — every existing row has RUN_CONDITION NULL,
-- which resolver.py reads as 'ALL', exactly what it did before. See
-- CFG_TASKS' own COMMENT ON COLUMN block for why it sits on this table
-- rather than as an OR-group on CFG_TASK_DEPENDENCY, and why the column
-- is named RUN_CONDITION and not RUN_TYPE. Carried to an already-deployed
-- database by sql/migrations/0001_add_run_condition.sql — the first real
-- migration file this repo has ever had.
-- ============================================================================
