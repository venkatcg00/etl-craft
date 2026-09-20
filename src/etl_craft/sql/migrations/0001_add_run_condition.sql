-- 0001_add_run_condition.sql
--
-- E2-41: conditional dependency cardinality. Adds CFG_TASKS.RUN_CONDITION and
-- CFG_TASKS.RUN_CONDITION_COUNT, mirroring the same change made directly in
-- sql/schema.sql (see its own POST-SIGNOFF CHANGES block). schema.sql stays
-- the authoritative full definition for a fresh install. This file is what
-- carries an already-deployed Engine DB forward to that same state.
--
-- Backward compatible by construction: every existing row gets
-- RUN_CONDITION = NULL, which resolver.py reads as 'ALL' — exactly the
-- behaviour those rows already had.
--
-- Deliberately written with no dollar-quoted body, and with no semicolon
-- anywhere except as a real statement terminator -- not even inside a comment
-- like this one. migrate.py splits a file on the semicolon character rather
-- than parsing it, a documented limitation of that runner (E2-05). An earlier
-- draft of this very file mentioned that character literally in this comment
-- and broke on itself, which is as good an argument as any for fixing the
-- splitter rather than relying on everyone remembering.

ALTER TABLE CFG_TASKS ADD COLUMN IF NOT EXISTS RUN_CONDITION VARCHAR;

ALTER TABLE CFG_TASKS ADD COLUMN IF NOT EXISTS RUN_CONDITION_COUNT INT;

-- Postgres has no ADD CONSTRAINT IF NOT EXISTS, and this runner has no
-- dollar-quoted DO block available to it, so each constraint is dropped
-- first. That is what makes the file re-runnable against a database already
-- carrying it, which sql/migrations/README.md asks of every migration.
ALTER TABLE CFG_TASKS DROP CONSTRAINT IF EXISTS ck_tasks_run_condition;

ALTER TABLE CFG_TASKS ADD CONSTRAINT ck_tasks_run_condition
    CHECK (RUN_CONDITION IS NULL OR RUN_CONDITION IN ('ALL','ANY','N'));

ALTER TABLE CFG_TASKS DROP CONSTRAINT IF EXISTS ck_tasks_run_condition_count;

ALTER TABLE CFG_TASKS ADD CONSTRAINT ck_tasks_run_condition_count
    CHECK (
        (RUN_CONDITION = 'N' AND RUN_CONDITION_COUNT IS NOT NULL AND RUN_CONDITION_COUNT >= 1)
        OR (RUN_CONDITION IS DISTINCT FROM 'N' AND RUN_CONDITION_COUNT IS NULL)
    );

COMMENT ON COLUMN CFG_TASKS.RUN_CONDITION IS 'ALL | ANY | N — how many of this task''s dependency edges must be satisfied. NULL means ALL.';

COMMENT ON COLUMN CFG_TASKS.RUN_CONDITION_COUNT IS 'How many edges must be satisfied when RUN_CONDITION = ''N''. Must be NULL for every other mode.';
