-- Behavioral smoke test for the PostgreSQL Engine DB's schema.sql (beside
-- this file) — exercises the mechanisms CLAUDE.md describes as load-bearing,
-- not just DDL syntax. Run against a disposable database (never against a
-- real Engine DB), from this directory:
--
--   createdb etl_craft_test
--   psql -d etl_craft_test -f schema.sql
--   psql -d etl_craft_test -f schema_test.sql
--   dropdb etl_craft_test
--
-- [DEVIATION, 2026-09-20, E2-26] This file is now SELF-ASSERTING and must be
-- run under -v ON_ERROR_STOP=1 (the Makefile and CI both do). Every
-- "EXPECT FAIL" case is wrapped in a DO block that runs the statement, raises
-- if it *succeeded*, and swallows only the specific SQLSTATE class named in
-- its own \echo. So psql's exit code now means something, and there is no
-- "verify by eye" step.
--
-- That note used to read: "There is no pass/fail summary line; verify by eye
-- that failures land exactly on the statements marked EXPECT FAIL." Nobody
-- was looking — CI ran this without ON_ERROR_STOP, so the step always exited
-- 0 and a regression that made an EXPECT FAIL case start *succeeding* (the
-- exact class of bug this file exists to catch) passed silently.
--
-- EXPECT SUCCEED statements are left as plain statements: under
-- ON_ERROR_STOP=1, any of them failing aborts the run, which is the assertion.

\echo '=== SETUP: two pipelines ==='
INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) VALUES ('PL_A', 'Pipeline A', 'INCREMENTAL') RETURNING PIPELINE_ID, CREATED_BY, CREATE_DATE, UPDATED_BY, UPDATED_DATE;
INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) VALUES ('PL_B', 'Pipeline B', 'FULL') RETURNING PIPELINE_ID;

\echo '=== TEST: audit trigger preserves CREATE_DATE, bumps UPDATED_DATE on UPDATE (expect: create_date_present=t, updated_date_changed=t) ==='
SELECT pg_sleep(0.05);
UPDATE CFG_PIPELINES SET DESCRIPTION = 'updated once' WHERE PIPELINE_CODE = 'PL_A';
SELECT
    (SELECT CREATE_DATE FROM CFG_PIPELINES WHERE PIPELINE_CODE='PL_A') = (SELECT MIN(CREATE_DATE) FROM CFG_PIPELINES WHERE PIPELINE_CODE='PL_A') AS create_date_present,
    CREATED_BY, UPDATED_BY, (UPDATED_DATE > CREATE_DATE) AS updated_date_changed
FROM CFG_PIPELINES WHERE PIPELINE_CODE='PL_A';

\echo '=== TEST: CFG_PIPELINES.PIPELINE_PARAMETERS round-trips arbitrary JSONB (expect the object back verbatim) ==='
INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE, PIPELINE_PARAMETERS)
VALUES ('PL_C', 'Pipeline C', 'INCREMENTAL',
    '{"CATCHUP": true, "TAGS": ["incremental", "critical"], "RETRIES": 2, "RETRY_DELAY_MINUTES": 10, "DEPENDS_ON_PAST": false, "EMAIL_ON_FAILURE": true, "EMAIL_RECIPIENTS": ["team@example.com"]}'::jsonb)
RETURNING PIPELINE_ID, PIPELINE_PARAMETERS;

\echo '=== EXPECT SUCCEED: PIPELINE_PARAMETERS defaults to NULL when omitted ==='
INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) VALUES ('PL_D', 'Pipeline D', 'FULL')
RETURNING PIPELINE_ID, PIPELINE_PARAMETERS;

\echo '=== TEST: two CFG_TASKS under pipeline A (SCRIPT_NAME now lives in CFG_TASK_PARAMETERS, not a column) ==='
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('extract', 'INGESTION', 1, 'PYTHON') RETURNING TASK_ID;
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('load', 'ETL', 1, 'SQL') RETURNING TASK_ID;

\echo '=== TEST: CFG_TASK_PARAMETERS holds SCRIPT_NAME/RETURN_VALUES/SCHEMA_EVOLUTION (no DB-level shape or requiredness check any more — see schema.sql CFG_TASKS comment) ==='
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) VALUES (1, 'SCRIPT_NAME', 'extract.py') RETURNING TASK_PARAMETER_ID;
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) VALUES (1, 'RETURN_VALUES', 'INGESTION_COUNT|LATEST_OFFSET_UPDATE|SOME_CUSTOM_VAR') RETURNING TASK_PARAMETER_ID;
INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE) VALUES (2, 'SCHEMA_EVOLUTION', 'true') RETURNING TASK_PARAMETER_ID;

\echo '=== EXPECT FAIL (check_violation): HANDLER not in allowed list ==='
DO $do$ BEGIN
    INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('bad_handler', 'INGESTION', 1, 'BOGUS');
    RAISE EXCEPTION 'EXPECT FAIL did not fail: HANDLER not in allowed list';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok (expected check_violation)';
END $do$;

\echo '=== EXPECT SUCCEED: EMAIL_ALERT handler (the added 4th value) ==='
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('alert', 'ETL', 1, 'EMAIL_ALERT') RETURNING TASK_ID, HANDLER;

\echo '=== RUN_CONDITION (E2-41) ==='
\echo '--- EXPECT SUCCEED: RUN_CONDITION omitted entirely (NULL means ALL) ---'
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('rc_default', 'ETL', 1, 'SQL') RETURNING TASK_ID, RUN_CONDITION, RUN_CONDITION_COUNT;

\echo '--- EXPECT SUCCEED: RUN_CONDITION = ANY with no count ---'
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, RUN_CONDITION) VALUES ('rc_any', 'ETL', 1, 'SQL', 'ANY') RETURNING TASK_ID, RUN_CONDITION;

\echo '--- EXPECT SUCCEED: RUN_CONDITION = N with a count ---'
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, RUN_CONDITION, RUN_CONDITION_COUNT) VALUES ('rc_n', 'ETL', 1, 'SQL', 'N', 2) RETURNING TASK_ID, RUN_CONDITION, RUN_CONDITION_COUNT;

\echo '--- EXPECT FAIL (check_violation): RUN_CONDITION outside ALL/ANY/N ---'
DO $do$ BEGIN
    INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, RUN_CONDITION) VALUES ('rc_bogus', 'ETL', 1, 'SQL', 'MOST');
    RAISE EXCEPTION 'EXPECT FAIL did not fail: RUN_CONDITION outside ALL/ANY/N';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok (expected check_violation)';
END $do$;

\echo '--- EXPECT FAIL (check_violation): RUN_CONDITION = N without a count ---'
DO $do$ BEGIN
    INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, RUN_CONDITION) VALUES ('rc_n_nocount', 'ETL', 1, 'SQL', 'N');
    RAISE EXCEPTION 'EXPECT FAIL did not fail: RUN_CONDITION = N without a count';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok (expected check_violation)';
END $do$;

\echo '--- EXPECT FAIL (check_violation): a count on a mode that ignores it ---'
DO $do$ BEGIN
    INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, RUN_CONDITION, RUN_CONDITION_COUNT) VALUES ('rc_any_count', 'ETL', 1, 'SQL', 'ANY', 2);
    RAISE EXCEPTION 'EXPECT FAIL did not fail: a count on a mode that ignores it';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok (expected check_violation)';
END $do$;

\echo '--- EXPECT FAIL (check_violation): RUN_CONDITION = N with a zero count ---'
DO $do$ BEGIN
    INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, RUN_CONDITION, RUN_CONDITION_COUNT) VALUES ('rc_n_zero', 'ETL', 1, 'SQL', 'N', 0);
    RAISE EXCEPTION 'EXPECT FAIL did not fail: RUN_CONDITION = N with a zero count';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok (expected check_violation)';
END $do$;

\echo '=== TEST: partial unique index lets a deactivated TASK_CODE be reused, blocks a second active duplicate ==='
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('reuse_me', 'ETL', 1, 'SQL') RETURNING TASK_ID;
UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_CODE = 'reuse_me' AND PIPELINE_ID = 1;
\echo '--- EXPECT SUCCEED: re-registering the now-inactive code ---'
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('reuse_me', 'ETL', 1, 'SQL') RETURNING TASK_ID, ACTIVE_FLAG;
\echo '--- EXPECT FAIL (unique_violation): a second ACTIVE row with the same code ---'
DO $do$ BEGIN
    INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('reuse_me', 'ETL', 1, 'SQL');
    RAISE EXCEPTION 'EXPECT FAIL did not fail: a second ACTIVE row with the same code';
EXCEPTION WHEN unique_violation THEN RAISE NOTICE 'ok (expected unique_violation)';
END $do$;

\echo '=== TEST: DEPENDS_ON_PIPELINE_ID auto-fills from PIPELINE_ID when NULL (expect depends_on_pipeline_id=1, not null) ==='
INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE)
VALUES (1, 2, NULL, 1, 'SUCCESS') RETURNING TASK_DEPENDENCY_ID, PIPELINE_ID, DEPENDS_ON_PIPELINE_ID;

\echo '=== EXPECT FAIL (check_violation): task depending on itself ==='
DO $do$ BEGIN
    INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) VALUES (1, 1, 1, 'SUCCESS');
    RAISE EXCEPTION 'EXPECT FAIL did not fail: task depending on itself';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok (expected check_violation)';
END $do$;

\echo '=== EXPECT FAIL (check_violation): pipeline depending on itself ==='
DO $do$ BEGIN
    INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE) VALUES (1, 1, 'SUCCESS');
    RAISE EXCEPTION 'EXPECT FAIL did not fail: pipeline depending on itself';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok (expected check_violation)';
END $do$;

\echo '=== EXPECT SUCCEED: real cross-pipeline dependency, A depends on B ==='
INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE) VALUES (1, 2, 'HAS_DATA') RETURNING PIPELINE_DEPENDENCY_ID;

\echo '=== THE key mechanism: partial unique index on AUD_PIPELINES_RUN_LOG(PIPELINE_ID) WHERE STATUS=IN-PROGRESS ==='
\echo '--- EXPECT SUCCEED: first IN-PROGRESS run for pipeline 1 ---'
INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (1, 'IN-PROGRESS') RETURNING PIPELINE_RUN_ID;
\echo '--- EXPECT FAIL (unique_violation): a second concurrent IN-PROGRESS run for the SAME pipeline ---'
DO $do$ BEGIN
    INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (1, 'IN-PROGRESS');
    RAISE EXCEPTION 'EXPECT FAIL did not fail: a second concurrent IN-PROGRESS run for the SAME pipeline';
EXCEPTION WHEN unique_violation THEN RAISE NOTICE 'ok (expected unique_violation)';
END $do$;
\echo '--- EXPECT SUCCEED: a different pipeline can have its own independent IN-PROGRESS run at the same time ---'
INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (2, 'IN-PROGRESS') RETURNING PIPELINE_RUN_ID;
\echo '--- EXPECT SUCCEED: a SUCCESS-status row for pipeline 1 does not collide with its IN-PROGRESS row (partial index only covers IN-PROGRESS) ---'
INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (1, 'SUCCESS') RETURNING PIPELINE_RUN_ID;

\echo '=== TEST: one AUD_TASK_RUN_LOG row per (task, pipeline_run) — resume not restart ==='
\echo '--- EXPECT SUCCEED: first attempt at task 1 under run 1 ---'
INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS) VALUES (1, 1, 'FAILED') RETURNING TASK_RUN_ID;
\echo '--- EXPECT FAIL (unique_violation): a SECOND row for the same task+run instead of updating the existing one in place ---'
DO $do$ BEGIN
    INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS) VALUES (1, 1, 'IN-PROGRESS');
    RAISE EXCEPTION 'EXPECT FAIL did not fail: a SECOND row for the same task+run instead of updating the existing one in place';
EXCEPTION WHEN unique_violation THEN RAISE NOTICE 'ok (expected unique_violation)';
END $do$;
\echo '--- EXPECT SUCCEED: the correct retry pattern is UPDATE, not INSERT ---'
UPDATE AUD_TASK_RUN_LOG SET STATUS = 'SUCCESS', END_DATE = now() WHERE TASK_ID = 1 AND PIPELINE_RUN_ID = 1 RETURNING TASK_RUN_ID, STATUS;

\echo '=== TEST: tracker tables share a PK with their CFG_ dependency row (1:1) ==='
INSERT INTO AUD_TASK_DEPENDENCY_TRACKER (TASK_DEPENDENCY_ID, TASK_ID, PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDS_ON_PIPELINE_ID)
SELECT TASK_DEPENDENCY_ID, TASK_ID, PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDS_ON_PIPELINE_ID FROM CFG_TASK_DEPENDENCY
RETURNING TASK_DEPENDENCY_ID;

INSERT INTO AUD_PIPELINE_DEPENDENCY_TRACKER (PIPELINE_DEPENDENCY_ID, PIPELINE_ID, DEPENDS_ON_PIPELINE_ID)
SELECT PIPELINE_DEPENDENCY_ID, PIPELINE_ID, DEPENDS_ON_PIPELINE_ID FROM CFG_PIPELINE_DEPENDENCY
RETURNING PIPELINE_DEPENDENCY_ID;

\echo '=== TEST: plain FK actually blocks orphan references ==='
\echo '--- EXPECT FAIL (foreign_key_violation): CFG_TASKS referencing a nonexistent pipeline ---'
DO $do$ BEGIN
    INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('orphan', 'ETL', 9999, 'SQL');
    RAISE EXCEPTION 'EXPECT FAIL did not fail: CFG_TASKS referencing a nonexistent pipeline';
EXCEPTION WHEN foreign_key_violation THEN RAISE NOTICE 'ok (expected foreign_key_violation)';
END $do$;

\echo '=== TEST: CFG_BUSINESS_RULES/AUD_BUSINESS_RULES_RESULTS accept the new REPORT type (expect both rows back with REPORT) ==='
INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, SEQUENCE_NUMBER)
VALUES ('report_rule', 1, 2, 'SELECT 1', 'REPORT', 'id', 'public.some_target', 1) RETURNING BUSINESS_RULE_ID, BUSINESS_RULE_TYPE;
INSERT INTO AUD_BUSINESS_RULES_RUN_LOG (BUSINESS_RULE_ID, TASK_RUN_ID, STATUS) VALUES (1, 1, 'SUCCESS') RETURNING BUSINESS_RULE_RUN_ID;
INSERT INTO AUD_BUSINESS_RULES_RESULTS (BUSINESS_RULE_RUN_ID, BUSINESS_RULE_ID, BUSINESS_RULE_KEY, TARGET_TABLE, STATUS) VALUES (1, 1, 'K1', 'public.some_target', 'REPORT') RETURNING BUSINESS_RULE_RESULT_ID, STATUS;

\echo '=== EXPECT FAIL (check_violation): BUSINESS_RULE_TYPE outside INCOMPLETE/REJECT/REPORT ==='
DO $do$ BEGIN
    INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, BUSINESS_RULE_SQL, BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE, SEQUENCE_NUMBER)
    VALUES ('bad_type_rule', 1, 2, 'SELECT 1', 'BOGUS', 'id', 'public.some_target', 1);
    RAISE EXCEPTION 'EXPECT FAIL did not fail: BUSINESS_RULE_TYPE outside INCOMPLETE/REJECT/REPORT';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok (expected check_violation)';
END $do$;

\echo '=== TEST: AUD_PIPELINES_RUN_LOG.SLA_STATUS records MET/BREACHED (Enforce_sla) ==='
\echo '--- EXPECT SUCCEED: a finished run judged BREACHED ---'
UPDATE AUD_PIPELINES_RUN_LOG SET SLA_STATUS = 'BREACHED' WHERE STATUS = 'SUCCESS' RETURNING PIPELINE_RUN_ID, SLA_STATUS;

\echo '=== EXPECT FAIL (check_violation): SLA_STATUS outside MET/BREACHED ==='
DO $do$ BEGIN
    UPDATE AUD_PIPELINES_RUN_LOG SET SLA_STATUS = 'LATE' WHERE STATUS = 'SUCCESS';
    RAISE EXCEPTION 'EXPECT FAIL did not fail: SLA_STATUS outside MET/BREACHED';
EXCEPTION WHEN check_violation THEN RAISE NOTICE 'ok (expected check_violation)';
END $do$;

\echo '=== FINAL ROW COUNTS (sanity check, not a strict assertion) ==='
SELECT 'CFG_PIPELINES' t, count(*) FROM CFG_PIPELINES
UNION ALL SELECT 'CFG_TASKS', count(*) FROM CFG_TASKS
UNION ALL SELECT 'CFG_TASK_PARAMETERS', count(*) FROM CFG_TASK_PARAMETERS
UNION ALL SELECT 'CFG_TASK_DEPENDENCY', count(*) FROM CFG_TASK_DEPENDENCY
UNION ALL SELECT 'CFG_PIPELINE_DEPENDENCY', count(*) FROM CFG_PIPELINE_DEPENDENCY
UNION ALL SELECT 'AUD_PIPELINES_RUN_LOG', count(*) FROM AUD_PIPELINES_RUN_LOG
UNION ALL SELECT 'AUD_TASK_RUN_LOG', count(*) FROM AUD_TASK_RUN_LOG
UNION ALL SELECT 'AUD_TASK_DEPENDENCY_TRACKER', count(*) FROM AUD_TASK_DEPENDENCY_TRACKER
UNION ALL SELECT 'AUD_PIPELINE_DEPENDENCY_TRACKER', count(*) FROM AUD_PIPELINE_DEPENDENCY_TRACKER
UNION ALL SELECT 'CFG_BUSINESS_RULES', count(*) FROM CFG_BUSINESS_RULES
UNION ALL SELECT 'AUD_BUSINESS_RULES_RUN_LOG', count(*) FROM AUD_BUSINESS_RULES_RUN_LOG
UNION ALL SELECT 'AUD_BUSINESS_RULES_RESULTS', count(*) FROM AUD_BUSINESS_RULES_RESULTS;
