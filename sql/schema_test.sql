-- Behavioral smoke test for sql/schema.sql — exercises the mechanisms
-- CLAUDE.md describes as load-bearing, not just DDL syntax. Run against a
-- disposable database (never against a real Engine DB):
--
--   createdb etl_craft_test
--   psql -d etl_craft_test -f sql/schema.sql
--   psql -d etl_craft_test -f sql/schema_test.sql
--   dropdb etl_craft_test
--
-- Each statement below is its own autocommit transaction (psql's default),
-- so a statement marked "EXPECT FAIL" rolling back does not abort the ones
-- after it. The ERROR lines you'll see in the output for those statements
-- are the test passing, not a problem — read the \echo above each block to
-- know which outcome is correct. There is no pass/fail summary line; verify
-- by eye that failures land exactly on the statements marked EXPECT FAIL.

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

\echo '=== TEST: CFG_PIPELINES Airflow-facing override columns round-trip, including the array types (expect tags/recipients back as arrays) ==='
INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE, CATCHUP, TAGS, RETRIES, RETRY_DELAY_MINUTES, DEPENDS_ON_PAST, EMAIL_ON_FAILURE, EMAIL_RECIPIENTS)
VALUES ('PL_C', 'Pipeline C', 'INCREMENTAL', TRUE, ARRAY['incremental','critical'], 2, 10, FALSE, TRUE, ARRAY['team@example.com'])
RETURNING PIPELINE_ID, CATCHUP, TAGS, RETRIES, RETRY_DELAY_MINUTES, DEPENDS_ON_PAST, EMAIL_ON_FAILURE, EMAIL_RECIPIENTS;

\echo '=== EXPECT SUCCEED: all seven override columns default to NULL when omitted (expect all null) ==='
INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE) VALUES ('PL_D', 'Pipeline D', 'FULL')
RETURNING PIPELINE_ID, CATCHUP, TAGS, RETRIES, RETRY_DELAY_MINUTES, DEPENDS_ON_PAST, EMAIL_ON_FAILURE, EMAIL_RECIPIENTS;

\echo '=== EXPECT FAIL (check_violation): negative RETRIES ==='
INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE, RETRIES) VALUES ('PL_BAD_RETRIES', 'Bad', 'FULL', -1);

\echo '=== EXPECT FAIL (check_violation): negative RETRY_DELAY_MINUTES ==='
INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, REFRESH_TYPE, RETRY_DELAY_MINUTES) VALUES ('PL_BAD_DELAY', 'Bad', 'FULL', -5);

\echo '=== TEST: two CFG_TASKS under pipeline A ==='
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, SCRIPT_NAME) VALUES ('extract', 'INGESTION', 1, 'PYTHON', 'extract.py') RETURNING TASK_ID;
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('load', 'ETL', 1, 'SQL') RETURNING TASK_ID;

\echo '=== EXPECT FAIL (check_violation): HANDLER not in allowed list ==='
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('bad_handler', 'INGESTION', 1, 'BOGUS');

\echo '=== EXPECT FAIL (check_violation): PYTHON handler without SCRIPT_NAME ==='
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('bad_script', 'INGESTION', 1, 'PYTHON');

\echo '=== EXPECT SUCCEED: EMAIL_ALERT handler (the added 4th value) ==='
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('alert', 'ETL', 1, 'EMAIL_ALERT') RETURNING TASK_ID, HANDLER;

\echo '=== EXPECT SUCCEED: RETURN_VALUES with both allowed tokens ==='
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, SCRIPT_NAME, RETURN_VALUES) VALUES ('good_rv', 'INGESTION', 1, 'PYTHON', 'x.py', 'INGESTION_COUNT,LATEST_OFFSET_UPDATE') RETURNING TASK_ID, RETURN_VALUES;

\echo '=== EXPECT FAIL (check_violation): RETURN_VALUES with a disallowed token ==='
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER, SCRIPT_NAME, RETURN_VALUES) VALUES ('bad_rv', 'INGESTION', 1, 'PYTHON', 'x.py', 'ROW_COUNT');

\echo '=== TEST: partial unique index lets a deactivated TASK_CODE be reused, blocks a second active duplicate ==='
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('reuse_me', 'ETL', 1, 'SQL') RETURNING TASK_ID;
UPDATE CFG_TASKS SET ACTIVE_FLAG = 'N' WHERE TASK_CODE = 'reuse_me' AND PIPELINE_ID = 1;
\echo '--- EXPECT SUCCEED: re-registering the now-inactive code ---'
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('reuse_me', 'ETL', 1, 'SQL') RETURNING TASK_ID, ACTIVE_FLAG;
\echo '--- EXPECT FAIL (unique_violation): a second ACTIVE row with the same code ---'
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('reuse_me', 'ETL', 1, 'SQL');

\echo '=== TEST: DEPENDS_ON_PIPELINE_ID auto-fills from PIPELINE_ID when NULL (expect depends_on_pipeline_id=1, not null) ==='
INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE)
VALUES (1, 2, NULL, 1, 'SUCCESS') RETURNING TASK_DEPENDENCY_ID, PIPELINE_ID, DEPENDS_ON_PIPELINE_ID;

\echo '=== EXPECT FAIL (check_violation): task depending on itself ==='
INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_TASK_ID, DEPENDENCY_TYPE) VALUES (1, 1, 1, 'SUCCESS');

\echo '=== EXPECT FAIL (check_violation): pipeline depending on itself ==='
INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE) VALUES (1, 1, 'SUCCESS');

\echo '=== EXPECT SUCCEED: real cross-pipeline dependency, A depends on B ==='
INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE) VALUES (1, 2, 'HAS_DATA') RETURNING PIPELINE_DEPENDENCY_ID;

\echo '=== THE key mechanism: partial unique index on AUD_PIPELINES_RUN_LOG(PIPELINE_ID) WHERE STATUS=IN-PROGRESS ==='
\echo '--- EXPECT SUCCEED: first IN-PROGRESS run for pipeline 1 ---'
INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (1, 'IN-PROGRESS') RETURNING PIPELINE_RUN_ID;
\echo '--- EXPECT FAIL (unique_violation): a second concurrent IN-PROGRESS run for the SAME pipeline ---'
INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (1, 'IN-PROGRESS');
\echo '--- EXPECT SUCCEED: a different pipeline can have its own independent IN-PROGRESS run at the same time ---'
INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (2, 'IN-PROGRESS') RETURNING PIPELINE_RUN_ID;
\echo '--- EXPECT SUCCEED: a SUCCESS-status row for pipeline 1 does not collide with its IN-PROGRESS row (partial index only covers IN-PROGRESS) ---'
INSERT INTO AUD_PIPELINES_RUN_LOG (PIPELINE_ID, STATUS) VALUES (1, 'SUCCESS') RETURNING PIPELINE_RUN_ID;

\echo '=== TEST: one AUD_TASK_RUN_LOG row per (task, pipeline_run) — resume not restart ==='
\echo '--- EXPECT SUCCEED: first attempt at task 1 under run 1 ---'
INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS) VALUES (1, 1, 'FAILED') RETURNING TASK_RUN_ID;
\echo '--- EXPECT FAIL (unique_violation): a SECOND row for the same task+run instead of updating the existing one in place ---'
INSERT INTO AUD_TASK_RUN_LOG (TASK_ID, PIPELINE_RUN_ID, STATUS) VALUES (1, 1, 'IN-PROGRESS');
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
INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER) VALUES ('orphan', 'ETL', 9999, 'SQL');

\echo '=== FINAL ROW COUNTS (sanity check, not a strict assertion) ==='
SELECT 'CFG_PIPELINES' t, count(*) FROM CFG_PIPELINES
UNION ALL SELECT 'CFG_TASKS', count(*) FROM CFG_TASKS
UNION ALL SELECT 'CFG_TASK_DEPENDENCY', count(*) FROM CFG_TASK_DEPENDENCY
UNION ALL SELECT 'CFG_PIPELINE_DEPENDENCY', count(*) FROM CFG_PIPELINE_DEPENDENCY
UNION ALL SELECT 'AUD_PIPELINES_RUN_LOG', count(*) FROM AUD_PIPELINES_RUN_LOG
UNION ALL SELECT 'AUD_TASK_RUN_LOG', count(*) FROM AUD_TASK_RUN_LOG
UNION ALL SELECT 'AUD_TASK_DEPENDENCY_TRACKER', count(*) FROM AUD_TASK_DEPENDENCY_TRACKER
UNION ALL SELECT 'AUD_PIPELINE_DEPENDENCY_TRACKER', count(*) FROM AUD_PIPELINE_DEPENDENCY_TRACKER;
