-- The Support Insights demo's pipelines, tasks, dependencies and business rules, as CFG_ rows.
--
-- Load it into an Engine DB that `etl-craft setup` created. It runs as written on SQLite and on
-- PostgreSQL: every id is looked up by code, never written out.
--
--   CLIENT_ALPHA      land Client Alpha's interactions, parse them, purge its test calls
--   CLIENT_BETA       land Client Beta's events, parse them
--   SUPPORT_DM        agents and support areas, then the support fact, its checks and summary,
--                     once both clients' pipelines have succeeded
--   SUPPORT_EXPORT    a slow export: it overruns its time limit and its SLA
--   SUPPORT_BACKFILL  a long backfill, for stopping a run and resuming it

INSERT INTO CFG_PIPELINES (PIPELINE_CODE, PIPELINE_NAME, DESCRIPTION, REFRESH_TYPE, SLA_IN_HOURS,
                           PIPELINE_PARAMETERS)
VALUES
    ('CLIENT_ALPHA', 'Client Alpha interactions',
     'Lands Client Alpha''s support interactions, as its desk exports them, and parses them into typed rows.',
     'INCREMENTAL', NULL, '{"TAGS": ["support", "client-alpha"]}'),
    ('CLIENT_BETA', 'Client Beta events',
     'Lands Client Beta''s support events and parses them into the shared shape.',
     'INCREMENTAL', NULL, '{"TAGS": ["support", "client-beta"]}'),
    ('SUPPORT_DM', 'Support data mart',
     'Builds the support fact from every client''s interactions, with agents and support areas, checks it, and summarises it per area and team.',
     'INCREMENTAL', 2, '{"TAGS": ["support", "mart"]}'),
    ('SUPPORT_EXPORT', 'Support export',
     'Exports the mart for a partner; it is slow, and overruns its time limit and its SLA.',
     'FULL', 0.0002, '{"EMAIL_RECIPIENTS": ["support-leads@example.com"]}'),
    ('SUPPORT_BACKFILL', 'Support backfill',
     'A long backfill: stopped part way, it resumes where it stopped.',
     'FULL', NULL, NULL);

INSERT INTO CFG_TASKS (TASK_CODE, TASK_TYPE, PIPELINE_ID, HANDLER)
SELECT v.column2, v.column3, p.PIPELINE_ID, v.column4
FROM (VALUES
    ('CLIENT_ALPHA', 'land', 'INGESTION', 'PYTHON'),
    ('CLIENT_ALPHA', 'setup_prs', 'ETL', 'SQL'),
    ('CLIENT_ALPHA', 'parse', 'ETL', 'SQL'),
    ('CLIENT_ALPHA', 'purge_test_calls', 'ETL', 'SQL'),
    ('CLIENT_BETA', 'land', 'INGESTION', 'PYTHON'),
    ('CLIENT_BETA', 'setup_prs', 'ETL', 'SQL'),
    ('CLIENT_BETA', 'parse', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'land_agents', 'INGESTION', 'PYTHON'),
    ('SUPPORT_DM', 'support_areas', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'flaky_feed', 'INGESTION', 'PYTHON'),
    ('SUPPORT_DM', 'on_failure', 'ETL', 'EMAIL_ALERT'),
    ('SUPPORT_DM', 'setup_agents', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'agents', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'retire_agents', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'setup_agent_history', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'agent_history', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'interactions', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'setup_fact', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'fact', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'quality', 'ETL', 'BUSINESS_RULES'),
    ('SUPPORT_DM', 'area_summary', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'source_counts', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'drop_source_counts', 'ETL', 'SQL'),
    ('SUPPORT_DM', 'alert', 'ETL', 'EMAIL_ALERT'),
    ('SUPPORT_EXPORT', 'export', 'ETL', 'PYTHON'),
    ('SUPPORT_BACKFILL', 'prepare', 'ETL', 'PYTHON'),
    ('SUPPORT_BACKFILL', 'backfill', 'ETL', 'PYTHON')
) v
JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1;

-- area_summary runs once ANY of its dependencies has succeeded (either means the fact is there);
-- source_counts once 2 of its 3 have (any two mean the interactions are there).
UPDATE CFG_TASKS SET RUN_CONDITION = 'ANY'
WHERE TASK_CODE = 'area_summary'
  AND PIPELINE_ID = (SELECT PIPELINE_ID FROM CFG_PIPELINES WHERE PIPELINE_CODE = 'SUPPORT_DM');
UPDATE CFG_TASKS SET RUN_CONDITION = 'N', RUN_CONDITION_COUNT = 2
WHERE TASK_CODE = 'source_counts'
  AND PIPELINE_ID = (SELECT PIPELINE_ID FROM CFG_PIPELINES WHERE PIPELINE_CODE = 'SUPPORT_DM');

INSERT INTO CFG_TASK_PARAMETERS (TASK_ID, PARAMETER_NAME, PARAMETER_VALUE)
SELECT t.TASK_ID, v.column3, v.column4
FROM (VALUES
    ('CLIENT_ALPHA', 'land', 'SCRIPT_NAME', 'alpha.py'),
    ('CLIENT_ALPHA', 'land', 'INPUT_PARAMS', '{"rows": 12}'),
    ('CLIENT_ALPHA', 'land', 'TARGET_OBJECT', 'lnd.client_alpha'),
    ('CLIENT_ALPHA', 'land', 'SOURCE_OBJECT', 'Client Alpha support desk'),
    ('CLIENT_ALPHA', 'land', 'DOCUMENTATION', 'Lands the interactions Client Alpha''s desk exported since the last one loaded, every field as text.'),
    ('CLIENT_ALPHA', 'setup_prs', 'SQL_ACTION', 'SETUP_TABLE'),
    ('CLIENT_ALPHA', 'setup_prs', 'TARGET_OBJECT', 'prs.alpha_interactions'),
    ('CLIENT_ALPHA', 'setup_prs', 'SOURCE_SQL', 'SELECT CAST(a.interaction_id AS BIGINT) AS interaction_id, a.agent_code, a.support_area, CAST(a.contact_date AS DATE) AS contact_date, a.status, CAST(a.duration_seconds AS INTEGER) AS handle_seconds, CAST(a.rating AS INTEGER) AS rating, CAST(0 AS BIGINT) AS parsed_by FROM lnd.client_alpha a WHERE 1 = 0'),
    ('CLIENT_ALPHA', 'parse', 'SQL_ACTION', 'APPEND_TABLE'),
    ('CLIENT_ALPHA', 'parse', 'TARGET_OBJECT', 'prs.alpha_interactions'),
    ('CLIENT_ALPHA', 'parse', 'SOURCE_SQL_FILE', 'prs_alpha.sql'),
    ('CLIENT_ALPHA', 'parse', 'PIPELINE_ID_SUBSTITUTION', 'true'),
    ('CLIENT_ALPHA', 'parse', 'DOCUMENTATION', 'Types Client Alpha''s landed interactions and appends the new ones, stamped with the run that parsed them.'),
    ('CLIENT_ALPHA', 'purge_test_calls', 'SQL_ACTION', 'DELETE_ROWS'),
    ('CLIENT_ALPHA', 'purge_test_calls', 'TARGET_OBJECT', 'prs.alpha_interactions'),
    ('CLIENT_ALPHA', 'purge_test_calls', 'SOURCE_SQL', 'SELECT interaction_id FROM prs.alpha_interactions WHERE agent_code = ''TEST'''),
    ('CLIENT_ALPHA', 'purge_test_calls', 'MERGE_KEY', 'interaction_id'),
    ('CLIENT_ALPHA', 'purge_test_calls', 'HARD_DELETE', 'true'),
    ('CLIENT_BETA', 'land', 'SCRIPT_NAME', 'beta.py'),
    ('CLIENT_BETA', 'land', 'INPUT_PARAMS', '{"rows": 8}'),
    ('CLIENT_BETA', 'land', 'TARGET_OBJECT', 'lnd.client_beta'),
    ('CLIENT_BETA', 'land', 'SOURCE_OBJECT', 'Client Beta event stream'),
    ('CLIENT_BETA', 'setup_prs', 'SQL_ACTION', 'SETUP_TABLE'),
    ('CLIENT_BETA', 'setup_prs', 'TARGET_OBJECT', 'prs.beta_interactions'),
    ('CLIENT_BETA', 'setup_prs', 'SOURCE_SQL', 'SELECT b.support_identifier AS interaction_id, b.agent AS agent_code, b.regarding AS support_area, CAST(b.event_time AS DATE) AS contact_date, b.status, CAST(b.handle_time AS INTEGER) AS handle_seconds, CAST(b.score AS INTEGER) AS rating, CAST(0 AS BIGINT) AS parsed_by FROM lnd.client_beta b WHERE 1 = 0'),
    ('CLIENT_BETA', 'parse', 'SQL_ACTION', 'APPEND_TABLE'),
    ('CLIENT_BETA', 'parse', 'TARGET_OBJECT', 'prs.beta_interactions'),
    ('CLIENT_BETA', 'parse', 'SOURCE_SQL_FILE', 'prs_beta.sql'),
    ('CLIENT_BETA', 'parse', 'PIPELINE_ID_SUBSTITUTION', 'true'),
    ('SUPPORT_DM', 'land_agents', 'SCRIPT_NAME', 'agents.py'),
    ('SUPPORT_DM', 'land_agents', 'TARGET_OBJECT', 'lnd.agents'),
    ('SUPPORT_DM', 'land_agents', 'SOURCE_OBJECT', 'HR agent directory'),
    ('SUPPORT_DM', 'support_areas', 'SQL_ACTION', 'CREATE_TABLE'),
    ('SUPPORT_DM', 'support_areas', 'TARGET_OBJECT', 'ds.support_areas'),
    ('SUPPORT_DM', 'support_areas', 'SOURCE_SQL_FILE', 'support_areas.sql'),
    ('SUPPORT_DM', 'flaky_feed', 'SCRIPT_NAME', 'flaky.py'),
    ('SUPPORT_DM', 'flaky_feed', 'DOCUMENTATION', 'A feed that is not ready the first time it is read.'),
    ('SUPPORT_DM', 'on_failure', 'EMAIL_TO', 'support-oncall@example.com'),
    ('SUPPORT_DM', 'on_failure', 'EMAIL_ON_STATUS', 'FAILED'),
    ('SUPPORT_DM', 'on_failure', 'EMAIL_SUBJECT', '$$pipeline_code: a feed failed'),
    ('SUPPORT_DM', 'on_failure', 'EMAIL_BODY', 'Run $$pipeline_id of $$pipeline_code failed: $$error_message'),
    ('SUPPORT_DM', 'setup_agents', 'SQL_ACTION', 'SETUP_TABLE'),
    ('SUPPORT_DM', 'setup_agents', 'TARGET_OBJECT', 'ds.agents'),
    ('SUPPORT_DM', 'setup_agents', 'SOURCE_SQL', 'SELECT agent_code, agent_name, team, email FROM lnd.agents'),
    ('SUPPORT_DM', 'agents', 'SQL_ACTION', 'SCD1_MERGE'),
    ('SUPPORT_DM', 'agents', 'TARGET_OBJECT', 'ds.agents'),
    ('SUPPORT_DM', 'agents', 'SOURCE_SQL', 'SELECT agent_code, agent_name, team, email FROM lnd.agents WHERE left_company = ''N'''),
    ('SUPPORT_DM', 'agents', 'MERGE_KEY', 'agent_code'),
    ('SUPPORT_DM', 'agents', 'MERGE_COMPARE_COLUMNS', 'agent_name|team|email'),
    ('SUPPORT_DM', 'agents', 'PRESERVE_TARGET', 'true'),
    ('SUPPORT_DM', 'agents', 'DOCUMENTATION', 'Keeps one row per agent, up to date; an email the feed leaves out keeps the one on file.'),
    ('SUPPORT_DM', 'retire_agents', 'SQL_ACTION', 'DELETE_ROWS'),
    ('SUPPORT_DM', 'retire_agents', 'TARGET_OBJECT', 'ds.agents'),
    ('SUPPORT_DM', 'retire_agents', 'SOURCE_SQL', 'SELECT agent_code FROM lnd.agents WHERE left_company = ''Y'''),
    ('SUPPORT_DM', 'retire_agents', 'MERGE_KEY', 'agent_code'),
    ('SUPPORT_DM', 'setup_agent_history', 'SQL_ACTION', 'SETUP_TABLE'),
    ('SUPPORT_DM', 'setup_agent_history', 'TARGET_OBJECT', 'cdc.agent_history'),
    ('SUPPORT_DM', 'setup_agent_history', 'SOURCE_SQL', 'SELECT agent_code, team FROM lnd.agents'),
    ('SUPPORT_DM', 'agent_history', 'SQL_ACTION', 'SCD2_MERGE'),
    ('SUPPORT_DM', 'agent_history', 'TARGET_OBJECT', 'cdc.agent_history'),
    ('SUPPORT_DM', 'agent_history', 'SOURCE_SQL', 'SELECT agent_code, team FROM lnd.agents'),
    ('SUPPORT_DM', 'agent_history', 'MERGE_KEY', 'agent_code'),
    ('SUPPORT_DM', 'agent_history', 'MERGE_COMPARE_COLUMNS', 'team'),
    ('SUPPORT_DM', 'agent_history', 'DOCUMENTATION', 'Keeps every team an agent has been in, and when.'),
    ('SUPPORT_DM', 'interactions', 'SQL_ACTION', 'CREATE_TABLE'),
    ('SUPPORT_DM', 'interactions', 'TARGET_OBJECT', 'pre_dm.interactions'),
    ('SUPPORT_DM', 'interactions', 'SOURCE_SQL_FILE', 'pre_dm_interactions.sql'),
    ('SUPPORT_DM', 'setup_fact', 'SQL_ACTION', 'SETUP_TABLE'),
    ('SUPPORT_DM', 'setup_fact', 'TARGET_OBJECT', 'dm.support_fact'),
    ('SUPPORT_DM', 'setup_fact', 'SOURCE_SQL_FILE', 'support_fact.sql'),
    ('SUPPORT_DM', 'fact', 'SQL_ACTION', 'OVERWRITE_TABLE'),
    ('SUPPORT_DM', 'fact', 'TARGET_OBJECT', 'dm.support_fact'),
    ('SUPPORT_DM', 'fact', 'SOURCE_SQL_FILE', 'support_fact.sql'),
    ('SUPPORT_DM', 'fact', 'DOCUMENTATION', 'One row per interaction, from every client, with the agent''s team and the support area''s name.'),
    ('SUPPORT_DM', 'area_summary', 'SQL_ACTION', 'CREATE_TABLE'),
    ('SUPPORT_DM', 'area_summary', 'TARGET_OBJECT', 'dm.area_summary'),
    ('SUPPORT_DM', 'area_summary', 'SOURCE_SQL_FILE', 'area_summary.sql'),
    ('SUPPORT_DM', 'source_counts', 'SQL_ACTION', 'CREATE_TABLE'),
    ('SUPPORT_DM', 'source_counts', 'TARGET_OBJECT', 'pre_dm.source_counts'),
    ('SUPPORT_DM', 'source_counts', 'SOURCE_SQL', 'SELECT source_system, COUNT(*) AS interactions FROM pre_dm.interactions GROUP BY source_system'),
    ('SUPPORT_DM', 'drop_source_counts', 'SQL_ACTION', 'DROP_TABLE'),
    ('SUPPORT_DM', 'drop_source_counts', 'TARGET_OBJECT', 'pre_dm.source_counts'),
    ('SUPPORT_DM', 'alert', 'EMAIL_TO', 'support-leads@example.com'),
    ('SUPPORT_DM', 'alert', 'EMAIL_SUBJECT', '$$pipeline_code: $$status'),
    ('SUPPORT_DM', 'alert', 'EMAIL_BODY', 'Run $$pipeline_id of $$pipeline_code ended $$status.'),
    ('SUPPORT_DM', 'alert', 'EMAIL_PIPELINES', 'CLIENT_ALPHA|CLIENT_BETA|SUPPORT_DM'),
    ('SUPPORT_EXPORT', 'export', 'SCRIPT_NAME', 'wait.py'),
    ('SUPPORT_EXPORT', 'export', 'INPUT_PARAMS', '{"seconds": 8}'),
    ('SUPPORT_EXPORT', 'export', 'TASK_TIMEOUT_SECONDS', '2'),
    ('SUPPORT_BACKFILL', 'prepare', 'SCRIPT_NAME', 'wait.py'),
    ('SUPPORT_BACKFILL', 'prepare', 'INPUT_PARAMS', '{"seconds": 0}'),
    ('SUPPORT_BACKFILL', 'backfill', 'SCRIPT_NAME', 'wait.py'),
    ('SUPPORT_BACKFILL', 'backfill', 'INPUT_PARAMS', '{"seconds": 30}')
) v
JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1
JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = v.column2;

INSERT INTO CFG_TASK_DEPENDENCY (PIPELINE_ID, TASK_ID, DEPENDS_ON_PIPELINE_ID, DEPENDS_ON_TASK_ID,
                                 DEPENDENCY_TYPE)
SELECT t.PIPELINE_ID, t.TASK_ID, u.PIPELINE_ID, u.TASK_ID, v.column5
FROM (VALUES
    ('CLIENT_ALPHA', 'setup_prs', 'CLIENT_ALPHA', 'land', 'SUCCESS'),
    ('CLIENT_ALPHA', 'parse', 'CLIENT_ALPHA', 'setup_prs', 'SUCCESS'),
    ('CLIENT_ALPHA', 'parse', 'CLIENT_ALPHA', 'land', 'HAS_DATA'),
    ('CLIENT_ALPHA', 'purge_test_calls', 'CLIENT_ALPHA', 'parse', 'SUCCESS'),
    ('CLIENT_BETA', 'setup_prs', 'CLIENT_BETA', 'land', 'SUCCESS'),
    ('CLIENT_BETA', 'parse', 'CLIENT_BETA', 'setup_prs', 'SUCCESS'),
    ('SUPPORT_DM', 'setup_agents', 'SUPPORT_DM', 'land_agents', 'SUCCESS'),
    ('SUPPORT_DM', 'agents', 'SUPPORT_DM', 'setup_agents', 'SUCCESS'),
    ('SUPPORT_DM', 'retire_agents', 'SUPPORT_DM', 'agents', 'SUCCESS'),
    ('SUPPORT_DM', 'setup_agent_history', 'SUPPORT_DM', 'land_agents', 'SUCCESS'),
    ('SUPPORT_DM', 'agent_history', 'SUPPORT_DM', 'setup_agent_history', 'SUCCESS'),
    ('SUPPORT_DM', 'on_failure', 'SUPPORT_DM', 'flaky_feed', 'FAILURE'),
    ('SUPPORT_DM', 'interactions', 'SUPPORT_DM', 'flaky_feed', 'SUCCESS'),
    ('SUPPORT_DM', 'interactions', 'CLIENT_ALPHA', 'purge_test_calls', 'SUCCESS'),
    ('SUPPORT_DM', 'interactions', 'CLIENT_BETA', 'parse', 'SUCCESS'),
    ('SUPPORT_DM', 'setup_fact', 'SUPPORT_DM', 'interactions', 'HAS_DATA'),
    ('SUPPORT_DM', 'setup_fact', 'SUPPORT_DM', 'retire_agents', 'SUCCESS'),
    ('SUPPORT_DM', 'setup_fact', 'SUPPORT_DM', 'support_areas', 'SUCCESS'),
    ('SUPPORT_DM', 'fact', 'SUPPORT_DM', 'setup_fact', 'SUCCESS'),
    ('SUPPORT_DM', 'quality', 'SUPPORT_DM', 'fact', 'SUCCESS'),
    ('SUPPORT_DM', 'area_summary', 'SUPPORT_DM', 'fact', 'SUCCESS'),
    ('SUPPORT_DM', 'area_summary', 'SUPPORT_DM', 'quality', 'SUCCESS'),
    ('SUPPORT_DM', 'source_counts', 'SUPPORT_DM', 'interactions', 'SUCCESS'),
    ('SUPPORT_DM', 'source_counts', 'SUPPORT_DM', 'setup_fact', 'SUCCESS'),
    ('SUPPORT_DM', 'source_counts', 'SUPPORT_DM', 'fact', 'SUCCESS'),
    ('SUPPORT_DM', 'drop_source_counts', 'SUPPORT_DM', 'source_counts', 'SUCCESS'),
    ('SUPPORT_DM', 'alert', 'SUPPORT_DM', 'drop_source_counts', 'ALWAYS'),
    ('SUPPORT_DM', 'alert', 'SUPPORT_DM', 'area_summary', 'ALWAYS'),
    ('SUPPORT_DM', 'alert', 'SUPPORT_DM', 'agent_history', 'ALWAYS'),
    ('SUPPORT_BACKFILL', 'backfill', 'SUPPORT_BACKFILL', 'prepare', 'SUCCESS')
) v
JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = v.column1
JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = v.column2
JOIN CFG_PIPELINES up ON up.PIPELINE_CODE = v.column3
JOIN CFG_TASKS u ON u.PIPELINE_ID = up.PIPELINE_ID AND u.TASK_CODE = v.column4;

-- SUPPORT_DM builds on both clients' pipelines: a run starts only after each has succeeded again.
INSERT INTO CFG_PIPELINE_DEPENDENCY (PIPELINE_ID, DEPENDS_ON_PIPELINE_ID, DEPENDENCY_TYPE)
SELECT p.PIPELINE_ID, u.PIPELINE_ID, 'SUCCESS'
FROM CFG_PIPELINES p
JOIN CFG_PIPELINES u ON u.PIPELINE_CODE IN ('CLIENT_ALPHA', 'CLIENT_BETA')
WHERE p.PIPELINE_CODE = 'SUPPORT_DM';

-- The support fact's checks: two in the first wave, one in the second.
INSERT INTO CFG_BUSINESS_RULES (BUSINESS_RULE_NAME, PIPELINE_ID, TASK_ID, BUSINESS_RULE_SQL,
                                BUSINESS_RULE_TYPE, BUSINESS_RULE_KEY_COLUMN, TARGET_TABLE,
                                SEQUENCE_NUMBER)
SELECT v.column1, t.PIPELINE_ID, t.TASK_ID, v.column2, v.column3, 'fact_key', 'dm.support_fact',
       CAST(v.column4 AS INTEGER)
FROM (VALUES
    ('unknown agent',
     'SELECT 1 FROM ds.support_areas s WHERE s.area_name = t.area_name AND t.team IS NULL',
     'INCOMPLETE', '1'),
    ('rating out of range',
     'SELECT 1 FROM ds.support_areas s WHERE s.area_name = t.area_name AND (t.rating < 1 OR t.rating > 5)',
     'REJECT', '1'),
    ('long call',
     'SELECT 1 FROM ds.support_areas s WHERE s.area_name = t.area_name AND t.handle_seconds > 600',
     'REPORT', '2')
) v
JOIN CFG_PIPELINES p ON p.PIPELINE_CODE = 'SUPPORT_DM'
JOIN CFG_TASKS t ON t.PIPELINE_ID = p.PIPELINE_ID AND t.TASK_CODE = 'quality';
