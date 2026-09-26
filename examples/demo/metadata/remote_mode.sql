-- For remote mode (Orchestration.Mode: remote), load this after support_insights.sql.
--
-- In remote mode the orchestrator is the only source of truth for scheduling, and it has no
-- equivalent of two of the demo's rules, so `etl-craft validate`, `generate-yml` and
-- `run --init-only` refuse them, naming each one:
--   - a HAS_DATA dependency (CLIENT_ALPHA.parse on land, SUPPORT_DM.setup_fact on interactions):
--     an orchestrator sees whether a step succeeded, not whether it wrote rows;
--   - RUN_CONDITION N (SUPPORT_DM.source_counts, 2 of 3): a trigger rule waits for all or one
--     of the upstream steps.
-- Here they become SUCCESS dependencies and ALL, which the orchestrator applies itself.

UPDATE CFG_TASK_DEPENDENCY SET DEPENDENCY_TYPE = 'SUCCESS' WHERE DEPENDENCY_TYPE = 'HAS_DATA';

UPDATE CFG_TASKS SET RUN_CONDITION = 'ALL', RUN_CONDITION_COUNT = NULL WHERE RUN_CONDITION = 'N';
