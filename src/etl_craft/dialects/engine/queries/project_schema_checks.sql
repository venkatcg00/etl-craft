-- The selected project's schema check settings.
SELECT SCHEMA_CHECK_SCHEDULE AS schema_check_schedule,
       SCHEMA_CHECK_ON_DEMAND AS schema_check_on_demand
FROM CFG_PROJECTS WHERE PROJECT_ID = :project_id
