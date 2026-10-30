-- Active projects with a schema check schedule, and the timezone the schedule reads in.
SELECT PROJECT_ID AS project_id, PROJECT_CODE AS project_code, TIMEZONE AS timezone,
       SCHEMA_CHECK_SCHEDULE AS schema_check_schedule
FROM CFG_PROJECTS
WHERE ACTIVE_FLAG = 'Y' AND SCHEMA_CHECK_SCHEDULE IS NOT NULL
ORDER BY PROJECT_ID
