-- The latest finished row of :task_id, and whether it wrote rows.
SELECT TASK_RUN_ID AS run_id, STATUS AS status,
       CASE WHEN TARGET_COUNT > 0 THEN 1 ELSE 0 END AS has_data
FROM AUD_TASK_RUN_LOG
WHERE TASK_ID = :task_id AND STATUS IN ('SUCCESS', 'FAILED', 'SKIPPED', 'CANCELLED')
ORDER BY TASK_RUN_ID DESC
LIMIT 1
