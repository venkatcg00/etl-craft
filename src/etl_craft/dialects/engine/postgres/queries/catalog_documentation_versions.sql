-- The latest recorded documentation version of every task that has one.
SELECT t.TASK_ID AS task_id, latest.VERSION AS version
FROM CFG_TASKS t
CROSS JOIN LATERAL (
    SELECT VERSION FROM AUD_TASK_DOCUMENTATION d
    WHERE d.TASK_ID=t.TASK_ID ORDER BY VERSION DESC LIMIT 1
) latest
