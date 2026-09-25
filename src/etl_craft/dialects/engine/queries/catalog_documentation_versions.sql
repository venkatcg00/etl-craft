-- The latest recorded documentation version of every task that has one.
SELECT TASK_ID AS task_id, MAX(VERSION) AS version
FROM AUD_TASK_DOCUMENTATION
GROUP BY TASK_ID
