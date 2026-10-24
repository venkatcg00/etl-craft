SELECT r.RESERVATION_ID AS reservation_id,r.ROLE AS role,r.HOST AS host,r.PID AS pid,
r.POOL_CONNECTIONS AS pool_connections,r.SESSION_CONNECTIONS AS session_connections,
r.LOCAL_TASK_CONNECTIONS AS local_task_connections,r.HEARTBEAT_AT AS heartbeat_at,r.EXPIRES_AT AS expires_at
FROM AUD_CONNECTION_RESERVATIONS r
WHERE r.RESERVATION_ID IN (
 SELECT future.RESERVATION_ID AS reservation_id FROM AUD_CONNECTION_RESERVATIONS future
 WHERE future.STOPPED_AT IS NULL AND future.EXPIRES_AT>:now
 UNION
 SELECT substring(a.APPLICATION_NAME,19) AS reservation_id FROM pg_stat_activity a
 WHERE a.DATNAME=current_database() AND a.APPLICATION_NAME LIKE 'etl-craft-process-%'
)
ORDER BY r.RESERVATION_ID
