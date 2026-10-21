SELECT r.RESOURCE_NAME AS resource_name, r.HOLDER_ID AS holder_id,
r.ACTOR AS actor, r.ACQUIRED_AT AS acquired_at, (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') AS db_now,
CASE WHEN r.LEASE_EXPIRES_AT <= (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')
  OR (r.ATTEMPT_ID IS NOT NULL AND NOT EXISTS (SELECT 1 FROM AUD_TASK_ATTEMPTS a
        WHERE a.ATTEMPT_ID = r.ATTEMPT_ID AND a.OWNER_ID = r.OWNER_ID
        AND a.STATUS IN ('CLAIMED','RUNNING') AND a.LEASE_EXPIRES_AT > (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')))
  THEN 1 ELSE 0 END AS stale
FROM AUD_RESOURCE_LOCKS r WHERE r.LOCK_KEY = :key
