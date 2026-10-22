WITH requested AS (SELECT CAST(json_extract(value, '$.lock_key') AS bigint) AS lock_key, json_extract(value, '$.resource_name') AS resource_name, json_extract(value, '$.confirmed') AS confirmed FROM json_each(:resources))
INSERT INTO AUD_RESOURCE_LOCKS
(LOCK_KEY, RESOURCE_NAME, HOLDER_ID, ATTEMPT_ID, OWNER_ID, ACTOR, ACTOR_KIND, ACQUIRED_AT, LEASE_EXPIRES_AT)
SELECT requested.lock_key, requested.resource_name, :holder, :attempt_id, :owner, :actor, :kind, (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), CASE WHEN CAST(:attempt_id AS bigint) IS NULL THEN (strftime('%Y-%m-%d %H:%M:%f', 'now', '+' || :seconds || ' seconds') || '000+00:00')
 ELSE (SELECT MIN(a.LEASE_EXPIRES_AT, (strftime('%Y-%m-%d %H:%M:%f', 'now', '+' || :seconds || ' seconds') || '000+00:00')) FROM AUD_TASK_ATTEMPTS a
 WHERE a.ATTEMPT_ID=:attempt_id AND a.OWNER_ID=:owner) END
FROM requested
WHERE CAST(:attempt_id AS bigint) IS NULL OR EXISTS (SELECT 1 FROM AUD_TASK_ATTEMPTS a
        WHERE a.ATTEMPT_ID = :attempt_id AND a.OWNER_ID = :owner
        AND a.STATUS IN ('CLAIMED','RUNNING') AND a.LEASE_EXPIRES_AT > (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'))
ORDER BY requested.lock_key
ON CONFLICT (LOCK_KEY) DO UPDATE SET
    RESOURCE_NAME = excluded.RESOURCE_NAME, HOLDER_ID = excluded.HOLDER_ID,
    ATTEMPT_ID = excluded.ATTEMPT_ID, OWNER_ID = excluded.OWNER_ID,
    ACTOR = excluded.ACTOR, ACTOR_KIND = excluded.ACTOR_KIND,
    ACQUIRED_AT = excluded.ACQUIRED_AT, LEASE_EXPIRES_AT = excluded.LEASE_EXPIRES_AT
WHERE AUD_RESOURCE_LOCKS.HOLDER_ID = (SELECT confirmed FROM requested WHERE requested.lock_key=AUD_RESOURCE_LOCKS.LOCK_KEY)
  AND (AUD_RESOURCE_LOCKS.LEASE_EXPIRES_AT <= (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')
   OR (AUD_RESOURCE_LOCKS.ATTEMPT_ID IS NOT NULL AND NOT EXISTS (SELECT 1 FROM AUD_TASK_ATTEMPTS a
        WHERE a.ATTEMPT_ID = AUD_RESOURCE_LOCKS.ATTEMPT_ID
        AND a.OWNER_ID = AUD_RESOURCE_LOCKS.OWNER_ID
        AND a.STATUS IN ('CLAIMED','RUNNING') AND a.LEASE_EXPIRES_AT > (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'))))
RETURNING LOCK_KEY AS lock_key, LEASE_EXPIRES_AT AS lease_expires_at, (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') AS db_now
