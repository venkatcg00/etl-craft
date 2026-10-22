INSERT INTO AUD_RESOURCE_LOCKS
(LOCK_KEY, RESOURCE_NAME, HOLDER_ID, ATTEMPT_ID, OWNER_ID, ACTOR, ACTOR_KIND, ACQUIRED_AT, LEASE_EXPIRES_AT)
SELECT :key, :name, :holder, :attempt_id, CAST(:owner AS varchar), :actor, :kind, clock_timestamp(), CASE WHEN CAST(:attempt_id AS bigint) IS NULL THEN clock_timestamp() + make_interval(secs => :seconds)
 ELSE (SELECT LEAST(a.LEASE_EXPIRES_AT, clock_timestamp() + make_interval(secs => :seconds)) FROM AUD_TASK_ATTEMPTS a
 WHERE a.ATTEMPT_ID=:attempt_id AND a.OWNER_ID=CAST(:owner AS varchar)) END
WHERE CAST(:attempt_id AS bigint) IS NULL OR EXISTS (SELECT 1 FROM AUD_TASK_ATTEMPTS a
        WHERE a.ATTEMPT_ID = :attempt_id AND a.OWNER_ID = CAST(:owner AS varchar)
        AND a.STATUS IN ('CLAIMED','RUNNING') AND a.LEASE_EXPIRES_AT > clock_timestamp())
ON CONFLICT (LOCK_KEY) DO UPDATE SET
    RESOURCE_NAME = excluded.RESOURCE_NAME, HOLDER_ID = excluded.HOLDER_ID,
    ATTEMPT_ID = excluded.ATTEMPT_ID, OWNER_ID = excluded.OWNER_ID,
    ACTOR = excluded.ACTOR, ACTOR_KIND = excluded.ACTOR_KIND,
    ACQUIRED_AT = excluded.ACQUIRED_AT, LEASE_EXPIRES_AT = excluded.LEASE_EXPIRES_AT
WHERE AUD_RESOURCE_LOCKS.HOLDER_ID = CAST(:confirmed AS varchar)
  AND (AUD_RESOURCE_LOCKS.LEASE_EXPIRES_AT <= clock_timestamp()
   OR (AUD_RESOURCE_LOCKS.ATTEMPT_ID IS NOT NULL AND NOT EXISTS (SELECT 1 FROM AUD_TASK_ATTEMPTS a
        WHERE a.ATTEMPT_ID = AUD_RESOURCE_LOCKS.ATTEMPT_ID
        AND a.OWNER_ID = AUD_RESOURCE_LOCKS.OWNER_ID
        AND a.STATUS IN ('CLAIMED','RUNNING') AND a.LEASE_EXPIRES_AT > clock_timestamp())))
RETURNING LOCK_KEY AS lock_key, LEASE_EXPIRES_AT AS lease_expires_at, clock_timestamp() AS db_now
