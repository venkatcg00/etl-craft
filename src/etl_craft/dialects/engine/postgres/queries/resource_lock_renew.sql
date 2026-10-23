UPDATE AUD_RESOURCE_LOCKS SET LEASE_EXPIRES_AT = CASE WHEN CAST(:attempt_id AS bigint) IS NULL THEN clock_timestamp() + make_interval(secs => :seconds)
 ELSE (SELECT LEAST(a.LEASE_EXPIRES_AT, clock_timestamp() + make_interval(secs => :seconds)) FROM AUD_TASK_ATTEMPTS a
 WHERE a.ATTEMPT_ID=:attempt_id AND a.OWNER_ID=CAST(:owner AS varchar)) END
WHERE LOCK_KEY IN :keys AND HOLDER_ID = :holder AND OWNER_ID = :owner
AND LEASE_EXPIRES_AT > clock_timestamp()
AND (CAST(:attempt_id AS bigint) IS NULL OR EXISTS (SELECT 1 FROM AUD_TASK_ATTEMPTS a
        WHERE a.ATTEMPT_ID = :attempt_id AND a.OWNER_ID = :owner
        AND a.STATUS IN ('CLAIMED','RUNNING') AND a.LEASE_EXPIRES_AT > clock_timestamp()))
RETURNING LEASE_EXPIRES_AT AS lease_expires_at, clock_timestamp() AS db_now
