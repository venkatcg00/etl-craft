UPDATE AUD_RESOURCE_LOCKS SET LEASE_EXPIRES_AT = CASE WHEN CAST(:attempt_id AS bigint) IS NULL THEN (strftime('%Y-%m-%d %H:%M:%f', 'now', '+' || :seconds || ' seconds') || '000+00:00')
 ELSE (SELECT MIN(a.LEASE_EXPIRES_AT, (strftime('%Y-%m-%d %H:%M:%f', 'now', '+' || :seconds || ' seconds') || '000+00:00')) FROM AUD_TASK_ATTEMPTS a
 WHERE a.ATTEMPT_ID=:attempt_id AND a.OWNER_ID=:owner) END
WHERE LOCK_KEY IN :keys AND HOLDER_ID = :holder AND OWNER_ID = :owner
AND LEASE_EXPIRES_AT > (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')
AND (CAST(:attempt_id AS bigint) IS NULL OR EXISTS (SELECT 1 FROM AUD_TASK_ATTEMPTS a
        WHERE a.ATTEMPT_ID = :attempt_id AND a.OWNER_ID = :owner
        AND a.STATUS IN ('CLAIMED','RUNNING') AND a.LEASE_EXPIRES_AT > (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')))
RETURNING LEASE_EXPIRES_AT AS lease_expires_at, (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') AS db_now
