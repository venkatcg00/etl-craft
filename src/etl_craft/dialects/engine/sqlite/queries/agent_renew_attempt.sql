UPDATE AUD_TASK_ATTEMPTS SET HEARTBEAT_AT=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), LEASE_EXPIRES_AT=(strftime('%Y-%m-%d %H:%M:%f', 'now', '+60 seconds') || '000+00:00')
WHERE ATTEMPT_ID=:attempt_id AND OWNER_ID=:owner AND STATUS IN ('CLAIMED','RUNNING')
AND LEASE_EXPIRES_AT>(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') RETURNING LEASE_EXPIRES_AT AS lease_expires_at, (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00') AS db_now
