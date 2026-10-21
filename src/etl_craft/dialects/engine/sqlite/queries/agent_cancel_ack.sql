UPDATE AUD_TASK_ATTEMPTS SET CANCEL_ACKNOWLEDGED_AT=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')
WHERE ATTEMPT_ID=:attempt_id AND OWNER_ID=:owner AND STATUS IN ('CLAIMED','RUNNING') AND LEASE_EXPIRES_AT>(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')
