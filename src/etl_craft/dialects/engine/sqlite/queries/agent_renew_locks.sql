UPDATE AUD_RESOURCE_LOCKS SET LEASE_EXPIRES_AT=(strftime('%Y-%m-%d %H:%M:%f', 'now', '+60 seconds') || '000+00:00')
WHERE ATTEMPT_ID=:attempt_id AND OWNER_ID=:owner AND LEASE_EXPIRES_AT>(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')
