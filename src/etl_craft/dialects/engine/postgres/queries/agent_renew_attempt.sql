UPDATE AUD_TASK_ATTEMPTS SET HEARTBEAT_AT=clock_timestamp(), LEASE_EXPIRES_AT=clock_timestamp() + interval '60 seconds'
WHERE ATTEMPT_ID=:attempt_id AND OWNER_ID=:owner AND STATUS IN ('CLAIMED','RUNNING')
AND LEASE_EXPIRES_AT>clock_timestamp() RETURNING LEASE_EXPIRES_AT AS lease_expires_at, clock_timestamp() AS db_now
