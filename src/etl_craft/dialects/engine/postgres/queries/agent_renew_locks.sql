UPDATE AUD_RESOURCE_LOCKS SET LEASE_EXPIRES_AT=clock_timestamp() + interval '60 seconds'
WHERE ATTEMPT_ID=:attempt_id AND OWNER_ID=:owner AND LEASE_EXPIRES_AT>clock_timestamp()
