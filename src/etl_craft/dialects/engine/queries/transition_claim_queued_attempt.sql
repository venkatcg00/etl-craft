-- PostgreSQL only: cluster execution refuses any other Engine DB before this runs.
UPDATE AUD_TASK_ATTEMPTS a SET STATUS='CLAIMED', OWNER_ID=:owner, ERROR_MESSAGE=NULL,
CLAIMED_AT=clock_timestamp(), HEARTBEAT_AT=clock_timestamp(), LEASE_EXPIRES_AT=clock_timestamp()+interval '60 seconds'
WHERE a.ATTEMPT_ID=(
 SELECT q.ATTEMPT_ID FROM AUD_TASK_ATTEMPTS q
 WHERE EXISTS (SELECT 1 FROM AUD_AGENTS g WHERE g.AGENT_ID=:agent_id
 AND g.STATUS='ACTIVE' AND g.HEARTBEAT_AT>clock_timestamp()-interval '30 seconds')
 AND q.STATUS='QUEUED' AND q.POOL=:pool
 AND ((q.SLOT_KIND='python' AND :python_free) OR (q.SLOT_KIND='warehouse' AND :warehouse_free))
 AND (q.NOT_BEFORE IS NULL OR q.NOT_BEFORE<=clock_timestamp())
 ORDER BY q.PRIORITY DESC, q.QUEUED_AT, q.ATTEMPT_ID FOR UPDATE SKIP LOCKED LIMIT 1)
RETURNING a.ATTEMPT_ID AS attempt_id, a.LEASE_EXPIRES_AT AS lease_expires_at, clock_timestamp() AS db_now
