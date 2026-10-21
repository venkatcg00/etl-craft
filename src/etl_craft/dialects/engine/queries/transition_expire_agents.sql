-- PostgreSQL only: cluster execution refuses any other Engine DB before this runs.
UPDATE AUD_AGENTS SET STATUS='GONE'
WHERE STATUS<>'GONE' AND HEARTBEAT_AT<=clock_timestamp()-interval '30 seconds'
RETURNING AGENT_ID AS agent_id
