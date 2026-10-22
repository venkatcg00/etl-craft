-- PostgreSQL only: cluster execution refuses any other Engine DB before this runs.
INSERT INTO AUD_AGENTS
(AGENT_ID, POOL, HOST, VERSION, PYTHON_SLOTS, WAREHOUSE_SLOTS, STARTED_AT, HEARTBEAT_AT, STATUS, RESOURCE_REPORT)
VALUES (:agent_id,:pool,:host,:version,:python_slots,:warehouse_slots,clock_timestamp(),clock_timestamp(),'ACTIVE',:report)
ON CONFLICT (AGENT_ID) DO UPDATE SET POOL=excluded.POOL, HOST=excluded.HOST, VERSION=excluded.VERSION,
PYTHON_SLOTS=excluded.PYTHON_SLOTS, WAREHOUSE_SLOTS=excluded.WAREHOUSE_SLOTS,
STARTED_AT=excluded.STARTED_AT, HEARTBEAT_AT=excluded.HEARTBEAT_AT, STATUS='ACTIVE', RESOURCE_REPORT=excluded.RESOURCE_REPORT
WHERE AUD_AGENTS.STATUS='GONE' RETURNING AGENT_ID AS agent_id
