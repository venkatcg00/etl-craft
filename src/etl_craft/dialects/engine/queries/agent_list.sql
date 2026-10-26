SELECT AGENT_ID AS agent_id, POOL AS pool, HOST AS host, VERSION AS version,
PYTHON_SLOTS AS python_slots, WAREHOUSE_SLOTS AS warehouse_slots, STATUS AS status,
STARTED_AT AS started_at, HEARTBEAT_AT AS heartbeat_at, RESOURCE_REPORT AS resource_report
FROM AUD_AGENTS WHERE STATUS IN :statuses ORDER BY AGENT_ID
