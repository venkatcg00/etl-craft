UPDATE AUD_AGENTS SET HEARTBEAT_AT=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), STATUS=:status,
RESOURCE_REPORT=:report, PYTHON_SLOTS=:python_slots, WAREHOUSE_SLOTS=:warehouse_slots
WHERE AGENT_ID=:agent_id AND STATUS <> 'GONE' RETURNING AGENT_ID AS agent_id
