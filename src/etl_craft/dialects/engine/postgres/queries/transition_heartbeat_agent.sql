UPDATE AUD_AGENTS SET HEARTBEAT_AT=clock_timestamp(), STATUS=:status,
RESOURCE_REPORT=:report, PYTHON_SLOTS=:python_slots, WAREHOUSE_SLOTS=:warehouse_slots
WHERE AGENT_ID=:agent_id AND STATUS <> 'GONE'
AND HEARTBEAT_AT > clock_timestamp() - interval '30 seconds' RETURNING AGENT_ID AS agent_id
