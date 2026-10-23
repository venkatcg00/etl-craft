UPDATE AUD_AGENTS SET HEARTBEAT_AT=(strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'), STATUS=:status,
CPU_CAPACITY=:cpu_capacity, MEMORY_CAPACITY_MB=:memory_capacity_mb, CAPACITY_SOURCE=:capacity_source,
RESOURCE_REPORT=:report, PYTHON_SLOTS=:python_slots, WAREHOUSE_SLOTS=:warehouse_slots
WHERE AGENT_ID=:agent_id AND STATUS <> 'GONE' RETURNING AGENT_ID AS agent_id
