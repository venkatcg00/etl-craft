-- PostgreSQL only: callers use this only for cluster admission diagnostics.
SELECT g.AGENT_ID AS agent_id, g.RESOURCE_REPORT AS resource_report
FROM AUD_AGENTS g WHERE g.POOL=:pool AND g.STATUS='ACTIVE'
AND g.HEARTBEAT_AT>clock_timestamp()-interval '30 seconds'
ORDER BY g.MEMORY_CAPACITY_MB DESC, g.CPU_CAPACITY DESC, g.AGENT_ID LIMIT 1
