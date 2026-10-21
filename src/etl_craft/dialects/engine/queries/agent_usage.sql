-- PostgreSQL only: cluster execution refuses any other Engine DB before this runs.
SELECT g.AGENT_ID AS agent_id, g.HOST AS host, g.RESOURCE_REPORT AS resource_report,
a.SLOT_KIND AS slot_kind, COUNT(a.ATTEMPT_ID) AS used
FROM AUD_AGENTS g LEFT JOIN AUD_TASK_ATTEMPTS a
ON a.OWNER_ID=CAST(g.AGENT_ID AS varchar) AND a.STATUS IN ('CLAIMED','RUNNING')
AND a.LEASE_EXPIRES_AT>clock_timestamp()
WHERE g.RESOURCE_REPORT\:\:jsonb->>'node_id'=:node_id GROUP BY g.AGENT_ID, g.HOST, g.RESOURCE_REPORT, a.SLOT_KIND
