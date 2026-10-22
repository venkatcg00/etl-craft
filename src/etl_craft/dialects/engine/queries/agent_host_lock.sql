-- PostgreSQL only: cluster execution refuses any other Engine DB before this runs.
SELECT AGENT_ID AS agent_id FROM AUD_AGENTS
WHERE RESOURCE_REPORT\:\:jsonb->>'node_id'=:node_id ORDER BY AGENT_ID FOR UPDATE
