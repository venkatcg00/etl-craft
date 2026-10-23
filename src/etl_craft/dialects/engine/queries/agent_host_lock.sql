-- PostgreSQL only: one deployment admission lock also serializes cross-pool warehouse limits.
SELECT g.AGENT_ID AS agent_id FROM CFG_POOLS p CROSS JOIN AUD_AGENTS g
WHERE p.POOL_NAME='default' AND g.RESOURCE_REPORT\:\:jsonb->>'node_id'=:node_id
ORDER BY g.AGENT_ID FOR UPDATE OF p,g
