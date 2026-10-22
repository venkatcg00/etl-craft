-- PostgreSQL only: agent operations require cluster execution.
SELECT AGENT_ID AS agent_id, POOL AS pool, HOST AS host, VERSION AS version,
PYTHON_SLOTS AS python_slots, WAREHOUSE_SLOTS AS warehouse_slots, STATUS AS status,
STARTED_AT AS started_at, HEARTBEAT_AT AS heartbeat_at, RESOURCE_REPORT AS resource_report,
EXISTS (SELECT 1 FROM CFG_API_TOKENS t JOIN CFG_ACCOUNTS a ON a.ACCOUNT_ID=t.ACCOUNT_ID
        WHERE t.TOKEN_ID=g.AGENT_ID AND t.TOKEN_KIND='AGENT' AND t.PROJECT_ID IS NULL
        AND t.ACTIVE_FLAG='Y' AND a.ACTIVE_FLAG='Y' AND t.REVOKED_AT IS NULL
        AND (t.EXPIRES_AT IS NULL OR t.EXPIRES_AT>clock_timestamp())) AS credential_live
FROM AUD_AGENTS g WHERE AGENT_ID=:agent_id
