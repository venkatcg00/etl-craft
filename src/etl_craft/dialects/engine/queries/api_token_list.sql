SELECT t.TOKEN_ID AS token_id, a.ACCOUNT_NAME AS name, a.ACCOUNT_KIND AS account_kind, t.TOKEN_KIND AS token_kind, a.ROLE AS role, t.PROJECT_ID AS project_id,
t.CREATED_BY AS created_by, t.CREATE_DATE AS created_at, t.EXPIRES_AT AS expires_at, t.REVOKED_AT AS revoked_at
FROM CFG_API_TOKENS t JOIN CFG_ACCOUNTS a ON a.ACCOUNT_ID=t.ACCOUNT_ID
ORDER BY t.TOKEN_ID
