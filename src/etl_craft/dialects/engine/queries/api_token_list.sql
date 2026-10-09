SELECT TOKEN_ID AS token_id, NAME AS name, ROLE AS role, PROJECT_ID AS project_id,
CREATED_BY AS created_by, CREATED_AT AS created_at, EXPIRES_AT AS expires_at, REVOKED_AT AS revoked_at
FROM CFG_API_TOKENS ORDER BY TOKEN_ID
