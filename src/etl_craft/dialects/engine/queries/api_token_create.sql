INSERT INTO CFG_API_TOKENS (NAME, TOKEN_SHA256, ACCOUNT_ID, CREATED_BY, CREATE_DATE, EXPIRES_AT,
    TOKEN_KIND, REFRESH_TOKEN_ID, SSO_GROUP_IDS, POOL, ATTEMPT_ID)
VALUES (:name, :digest, :account_id, :actor, :now, :expires_at, :kind, :refresh_token_id, :sso_group_ids, :pool, :attempt_id)
RETURNING TOKEN_ID AS token_id
