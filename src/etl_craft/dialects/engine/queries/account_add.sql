INSERT INTO CFG_ACCOUNTS
(ACCOUNT_NAME, ACCOUNT_KIND, ROLE, EMAIL, PASSWORD_HASH, OIDC_SUBJECT, CREATED_BY, UPDATED_BY)
VALUES (:name, :kind, :role, :email, :password_hash, :oidc_subject, :actor, :actor)
RETURNING ACCOUNT_ID AS account_id
