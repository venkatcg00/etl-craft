-- A session token, and every access token renewed from it when it is a refresh token.
UPDATE CFG_API_TOKENS SET REVOKED_AT=:now
WHERE REVOKED_AT IS NULL AND (TOKEN_SHA256=:digest OR REFRESH_TOKEN_ID=(
    SELECT TOKEN_ID FROM CFG_API_TOKENS WHERE TOKEN_SHA256=:digest))
