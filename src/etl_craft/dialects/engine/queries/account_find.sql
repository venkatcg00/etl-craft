SELECT ACCOUNT_ID AS account_id, ACCOUNT_NAME AS name, ACCOUNT_KIND AS kind,
ROLE AS role, EMAIL AS email, PASSWORD_HASH AS password_hash, OIDC_SUBJECT AS oidc_subject,
FAILED_SIGNINS AS failed_signins, LOCKED_DATE AS locked_date, MUST_CHANGE_PASSWORD AS must_change_password,
CAST(UI_SETTINGS AS text) AS ui_settings, ACTIVE_FLAG AS active_flag FROM CFG_ACCOUNTS WHERE ACCOUNT_NAME=:name
