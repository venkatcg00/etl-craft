-- Each account's failed sign-ins in a row, when it was locked, and its last successful sign-in.
SELECT a.ACCOUNT_ID AS account_id, a.FAILED_SIGNINS AS failed_signins, a.LOCKED_DATE AS locked_at,
(SELECT x.ENDED_AT FROM AUD_ACTIONS x WHERE x.ACTOR=a.ACCOUNT_NAME AND x.ACTOR_KIND='USER'
 AND x.COMMAND='login' AND x.OUTCOME='SUCCESS' ORDER BY x.ACTION_ID DESC LIMIT 1) AS last_signin_at
FROM CFG_ACCOUNTS a
