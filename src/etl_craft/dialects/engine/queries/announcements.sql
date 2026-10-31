-- Announcements newest first: only those showing at :now unless :all, at most :limit.
SELECT ANNOUNCEMENT_ID AS announcement_id, LEVEL AS level, MESSAGE AS message,
       STARTS_AT AS starts_at, ENDS_AT AS ends_at, REASON AS reason,
       CREATED_BY AS created_by, CREATED_AT AS created_at,
       ENDED_BY AS ended_by, ENDED_AT AS ended_at
FROM AUD_ANNOUNCEMENTS
WHERE :all = 1
   OR (ENDED_AT IS NULL AND STARTS_AT <= :now AND (ENDS_AT IS NULL OR ENDS_AT > :now))
ORDER BY ANNOUNCEMENT_ID DESC
LIMIT :limit
