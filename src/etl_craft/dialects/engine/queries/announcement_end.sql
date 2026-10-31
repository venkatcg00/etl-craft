-- End an announcement that is not already ended; returns nothing when it was.
UPDATE AUD_ANNOUNCEMENTS SET ENDED_AT = :now, ENDED_BY = :actor, ENDED_BY_KIND = :actor_kind
WHERE ANNOUNCEMENT_ID = :announcement_id AND ENDED_AT IS NULL
