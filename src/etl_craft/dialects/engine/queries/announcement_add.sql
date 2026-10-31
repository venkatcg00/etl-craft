-- Record an administrator's announcement; it shows from STARTS_AT until ENDS_AT or until ended.
INSERT INTO AUD_ANNOUNCEMENTS
(LEVEL, MESSAGE, STARTS_AT, ENDS_AT, REASON, CREATED_BY, CREATED_BY_KIND, CREATED_AT)
VALUES (:level, :message, :starts_at, :ends_at, :reason, :actor, :actor_kind, :now)
RETURNING ANNOUNCEMENT_ID AS announcement_id
