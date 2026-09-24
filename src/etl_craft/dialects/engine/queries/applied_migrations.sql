-- Every migration file already applied, in the order each stream applies them.
SELECT SOURCE AS source, VERSION AS version, CHECKSUM AS checksum, APPLIED_AT AS applied_at
FROM SCHEMA_MIGRATIONS
ORDER BY SOURCE, VERSION
