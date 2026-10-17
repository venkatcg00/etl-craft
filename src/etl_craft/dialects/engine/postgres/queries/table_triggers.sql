-- The user triggers on table :name.
SELECT tgname AS name FROM pg_trigger WHERE tgrelid=to_regclass(:name) AND NOT tgisinternal
