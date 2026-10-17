-- The indexes on table :name in the current schema and their definitions.
SELECT indexname AS name,indexdef AS definition FROM pg_indexes
WHERE schemaname=current_schema() AND tablename=:name
