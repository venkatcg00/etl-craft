-- The named indexes on table :name and their definitions.
SELECT name AS name, sql AS definition FROM sqlite_schema
WHERE type='index' AND upper(tbl_name)=upper(:name) AND sql IS NOT NULL
