-- The triggers on table :name, in any case.
SELECT name AS name FROM sqlite_schema WHERE type='trigger' AND upper(tbl_name)=upper(:name)
