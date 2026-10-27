-- Active columns cannot outlive their own project table.
SELECT c.COLUMN_ID AS column_id, t.TABLE_ID AS table_id
FROM CFG_COLUMNS c JOIN CFG_TABLES t ON t.TABLE_ID=c.TABLE_ID
WHERE c.ACTIVE_FLAG='Y' AND t.ACTIVE_FLAG='N' AND t.PROJECT_ID=:project_id
