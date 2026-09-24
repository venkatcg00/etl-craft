-- Which of :names (lower case) exist as tables.
SELECT name AS table_name
FROM sqlite_master
WHERE type = 'table'
  AND LOWER(name) IN :names
