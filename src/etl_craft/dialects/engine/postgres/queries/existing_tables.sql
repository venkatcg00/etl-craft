-- Which of :names (lower case) exist as tables in the current schema.
SELECT table_name AS table_name
FROM information_schema.tables
WHERE table_schema = current_schema()
  AND LOWER(table_name) IN :names
