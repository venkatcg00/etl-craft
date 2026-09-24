-- Record one applied migration file with the SHA-256 of its content.
INSERT INTO SCHEMA_MIGRATIONS (SOURCE, VERSION, CHECKSUM)
VALUES (:source, :version, :checksum)
