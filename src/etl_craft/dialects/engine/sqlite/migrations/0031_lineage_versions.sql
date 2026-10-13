-- Replace derived task cache; table lineage history begins with the next computation.
DROP TABLE AUD_COLUMN_LINEAGE;
CREATE TABLE AUD_COLUMN_LINEAGE (
    COLUMN_LINEAGE_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    TASK_ID BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    SOURCE_SQL_HASH VARCHAR NOT NULL,
    TARGET_CATALOG_NAME VARCHAR,
    TARGET_SCHEMA_NAME VARCHAR NOT NULL,
    TARGET_TABLE_NAME VARCHAR NOT NULL,
    TARGET_COLUMN VARCHAR NOT NULL,
    SOURCE_CATALOG_NAME VARCHAR,
    SOURCE_SCHEMA_NAME VARCHAR,
    SOURCE_TABLE_NAME VARCHAR,
    SOURCE_COLUMN VARCHAR,
    TRANSFORMATION VARCHAR,
    COMPUTED_AT TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00')
);
CREATE INDEX ix_column_lineage_task ON AUD_COLUMN_LINEAGE (TASK_ID, SOURCE_SQL_HASH);
CREATE INDEX ix_column_lineage_target ON AUD_COLUMN_LINEAGE (TARGET_CATALOG_NAME, TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, TARGET_COLUMN);
CREATE INDEX ix_column_lineage_source ON AUD_COLUMN_LINEAGE (SOURCE_CATALOG_NAME, SOURCE_SCHEMA_NAME, SOURCE_TABLE_NAME, SOURCE_COLUMN);
-- Table lineage fingerprints and validity intervals, retained when edges change.
CREATE TABLE AUD_LINEAGE_VERSIONS (
    LINEAGE_VERSION_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    TARGET_CATALOG_NAME VARCHAR,
    TARGET_SCHEMA_NAME VARCHAR NOT NULL,
    TARGET_TABLE_NAME VARCHAR NOT NULL,
    VERSION_NUMBER INTEGER NOT NULL CHECK (VERSION_NUMBER > 0),
    EDGES_HASH VARCHAR(64) NOT NULL,
    VALID_FROM TIMESTAMP NOT NULL,
    VALID_TO TIMESTAMP NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL CHECK (CURRENT_FLAG IN ('Y','N')),
    CHECK (VALID_TO >= VALID_FROM)
);
CREATE UNIQUE INDEX ux_lineage_version_number ON AUD_LINEAGE_VERSIONS (COALESCE(TARGET_CATALOG_NAME,''), TARGET_SCHEMA_NAME, TARGET_TABLE_NAME, VERSION_NUMBER);
CREATE UNIQUE INDEX ux_lineage_version_current ON AUD_LINEAGE_VERSIONS (COALESCE(TARGET_CATALOG_NAME,''), TARGET_SCHEMA_NAME, TARGET_TABLE_NAME) WHERE CURRENT_FLAG='Y';
-- Recorded column sources and writing tasks belonging to a table lineage version.
CREATE TABLE AUD_LINEAGE_VERSION_EDGES (
    LINEAGE_VERSION_EDGE_ID INTEGER PRIMARY KEY AUTOINCREMENT,
    LINEAGE_VERSION_ID BIGINT NOT NULL REFERENCES AUD_LINEAGE_VERSIONS(LINEAGE_VERSION_ID),
    TASK_ID BIGINT NOT NULL REFERENCES CFG_TASKS(TASK_ID),
    TARGET_COLUMN VARCHAR NOT NULL,
    SOURCE_CATALOG_NAME VARCHAR,
    SOURCE_SCHEMA_NAME VARCHAR,
    SOURCE_TABLE_NAME VARCHAR,
    SOURCE_COLUMN VARCHAR,
    TRANSFORMATION VARCHAR
);
CREATE INDEX ix_lineage_version_edges ON AUD_LINEAGE_VERSION_EDGES (LINEAGE_VERSION_ID);
CREATE TRIGGER trg_actor_guard_aud_column_lineage_insert BEFORE INSERT ON AUD_COLUMN_LINEAGE
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_COLUMN_LINEAGE is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_column_lineage_update BEFORE UPDATE ON AUD_COLUMN_LINEAGE
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_COLUMN_LINEAGE is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_column_lineage_delete BEFORE DELETE ON AUD_COLUMN_LINEAGE
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_COLUMN_LINEAGE is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_lineage_versions_insert BEFORE INSERT ON AUD_LINEAGE_VERSIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_LINEAGE_VERSIONS is written only by etl-craft') END;
END;
CREATE TRIGGER trg_actor_guard_aud_lineage_versions_update BEFORE UPDATE ON AUD_LINEAGE_VERSIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_LINEAGE_VERSIONS is written only by etl-craft') END;
END;
CREATE TRIGGER trg_actor_guard_aud_lineage_versions_delete BEFORE DELETE ON AUD_LINEAGE_VERSIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_LINEAGE_VERSIONS is written only by etl-craft') END;
END;
CREATE TRIGGER trg_actor_guard_aud_lineage_version_edges_insert BEFORE INSERT ON AUD_LINEAGE_VERSION_EDGES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_LINEAGE_VERSION_EDGES is written only by etl-craft') END;
END;
CREATE TRIGGER trg_actor_guard_aud_lineage_version_edges_update BEFORE UPDATE ON AUD_LINEAGE_VERSION_EDGES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_LINEAGE_VERSION_EDGES is written only by etl-craft') END;
END;
CREATE TRIGGER trg_actor_guard_aud_lineage_version_edges_delete BEFORE DELETE ON AUD_LINEAGE_VERSION_EDGES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_LINEAGE_VERSION_EDGES is written only by etl-craft') END;
END;
CREATE TRIGGER trg_lineage_versions_immutable BEFORE UPDATE ON AUD_LINEAGE_VERSIONS
WHEN NOT (OLD.CURRENT_FLAG='Y' AND NEW.CURRENT_FLAG='N' AND NEW.VALID_TO >= OLD.VALID_FROM AND NEW.LINEAGE_VERSION_ID IS OLD.LINEAGE_VERSION_ID AND NEW.TARGET_CATALOG_NAME IS OLD.TARGET_CATALOG_NAME AND NEW.TARGET_SCHEMA_NAME IS OLD.TARGET_SCHEMA_NAME AND NEW.TARGET_TABLE_NAME IS OLD.TARGET_TABLE_NAME AND NEW.VERSION_NUMBER IS OLD.VERSION_NUMBER AND NEW.EDGES_HASH IS OLD.EDGES_HASH AND NEW.VALID_FROM IS OLD.VALID_FROM)
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_LINEAGE_VERSIONS is written only by etl-craft; record lineage with etl-craft lineage') END;
    SELECT RAISE(ABORT, 'AUD_LINEAGE_VERSIONS rejects UPDATE: lineage history is append-only; only closing a current version is allowed; recompute with etl-craft lineage');
END;
CREATE TRIGGER trg_aud_lineage_versions_immutable_delete BEFORE DELETE ON AUD_LINEAGE_VERSIONS
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_LINEAGE_VERSIONS is written only by etl-craft; record lineage with etl-craft lineage') END;
    SELECT RAISE(ABORT, 'AUD_LINEAGE_VERSIONS rejects DELETE: lineage history is append-only; only closing a current version is allowed; recompute with etl-craft lineage');
END;
CREATE TRIGGER trg_aud_lineage_version_edges_immutable_update BEFORE UPDATE ON AUD_LINEAGE_VERSION_EDGES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_LINEAGE_VERSION_EDGES is written only by etl-craft; record lineage with etl-craft lineage') END;
    SELECT RAISE(ABORT, 'AUD_LINEAGE_VERSION_EDGES rejects UPDATE: lineage history is append-only; only closing a current version is allowed; recompute with etl-craft lineage');
END;
CREATE TRIGGER trg_aud_lineage_version_edges_immutable_delete BEFORE DELETE ON AUD_LINEAGE_VERSION_EDGES
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_LINEAGE_VERSION_EDGES is written only by etl-craft; record lineage with etl-craft lineage') END;
    SELECT RAISE(ABORT, 'AUD_LINEAGE_VERSION_EDGES rejects DELETE: lineage history is append-only; only closing a current version is allowed; recompute with etl-craft lineage');
END;
