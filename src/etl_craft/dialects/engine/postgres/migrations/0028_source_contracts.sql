-- etl-craft: convert-source-contracts
-- Project-scoped object contracts; deployed warehouse structure remains authoritative.
CREATE TABLE CFG_TABLES (
    TABLE_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    CATALOG_NAME VARCHAR,
    SCHEMA_NAME VARCHAR NOT NULL,
    TABLE_NAME VARCHAR NOT NULL,
    OBJECT_KIND VARCHAR,
    ACTIVE_FLAG VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY VARCHAR NOT NULL,
    CREATE_DATE TIMESTAMPTZ NOT NULL,
    UPDATED_BY VARCHAR NOT NULL,
    UPDATED_DATE TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_tables_active CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_tables_catalog CHECK (CATALOG_NAME IS NULL OR length(CATALOG_NAME) > 0),
    CONSTRAINT ck_tables_schema CHECK (length(SCHEMA_NAME) > 0),
    CONSTRAINT ck_tables_name CHECK (length(TABLE_NAME) > 0),
    CONSTRAINT ck_tables_kind CHECK (OBJECT_KIND IS NULL OR OBJECT_KIND IN ('TABLE','VIEW'))
);
CREATE UNIQUE INDEX ux_tables_catalog_identity ON CFG_TABLES (CATALOG_NAME, SCHEMA_NAME, TABLE_NAME) WHERE CATALOG_NAME IS NOT NULL;
CREATE UNIQUE INDEX ux_tables_no_catalog_identity ON CFG_TABLES (SCHEMA_NAME, TABLE_NAME) WHERE CATALOG_NAME IS NULL;

-- Declared columns of project object contracts, with stable identities across retirement.
CREATE TABLE CFG_COLUMNS (
    COLUMN_ID BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    TABLE_ID BIGINT NOT NULL REFERENCES CFG_TABLES(TABLE_ID),
    COLUMN_NAME VARCHAR NOT NULL,
    DATA_TYPE VARCHAR NOT NULL,
    COLUMN_POSITION INTEGER NOT NULL,
    IS_NULLABLE VARCHAR,
    ACTIVE_FLAG VARCHAR NOT NULL DEFAULT 'Y',
    CREATED_BY VARCHAR NOT NULL,
    CREATE_DATE TIMESTAMPTZ NOT NULL,
    UPDATED_BY VARCHAR NOT NULL,
    UPDATED_DATE TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_columns_active CHECK (ACTIVE_FLAG IN ('Y','N')),
    CONSTRAINT ck_columns_name CHECK (length(COLUMN_NAME) > 0),
    CONSTRAINT ck_columns_type CHECK (length(DATA_TYPE) > 0),
    CONSTRAINT ck_columns_position CHECK (COLUMN_POSITION > 0),
    CONSTRAINT ck_columns_nullable CHECK (IS_NULLABLE IS NULL OR IS_NULLABLE IN ('Y','N'))
);
CREATE UNIQUE INDEX ux_columns_identity ON CFG_COLUMNS (TABLE_ID, lower(COLUMN_NAME));
CREATE UNIQUE INDEX ux_columns_position_active ON CFG_COLUMNS (TABLE_ID, COLUMN_POSITION) WHERE ACTIVE_FLAG = 'Y';

-- Source rows at conversion time, retained only for historical reconstruction.
CREATE TABLE AUD_SOURCE_COLUMNS_BASELINE (
    SOURCE_COLUMN_ID BIGINT PRIMARY KEY,
    TABLE_NAME VARCHAR NOT NULL,
    COLUMN_NAME VARCHAR NOT NULL,
    DATA_TYPE VARCHAR NOT NULL,
    COLUMN_POSITION INTEGER NOT NULL CONSTRAINT ck_source_baseline_position CHECK (COLUMN_POSITION > 0),
    ACTIVE_FLAG VARCHAR NOT NULL DEFAULT 'Y' CONSTRAINT ck_source_baseline_active CHECK (ACTIVE_FLAG IN ('Y','N')),
    CREATED_BY VARCHAR NOT NULL,
    CREATE_DATE TIMESTAMPTZ NOT NULL,
    UPDATED_BY VARCHAR NOT NULL,
    UPDATED_DATE TIMESTAMPTZ NOT NULL
);


-- Immutable versions of the CFG_TABLES contract.
CREATE TABLE AUD_CFG_TABLES_HISTORY (
    CHANGE_ID BIGINT PRIMARY KEY,
    VALID_FROM TIMESTAMPTZ NOT NULL,
    VALID_TO TIMESTAMPTZ NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION VARCHAR NOT NULL,
    ACTOR VARCHAR NOT NULL,
    ACTOR_KIND VARCHAR NOT NULL,
    MIGRATION VARCHAR,
    TABLE_ID bigint,
    CATALOG_NAME character varying,
    SCHEMA_NAME character varying,
    TABLE_NAME character varying,
    OBJECT_KIND character varying,
    ACTIVE_FLAG character varying,
    CREATED_BY character varying,
    CREATE_DATE timestamp with time zone,
    UPDATED_BY character varying,
    UPDATED_DATE timestamp with time zone
);

-- Immutable versions of the CFG_COLUMNS contract.
CREATE TABLE AUD_CFG_COLUMNS_HISTORY (
    CHANGE_ID BIGINT PRIMARY KEY,
    VALID_FROM TIMESTAMPTZ NOT NULL,
    VALID_TO TIMESTAMPTZ NOT NULL,
    CURRENT_FLAG VARCHAR(1) NOT NULL,
    OPERATION VARCHAR NOT NULL,
    ACTOR VARCHAR NOT NULL,
    ACTOR_KIND VARCHAR NOT NULL,
    MIGRATION VARCHAR,
    COLUMN_ID bigint,
    TABLE_ID bigint,
    COLUMN_NAME character varying,
    DATA_TYPE character varying,
    COLUMN_POSITION integer,
    IS_NULLABLE character varying,
    ACTIVE_FLAG character varying,
    CREATED_BY character varying,
    CREATE_DATE timestamp with time zone,
    UPDATED_BY character varying,
    UPDATED_DATE timestamp with time zone
);

-- Retained source conversion evidence cannot be rewritten by a later config change.
CREATE FUNCTION etl_craft_source_baseline_guard() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
    IF COALESCE(current_setting('etl_craft.actor', true), '') = '' THEN
        RAISE EXCEPTION 'audit rows are written only by etl-craft';
    END IF;
    IF COALESCE(current_setting('etl_craft.purpose', true), '') <> 'retention' THEN
        RAISE EXCEPTION 'source conversion baseline is immutable';
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$fn$;
CREATE TRIGGER trg_source_baseline_immutable BEFORE INSERT OR UPDATE OR DELETE
    ON AUD_SOURCE_COLUMNS_BASELINE FOR EACH ROW EXECUTE FUNCTION etl_craft_source_baseline_guard();
CREATE TRIGGER trg_source_baseline_truncate BEFORE TRUNCATE
    ON AUD_SOURCE_COLUMNS_BASELINE FOR EACH STATEMENT EXECUTE FUNCTION etl_craft_source_baseline_guard();
