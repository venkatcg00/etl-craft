-- etl-craft: rebuild-metadata
-- Script offsets may hold a stream's next position per partition.
CREATE TABLE AUD_TASK_OFFSET_TRACKER_new (
    TASK_ID                 BIGINT PRIMARY KEY REFERENCES CFG_TASKS(TASK_ID),
    OFFSET_TYPE             VARCHAR NOT NULL,
    OFFSET_VALUE            VARCHAR,
    LAST_UPDATED_TIMESTAMP  TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    CONSTRAINT ck_offset_type CHECK (OFFSET_TYPE IN ('NUMBER','TEXT','TIMESTAMP','PARTITIONS'))
);
INSERT INTO AUD_TASK_OFFSET_TRACKER_new (TASK_ID, OFFSET_TYPE, OFFSET_VALUE, LAST_UPDATED_TIMESTAMP) SELECT TASK_ID, OFFSET_TYPE, OFFSET_VALUE, LAST_UPDATED_TIMESTAMP FROM AUD_TASK_OFFSET_TRACKER;
DROP TABLE AUD_TASK_OFFSET_TRACKER;
ALTER TABLE AUD_TASK_OFFSET_TRACKER_new RENAME TO AUD_TASK_OFFSET_TRACKER;
CREATE TRIGGER trg_actor_guard_aud_task_offset_tracker_insert BEFORE INSERT ON AUD_TASK_OFFSET_TRACKER
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_OFFSET_TRACKER is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_offset_tracker_update BEFORE UPDATE ON AUD_TASK_OFFSET_TRACKER
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_OFFSET_TRACKER is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
CREATE TRIGGER trg_actor_guard_aud_task_offset_tracker_delete BEFORE DELETE ON AUD_TASK_OFFSET_TRACKER
BEGIN
    SELECT CASE WHEN etl_craft_actor() IS NULL THEN RAISE(ABORT, 'AUD_TASK_OFFSET_TRACKER is written only by etl-craft; use etl-craft mark, cancel or run, or a project migration (etl-craft migrate)') END;
END;
