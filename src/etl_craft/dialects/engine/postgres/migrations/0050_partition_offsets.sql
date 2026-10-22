-- Script offsets may hold a stream's next position per partition.
ALTER TABLE AUD_TASK_OFFSET_TRACKER DROP CONSTRAINT ck_offset_type;
ALTER TABLE AUD_TASK_OFFSET_TRACKER ADD CONSTRAINT ck_offset_type CHECK (OFFSET_TYPE IN ('NUMBER','TEXT','TIMESTAMP','PARTITIONS'));
