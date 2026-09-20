-- 0003_task_attempt_count.sql
--
-- E2-21: AUD_TASK_RUN_LOG.ATTEMPT_COUNT. Mirrors the same change made directly
-- in schema.sql -- see its own POST-SIGNOFF CHANGES block.
--
-- The one-row-per-task-per-run rule is load-bearing and unchanged. This counts
-- attempts within that row rather than adding rows, so AUD_TASK_RUN_LOG can
-- answer "how many times did this fail before it worked?".
--
-- Existing rows default to 1, which is accurate: every row that exists was
-- dispatched at least once, and nothing recorded a second attempt before now.

ALTER TABLE AUD_TASK_RUN_LOG ADD COLUMN IF NOT EXISTS ATTEMPT_COUNT INT NOT NULL DEFAULT 1;
