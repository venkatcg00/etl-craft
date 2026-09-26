-- The date each run ran as of (RUN_DATE, which SQL tasks read as $$run_date), and whether it was
-- part of a backfill. A run made before this had its start date as its run date.
ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN RUN_DATE DATE;
ALTER TABLE AUD_PIPELINES_RUN_LOG ADD COLUMN BACKFILL VARCHAR(1) NOT NULL DEFAULT 'N' CHECK (BACKFILL IN ('Y','N'));
UPDATE AUD_PIPELINES_RUN_LOG SET RUN_DATE = (START_DATE AT TIME ZONE 'UTC')::date WHERE RUN_DATE IS NULL;
