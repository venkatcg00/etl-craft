-- Each time a pipeline was paused: while a pause has no RESUMED_AT, `run` starts nothing of the
-- pipeline, and a run in progress starts no more tasks until the pipeline is resumed.
CREATE TABLE AUD_PIPELINE_PAUSES (
    PIPELINE_PAUSE_ID  INTEGER PRIMARY KEY AUTOINCREMENT,
    PIPELINE_ID        BIGINT NOT NULL REFERENCES CFG_PIPELINES(PIPELINE_ID),
    PAUSED_AT          TIMESTAMP NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f', 'now') || '000+00:00'),
    PAUSED_BY          VARCHAR NOT NULL,
    REASON             VARCHAR NOT NULL,
    RESUMED_AT         TIMESTAMP,
    RESUMED_BY         VARCHAR,
    RESUME_REASON      VARCHAR
);

-- At most one open pause per pipeline.
CREATE UNIQUE INDEX ux_pipeline_pauses_open
    ON AUD_PIPELINE_PAUSES (PIPELINE_ID) WHERE RESUMED_AT IS NULL;
