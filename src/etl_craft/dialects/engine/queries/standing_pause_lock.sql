-- SQLite transactions already hold the write lock; catalog introspection uses a no-op.
UPDATE AUD_PIPELINE_PAUSES SET RESUMED_AT=RESUMED_AT WHERE 1=0
