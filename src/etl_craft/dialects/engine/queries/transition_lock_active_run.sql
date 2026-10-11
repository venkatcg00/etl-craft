-- Serialize event completion with cancellation and refuse writes after the run ends.
UPDATE AUD_PIPELINES_RUN_LOG SET STATUS=STATUS
WHERE PIPELINE_RUN_ID=:row_id AND STATUS='IN-PROGRESS'
