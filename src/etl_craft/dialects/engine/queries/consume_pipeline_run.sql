-- Record that :pipeline_run_id, a run of :pipeline_id, consumed upstream run :run_id through
-- pipeline dependency :dependency_id.
INSERT INTO AUD_DEPENDENCY_CONSUMPTION (PIPELINE_DEPENDENCY_ID, PIPELINE_ID, PIPELINE_RUN_ID,
                                        DEPENDS_ON_PIPELINE_ID, CONSUMED_PIPELINE_RUN_ID,
                                        CONSUMED_AT)
VALUES (:dependency_id, :pipeline_id, :pipeline_run_id, :depends_on_pipeline_id, :run_id, :now)
