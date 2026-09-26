-- Record that :task_id, under :pipeline_run_id, consumed upstream task run :run_id through task
-- dependency :dependency_id.
INSERT INTO AUD_DEPENDENCY_CONSUMPTION (TASK_DEPENDENCY_ID, PIPELINE_ID, PIPELINE_RUN_ID, TASK_ID,
                                        DEPENDS_ON_PIPELINE_ID, CONSUMED_PIPELINE_RUN_ID,
                                        CONSUMED_TASK_RUN_ID, CONSUMED_AT)
SELECT :dependency_id, :pipeline_id, :pipeline_run_id, :task_id, :depends_on_pipeline_id,
       PIPELINE_RUN_ID, :run_id, :now
FROM AUD_TASK_RUN_LOG
WHERE TASK_RUN_ID = :run_id
