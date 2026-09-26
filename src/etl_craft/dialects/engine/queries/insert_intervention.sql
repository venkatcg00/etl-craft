-- Record one change an operator made to :pipeline_run_id, or to :task_id under it.
INSERT INTO AUD_RUN_INTERVENTIONS (PIPELINE_ID, PIPELINE_RUN_ID, TASK_ID, ACTION, FROM_STATUS,
                                   TO_STATUS, TARGET_COUNT, PREVIOUS_MESSAGE, REASON,
                                   REQUESTED_BY, REQUESTED_AT)
VALUES (:pipeline_id, :pipeline_run_id, :task_id, :action, :from_status, :to_status,
        :target_count, :previous_message, :reason, :requested_by, :now)
