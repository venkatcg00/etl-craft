INSERT INTO AUD_GATE_DECISIONS (PIPELINE_RUN_ID, ATTEMPT_ID, PIPELINE_DEPENDENCY_ID,
TASK_DEPENDENCY_ID, SELECTED_PIPELINE_RUN_ID, SELECTED_TASK_RUN_ID, SELECTED_REVISION,
RESULT, REASON, DECIDED_AT)
VALUES (:run_id, :attempt_id, :pipeline_dependency_id, :task_dependency_id,
:selected_pipeline_run_id, :selected_task_run_id, :revision, :result, :reason, :now)
