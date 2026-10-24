INSERT INTO AUD_ACTIONS
(STARTED_AT, ENDED_AT, ACTOR, ACTOR_KIND, HOST, COMMAND, ARGUMENTS,
 PROJECT_ID, PIPELINE_ID, TASK_ID, OUTCOME, EXIT_CODE)
VALUES (:started, :ended, :actor, :kind, :host, :command, :arguments,
        :project, :pipeline, :task, :outcome, :exit_code)
