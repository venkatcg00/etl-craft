# Guides

How to model and run pipelines with etl-craft.

- [Dependencies and run conditions](dependencies.md): dependency types, `ALL`, `ANY` and `N`
  conditions, waves, and which tasks run, wait or are skipped.
- [SQL tasks](sql-tasks.md): the SELECT, inline or from `sql_files/`, the pipeline-id tokens,
  the eight actions, and what the engine checks and logs.
- [Business rules](business-rules.md): writing rules, flagging and clearing rows, waves and
  retries.
- [Ingestion scripts](ingestion-scripts.md): the script contract, `INPUT_PARAMS`, offsets, and
  where a script's output goes.
- [Email alerts](email-alerts.md): the alert task, its outcomes and tokens, SLA emails, and
  sending through SMTP or `sendmail`.
- [Validating pipelines](validating.md): `validate`, every check it makes, and what fails.
- [Inspecting pipelines](inspecting.md): `list`, `graph`, `steps` and `history`.
- [Lineage and documentation versions](lineage.md): column lineage across tasks and pipelines,
  and `docs-version`.
- [The catalog site](catalog.md): `generate-docs`, the pages, lineage graphs and search.
- [Running a pipeline](running-pipelines.md): dependency waves, resuming a run, connection
  tests, orchestrator steps, and the SLA.
- [Running a task and reading its logs](running-tasks.md): `etl-craft run --task_code`,
  retries, time limits, and where each attempt's log goes.
- [Stepping in: mark, cancel, rerun and bypasses](run-control.md): marking a task or a run,
  stand-in runs, cancelling, running a task again or without its dependencies, relaxing
  dependency gates, and the record every change leaves; local mode.

!!! note "Planned"
    These guides are written as each feature reaches the new code base:

    - **Pipelines and tasks**: the configuration rows, task handlers and parameters.
