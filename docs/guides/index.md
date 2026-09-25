# Guides

How to model and run pipelines with etl-craft.

- [Dependencies and run conditions](dependencies.md): dependency types, `ALL`, `ANY` and `N`
  conditions, waves, and which tasks run, wait or are skipped.
- [SQL tasks](sql-tasks.md): the SELECT, inline or from `sql_files/`, the pipeline-id tokens,
  the eight actions, and what the engine checks and logs.
- [Business rules](business-rules.md): writing rules, flagging and clearing rows, waves and
  retries.
- [Running a pipeline](running-pipelines.md): dependency waves, resuming a run, connection
  tests, orchestrator steps, and the SLA.
- [Running a task and reading its logs](running-tasks.md): `etl-craft run --task_code`,
  retries, time limits, and where each attempt's log goes.

!!! note "Planned"
    These guides are written as each feature reaches the new code base:

    - **Pipelines and tasks**: the configuration rows, task handlers and parameters.
    - **Python ingestion scripts**: the script contract and offset tracking.
    - **Email alerts**: completion alerts, their three flavours and templates.
    - **Lineage and documentation**: table and column lineage, and documentation versions.
