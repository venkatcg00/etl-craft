# Guides

How to model and run pipelines with etl-craft.

- [Dependencies and run conditions](dependencies.md): dependency types, `ALL`, `ANY` and `N`
  conditions, waves, and which tasks run, wait or are skipped.

!!! note "Planned"
    These guides are written as each feature reaches the new code base:

    - **Pipelines and tasks**: the configuration rows, task handlers and parameters.
    - **Run lifecycle and retries**: how runs are created, resumed and finished.
    - **SQL actions**: the seven actions, audit columns, schema evolution and deduplication.
    - **Business rules**: rule waves, and how rows are flagged and cleared.
    - **Python ingestion scripts**: the script contract and offset tracking.
    - **Email alerts**: completion alerts, their three flavours and templates.
    - **Incremental and full refresh**: `$$pipeline_id` and refresh types.
    - **Lineage and documentation**: table and column lineage, and documentation versions.
