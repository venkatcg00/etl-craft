# Guides

How to model and run pipelines with etl-craft.

!!! note "Planned"
    These guides are written as each feature reaches the new code base:

    - **Pipelines and tasks**: the configuration rows, task handlers and parameters.
    - **Dependencies and run conditions**: dependency types, `ALL`, `ANY` and `N` conditions,
      and dependencies between pipelines.
    - **Run lifecycle and retries**: how runs are created, resumed and finished.
    - **SQL actions**: the seven actions, audit columns, schema evolution and deduplication.
    - **Business rules**: rule waves, and how rows are flagged and cleared.
    - **Python ingestion scripts**: the script contract and offset tracking.
    - **Email alerts**: completion alerts, their three flavours and templates.
    - **Incremental and full refresh**: `$$pipeline_id` and refresh types.
    - **Lineage and documentation**: table and column lineage, and documentation versions.
