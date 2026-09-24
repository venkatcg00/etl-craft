# Deploying

Running etl-craft on a machine or under an orchestrator.

!!! note "Planned"
    - **Local mode**: etl-craft runs each pipeline's waves itself.
    - **Orchestrator mode**: `generate-yml` emits one DAG description per pipeline, which you
      convert for your scheduler.
    - **Operations**: backups, upgrades and Engine DB migrations.
    - **Security**: secrets, credentials and least-privilege roles.
