# Release and rollout checklist

Use this checklist for a tagged package release and for a customer rollout. Record the artifact
version, commit, date, owner, and the evidence for each applicable item.

## Package release

- [ ] The version, changelog entry, and release notes describe the actual scope and known limits.
- [ ] `make check` passes on every supported Python version in CI.
- [ ] The built wheel installs and passes the wheel smoke test outside the source checkout.
- [ ] The canonical environment-backed and file-backed configuration examples parse successfully.
- [ ] Documentation links, command names, configuration field names, and authentication claims match
      the released artifact.
- [ ] Packaged migrations and a representative project migration upgrade a copy of an existing
      Engine DB. An altered applied migration is rejected as expected.
- [ ] The release artifact is pinned and its integrity information is recorded by the publisher.

## Warehouse acceptance

- [ ] PostgreSQL Engine DB and PostgreSQL warehouse acceptance run passed.
- [ ] If Trino/Iceberg is in scope, the selected catalog was verified as Iceberg and the task action
      vocabulary was exercised against it.
- [ ] If Databricks is in scope, its credential-gated acceptance test passed against the target
      workspace and catalog.
- [ ] If Snowflake is in scope, its credential-gated acceptance test passed against the target
      account. Iceberg acceptance exercises the selected storage mode: Snowflake-managed storage
      (the default, requiring neither storage parameter), or a customer `EXTERNAL_VOLUME` paired
      with `BASE_LOCATION`. Iceberg cloning has separate explicit storage requirements; verify
      its configured external volume and base location when cloning is enabled.
- [ ] DuckDB is used only where its single-writer behavior is acceptable.

## Customer rollout

- [ ] An Engine DB backup was created and restoration was tested in a nonproduction database.
- [ ] Task dispatch is paused or drained before migration; all runners receive the same pinned
      package artifact.
- [ ] `etl-craft migrate`, `etl-craft doctor`, and `etl-craft validate` pass after deployment.
- [ ] A representative pipeline passes and its `history` and task audit rows are reviewed.
- [ ] Scheduler command logs and alerts for nonzero exits are connected to the owning team's
      monitoring system.
- [ ] The owner has a documented secret-rotation, backup, and rollback contact path.

This repository does not provide deployment automation, a managed service, or a response-time
support commitment. The rollout owner supplies those operational controls.
