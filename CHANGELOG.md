# Changelog

This file records release-facing changes from the first public release process onward. Earlier
development history remains available in the Git log and `ITERATION_2.md`.

## Unreleased

### Changed

- Recommended cloud connections use separate fields: Databricks uses JDBC URL, catalog, schema,
  and token; Snowflake uses user, account, database, schema, warehouse, role, and token.
- Databricks `iceberg` targets use Delta with UniForm enabled. Snowflake Iceberg targets default
  to Snowflake-managed storage; customer external volumes still require a base location.
- Established the canonical `Orchestration` / `Secrets` / `Engine` configuration contract while
  retaining legacy manifest parsing for migration.
- Made package and project migrations separate, checksummed streams so project migrations cannot
  mask engine migrations or silently change after application.
- Scoped the documented launch path to PostgreSQL Engine DB and PostgreSQL warehouse, with clear
  acceptance requirements for optional warehouse integrations.

### Added

- SCD1 `PRESERVE_TARGET`, defaulting to `false`. When enabled, updates use
  `COALESCE(source_value, target_value)` so source nulls retain existing values. Change detection
  and stored hashes use the resulting values; inserts are unchanged.
- Deployment, backup, upgrade, monitoring, security, and release-rollout documentation.
- A release checklist for package publishing and customer environments.

### Fixed

- Removed credential-bearing parameters from preferred Databricks JDBC URLs before setup can
  persist them in a legacy manifest.
- Preserved the original dependency-consumption watermark when execution joins an existing
  pipeline run.
- Rejected unused fields in token connection profiles and aligned validation with supported
  Snowflake-managed Iceberg storage.
- Preserved canonical connection variable mappings and cloning storage settings during setup.
- Corrected cross-pipeline documentation version lookups, email-alert dependency validation,
  and business-rule failure recording when committing results fails.

### Verification and known limits

- Local verification: 619 tests passed, four credential-gated cloud tests skipped, and 95.36%
  coverage. Schema, typing, formatting, lint, documentation-style, and clean-wheel smoke checks
  passed. These results do not establish the supported-Python CI matrix or customer acceptance.
- Earlier live Databricks and Snowflake checks are recorded in `CLAUDE.md`; cloud acceptance was
  not rerun for the latest remediation, including `PRESERVE_TARGET`.
- The release remains Alpha. Structured logging and task-output capture remain deferred
  (E2-18/E2-20); deployment owners must provide scheduler logging and alerting.

No package release has been tagged from this changelog yet.
