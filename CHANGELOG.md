# Changelog

This file records release-facing changes from the first public release process onward. Earlier
development history remains available in the Git log and `ITERATION_2.md`.

## Unreleased

### Changed

- Established the canonical `Orchestration` / `Secrets` / `Engine` configuration contract while
  retaining legacy manifest parsing for migration.
- Made package and project migrations separate, checksummed streams so project migrations cannot
  mask engine migrations or silently change after application.
- Scoped the documented launch path to PostgreSQL Engine DB and PostgreSQL warehouse, with clear
  acceptance requirements for optional warehouse integrations.

### Added

- Deployment, backup, upgrade, monitoring, security, and release-rollout documentation.
- A release checklist for package publishing and customer environments.

No package release has been tagged from this changelog yet.
