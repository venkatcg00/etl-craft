# Governance

etl-craft is an Apache-2.0 open-source project with one maintainer, [venkatcg00](https://github.com/venkatcg00).

## Who decides

The maintainer decides scope, design and releases. Decisions, and the reasons for them, are
recorded where everyone can read them: the roadmap and its handover notes
(`docs/development/road-to-1.0.0.md`), the design documents in `docs/development/`, and the
pull requests.

## Scope

etl-craft moves data: ETL and ELT pipelines, their scheduling and run control, data quality
rules, lineage and documentation. It is not a general-purpose workflow orchestrator. A feature
is accepted when it serves moving data correctly and operably; features that only serve
general orchestration are declined. `AGENTS.md` → "Your role" states the rules every change is
held to.

## How changes are accepted

- Open an issue describing the problem before a large change. Small fixes can go straight to a
  pull request.
- A change that adds public surface (a command, flag, setting, config-file column or Engine DB
  table) needs the maintainer's agreement on the design first.
- Every pull request fills in the template, passes the complete CI suite, and keeps upgrades
  working from every released version.
- AI coding agents work on this project under the same rules (`AGENTS.md`); their pull requests
  say so, and the maintainer reviews and merges them.

## Releases and compatibility

Releases follow semantic versioning. From 0.9.0, each release upgrades from every earlier
release with `etl-craft migrate`, and a rolling upgrade or rollback within a minor line keeps
working. Release notes are in `CHANGELOG.md`.

## Continuity

If the maintainer can no longer maintain the project, the repository will say so in its README
and its last release notes. The code stays available under the Apache-2.0 license, and anyone
may fork and continue it. Further maintainers with commit access are added as regular
contributors appear; this file will name them and how decisions are made among them.

## Security

Report vulnerabilities privately as described in [SECURITY.md](SECURITY.md).

## The name

"etl-craft" names this project and its releases. Forks are welcome under the license; please
give a fork a different name so users can tell them apart.
