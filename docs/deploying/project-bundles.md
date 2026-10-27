# Pinned project bundles

Publish a reviewed project before starting runs on agents. Every run records its bundle ID,
metadata version and admitted execution definition. Retries and recovery use that same
publication and graph, including after a controller restart. File or metadata edits affect the
next run. `status`, `explain` and their API documents show the pinned version and whether the
current metadata differs.

## Configure storage

For a shared filesystem, configure the API and controller hosts:

```yaml
Artifacts:
  Store: file:///srv/etl-craft/artifacts
```

For S3-compatible storage, install `etl-craft[aws]` and name an object-store profile:

```yaml
Object_stores:
  artifacts:
    Endpoint: https://objects.example.com
    Region: us-east-1
    Url_style: PATH
    Auth_mode: KEYS
    Access_key_id: ARTIFACT_ACCESS_KEY
    Secret_access_key: ARTIFACT_SECRET_KEY
Artifacts:
  Store: s3://project-artifacts/production
  Profile: artifacts
```

The key fields contain Secrets-provider variable names, never values. Loading an unused
profile does not require its credentials; the serving host resolves them when it opens the store. `Auth_mode: DEFAULT`
uses the host's credential chain; omit both key fields. Omit `Endpoint` for AWS. `Url_style`
is `PATH` or `VIRTUAL`. `doctor` checks list, write and read permissions on the configured
prefix. Keep the recorded profile and objects available for the lifetime of admitted runs.
The manifest stores the location and profile name, not their credentials.

## Publish and run

```bash
etl-craft bundle publish --project-dir ./project --api https://craft.example.com
etl-craft run --pipeline-code SALES --api https://craft.example.com
```

The publishing CI runner needs project CAN_MANAGE on its API token and the project files; it needs no
Engine DB or artifact-store credentials. The API validates and stores the upload. Publication
contains `craft-connector.yml` and every regular file under `sql_scripts/`, `python_scripts/`
and `seeds/`, with a SHA-256 per file. Symlinks, traversal and special files are refused.
Remove generated files before publishing: a publication permits at most 10,000 files and
64 MiB of decoded content. HTTP requests are capped at 128 MiB before JSON decoding,
including requests without Content-Length. Shorten paths or reduce content if this limit is
reached. File bytes are omitted from action audit arguments.

With `Artifacts.Store` set, starting a run without any publication fails before admission.
A new publication becomes the next run's bundle, even when its files match an earlier one.
Local development with no store uses the working directory; its metadata is still pinned.

Agents download their assigned bundle through fenced API requests. They need no access to
the Engine DB or artifact store and no separate copy of the execution files. Their bootstrap
configuration supplies Engine DB, authentication and artifact-store credentials and host
resource ceilings. Each bundle supplies Warehouse, Email and Cloning definitions. An admin
must publish the initial connection contract and approve each changed contract by publishing
it; a developer can publish only definitions matching the latest approved contract. Credential
fields remain variable names, resolved on the serving host or agent. Execution files and
settings come from the publication; diagnostics withhold resolved host values.
Relative connection file paths resolve beside the host's bootstrap configuration, so a local
warehouse database is not placed in the verified cache. Place generated outputs outside that root.
Use absolute host/shared paths for external file arrivals: relative `EVENT_FILE` patterns also
resolve inside the materialized project, which must remain unchanged.

Agents and local children cache under `~/.cache/etl-craft/bundles/<id>`. Each use checks the
manifest and hashes. Missing, modified, linked or unexpected cached files fail the task;
remove the damaged cache directory and retry after checking the artifact store. The package
never silently repairs a corrupted publication.

## Check an agent image

```bash
etl-craft doctor --agent --api https://craft.example.com
```

Use an agent token on the agent host. This parses the latest bundle's Python files with AST
and checks top-level module availability in the calling interpreter; it never imports project
code. Install the reported packages in the image. Conditional imports are included; dynamic
imports cannot be discovered statically.

## Upgrade

Apply migration `0053_project_bundles.sql` before running this package. Active and queued
pre-bundle runs receive their current metadata definition during upgrade and retain local
working-tree execution. Completed older runs have no historical snapshot and cannot be
reopened: start a new run. Publish before admitting new agent runs. Apply `0054_engine_projects.sql` for project ownership. Existing metadata, publications,
lineage and project migration records move to `default`. Republish existing bundles as an
admin to establish their connection approvals before admitting new bundled runs.

## Several projects in one Engine DB

Select `--project CODE` or set `Orchestration.Project` explicitly. There is no implicit default;
a conflicting flag and file value are refused, naming both.
`--project-dir` selects a filesystem directory for config plan/apply, publication or agent bootstrap.
Each project has its own warehouse. Deployment roles remain shared, with pipeline grants
scoped to their project. Pools remain deployment-wide.

Administrators create projects before applying pipeline metadata:

```bash
etl-craft project add --project sales --name Sales --timezone UTC --default-pool default --max-running-tasks 8 --reason "Sales onboarding"
etl-craft --project sales config apply
etl-craft project set --project sales --max-running-tasks 12 --reason "Capacity review"
etl-craft project list --reason "Review projects"
```

`project add|set|disable` require `--project` on the command line. `set` changes only supplied
fields; no field is an error. `disable` takes the identity and reason, preserving metadata.
Add defaults to pool `default`; the pool must be active. `DEFAULT_POOL` is used for tasks with
no `POOL`; `MAX_RUNNING_TASKS` caps claims across the project's pipelines. A full project does
not prevent another from claiming work. Project mutations and pool administration require deployment admin; project lists show only
projects the caller can view.

Publish and run with the same selection:

```bash
etl-craft bundle publish --project sales --project-dir ./sales --api https://craft.example.com
etl-craft run --project sales --pipeline-code DAILY --api https://craft.example.com
etl-craft pause --project sales --reason "Maintenance"
```

Project API routes use `/api/v1/projects/sales/...`; deployment accounts, tokens, groups,
agent claims and health routes keep their deployment paths. Exact execution IDs must belong
to the selected project. Cross-project dependencies retain their upstream IDs and recorded
consumption decisions; set `DEPENDS_ON_PROJECT_CODE` in the dependency CSV to name another
project, or leave it empty for the selected project.

Cloning copies only the selected project's rows and retained history, plus shared deployment
configuration and runtime records. Credential hashes are excluded. As with the static catalog,
project-wide mirrors include all pipelines in that project; warehouse access must be reviewed
by the admin approving its connection contract.
