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
etl-craft bundle publish --project ./project --api https://craft.example.com
etl-craft run --pipeline-code SALES --api https://craft.example.com
```

The publishing CI runner needs a developer API token and the project files; it needs no
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
configuration supplies Engine, Warehouse, Email and Cloning connections, authentication,
store credentials and resource limits. Those connection definitions remain host-local until
multi-project support; uploaded configuration cannot redirect them. Execution files and
project execution settings come from the publication. Bundle parsing errors withhold resolved
host values; validate the source configuration locally for detailed diagnostics. Files remain relative to the materialized project root.
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
reopened: start a new run. Publish before admitting new agent runs. Bundle records are
currently deployment-scoped; project ownership is introduced with multi-project support.
