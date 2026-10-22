# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `Offset.partitions({...})` returns a `PARTITIONS` offset for streams: partition keys mapped to
  the next position to read, stored as sorted JSON. An attempt fails, storing nothing, when a
  partition moves backwards or a stored partition is missing; new partitions are logged.
  `etl-craft mark --task-code T --offset VALUE --reason ...` resets a Python task's offset (a
  JSON object sets, adds or, with `null`, removes partitions) and is audited. New guide:
  "Reading from streams". Apply `0050_partition_offsets.sql` before using the type.
- Keyed warehouse event waits consume each `EVENT_KEY_COLUMN` once per task in
  `AUD_EVENT_KEYS`, atomically with success. Downstream SQL uses `$$event_keys`; Python
  receives `task.event_keys` and `task.event_rows`, including on retry and recovery.
  `EVENT_MAX_ROWS` bounds SQL probes (default 10000); invalid keys and unconditional
  aggregates fail validation. Apply `0049_event_keys.sql` before using keyed waits.

- PostgreSQL queue execution for `Deployment.Profile: CONTROLLER_AGENTS`, with API-only
  agents, 30-second claims, measured capacity, leases, fenced reports and shared log chunks.
  `etl-craft agent` runs from its own `--project` copy; `agents` lists capacity and
  `agent drain|resume|stop --agent-id` records durable controls. Agent tokens cannot invoke
  people's actions. Apply `0048_agent_queue.sql` before starting the roles.
- Optional PostgreSQL `Engine.<profile>.Listen_jdbc_url` reuses profile authentication for
  direct or session-pooled notifications. UI/controller startup and doctor verify delivery.

- `HANDLER=SQL_SCRIPT` executes `SCRIPT_FILE` under `sql_scripts/` in order, with bound run
  context and `INPUT_PARAMS`. Each statement is audited in the attempt log. Native autocommit
  preserves earlier statements on failure; files may use explicit transactions where supported.
  These tasks bypass managed hashes, target locks and ROW_ID; `validate` warns about managed
  target overlap, and lineage/catalog label their inferred effects unmanaged. Apply migration
  `0047_sql_script.sql` before using the handler. Prefer `RETRY_ON=NONE` for scripts.

### Fixed

- Waiting agent claims recheck token revocation, expiry and account activity before admitting
  queued work, including credentials changed while the long poll waits.
- Warehouse session labels distinguish deployments that reuse attempt numbers. Where warehouse
  fencing is supported, failed work confirms its statements stopped before unlocking, retaining
  the lock when confirmation fails.
  Agents check their own warehouse's fencing capability before registration, and claims clear
  obsolete queue-wait reasons.

- Upgrading a SQLite Engine DB from 0.3.0 keeps each attempt's RETRYABLE value.
- SQLite upgrades retain each pipeline run's configuration fingerprint. Development Engine DBs
  that applied the earlier unreleased 0035 or 0044 migration must be recreated; `migrate`
  refuses changed applied files. No released installation has applied those migrations.
- Audit arguments mask secret-named keys inside nested lists and mappings, and credentials in
  connection URLs. Text and JSON logging use the same masking for messages, arguments,
  context, extra fields and exceptions; audit CLI JSON and API responses retain that redaction.

### Changed

- Fewer warehouse and Engine DB round trips. A task uses one warehouse connection for all its
  steps, including repeated `task.warehouse()` calls, keeping single-writer reservations and
  attempt tags. Managed SQL actions read each table's shape once until their own DDL, fold
  row and key counts into one statement, merge with one `MERGE` where the warehouse has it,
  compute `HASH_KEY` while staging, and drop no temporary tables. Target and single-writer
  locks are taken and released together. On Snowflake an action's relation checks are one
  metadata scan. A running pipeline re-reads its run from the Engine DB at most once a second,
  instead of on every scheduler tick, while acting at once on an attempt that ends. A new
  Snowflake or Databricks target is published by renaming its candidate. No lease, ownership
  or fencing check is cached. Cloud acceptance uses parallel pytest workers; release evidence
  is collected and written once by the controller, preserving serial test IDs and outcomes.

- Target, event and single-writer warehouse ownership use shared durable resource locks.
  Agents stop their children before lease expiry when renewal fails. A lock left by an ended
  or expired holder passes only after the next holder stops that holder's tagged warehouse
  statements and confirms none still run; otherwise the lock stays with it and the new writer
  fails, naming the target and holder. `CONTROLLER_AGENTS` refuses Databricks warehouses, which
  cannot yet stop another session's statements; use local execution there.
- Removed `ScriptTask.engine_db`; scripts use `task.warehouse()` locally and on agents.

- Script naming is consistent: `python_scripts/` replaces `ingestion_scripts/`,
  `sql_scripts/` replaces `sql_files/`, Python tasks use `SCRIPT_FILE` in place of `SCRIPT_NAME`,
  and `Orchestration.Local_python_slots` replaces `Local_ingestion_slots`.
  One parameter names every file a task runs: `SCRIPT_FILE` replaces `SOURCE_SQL_FILE` for SQL
  actions, as for `PYTHON` and `SQL_SCRIPT` tasks, and an inline SELECT is `SELECT_QUERY` in
  place of `SOURCE_SQL`. `SCRIPT_FILE` is a path from the project directory, like every other
  path: `python_scripts/load.py` or `sql_scripts/orders.sql`. A value without its folder is
  refused, naming the path to write.
  Migration `0047_sql_script.sql` renames existing parameter rows and adds the folder to
  existing file values. Move project folders and update configuration files before running;
  old names have no aliases.

- Engine DB scheduling, run inspection and catalog reads use selective history indexes and
  bounded PostgreSQL lookups. Migration `0046_query_indexes.sql` adds the indexes on both
  Engine DBs without changing results or commands.

- Split HTTP from supervision with `etl-craft ui` on PostgreSQL and
  `etl-craft controller --no-ui` serving only unsigned `GET /health` with the standard library.
  All-in-one remains the default with HTTP dependencies installed; SQLite supports that layout.
  API copies share durable sessions, refresh tokens and run state without sticky sessions.
  Both roles refuse missing, unknown or changed Engine DB migrations for their package.
  UI readiness recovers from Engine DB outages, SIGTERM rejects new requests and drains
  existing ones within `Shutdown_grace_seconds`, and explicit `Docs_site.Trusted_proxies`
  controls forwarded addresses/schemes recorded under audit arguments' `_http` metadata.
  Unsigned readiness responses report safe causes without exposing database exception details.
- `etl-craft[ui]` replaces `[server]`, and `[all]` includes UI and every warehouse extra.
  The remote CLI uses `--api` / `ETL_CRAFT_API`; `ApiClient` replaces `ControllerClient`.
  Old names have no compatibility aliases. Apply Engine DB migrations before starting roles.

- Consistent names, with no aliases: the old spellings are refused by name.
  - Flags are kebab-case: `--pipeline-code` and `--task-code`. `list` is `pipelines`.
  - The remote address is `--api` / `ETL_CRAFT_API`.
  - Every `craft-connector.yml` key is `Capitalized_snake`: `Jdbc_url`, `User`, `Secret`,
    `Schema`, `Auth_mode`, `Key_file`, `Host`, `Port`, `From_address`, `Tls_mode`,
    `Transport`, `Catalog`, `Token`, `Account` and the rest of each connection and email
    block. A lowercase key is refused with its correct spelling.
  - Every closed value is upper case and matched exactly: `Auth_mode` (`NONE`, `PASSWORD`,
    `TOKEN`, `KEY_FILE`, `OAUTH`, `SSO`, `STS`), `Tls_mode`, `Transport`, `Table_format`,
    `Cloning.Scope`, `Dependency_gates`, `Secrets.Source_type` (`ENVIRONMENT`, `FILE`) and
    `Deployment.Profile` (`LAPTOP`, `CONTROLLER`, `CONTROLLER_AGENTS`). So are a variable's
    value that sets one, and the task parameter `TABLE_FORMAT`.
  - Help lines are one sentence with a capital and a full stop.
  - Python modules: `api.server` is `api.serve`, `cli.remote` is `cli.client`
    (`ControllerClient`), `execution.supervisor` is `execution.processes`,
    `execution.failures` is `execution.classify`, `dialects.failures` is
    `dialects.failure_evidence`, `engine.repository.interventions` is
    `engine.repository.intervention_rows`, and the `services.catalog_*` modules are the
    `services.catalog` package. `docs/deploying/server.md` is `controller.md`.

- One action table (`etl_craft.services.operations.actions`) holds every action's role,
  audit, placement and HTTP route; `invoke(ctx, ActionName, request)` is the only way an
  interface reaches an operation. Operations take one request dataclass each and no longer
  check roles themselves. `docs/reference/actions.md` is generated from the table.
- The HTTP API has one generated route per action, with run selection in the query or body:
  `POST /api/v1/runs` (runs, task runs, reruns, skips and backfills), `POST /api/v1/marks`,
  `POST /api/v1/controls` (scoped controls, and `PAUSE`/`RESUME` of a definition),
  `GET /api/v1/status`, `/steps`, `/graph`, `/explanations`, `/history`, `/capabilities`,
  `/audit`, `/lineage`, `/validation`, `/accounts` and more; see the actions reference. The
  `/pipelines/{code}/...` and `/runs/{id}/tasks/...` variants are removed. A malformed or
  unknown field answers 400 naming the field.
- Every action that can run anywhere works with `--controller`, including `token`, `account`,
  `graph`, `steps`, `audit`, `lineage`, `validate`, `clone`, `rehash`, `upgrade-targets`,
  `generate-docs` and `docs-version`. Host-only actions (`config`, `migrate`, `init-db`,
  `setup`, `doctor`, `controller`, `publish-docs`) are refused there with where to run them.
- `status --capabilities` is now `capabilities`, and `status --request-id N` is now
  `control-request --control-id N`.
- Messages from services name request fields (`` `task_code` ``); the CLI shows them as flags.
- A run `reason` is accepted on any run request and recorded with it.
- `lineage` without `--refresh` and `setup --print-grants` are reads and no longer write an
  `AUD_ACTIONS` row; `lineage --refresh` is its own audited `lineage refresh` action. Pause
  and resume are recorded as `control` with their action.
- `etl-craft account` changes and `token revoke` answer with the account or token list.
- A `mark` status other than `SUCCESS`, `FAILED` or `SKIPPED` is refused when the request is
  read, naming `status`; over HTTP that is a 400.
- `doctor` names file checks by their `craft-connector.yml` key (`Engine DB Key_file`,
  `Email Ca_file`, `Email Sendmail_path`) and shows `Tls_mode=` and `Ca_file=` in the
  email TLS check.

- `Orchestration.Allow_schedule` now controls the controller: `false` in an environment's
  profile stops that environment's controller from creating scheduled runs, without affecting
  other environments. Manual runs and catalog builds continue. It was previously read only by
  the Airflow export.

- The controller rebuilds the catalog on `Docs_site.Schedule` in the project timezone,
  without blocking task supervision. Only one scheduled build runs at a time; missed ticks
  are skipped. Manual and scheduled builds record one immutable `REQUESTED` row and one
  `SUCCESS`/`FAILURE` completion row, sharing timestamp and arguments, with the completion's
  exit code. Failed builds are logged and tried again at the next tick.

- The scheduling process is the controller: start it with `etl-craft controller`.
  `etl_craft.controller` replaces `etl_craft.overseer`; migration `0042` renames
  `AUD_OVERSEERS.OVERSEER_ID` to `AUD_CONTROLLERS.CONTROLLER_ID` without losing process history.
  Deployment profiles are `laptop`, `controller` and `controller-agents`; the old `server`
  command and `server`/`server-workers` profile values have no aliases. Update launch commands
  and configuration files, stop the old process, then migrate before starting the controller.
  Task execution uses agent terminology; the cluster agent itself remains S5.C.

### Removed

- Remote mode: `Orchestration.Mode`, `Orchestration.Name`, the `etl-craft run --finalize-only`
  flag and the orchestrated finalization it reached. The controller always schedules;
  `--init-only` creates a run it picks up, and another scheduler starts work with
  `etl-craft run --server` or `POST /api/v1/pipelines/{code}/runs`. The DAG-only settings
  `Global_dag`, `Tags`, `Retry_delay_minutes`, `Depends_on_past`, `Email_on_failure` and
  `Orchestration.Catchup` are removed; `Orchestration.Retries` and
  `Email_recipients` stay because runs and alerts read them. Each removed setting fails as an
  unknown key naming where it was found. `RemoteUnsupportedError` is gone; exit code `18`
  stays reserved so recorded outcomes still read back.
  With `Mode` gone, the `Orchestration` section is optional: every setting in it has a default.
- `PIPELINE_PARAMETERS` keys `CATCHUP`, `DEPENDS_ON_PAST`, `EMAIL_ON_FAILURE`, `RETRIES` and
  `RETRY_DELAY_MINUTES`; `TAGS` and `EMAIL_RECIPIENTS` stay. Migration `0043` removes the five
  keys from stored pipelines as etl-craft, leaving history rows unchanged. Any unknown key now fails `validate` and
  `config apply`, naming the pipeline, the key and the allowed keys, instead of warning.
  Remote-era audit rows (`orchestrator:` run keys, `ORCHESTRATOR` kinds) still read back.

- Airflow DAG export: `etl-craft generate-yml` (including `--pipeline_code`, `--global`,
  `--docs`, `--airflow-version` and `--output` on that command), the `etl-craft-airflow`
  integration package, `services.generate_yml`, `documents.dag_yaml`, the packaged DAG YAML
  schema and `Auth.Airflow_connection_id`. Removed inputs use the ordinary unknown-input
  errors; there are no compatibility aliases. The unused `jsonschema` runtime dependency
  and its development stubs are removed, together with export-only tests and CI jobs.

### Added

- Pipeline permission groups: admin-only `group add|disable|add-member|remove-member|list`
  and matching API actions, plus `grants.csv` for direct/tag viewer/operator grants. Restricted
  pipelines use current group grants, lists omit inaccessible pipelines, and a grant can elevate
  a deployment viewer to pipeline operator. Grant changes require admin and config apply is atomic.
- Optional flat `Auth.Oidc_groups_claim` maps SSO names to durable group IDs. Sessions retain
  claims through refresh within its lifetime; admitted runs retain them for children and
  grandchildren. Each child admission/start rechecks the active account, groups and current grants.
  Provider membership removal is reflected at the next SSO sign-in. Engine migration 0045 adds
  groups, memberships, grants, typed histories and JSON-text token/run claim columns on both DBs.
  Static generated catalogs remain whole-project and now state that grants cannot filter them.

- `RUN_PIPELINE` tasks start child pipelines in `WAIT` or `DETACH` mode. DETACH succeeds at
  durable QUEUED admission; WAIT hands off to the controller without retaining an agent slot,
  then mirrors child status and failure details without aggregating row counts. Timeouts and parent
  interventions are durable. Children inherit the parent's logical date and initiating actor.
  Automatic retries reuse the child; explicit reruns advance a trigger generation. Validation
  rejects invalid targets and trigger/dependency cycles. Status, explanations, run-detail JSON
  and catalog pages expose parent/child links. Both Engine DBs migrate existing audit history.

- Pipeline tags: at most 20 unique labels of 1–64 lowercase ASCII letters, digits, `-`, `_`
  or `:`. Invalid metadata fails `validate` and is refused by `config apply`, without normalization.
- Repeatable `--tag`/`?tag=` AND filters on the existing pipeline list, status, history, audit,
  validation and catalog-gap reads. Pipeline text/JSON includes `tags`; cross-pipeline status
  lists non-terminal runs and history retains bounded pagination. Unknown tags match nothing.
- `pause` and `resume` accept tag selections with `--yes`, resolve codes before mutation and
  return one outcome per pipeline through the existing bulk-control action. Audits store codes.
  Catalog pipeline pages and the DAGs overview show linked tag chips and a shareable URL filter.

- Failure-aware automatic retries: `Orchestration.Retry_on` and task `RETRY_ON` select
  `NONE`, `TRANSIENT` (default), `TRANSIENT_AND_UNKNOWN` or `ANY`. Permanent warehouse errors
  stop after one attempt by default. Scripts can declare `TransientError` or `PermanentError`
  from `etl_craft.scripting` (exit codes 28/29). Migration `0041` records each attempt's failure
  class, original exception, SQLSTATE/driver code and effective retry policy on both Engine DBs;
  explain, status JSON, the API and catalog run detail expose the evidence. Existing attempts
  remain `UNKNOWN` with no reconstructed policy.

- Team-owned extra `CFG_` tables accept ordinary SQL writes without package base fields,
  actor guards or history capture. They stay outside config CSVs, catalog documentation and
  warehouse cloning. Package-owned config tables use a fixed schema-side ownership list and
  retain their protections. Migration `0040` releases generated team triggers on both Engine DBs,
  preserves team triggers and retains old history read-only. Reserved `AUD_` names and future
  package-table collisions fail with a rename remedy before the conflicting migration writes.

- Deployment profiles: an optional `Deployment` section with `Profile: laptop | server |
  server-workers`. Without it nothing changes. With it, `run` and `server` measure the CPU and
  memory the process may use (CPU affinity, cgroup v2 or v1 quotas and memory limits including
  ancestors, else host totals) and size task slots, HTTP server threads and the Engine DB pool
  from them; written `Max_parallel_tasks` and `Local_*_slots` values become ceilings. `laptop`
  keeps the server on loopback; `server-workers` needs a PostgreSQL Engine DB and refuses to
  execute until workers exist. `doctor` and `setup` report every measurement, the recommended
  or selected profile, each derived size with its binding constraint, fallbacks, and the
  PostgreSQL connection budget. etl-craft never writes `craft-connector.yml`.
- Exact-run recovery and restart preparation through `run --recover` / `--restart`, with reasons,
  idempotency keys and durable `execution_run_id` inspection. Recovery preserves SUCCESS/SKIPPED
  and committed checkpoints; restart creates a new context retaining current offsets/watermarks
  and source history. Neither releases human locks or guesses process/query completion.
- `run --allow-paused` grants one exact run permission while its definition remains paused;
  `status --capabilities` shares scoped action availability and remedies with the HTTP API.
  `run --controls-file` and `POST /api/v1/controls/bulk` apply an explicit bounded selection with
  independent durable outcomes. Migration `0039` extends control actions in both Engine DBs.

- Scoped run/task controls: `hold`/`release` block and unblock dispatch, `stop` drains running
  work and `abort` stops its task processes, each on the selected run or one `--task_code`. Controls
  return durable `etl-craft/control/1` requests, optional `--request-key` idempotency, and
  `status --request-id` inspection locally or through `--server`. Shared HTTP routes expose
  the same operator controls and viewer inspection. Migration `0038` adds control requests
  and append-only process/query acknowledgements on SQLite and PostgreSQL.
- Stop and abort report acceptance separately from physical completion, preserving successful
  evidence. The run becomes `CANCELLED` only after affected process and query endings are observed.
  Server-side warehouse aborts without a cancellation acknowledgement adapter
  refuse explicitly; graceful stop remains available for supported handlers. Server-side Python
  scripts need a query adapter for scoped stop; definition pause remains available. Lost leases never imply query termination.

- `run --task_code TASK --standalone` starts an independent manual selection;
  `--with-downstream` includes its continuation from metadata edges. Explicit run identities
  resume the same selection; manual results never satisfy or consume pipeline dependencies.
  Email-only selections may overlap whole-pipeline execution. SQL selections retain exclusive
  admission and target locks; shared offsets, watermarks, business-rule flags and event
  checkpoints are refused with a remedy. The task-run HTTP route uses the same operation.
  Migration `0037` adds `AUD_PIPELINES_RUN_LOG.TASK_SELECTION` and `EXCLUSIVE_RUN` on both
  Engine DBs. Run JSON adds `selected_task_ids`, `ignore_dependencies` and `exclusive_run`.

- Shared SCD value edit, provenance and unlock services and HTTP routes under
  `/api/v1/pipelines/{code}/tasks/{task}/values`. Human admin inserts protect their row;
  edits protect changed values. ETL and authenticated service integrations respect those
  locks, with effective hashes/counts and independent column attribution. Only human admins
  may unlock. Native PostgreSQL and DuckDB commit data and warehouse-side evidence together;
  other warehouses refuse protected edits. Unobserved writes and replacement paths fail
  explicitly. No Engine DB migration or new dependency is required.

- A local installation operates a server: `--server https://<host>` (or `ETL_CRAFT_SERVER`)
  sends `status`, `explain`, `history`, `logs`, `whoami`, `run` (a run, `--backfill` or
  `--rerun`), `pause`, `resume`, `mark` and `cancel` to the server's API, within the account's
  role, with the same text and JSON output and exit codes as on the server. Plain HTTP is
  refused except to `localhost`. Commands that change metadata or the Engine DB are refused
  against a server, naming the server-side way. CI keeps using `ETL_CRAFT_TOKEN`.
- `etl-craft login --server` signs in with a password or, for SSO, the device flow the server
  runs. It stores a one-hour access token and a refresh token, `Auth.Refresh_days` (default
  30), and renews access automatically; `logout --server` revokes the refresh token and its
  access tokens. Engine DB and warehouse credentials never reach the local installation.
- `etl-craft logs --attempt-id N [--offset BYTES]` prints an attempt's log, locally or from a
  server.
- HTTP sign-in routes under `/api/v1/login/` (methods, password, SSO start and poll, refresh,
  revoke), `GET /api/v1/whoami`, and pipeline-code routes for status, explain, cancel, mark and
  rerun that select a run with `run_id`, `run_key` or the active run, as the CLI does.
- Engine DB migration `0036` adds `CFG_API_TOKENS.TOKEN_KIND` (`SERVICE`, `ACCESS`, `REFRESH`)
  and `REFRESH_TOKEN_ID`; existing user sessions become access tokens. Token JSON includes
  `token_kind`.
- Exit status 27 (`SERVER`, `ServerError`): a server could not be reached or answered with
  something other than etl-craft JSON.

- One service-operation role policy shared by local CLI, HTTP/UI routes and Python callers.
  Reads require viewer, run controls operator, metadata changes/project migrations developer,
  and administration admin. Administrative maintenance includes clone, rehash, server startup
  and documentation publishing/version writes; documentation generation and lineage refresh
  require developer. Read-only checks and exports remain viewer operations.
- Permission refusals name the account and required role, return CLI exit 26
  (`PermissionDeniedError`) or HTTP 403, and append `REFUSED` to `AUD_ACTIONS` before the
  action can run. Python operation callers must supply the authenticated account's role in
  `OperationContext`; missing roles are refused. Packaged Engine DB migrations require admin
  even when a developer invokes project migrations.

- Local account login with passwords or registered OIDC device-flow subjects, private expiring
  sessions, `logout`, `whoami`, admin-only account commands and interactive first-admin/recovery
  setup. `Auth.Session_hours` defaults to 12; OIDC uses `Auth.Oidc_issuer` and `Auth.Oidc_client_id`.
- Remote Airflow DAG credentials come from the connection named by `Auth.Airflow_connection_id`;
  remote `run --reason` records requester context without creating an intervention.

- Deployment accounts in `CFG_ACCOUNTS`; API tokens inherit their account's current role and
  stop authenticating immediately when it is inactive. Roles now include `developer`, between
  `operator` and `admin`. Account history and warehouse mirrors exclude password hashes.
- Engine DB migration `0034` preserves existing token values and creates one service account
  per legacy token. Invalid actor names or duplicate token names, including revoked credentials,
  stop the upgrade before any pending migration writes; resolve them through a project migration on the old
  version first. `token create --name` creates or reuses that account and refuses a role that
  disagrees with it. Account administration and login use the commands described above.

- A task process starts only when its log folder under `Orchestration.Log_dir` can be created
  and written, with at least 64 MiB free. Otherwise the attempt is recorded `FAILED` with a
  `ConfigurationError` naming the folder, what was found and the remedy, before the task does
  any work.
- `etl-craft server` binds `Orchestration.Api_address` before it takes leadership or
  supervises anything. An address already in use refuses the server with exit code 10, naming
  the address and the remedy.

- A PostgreSQL Engine DB restart or managed failover no longer stops etl-craft. A process that
  has reached the Engine DB waits up to `Orchestration.Engine_db_outage_seconds` (default 120;
  0 fails at once) for it to answer again:
  - refused connections are retried;
  - a run's scheduler step, a lease heartbeat and the server loop repeat a dropped statement;
  - the server takes its leadership lock again before supervising, and stops if another server
    took it.

  A database never reached still fails at once. Past the budget, the error names the outage's
  length and the setting.

- Catalog generation bulk-loads task parameters, lineage caches and shown-run interventions,
  bounds history/consumption to the displayed runs, and separates scheduled, backfill, repair
  and other logical-run KPIs. Migration `0032` adds indexed recent-history reads on both Engine
  DBs without changing config files. Unchanged asset/run pages reuse verified HTML using the
  generated `.etl-craft-manifest.json`; aggregate pages remain fresh. Reused pages retain their
  generation timestamp. Missing manifests trigger a full build; invalid manifests or altered
  cached HTML fail with a remedy to remove only the manifest and regenerate.

- Catalog publication supports organization-managed URLs with optional path prefixes using
  `Docs_site.External_url` and explicit `Trusted_proxies`, without ngrok. Forwarded visitor
  addresses are accepted only from trusted peers; public URL changes still require explicit
  acceptance. Proxy and systemd examples cover persistent hosting and external authentication.
  Generation and serving coordinate through stable sibling `.build.lock`/`.publish.lock` files
  so requests cannot observe the directory exchange. Regenerate old sites before serving them
  from read-only storage, and grant the server read access to the publication sidecar. Existing
  local/ngrok selection is unchanged when `External_url` is absent. No Engine DB migration is needed.

- Generated project catalogs include a schema/lineage gaps page, coloured warning rows,
  action-specific engine-column contracts, graph reasons and affected-task links. Storage type
  normalization avoids false Iceberg engine-column warnings; failed SQL inference leaves unknown
  output coverage unavailable. Action parsing uses execution's normalization rules. Offline,
  unavailable and stale comparisons remain explicit; inferred names do not invent type contracts.
  Missing tables/views distinguish pending first deployment from missing referenced objects.
  `gaps.json` and authenticated `GET /api/v1/catalog/gaps?compare=true` share a dated input-versioned
  report. API comparisons read metadata only and never publish lineage history. No config,
  migration, dependency or warehouse business-data change is required.

- Table lineage history records a new version only when combined writer edges change.
  Catalog table pages show current and historical versions with added/removed column sources.
  `lineage --as-of TIME` reads stored graphs; `lineage --table schema.table --version NUMBER`
  selects a table's version and the graph at its opening time. Historical reads never parse
  current SQL. Migration `0031` is plain SQL in both Engine DBs: it resets the derived task
  cache to separate endpoint components and starts history at the first post-upgrade computation.
  Failed writers close their table's current history without opening a replacement; CLI/catalog
  output names the task and reason. Publication accepts only this collection's confirmed cache
  keys. Lineage recording waits at most 30 seconds for its named Engine DB lock and reports the
  recorder/remedy on timeout. The Python operation takes a frozen `LineageQuery`, validated once.
  No older lineage can be recovered from the discarded cache. No config files or parameters change.


- Task targets now reference the existing scoped `CFG_TABLES` contract through
  `CFG_TASKS.TARGET_TABLE_ID`; config files use separate target components in `tasks.csv`.
  `TARGET_OBJECT` is derived for handlers and scripts and is refused as a writable parameter.
  Migration `0030` preserves task IDs/stamps, disabled target intent, parameter history and
  historical exports. Writer conflicts compare table references across pipelines. Export after
  upgrading and apply target edits to the task file; keep applied project SQL unchanged.

- Source definitions now use scoped `CFG_TABLES` and `CFG_COLUMNS` contracts, separate
  catalog/schema/table components, stable column identities and parent foreign keys. Config
  export/apply uses `tables.csv` and `columns.csv`; the old source file is explicitly refused.
  Migration `0028` preserves IDs, audit stamps, legacy history and historical exports, and
  refuses ambiguous rows or custom source schema objects before changing any pending schema.
  Existing deployed structure remains queried from the warehouse rather than copied wholesale.
  Optional declared object kind and nullability are seeded and checked against deployed
  structure, with differences included in generated documentation.

- Pipeline, task, parameter, dependency and rule identities now remain unique when retired.
  Engine migration `0027_config_identities.sql` refuses ambiguous legacy keys before changing
  constraints and preserves existing row IDs, timestamps and typed history. Reactivate an
  existing row to use its identity again; the configuration examples demonstrate this, with
  the original retire-and-reuse SQL retained unchanged for already-applied projects.

- Deployment-wide `validate` and config plan/apply validation now warn about legacy metadata
  that cannot convert unambiguously to normalized contracts: duplicate active/inactive keys,
  resolved source-column/ordinal collisions and invalid task/rule object references. Findings
  name row IDs, active flags and the repair needed. The check is read-only and respects
  warehouse identifier case rules and quoted dots; pipeline-scoped validation stays scoped.

- Every core and project `CFG_` table now requires one primary key, mandatory creation/update
  actors and UTC timestamps, and an active flag accepting only `Y`/`N`. Engine migrations
  `0025`/`0026` preserve IDs, creation records, credential hashes and existing history while
  filling missing audit fields. Token storage uses `CREATE_DATE`, records safe typed lifecycle
  history without credential hashes, and refuses inactive tokens immediately. Public token JSON
  keeps `created_at`. Existing custom config tables must meet the base contract before upgrading.

- SCD1 targets now carry `ACTIVE_FLAG` alongside creation/update actors and dates and
  `DELETE_FLAG`. `upgrade-targets` derives legacy SCD1 active state from known deletion flags
  under the target lock, with a dry run and repeatable backfill that preserves historical stamps.
  It also deactivates legacy active/deleted SCD2 versions without rewriting their stamps.
  Soft deletes deactivate current live rows and leave closed SCD2 history unchanged. Revival
  restores SCD1 in place or inserts a new SCD2 version. Mixed SCD1/SCD2 target contracts are refused.

- Source definitions in `CFG_SOURCE_COLUMNS` / `source_columns.csv` let lineage expand stars
  and resolve bare join columns without a warehouse connection. Upstream SQL targets supply
  their traced output schemas, and schema edits invalidate cached lineage. Config export seeds
  existing external and script-landed tables; historical exports remain offline. Validation
  reports source schema drift. Catalogs built with `--with-warehouse` preserve declared types,
  show deployed types and report column gaps or unavailable comparisons. Migration `0024_source_columns.sql` adds the actor-audited table.

- Local `EVENT_WAIT` tasks wait durably for settled, unconsumed files or a read-only warehouse
  query returning a row, without worker slots. Timeouts fail or skip, and evidence and file
  consumption commit with success. Downstream SQL reads `$$event_files`; Python scripts receive
  `task.event_files` and can use the token in `INPUT_PARAMS`. Single-task runs return `WAITING`;
  remote mode refuses these local scheduling waits. Indefinite deadlines and native file
  notifications support long waits with configurable fallback scans and SQL probe intervals.
  Engine DB migration `0023_event_waits.sql` adds immutable evidence/consumption and separates
  event and dependency waits.

- SQL tasks can read source changes with `WATERMARK_COLUMN`, `WATERMARK_TYPE`,
  `WATERMARK_START` and `$$watermark`. The staged high-water mark commits with a successful
  attempt; failures, empty batches and backfills keep the stored value. Each run and attempt
  records `WATERMARK_FROM` and `WATERMARK_TO`, shown by explain and the catalog.
  `mark --watermark VALUE --reason ...` resets a SQL task's position under an exact run identity,
  with actor and before/after values recorded; the HTTP mark endpoint accepts the same reset.
  Upgrade the Engine DB with migration `0022_watermarks`.

- `SQL_ACTION=CREATE_VIEW` creates or replaces the target as a view over the task's SELECT, and
  refuses when a table has that name. Replacing a view keeps the grants on it. `SECURE_VIEW =
  true` makes it a secure view: `SECURE` on Snowflake, `security_barrier` on PostgreSQL; other
  warehouses fail the task. A view takes no `TABLE_FORMAT` or storage parameter, and DuckDB over
  an Iceberg catalog, which cannot create views there, fails the task with the remedy.
  `validate` reports both refusals before a run. A view is replaced only when the statement that
  makes it changes, so an unchanged view keeps its locks, grants and policies. Upgrade the Engine
  DB with migration `0019_target_view_statement`.
- `REJECT_LIMIT` on `APPEND_TABLE`, `OVERWRITE_TABLE` and the merges sets aside the rows the
  target cannot take rather than failing the task: a NULL merge key, a NULL in a `NOT NULL`
  column, a value that does not cast to its column's declared type or is longer than its
  declared length, or a merge key returned more than once without `MERGE_DEDUPE_ORDER`. They go
  to `<target>__rejects` with their values as text, the run's audit columns, `REJECT_REASON` and
  `REJECT_COLUMN`, and the other rows load cast to the target's types. The limit is a row count
  or a percentage of the SELECT's rows; over it, the target is not written and the task fails
  naming the most common reasons, with the rows still set aside unless
  `SKIP_REJECTS_OVER_LIMIT = true`. `REJECT_COUNT` is recorded with each task run and attempt
  and shown by `status`, `explain`, `history` and the catalog. On PostgreSQL it needs version
  16 or later. Upgrade the Engine DB with migration `0020_reject_count`.
- Configuration history: each `CFG_` table has a history table, `AUD_<table>_HISTORY`, holding
  every version of its rows with every column typed as in the table, `VALID_FROM` and `VALID_TO`
  (9999-12-31 while current), `CURRENT_FLAG`, and who changed it under which migration or
  `config@<revision>`. A version never changes except to be closed by the next one.
  `etl-craft config export --as-of <pipeline_run_id or ISO time>` writes the config files as the
  rows stood then. Upgrade the Engine DB with migration `0021_config_history`, which moves the
  existing change records into the history tables.
- Configuration files: a project keeps its pipelines as CSV files in `config/`, one per `CFG_`
  table, each row with an `ACTIVE_FLAG`. `etl-craft config apply` merges them into the Engine DB
  by key in one transaction: it inserts new rows, updates changed rows in place (keeping their
  ids and history), retires rows flagged `N` or no longer in the files, and reactivates retired
  rows flagged `Y` again. `validate` checks the result in the same transaction, and a
  configuration it fails is not applied. `config plan` shows the changes and the findings
  without making them, and `config export` writes the Engine DB's rows as files. Every change
  is audited with its actor and `config@<revision>`. A file problem exits with the new status 25
  (`METADATA_FILE`), naming the file, line, column, value and what was expected.

- Guides to configuration files and to configuration migrations, with a complete example
  `config/` folder and an example migration for each kind of change, both tested on SQLite and
  PostgreSQL.

- Task processes run through a pool: whole-pipeline runs and the server admit each attempt and
  submit it to the local pool, which runs it on this host; `run --task_code` runs its one attempt
  in the foreground the same way. New
  `Orchestration.Local_ingestion_slots` and `Local_warehouse_slots` cap how many Python ingestion
  tasks and how many SQL, business-rule and alert tasks run at once, within
  `Max_parallel_tasks`; unset, either kind may use every slot, as before.

### Changed

- `etl-craft cancel` is replaced by `etl-craft abort` (stop task processes now) and
  `etl-craft stop` (let running attempts finish); both end the run `CANCELLED`. `runs.cancel_run`
  and the `POST /api/v1/pipelines/{code}/cancel` and `/runs/{id}/cancel` routes are removed: use
  `POST /api/v1/pipelines/{code}/controls` with `action` `ABORT` or `STOP`. `pause` and `resume`
  act only on the pipeline definition; scoped holds are `hold` and `release`. Audit records name
  the operation `stop`, `abort`, `hold` or `release` instead of `cancel`.
- `GET /api/v1/pipelines/{code}/runs` returns the `etl-craft/history/1` document that
  `etl-craft history` prints (`entries`, `changes`, and the next page's `before`) instead of
  `etl-craft/runs/1`. Run and task history list the newest run id first.
- Every action has one implementation in `services.operations`, which the CLI, the HTTP API
  and later the UI call. Python callers use `operations.maintenance` (`initialize`, `migrate`,
  `clone_tables`, `rehash_target`, `upgrade_audit_columns`, `setup_engine`, `grants`) and
  `operations.documents` (`generate_docs`, `documentation_versions`, `dag_yaml`) instead of
  the service modules. `sessions.sign_in` returns a `Session` (`access_token`, and a refresh
  token for server sign-in); `services.oidc.device_subject` is replaced by `start_device` and
  `poll_device`, which `sessions.start_sso` and `sessions.poll_sso` call; `api_reads.get_runs`
  is removed. Sign-out records the token's own account whatever starts it.

- Warehouse `CREATED_BY` and `UPDATED_BY` columns written by ETL now hold the run's writer,
  `etl-craft`, instead of the warehouse login. Engine DB migration `0035` adds the writer to
  every run and accepts the account kinds `USER`, `SERVICE` and `SYSTEM`; historical names and
  kinds are kept unchanged.
- Nothing is recorded under an actor nobody set: `current_actor()` raises `ConfigurationError`
  outside `acting_as`. Finalization, reconciliation, gate skips, retries and local task processes
  act as the system account `etl-craft`. `etl-craft server` needs a SERVICE account's token, and
  its schedules act as that account. Token metadata includes the account's `account_kind`.
- `ActorKind` holds only `USER`, `SERVICE` and `SYSTEM`. Python callers replace `HUMAN` with
  `USER`, and `SCHEDULE`, `ORCHESTRATOR` and `WORKER` with `SERVICE`.

- Commands require a proven account through `ETL_CRAFT_TOKEN` or a stored session.
  `ETL_CRAFT_ACTOR` and `ETL_CRAFT_ACTOR_KIND` no longer select an identity. Run `setup` locally
  to provision an admin, then `login`; supply service tokens explicitly in automation.
  Python callers replace `core.actor.resolve_actor` with an explicitly scoped `Actor`.
- `doctor` fails without an active admin. Disabling accounts or resetting passwords revokes
  existing credentials; token creation is reserved for SERVICE accounts.

- An SLA breach whose Engine DB write fails is written again every 15 seconds until it lands or
  the run ends. The breach is then recorded and its lapse alert sent once, instead of both being
  lost.
- A warehouse whose dialect cannot be resolved raises `ConfigurationError` when the engine checks
  whether it admits a single writer. It is no longer treated as admitting many writers, which
  could skip the DuckDB writer lock.

- `AUD_TARGET_HASH_VERSION`, `AUD_TARGET_VIEW_STATEMENT` and `AUD_BUSINESS_RULES_RESULTS` store
  the target as `TARGET_CATALOG_NAME`, `TARGET_SCHEMA_NAME` and `TARGET_TABLE_NAME` instead of
  one combined `TARGET_OBJECT` or `TARGET_TABLE` name. Migration `0033` converts recorded rows
  from their own spelling, keeping values and times. A rule result recorded as `schema.table`
  has no catalog. A recorded name that cannot be split stops the upgrade before any change and
  names the row. Target locks use the resolved `catalog.schema.table`, so one target named with
  or without its catalog takes one lock.

- Developer CI partitions complete test collections across jobs and isolated worker processes,
  shares the built wheel with the installed demos, and verifies test identities and matrix results
  before accepting combined Python/platform coverage at the unchanged 90% threshold. Release evidence
  retains its strict no-skip policy; manual cold/warm timing validation is separate from PR runs.

- SQL tasks and `validate` refuse repeated column names in `MERGE_KEY` and
  `MERGE_COMPARE_COLUMNS`, compared case-insensitively. Errors identify the task, parameter,
  repeated column and correction. Lists remain ordered task parameters; valid contracts and
  stored hashes are unchanged. After removing a repeated compare column from an existing
  target's contract, rehash it before resuming merges to avoid false changes.

- Business rules reference scoped table contracts through `TARGET_TABLE_ID`. Config files
  hold separate catalog/schema/table fields; migration 0029 preserves rule IDs, stamps,
  counters and historical target names. Export and adapt legacy rule CSVs after upgrading.

- `etl-craft rehash` records its request in `AUD_ACTIONS`, as every other command that changes
  something does; `--dry-run` writes nothing and records nothing.

- `AUD_METADATA_CHANGES` is a view over the configuration history tables rather than a table: the
  same columns, one row per change with its values before and after as JSON, as `etl-craft audit`
  shows them. On SQLite it now records a change to a pipeline's schedule columns, which a new
  SQLite Engine DB did not.

- Run-condition and self-dependency errors name the task by its code, such as `task publish
  requires 5 satisfied dependencies but only has 2`, rather than by its id.

- The read-only check of a SQL task's SELECT, from a file or inline, and of business-rule SQL
  reads the statement with sqlglot in the warehouse's dialect: it refuses anything that is not a
  query or that writes anywhere in its tree, such as a `DELETE` inside a `WITH`, and accepts
  columns named like statements, such as `copy`. SQL sqlglot cannot parse is checked word by
  word as before, and `validate` warns about it with the line and column where parsing stopped.

## [0.3.0] - 2026-10-10

### Fixed

- `etl-craft run` acts on Ctrl-C, `SIGTERM` and `SIGHUP` at its next safe point between Engine DB
  transactions, or at once while it waits. A signal that landed inside a transaction's cleanup
  could return a SQLite connection to the pool with its write lock held, so stopping the run's
  tasks waited out the 60-second busy timeout. A second signal while the first still waits for a
  safe point stops the command at once.

- `lineage`, `generate-docs` and other column-lineage readers work with sqlglot 30.22, whose
  `lineage()` requires a schema argument; etl-craft passes an empty one, as before. The lock
  file moves to sqlglot 30.22.0.

- Snowflake renames use the fully qualified destination, keeping Iceberg replacements and their
  recovery copies in the target schema even when the connection uses a different default schema.

- Databricks identity replacements accept string column collations inherited from the preserved
  table default, including current `SHOW CREATE TABLE` output. Explicit overrides, changed
  candidate collations and other business column constraints remain refused before publication.

- `Docs_site.Schedule` uses the same cron validation as pipeline schedules, rejecting invalid
  ranges and impossible calendar dates; both accept `@yearly` and `@annually`.

- CLI initialization accepts its exact queued run when the server admitted it concurrently,
  without rewriting gate decisions or taking over a different run.

### Changed

- `ConnectorConfig.engine`, `.warehouse` and `.email` hold the selected profile itself; the
  `ConnectionSection` and `EmailConfig` wrappers and their `.active` and `.active_profile` are
  removed. Python callers read `config.engine.schema` and `config.engine.name` directly.

- `Orchestration.Email.use_tls` is removed; set `tls_mode` (`starttls`, the default, `ssl` or
  `none`). A file that still sets `use_tls` is refused as an unknown key.

- A warehouse URL without its own dialect (such as `jdbc:mysql:` or `jdbc:oracle:`) is refused,
  naming the supported warehouses, rather than treated as a plain ANSI warehouse.

- etl-craft no longer depends on `packaging`. `generate-yml --airflow-version` accepts one
  numeric release (`2.2.0`) or comma-separated constraints of numeric releases (`>=2.2,<3`,
  `~=2.3`, `==2.10.*`); pre-release, post-release and epoch versions are refused. The
  process-control and file-lock code is POSIX only, matching the supported Linux and macOS.

- `APPEND_TABLE` refuses a target without `TASK_RUN_ID` before staging anything, naming
  `etl-craft upgrade-targets --action APPEND_TABLE --target <table>`, rather than appending with a
  warning that a retry can duplicate rows. Rows appended before the upgrade keep a NULL
  `TASK_RUN_ID` and are never replaced.

- An earlier `craft-connector.yml` layout is refused by the existing unknown-section and
  unknown-profile checks, which name the expected sections, rather than by a dedicated message.

- Run commands exit 22 when work remains unfinished and 23 when a single task recorded nothing
  because its dependencies are not met. A paused pipeline that starts nothing and an orchestrator
  `--init-only` step retain exit 0; a stopped backfill exits 22 unless it failed or was cancelled.

- Task completion passes one frozen count record through the handler and guarded transitions.
  Run selection and pipeline graph loading share their existing rules; dependency readers always
  return the stored repair policy. Direct query-to-record mappings preserve Boolean JSON values.
  Authentication fields are checked once by configuration parsing before connections are built.
  Python lifecycle callers use `resolve_run` and `counts=Counts(...)` for task completion.

- Shared UTC conversion, enum parsing, credential masking and warehouse column discovery replace
  duplicated helpers. Email TLS modes accept case-insensitive values. Warehouse session setup
  closes cursors on failure and retains PostgreSQL's commit and Iceberg extension setup.
  Engine dialects own metadata trigger refresh; migration policy markers replace filename checks
  without changing existing SQL bytes or checksums.

- Service operations take pipeline codes directly and build inspection documents from stored rows
  without intermediate summary models or duplicate reads. Run and mark calls share their audit
  dispatch; pause and resume share one service and CLI implementation. JSON schemas and CLI
  text remain unchanged. Python callers use `set_pause(..., verb="pause")` or `verb="resume"`.

- Removed unused internal helpers and lifecycle shortcuts. Tests now use the production
  attempt queue/claim/finish path and atomic dependency consumption; test scene setup stays
  in fixtures. No configuration, schema or command behavior changes.

- A Nightly workflow tests the newest release of every dependency, so an upstream release that
  breaks etl-craft fails there rather than on an unrelated pull request.

- CI validates pull requests without repeating after merge. Twenty consecutive chaos passes per
  Engine DB run in release validation after the evidence check, rather than on every PR.

- Execution identity names have one meaning across SQL, ingestion, business rules and email:
  `pipeline_id` is the definition, `pipeline_run_id` the pipeline execution, and `task_run_id` the
  task execution. SQL uses matching substitution switches. Migrate SQL and email templates that
  used `$$pipeline_id` for a run to `$$pipeline_run_id`; rename `PIPELINE_ID_SUBSTITUTION` to
  `PIPELINE_RUN_ID_SUBSTITUTION` for those SQL tasks. Rename `$$pipeline_id_filter` and
  `PIPELINE_ID_FILTER` to `$$pipeline_run_id_filter` and `PIPELINE_RUN_ID_FILTER`.

### Added

- Generated DAG YAML carries version 1 and validates against its packaged JSON Schema. A separate
  `etl-craft-airflow` distribution loads pipeline, global and docs exports as real Airflow DAGs
  on Airflow 2.11.x and 3.3.x, preserving their commands, actor identity, graph and trigger rules.

- Optional authenticated HTTP API and OpenAPI contract, served with the local overseer;
  print-once bearer tokens with viewer, operator and admin roles and immediate revocation.

- Read-only `status` and `explain` commands with exact run selection, task summaries, dependency
  reasons, persisted waits and retry schedules. JSON output now covers inspection, `validate`,
  `doctor` and `lineage` through the shared service serializer.

- Local automatic task retries with persisted due times and budgets, capped exponential backoff,
  retryability recorded by the child, and fenced claims. Delayed attempts consume no worker slots;
  cancelled attempts and SQL guards never retry. Remote retries remain owned by the orchestrator.
  Upgrade the Engine DB with migration `0017_retries`.

- Ready-task scheduling shared by the local CLI and server: each completion releases ready
  downstream work immediately. Gate waits persist deadlines and look counts in `AUD_GATE_WAITS`
  without holding task workers, and survive restarts. Upgrade with migration `0016_gate_waits`.
  The server uses one bounded task pool and cooperative pipeline supervision. Gate averages
  exclude stand-in runs as well as backfills.

- Local server schedules support five-field cron, timezone-aware ticks, bounded catch-up,
  overlap policies and durable queued runs with exact UTC tick keys. Upgrade the Engine DB
  for migration `0015_schedules`; validate schedule metadata before starting the server.
  Manual run dates default to `Orchestration.Timezone` (UTC unless configured), while audit
  timestamps stay UTC. Generated DAGs preserve schedule timezone and logical-date semantics.

- `etl-craft server` supervises active local runs under one deployment leader, with PostgreSQL
  session locking and committed execution notifications, SQLite file locking, bounded active
  graph caching, exact-run crash recovery and configurable graceful shutdown. Upgrade the
  Engine DB for the new `AUD_OVERSEERS` process history.

- Actor-scoped Python operations for run lifecycle, tasks, pipeline controls, backfills and
  inspection, with frozen views and canonical execution identities. The corresponding CLI
  commands use `--format json` to return the same schema-versioned documents. Requests remain
  append-only and CLI delegation records each request once; related views share a stable
  database snapshot.

- A chaos suite for concurrent admission, process loss, fenced recovery, operator races and
  remote deliveries, including a five-second PostgreSQL outage. Release validation requires twenty
  consecutive passes per Engine DB and uploads each iteration's results.

- Consistent `PIPELINE_ID`, `PIPELINE_RUN_ID` and `TASK_RUN_ID` audit columns on every SQL target,
  with those identities also exposed to ingestion scripts and business-rule SQL. Append retries
  replace only their task run's batch under the target lock; legacy append targets warn on every
  write. `etl-craft upgrade-targets [--action APPEND_TABLE] [--target S.T] [--dry-run]` adds missing
  nullable identity columns to configured SQL and ingestion targets without changing history.

- SQL validation refuses active writers that resolve different table formats for the same
  qualified target, including writers in other pipelines. Creation, setup, overwrite and merges
  check the existing Databricks Delta/UniForm or Snowflake native/Iceberg format before staging
  or changing the target; format changes require an explicit migration or another target.

- Native cloud SQL targets use generated `ROW_ID` keys: Databricks Delta and UniForm identity
  columns and ordered Snowflake autoincrement. Atomic clone publication keeps identity-backed
  `CREATE_TABLE` replacement safe; overwrites and schema evolution retain the generator.
  Older computed-key targets remain writable. Concurrent Trino append processes share the
  qualified target lock through warehouse commit, with SQLite and PostgreSQL Engine DB coverage.

- SQL schema evolution adds nullable columns through `ALTER TABLE`, retaining existing rows,
  keys, comments, dependencies, storage locations and supported table properties. Complete
  warehouse types preserve precision, scale and supported lengths and nested types. Opted-in
  evolution refuses existing-column type changes before DDL; DuckDB Iceberg nested additions
  are refused with a catalog-engine remedy. Partial non-transactional additions can be retried.

- Safe SQL table replacement: native PostgreSQL and DuckDB roll back failed replacements;
  Snowflake, Databricks and Trino use atomic publication where supported. DuckDB and Snowflake
  Iceberg retain recovery tables and restore failed writes, leaving backups available if
  restoration also fails. Table comments and supported properties survive; unsupported layouts
  are refused before publication.

- Joined SCD1 updates and SCD2 version closing use warehouse-specific set-based statements.
  PostgreSQL indexes and analyzes merge stages; a 100,000-row SCD1 regression checks the
  30-second performance budget. Composite keys, NULL preservation and unchanged-row audit
  fields retain their behavior across native and Iceberg warehouses.

- Canonical version-2 merge hashes distinguish NULL, empty text and embedded separators, and
  normalize UTC timestamps, decimal scales and booleans across warehouses. Floating-point
  compare columns are refused. `etl-craft rehash --target S.T [--dry-run]` upgrades all stored
  hashes, including SCD2 history, and migration `0011_target_hash_version.sql` tracks the contract
  published after warehouse commits. Target locks coordinate SQL mutations and hash upgrades.

- Atomic Engine DB endings: successful attempts commit their outcome, summary, returned Python
  offset and recorded task-dependency consumption together. Pipeline endings commit status, SLA
  and recorded pipeline-dependency consumption together, before finalization hooks run.

- Recorded gate decisions at pipeline and task-attempt admission. Successful downstream work
  consumes the exact selected upstream identities and revisions, including after upstream repairs
  or metadata changes. Dependencies accept newer published revisions of the same upstream run
  when `CONSUME_REPAIRS = 'Y'`; `N` accepts only newer run identities. Migration
  `0010_gate_repairs.sql` tracks pending repairs so failed repairs do not publish a revision.

- Run and attempt ownership with 60-second leases and 15-second heartbeats. Every run reconciles
  expired attempts; `etl-craft reconcile` also supports pipeline and task filters. Lost attempts
  preserve uncertain outcomes, verified local child processes are stopped, and expired idle
  runs resume under their original identity. `mark --stale` reconciles before marking and
  refuses live leases. Children receive explicit attempt and owner identities.

- Explicit `--run-id` and `--run-key` selection for run, mark, cancel, history and steps.
  Completed runs require an explicit identity; `history --all` lists runs. Pipeline tasks
  receive their parent's selected run. Remote DAG steps share an orchestrator run key and
  logical date, so clearing an older DAG run reopens that run. Remote generation requires
  an Airflow version floor of 2.2.0 (`generate-yml --airflow-version`).

- Centralized guarded run and attempt transitions, with `StaleTransitionError` (exit 20).
  Execution records immutable attempt outcomes and updates their task summaries atomically;
  concurrent admission, wrong owners and superseded results are refused. Reopening records
  its intervention in the same transaction. Migration `0009_preserve_request_actors.sql`
  keeps unknown historical requesters unknown when their attempts change.

- Actor identities on run starts and endings, attempts, interventions and pauses; immutable
  command requests and metadata before/after history, with `etl-craft audit` filters. Migration
  `0008_actors_and_audit_guards.sql` protects audit and metadata writes on SQLite and PostgreSQL.
  Generated remote DAGs pass their actor, `setup --print-grants` prints deployment role SQL,
  and `doctor` checks extra write grants and SQLite file permissions.

- Run identities and trigger kinds, owner and lease fields, output revisions and configuration
  fingerprints; attempt and gate-decision tables, consumed revisions and repair-consumption flags.
  Migration `0007_identity.sql` preserves historical run identities and copies the latest known
  execution attempts. New runs receive manual, backfill or stand-in keys. Admission records gate decisions and consumes their selected revisions.

### Fixed

- The catalog's search builds links only from the site's own relative pages: a result whose path
  is not one falls back to the home page, and the page's way back to the site root must be made
  of `../` steps.

### Verified

- All 15 required release suites passed from clean commit `9517c30`, with 3,269 passing
  test outcomes and no skips. The built wheel was exercised by pip, uv, package checks and
  the complete Databricks and Snowflake suites, including both table formats. The evidence
  and artifact checksums are in `release/evidence/0.3.0/`.

## [0.2.0] - 2026-10-04

### Added

- Named development fault points, a synchronized admission-race helper and a reusable real-CLI
  process harness. Stabilization defects have an explicit regression mapping checked by CI.

- Secrets files accept UTF-8 byte-order marks and `export KEY=VALUE`; missing secret variables
  include suggestions for similarly named keys.

- SMTP `tls_mode`: verified STARTTLS (the default), implicit TLS (`ssl`), or unauthenticated
  plain SMTP (`none`), with an optional project-relative `ca_file`. `use_tls` stays supported.
- Run history and KPIs in the catalog: each pipeline's and task's latest 30 runs, with success
  rates, failures, SLA misses, average and longest durations, average rows written, and a bar
  per run coloured by status; a page per run with its tasks, interventions, and the upstream
  runs it was built from and the downstream runs that used it, from the consumption log. The
  DAGs tab shows each pipeline's success rate and average run.

- `AUD_DEPENDENCY_CONSUMPTION`, an append-only log of every upstream run consumed through a
  cross-pipeline dependency: one row per downstream run (or task), dependency and upstream run.
  A dependency's latest row is what it last consumed, so gates behave as before, and the log
  says which upstream runs each run was built from. Migration `0003_dependency_consumption.sql`
  carries each tracker's last consumed run into it and drops `AUD_PIPELINE_DEPENDENCY_TRACKER`
  and `AUD_TASK_DEPENDENCY_TRACKER`.
- `Orchestration.Gate_wait_minutes`: how long a cross-pipeline gate waits for a running upstream
  (60 unless set; `0` judges at once).

- `etl-craft pause` and `resume`: while a pipeline is paused, `run` starts nothing of it and
  exits `0`, and a run in progress starts no more tasks, to go on once resumed. `list` and the
  catalog show paused pipelines; `AUD_PIPELINE_PAUSES` keeps every pause.
- `etl-craft run --skip --reason`: records a run `SKIPPED` on purpose, for the pipelines that
  depend on it to see.
- Run dates and backfills: every run runs as of a date (`AUD_PIPELINES_RUN_LOG.RUN_DATE`): the
  day it started, `run --run-date`, or `--init-only --run-date`, which generated DAGs pass the
  orchestrator's date with. SQL tasks read it as `$$run_date` (with `RUN_DATE_SUBSTITUTION`),
  scripts as `task.run_date`. `run --backfill FROM:TO --reason` runs the pipeline once per date,
  as backfill runs that check no cross-pipeline gate, consume nothing, and neither read nor store
  script offsets; `history` shows each run's date. Migration `0002_run_date.sql` adds the columns
  and dates the earlier runs by the day they started.
- The Engine DB's first migration, `0001_pipeline_pauses.sql`: `etl-craft setup` (or `migrate`)
  applies it to an Engine DB made by 0.1.0. A test upgrades each released schema and compares
  it with a new one.

### Fixed

- Released documentation versions use the maintained API page renderer against their own
  tagged source, so API landing and package pages receive documentation corrections. Every
  version and the `latest` alias pass strict builds and rendered API content checks.

- Fresh initialization creates the Engine DB schema and packaged migration ledger atomically
  under the migration lock. Migration `0006_run_backfill_constraint.sql` aligns the named
  backfill constraint with fresh databases on PostgreSQL and SQLite, preserving run history.
- PostgreSQL lock errors distinguish contention from connection failures; remote initialization
  conflicts name `--finalize-only`, and `--force` refuses cancelled runs with a new-run remedy.
- Cancellation watcher logs retain run/task context. Lineage caches include the active catalog
  and sqlglot version. Versioned docs subprocesses find tools beside their Python interpreter.
- SQL substitutions preserve tokens inside strings, quoted identifiers and comments; the
  shared statement scanner also handles nested comments and PostgreSQL escaped strings.

- Clearing business-rule flags sends at most 1,000 keys per Engine DB statement, avoiding
  parameter-limit failures for large sets on SQLite and PostgreSQL. All batches share the
  rule's transaction, so a failure cannot leave partially cleared flags or a false success.

- Release evidence must name the exact required marker and cover every test collected on HEAD,
  with one passing outcome per test and consistent counts. Subsets, extra tests and selection
  overrides cannot satisfy the gate; platform exceptions require explicit suite declarations.

- Pipeline and task codes use one database-enforced rule: an ASCII letter followed by letters,
  digits or underscores, at most 128 characters. Migration reports invalid legacy codes before
  applying anything; generated commands quote arguments and refuse invalid codes and control steps.
- Published catalog GET and HEAD requests reject encoded hidden paths, traversal and symlinks
  outside the site, including directory indexes.
- Python API landing and package pages link to their modules and the scripting contracts.
  Documentation builds check rendered API content as well as strict MkDocs validation.

- Duplicate configuration keys, empty secret variables, credential-bearing JDBC query keys,
  zero task concurrency and invalid TCP ports are refused with setting-specific errors.
- Key and certificate paths, secrets files and SQLite databases resolve beside the config,
  including symlinked configs. Doctor checks configured files for readability.
- DuckDB Iceberg storage secrets are retained as variable names and resolved for each new
  connection; HTTP and SDK INFO logs are suppressed in routine attempt logs.

- Email subjects collapse whitespace, drop control characters and cap their length at 200
  characters; the body keeps the full error. Header failures name the transport and step.
- SMTP verifies certificates and hostnames and refuses password or OAuth login without TLS.
- Partial recipient refusal succeeds with `EMAIL_WARNING` in the task audit and a WARNING in
  the attempt log; total refusal fails the task.
- Ingestion scripts load as registered modules with their own future imports, so dataclasses,
  enums, pickling, type-hint resolution and fork process pools work. Timestamp offsets refuse
  nanoseconds the store cannot keep, and invalid result variables fail before an offset is saved.
- A transaction on a SQLite Engine DB is now all or nothing. The driver no longer manages
  transactions itself; every transaction opens with `BEGIN IMMEDIATE`, so a savepoint that was a
  transaction's first write no longer commits early, DDL rolls back with its transaction, and a
  failure between starting a run and recording that its gate refused it can no longer leave the
  run `IN-PROGRESS` to be resumed without the gate.
- A run is never taken over, skipped or ended by a process that does not own it. Starting a run
  never hands back one another process started; a refused gate skips only the run it started; and
  a run with a task still `IN-PROGRESS` in another process stays `IN-PROGRESS`, naming the task.
  `mark --task_code X --status FAILED --stale --reason ...` releases a task whose process is gone.
- Backfills and scheduled runs are kept apart. A plain `run` does not resume a backfill's run and
  a backfill does not take over a scheduled run; a backfill that meets another run between dates
  stops with the command that takes up from there. Dependency gates never see backfill runs, and
  a backfill counts a `FAILURE` dependency on another pipeline as not met. Run-length averages use
  only `SUCCESS` and `FAILED` runs that are not backfills.
- `run --task_code --rerun` of a task whose failure ended the run leaves the tasks skipped because
  of it to run, instead of ending the run `SUCCESS` without them. Resetting skipped tasks (by
  `mark` or a rerun) keeps a `SKIPPED` row another pipeline's run consumed, naming that run.
- A task's settings are checked before its row is bound. When its process cannot start, or the
  command is interrupted, the attempt is recorded `FAILED` with the cause instead of being left
  `IN-PROGRESS`; recording an attempt's outcome retries a dropped Engine DB connection.
- A task that already ended `FAILED` is never rewritten `SKIPPED`, and a refused `--rerun` no
  longer reopens the run.
- A run skipped on purpose counts as ended: `run --task_code` refuses it like any ended run.
- A run's SLA is decided once. Ending a reopened run again keeps the `MET` or `BREACHED` it had,
  and sends no breach email for it.
- A run cancelled while it was being ended stays `CANCELLED`: nothing is consumed and no
  end-of-run hook runs.
- `SIGTERM` or `SIGHUP` to `etl-craft run` stops the task processes it started and records their
  attempts `FAILED` with the signal, on `run --task_code` and `--rerun` as on whole-pipeline runs
  and backfills.
- A task process exits as soon as its outcome is recorded, instead of holding the task's slot
  until a thread its script left running ends; the threads are named in the attempt's log.
- A task process writes its output unbuffered and flushes it when stopped, so the attempt's log
  keeps what a script printed before it hung or timed out.

- SQL tasks check the SELECT's columns before touching the target: a column the engine writes
  itself (`PIPELINE_RUN_ID`, `ROW_ID`, the audit columns), as a `SELECT *` over an engine-written
  table returns, or a name that needs quoting, fails the task naming it. `validate` reports the
  first when the SELECT lists its columns.
- A NULL in a merge key column fails `SCD1_MERGE`, `SCD2_MERGE` and `DELETE_ROWS` before the
  target changes, instead of inserting the row again on every run.
- A `MERGE_DEDUPE_ORDER` that ties between rows that differ fails the task instead of keeping
  one of them at random.
- A SELECT ending in a `--` comment works with every action, as one ending in `;` did.
- Scratch tables carry a token per attempt and, where the warehouse has no temporary tables, live
  in the target's schema, so a table of the same name elsewhere is never read as one.
- A soft `DELETE_ROWS` leaves rows already flagged deleted as they were, and a merge brings a
  soft-deleted key back when its SELECT returns it again.
- A business rule clears the flag of a key with no row left in its table, and judges only the
  active versions of a table with `ACTIVE_FLAG`.

### Changed

- A failed email preflight is logged and data tasks may still run. Failed `EMAIL_ALERT` tasks
  stay failed and are logged at ERROR, but data tasks determine a data pipeline's final outcome.
  Pipelines containing only alerts still use their alert tasks' outcomes.
- `HAS_DATA` means the upstream wrote rows: inserted, updated or deleted, as the new
  `AUD_TASK_RUN_LOG.ROWS_WRITTEN` records (migration `0004_rows_written.sql`). An append of no
  rows, or a merge that changed nothing, no longer satisfies it because the target holds rows.
  Rows recorded before the migration are judged by `TARGET_COUNT`; `mark --rows` sets both.
- A local run whose tasks were all `SKIPPED` ends `SKIPPED`, as in remote mode, so a pipeline
  that depends on it with `SUCCESS` is skipped too.
- `run --task_code --force` onto an ended run reopens it, records the `REOPEN`, and ends it again
  from its tasks' statuses after the task: a forced task that fails leaves the run `FAILED`.

### Verified

- All 14 required release suites passed from clean commit `63e1c6d`, with 2,118 passing
  test outcomes and no skips. The built wheel was exercised by pip, uv, package checks and
  the complete Databricks and Snowflake suites, including both table formats. The evidence
  and artifact checksums are in `release/evidence/0.2.0/`.

## [0.1.0] - 2026-09-26

The first release: a rewrite of the earlier implementation (kept at the `archive/iteration-2`
tag) with the same design, in a layered package with its own tests, documentation and release
evidence.

### Pipelines as metadata

- Pipelines, tasks, their parameters, dependencies and business rules are rows in the Engine
  DB's `CFG_` tables, which your team writes and etl-craft only reads; history is written to
  the `AUD_` tables. The Engine DB is a SQLite file or a PostgreSQL schema, created by
  `etl-craft setup` (or `init-db`) and carried forward by `migrate`, with your project's own
  migrations beside the packaged ones.
- `craft-connector.yml`, written by your team, holds the connections, per profile (`dev`,
  `prod`, ...). Every setting is a variable or a value; a secret always names a variable that
  must be set. One complete example per choice of Engine DB, warehouse, secrets source, mode
  and authentication is published with the documentation.

### Running

- `etl-craft run --pipeline_code X` runs a pipeline in dependency waves, one process per task,
  with bounded parallelism, time limits, per-attempt log files, and resumption: a stopped or
  retried run never repeats finished tasks. `--task_code` runs one task. The run each task
  belongs to is resolved from the Engine DB, never passed in.
- Dependencies of type `SUCCESS`, `FAILURE`, `ALWAYS` and `HAS_DATA`, run conditions `ALL`,
  `ANY` and `N`, and dependencies on other pipelines and their tasks, judged on the upstream's
  last finished run and consumed once. SLA tracking on every run, with SLA emails.
- Four task handlers: `SQL` (one read-only SELECT, wrapped by the engine in one of eight
  actions: `CREATE_TABLE`, `SETUP_TABLE`, `OVERWRITE_TABLE`, `APPEND_TABLE`, `SCD1_MERGE`,
  `SCD2_MERGE`, `DROP_TABLE`, `DELETE_ROWS`), `PYTHON` ingestion scripts with offsets and
  `INPUT_PARAMS`, `BUSINESS_RULES` that flag and clear failing rows in waves, and
  `EMAIL_ALERT` through SMTP (password or XOAUTH2) or `sendmail`.
- Warehouses: PostgreSQL, DuckDB, DuckDB over an Iceberg REST catalog, Trino over Iceberg,
  Databricks (Delta and UniForm) and Snowflake (native and Iceberg). The third-party drivers
  are extras: `trino`, `databricks`, `snowflake`, `aws`.
- Local mode: etl-craft is the orchestrator. `mark` sets a task or a run's status with a
  reason (a failed task marked `SUCCESS` lets its dependents run when the run resumes),
  `mark --new-run` records a stand-in upstream run, `cancel` stops a run and ends it
  `CANCELLED`, `run --rerun [--with-downstream]` and `--ignore-dependencies` run a task past
  the usual checks, and `Orchestration.Dependency_gates: enforce | warn | off` relaxes
  cross-pipeline gates per profile. Every intervention is recorded in
  `AUD_RUN_INTERVENTIONS`, and shown by `history` and the catalog.
- Remote mode: the orchestrator is the only source of truth. `generate-yml` writes each
  pipeline as a DAG holding every rule (trigger rules, sensors on other pipelines and their
  tasks, `max_active_runs: 1`); tasks run when the orchestrator says. Rules an orchestrator
  cannot express (`N` run conditions, `HAS_DATA`, mixed dependency types) fail in `validate`,
  `generate-yml` and `run --init-only`, naming each one.

### Checking and understanding

- `doctor` reports every check (settings, secrets, connections, schemas, migrations, email,
  cloning, relaxed gates) and `setup` sets up or upgrades in one command. `validate` checks
  every pipeline's metadata with the handlers' own rules without running anything.
- `list`, `graph`, `steps` and `history` inspect pipelines and runs. `lineage` traces columns
  across tasks and pipelines; `docs-version` keeps task documentation in versions.
- `generate-docs` writes a searchable catalog site: pipelines with their DAGs, tables with
  columns, lineage graphs, business rules, last runs and interventions, refreshed on a schedule
  (`generate-yml --docs`). `publish-docs` serves it through an ngrok tunnel, limited to
  `Docs_site.Allowed_ips`.
- Cloning copies the Engine DB tables into the warehouse after every run, and `etl-craft clone`
  on demand.
- Every error class has its own exit status, and every failure names the object, the value
  found, what was expected and the remedy.

### Documentation and examples

- The Support Insights demo (`examples/demo`) and a Quick Start that runs it; guides for every
  feature; deployment, connector and security pages; and references generated from the code:
  the command line, `craft-connector.yml`, task parameters, the Engine DB schema and the Python
  API.

### Verified

- The release was tested from the built wheel, installed with pip and with uv, on Linux, with
  macOS and Python 3.11 to 3.13 in CI: every Engine DB with every local warehouse, the demo end
  to end, locally verifiable authentication modes, and the demo's warehouse work on Databricks
  and Snowflake. The evidence is in `release/evidence/0.1.0/`.

[Unreleased]: https://github.com/venkatcg00/etl-craft/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/venkatcg00/etl-craft/releases/tag/v0.3.0
[0.2.0]: https://github.com/venkatcg00/etl-craft/releases/tag/v0.2.0
[0.1.0]: https://github.com/venkatcg00/etl-craft/releases/tag/v0.1.0
