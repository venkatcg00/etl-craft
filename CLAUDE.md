# CLAUDE.md — etl-craft

## What this is

`etl-craft` is a standalone, metadata-driven ETL orchestration engine. It is barebones by design, ships as a PyPI package (uv-compatible), and is meant to be adoptable by any team, independent of which orchestrator — if any — they use. This repo is **Repo 1** of a two-repo split: the engine itself, with zero domain knowledge and no team-specific pipelines baked in. A separate `metadata-etl-implementation` repo (not this one) will hold a reference implementation ("Support Insights") that proves the engine works at scale, and evolves independently of the engine.

The whole pipeline/task/step/dependency structure is modeled as **rows in Postgres**, not as Python DAG files or model files. The engine reads that metadata and executes it; it never writes logic it wasn't explicitly given.

## Where things stand / where to start

**Schema is signed off (2026-09-19).** `sql/schema.sql` is reconciled against two pasted schema snapshots plus every verbal correction made after them. It makes a real number of judgment calls translating ambiguous or conflicting source material into concrete SQL — every one of them is flagged inline with `[DEVIATION]`, `[ADDITION]`, or `[CHOICE]` tags (explained in the file's header) and summarized in a block at the end of the file. Every flag was walked through individually and confirmed as written — none required a change. **Read that summary anyway before touching the schema further** — it's the map of every non-obvious decision baked into it.

Build order, per the natural sequencing agreed once schema was confirmed — resolver → CLI (`run` first) → `generate-yml` → connector/auth layer:

- **Resolver — done.** `src/etl_craft/resolver.py`. Pure, DB-free dependency-graph logic scoped to same-pipeline `CFG_TASK_DEPENDENCY` edges only (cross-pipeline edges are explicitly out of scope here — those resolve via the tracker-table polling described above, not via this module). Covers: cycle/self-dependency validation (`build_graph`), static topological waves for `graph`/`generate-yml` (`DependencyGraph.waves`), and the retry-aware `ready()` computation implementing the SUCCESS/FAILURE/ALWAYS/HAS_DATA edge semantics plus "retry resumes, not restarts" (skip SUCCESS/SKIPPED, re-attempt FAILED/never-run, never re-dispatch an already-IN-PROGRESS task). 21 tests in `tests/test_unit.py`, all passing.
- **Engine DB access layer — done (2026-09-19).** The plumbing `run` needs, built ahead of the CLI itself since it has the most unresolved ambiguity to flag:
  - `src/etl_craft/config.py` — loads and validates `craft-connector.yml`. **[CHOICE]**: the file's own example renders section names as bracketed INI-style headers (`[Execution]`, `[Postgres]`, ...) inside a yaml fence, which isn't literal YAML — the loader expects plain nested top-level keys instead (`Execution:`, `Postgres:`, ...). **[ADDITION]**: each profile stores a `jdbc_url` (per "JDBC URL is the preferred storage format") plus `user`/`auth_mode`; secret material (password/token/passphrase) is never in the file — it's looked up via an env var name resolved through `[Source]`'s file-or-environment mechanism, defaulting to `ETL_CRAFT_{SECTION}_{PROFILE}_SECRET` unless a profile sets `secret_var:` explicitly. The Data DB section (name still unresolved — open question #1) is deliberately not parsed yet; only `[Execution]`/`[Source]`/`[Postgres]`/`[Cloning]` are.
  - `src/etl_craft/db.py` — `AUTH_REGISTRY` keyed by `auth_mode`, each entry building a `creator` callable handed to `create_engine(creator=...)` uniformly (per CLAUDE.md, not just for token/sso). `password` and `key_file` are fully implemented; `token` and `sso` raise `NotImplementedError` — CLAUDE.md establishes *that* they mint short-lived credentials but not which provider/SDK, which is necessarily team-specific.
  - `src/etl_craft/runlog.py` — implements "Run-id resolution" above as two distinct entry points, not one: `find_or_create_active_run` (for whatever mints the run — the local orchestrator before spawning task subprocesses, or the Airflow DAG's synthetic first step) versus `resolve_run_for_task` (what a plain `run --task_code` binds to: the active run if one exists, else CLAUDE.md point 5's dev/ad-hoc fallback to the latest logged run). **[CHOICE]**: "updates its dates" in that fallback is implemented as touching only `END_DATE` — `STATUS` and `START_DATE` are left alone, since reopening a terminal run's status could collide with `ux_pipeline_run_one_active` the next time that pipeline genuinely kicks off. Also includes `find_or_create_task_run` (the per-task binding, short-circuits on an existing `SUCCESS`) and `update_task_run` (in-place update, never a second row per retry).
  - Tested against an in-memory SQLite stand-in (50 tests, part of `tests/test_unit.py`) for the control-flow logic, plus a real-Postgres integration suite (part of `tests/test_integration.py`, 5 tests) that includes an actual multi-thread concurrency test proving `ux_pipeline_run_one_active` — not application code — is what makes concurrent run creation race-safe. See "Local Postgres (Docker)" below for how to run it.
- **Single-task execution core (`run --task_code`) — done (2026-09-19).** `src/etl_craft/runner.py`'s `run_task()` is what that invocation actually does — dependency check, short-circuit, bind, dispatch, finalize — built and verified against real Postgres.
  - `src/etl_craft/cfg.py` — read-only `CFG_PIPELINES`/`CFG_TASKS`/`CFG_TASK_DEPENDENCY` queries: `resolve_pipeline_id`/`resolve_task_id` (code → id, active rows only), `fetch_pipeline_graph` (active tasks + same-pipeline edges shaped for `resolver.build_graph`, plus `cross_pipeline_task_ids` — surfaced but not yet acted on, since cross-pipeline polling isn't built), `fetch_task_handler`.
  - `src/etl_craft/handlers.py` — a stub `HANDLER_REGISTRY`/`dispatch()` seam; every handler body (`PYTHON`/`SQL`/`BUSINESS_RULES`/`EMAIL_ALERT`) raises `NotImplementedError` — each is its own substantial unbuilt piece (closed SQL action vocabulary, `$$pipeline_id` substitution, Data DB connection, script invocation, business-rule sequencing).
  - `src/etl_craft/runner.py`'s `run_task()` sequence: refuse `--force` under `Mode=orchestrator` first (before any DB access); resolve pipeline/task ids; resolve `pipeline_run_id` via `resolve_run_for_task` (never mints a fresh run itself — see runlog.py above, so a single-task invocation against a pipeline that's *never* run at all correctly raises `RunLogError`, not silently minting one); unless `--force`, short-circuit on an existing `SUCCESS` binding, then live-check same-pipeline dependencies by building the full pipeline graph via `resolver.build_graph`/`ready()` fed with current `AUD_TASK_RUN_LOG` state (`runlog.fetch_run_state`) — **[CHOICE]**: if unmet, raises `DependenciesNotMetError` without writing anything to `AUD_TASK_RUN_LOG` at all (no dangling `IN-PROGRESS` row for a task that never started), rather than logging a `FAILED` attempt; only then bind via `find_or_create_task_run` and dispatch to the (stub) handler, marking `FAILED`/`SUCCESS` on the way out.
  - Deliberately **not** included yet: the crash-detection fork/monitor wrapper (premature while `handlers.dispatch` is a stub — nothing real to monitor) and cross-pipeline dependency polling.
  - 19 integration tests against real Postgres (part of `tests/test_integration.py`), all passing — 74 total across the whole suite. Caught the same identifier-case-folding pitfall noted above, this time in a test assertion, not production code.
- **CLI entry point — done (2026-09-19).** `src/etl_craft/cli.py`'s `main(argv)` wires up `etl-craft run --pipeline_code X --task_code Y [--force]` (matching the CLI surface table below exactly — `--pipeline_code` required, `--task_code` optional). `--task_code` given dispatches to `runner.run_task`; omitted dispatches to `orchestrator.run_pipeline` (below). Every exception either `run_task` or `run_pipeline` can raise for a non-bug reason (`CfgError`, `RunLogError`, `ConfigError`/`ConnectionError_`, `ForceNotAllowedError`, `DependenciesNotMetError`) is caught and printed as a clean one-line `error: ...` rather than a raw traceback. `src/etl_craft/__init__.py`'s `main()` (the `[project.scripts]` target) and `__main__.py` (for `python -m etl_craft`) both delegate to it.
- **Pipeline-level orchestration (`run --pipeline_code X`, `--task_code` omitted) — done (2026-09-19).** `src/etl_craft/orchestrator.py`'s `run_pipeline()` — CLAUDE.md's "the engine becomes its own tiny scheduler": mints/reuses the run via `find_or_create_active_run` (unlike `run_task`, which never mints), then loops — compute the ready wave via `resolver.build_graph`/`ready()` fed live `AUD_TASK_RUN_LOG` state, spawn one genuine `python -m etl_craft run --task_code` **subprocess** per ready task (not an in-process call — this is what makes local execution mirror Airflow-side execution), wait for the whole wave, repeat — until every task is settled (`SUCCESS`/`SKIPPED`) or none are ready (stuck: some task's dependency will never be satisfied this pass), then finalizes `AUD_PIPELINES_RUN_LOG` `SUCCESS`/`FAILED` accordingly.
  - **[Bug caught and fixed]**: the first version re-selected and re-spawned a permanently-FAILED task forever within one `run_pipeline()` call — real infinite loop, reproduced as an actual test hang (~150s before it was killed), not a theoretical concern. `resolver.ready()` correctly treats `FAILED` as retry-eligible *in general* (point of the whole "retry resumes" design), but that's for a **separate, later** `run` invocation to pick up — not for the orchestrator to retry within a single pass. Fixed by tracking an `attempted` set scoped to one `run_pipeline()` call. Also added `pytest-timeout` (60s, `[tool.pytest.ini_options]`) project-wide as a safety net against this whole bug class recurring, since a hang fails a lot less legibly than an assertion.
  - `--force`: since it bypasses dependency checks entirely, `ready()` can't be used to gate anything — falls back to `graph.waves()` (static topological order) instead, spawning every task in each wave regardless of current status.
  - Deliberately **not** included yet: cross-pipeline dependency polling (the self-check/poll step CLAUDE.md describes before run-id minting), and orchestrator-level crash detection for a subprocess that dies without writing its own terminal status (CLAUDE.md's crash detection is specifically about `run_task`'s own internal fork/monitor, still deferred — a task stuck that way just can't become ready again, so the existing stuck-detection already keeps the pipeline from hanging on it).
  - 6 real end-to-end tests spawning actual subprocesses against Docker Postgres (independent tasks, blocked-downstream/stuck, retry-skip-already-SUCCESS, `--force`, mode refusal, empty pipeline) plus manual CLI smoke tests, all passing — 80 total across the whole suite (up from 74).
- **`generate-yml`, connector/auth layer (Data DB, cloning) — not started.**

**Tooling (added 2026-09-19).** The package is uv-managed (`pyproject.toml` + `uv.lock`, src-layout under `src/etl_craft/`, Python ≥3.11). Dev tooling: `pytest`, `black` (formatter, line-length 100), `ruff` (lint-only — `E,F,I,UP,B,SIM`; deliberately excludes `D` so it never duplicates/conflicts with pydocstyle), and `pydocstyle` (`select=D` in `[tool.pydocstyle]`). One deviation worth knowing about: a bare `select=D` selects two internally contradictory rule pairs — `D203`/`D211` (blank line before a class docstring: required vs. forbidden) and `D212`/`D213` (multi-line summary on the first line vs. the second) — so *any* style choice leaves one side of each pair permanently violated. `add-ignore = "D203,D213"` resolves this the same way pydocstyle's own `pep257` convention does; the alternative (writing every docstring as a one-line summary with extended rationale kept as plain `#` comments underneath, as `resolver.py` now does) was adopted project-wide to sidestep the conflict rather than fight it file by file. `.github/workflows/ci.yml` runs `black --check`, `ruff check`, `pydocstyle`, and `pytest` via `uv sync --locked --all-groups` on every PR into `main`.

**Local Postgres (Docker), added and verified end-to-end 2026-09-19.** `docker-compose.yml` runs Postgres 16 on host port `55432` (not `5432`, to avoid colliding with a locally-installed Postgres) with `sql/schema.sql` auto-applied via `docker-entrypoint-initdb.d` on first boot. `make db-up` / `make db-down` / `make db-reset` (tears down the volume too, since `schema.sql` is plain `CREATE TABLE` — not idempotent — by design, meant to run once against an empty DB) / `make db-schema-test` (runs `sql/schema_test.sql` inside the container) / `make test` / `make check` (mirrors CI). The Postgres-backed integration suite (`tests/test_integration.py`, via `tests/conftest.py`) skips itself with a clear message if nothing is reachable at `ETL_CRAFT_TEST_DATABASE_URL` (defaults to the compose file's own connection string), so `pytest -q` alone never requires Docker. CI runs the same integration suite for real, via GitHub Actions' native `services:` (no Compose needed there) rather than skipping.

Ran `make db-schema-test` and `make check` for real once Docker was available: `sql/schema_test.sql` passed line-for-line as documented against actual Postgres 16, and the integration suite immediately caught a real bug — `find_or_create_task_run` read a Row's `TASK_RUN_ID`/`STATUS` attributes by their as-written uppercase names, which works against the SQLite stand-in (preserves declared case) but not real Postgres (folds unquoted identifiers to lowercase), raising `NoSuchColumnError`. Fixed by adding explicit lowercase `AS` aliases to those `SELECT`s. **Lesson for any future raw-SQL code here**: always alias `SELECT` columns explicitly in lowercase (or another fixed casing) rather than relying on a Row's attribute-name matching an unquoted identifier's original casing — SQLite and Postgres disagree on what that casing even is.

## Core design principles

These aren't stylistic preferences — violating them undoes decisions made deliberately over a long design conversation:

- **Config vs. code, drawn by blast radius, not convenience.** A step that only reads/writes within the platform (a `CFG_` row) is config — low-friction, eventually CLI-driven. A new external connection, a new action type, or a destructive schema change is code — reviewed, deliberately harder to change. Creating a new pipeline is *always* manual (git-managed inserts/migrations), never a CLI verb.
- **Closed vocabulary of SQL actions.** create table, insert overwrite, SCD1 merge, SCD2 merge, drop table, delete rows. Every SQL-handler task supplies a bare, validated, read-only `SELECT`; the engine wraps it in whatever statement the declared action calls for. The engine owns every write — a step cannot touch the warehouse outside its declared action. Cover new needs by extending an existing action's metadata, not by adding a new action type casually.
- **Order comes from data.** `CFG_TASK_DEPENDENCY` / `CFG_PIPELINE_DEPENDENCY` rows are the DAG. Nothing enumerates order in code; the resolver just walks dependency rows. Same-pipeline order compiles into Airflow's native task chaining when generating YAML; cross-pipeline order has no structural equivalent in any orchestrator and is always resolved by self-check/polling at runtime.
- **Idempotent by construction, retry resumes.** Re-running a step must produce the same result whether it runs once or five times. A retry reads the log, skips what's already `SUCCESS` or `SKIPPED`, and only re-attempts what actually failed or never ran.
- **One run id, resolved, never passed.** `pipeline_run_id` is never handed between tasks — no XCom, no injected env var. Every task independently resolves the currently-active run by querying `AUD_PIPELINES_RUN_LOG`. See "Run-id resolution" below; this is probably the single most load-bearing mechanism in the whole design.
- **Full-refresh/derived state by default; incremental only where provably safe.** A table either accumulates every run (history) or holds exactly one run's truth (current state) — never a mix.

## Non-goals — things explicitly considered and rejected

Do not reintroduce these without a real reason; each was deliberately ruled out during design:

- REST API calls to any orchestrator, including Airflow, at execution time. The only hard runtime dependency, checked by every action, is a valid Engine DB (Postgres) connection.
- XCom, or any other orchestrator-specific mechanism, for passing `pipeline_run_id` between tasks.
- Importing `dag-factory` or any other orchestrator-specific library. `generate-yml` is entirely hand-rolled — the YAML shape takes visual inspiration from how Airflow/dag-factory-style YAML looks, for familiarity, but is not required to match any external tool's schema, and no such library is a dependency.
- Support for more than one Data DB (warehouse) connection per deployment. One warehouse, full stop — the explicit reasoning given for this was "else we will become Informatica."
- Any runtime detection scheme for whether execution mode is being spoofed (credential-probing, install-time locks were both considered and dropped). Final stance: trust the developer. `--mode` is never a runtime flag typed by a human for orchestrator runs — it's baked in once, at setup time, via `set-execution-mode`, and persists in `craft-connector.yml` until the environment is rebuilt.
- Sourcing credentials from an orchestrator's own connection store (e.g. reading Airflow Connections directly). All connections resolve through `craft-connector.yml` only, to stay orchestrator-agnostic — this includes the Data DB connection, "even on the Airflow side."
- Bundling third-party SQLAlchemy dialects (Snowflake, Databricks, BigQuery, Redshift, ClickHouse) as hard dependencies. They're optional extras a team installs itself; SQLAlchemy discovers an installed dialect automatically via setuptools entry points, so engine code never imports one directly.
- A heartbeat column plus an external "reaper" process for detecting dead tasks. Superseded by parent/child process monitoring — see "Crash detection" below.
- A staging/cross-connection copy path for ordinary tasks. Moot by construction once there's only one Data DB — `source_object`/`target_object` are always in the same place.

## Architecture at a glance

**Two databases, two very different rule sets:**
- **Engine DB** — always Postgres, no exceptions. Holds every `CFG_`/`AUD_` table. Required specifically for Postgres's constraint guarantees — most importantly, a partial unique index is what makes run-id creation race-safe (see below). Every engine action checks for a valid Engine DB connection first, before anything else.
- **Data DB / warehouse** — exactly one per deployment, any SQLAlchemy-supported relational engine. Because there's only one, `source_object`/`target_object` in `CFG_TASK_PARAMS` are always in the same database — there is no cross-connection staging path to build for ordinary tasks.

**Connections & auth** — all resolved through `craft-connector.yml` (below), never hardcoded, never sourced from an orchestrator. JDBC URL is the preferred storage format for a connection string wherever one is persisted; a small internal translator converts it to the right SQLAlchemy dialect URL at connection time. Auth is a small registry of functions keyed by each profile's `auth_mode` (`password`, `token`, `sso`, `key_file`), handed to `create_engine(creator=...)` — the `sso`/`token` modes mint or refresh short-lived credentials per connection. For those two specifically, `pool_recycle` should sit comfortably under the credential's real lifetime so a checked-out connection always has meaningful life left — that's a safety margin, not a guarantee; a connection dying mid-operation is just another failure the idempotent-retry design already absorbs. `pool_pre_ping` is a worthwhile optimization on top but not load-bearing.

**Execution — one primitive.** Everything runs through `etl-craft run --pipeline_code X [--task_code Y] [--force]`:
- `--task_code` given → runs exactly one task. This is the literal form Airflow's generated `BashOperator` tasks shell out to.
- `--task_code` omitted → runs the whole pipeline. The engine becomes its own tiny scheduler: resolve the ready wave from `CFG_TASK_DEPENDENCY` (the same resolver `graph` uses to walk the same data, just consumed differently), spawn one `run --task_code` subprocess per ready task, wait on the wave, repeat. Local runs are built to mimic exactly what an orchestrator-driven run does — sequential/parallel waves, subprocess-per-task, crash monitoring — unless `--force` is passed.
- `--force` bypasses all dependency/state checks entirely, for a pipeline or a single task. Only legal under `Mode = local`; refused outright under `Mode = orchestrator`.
- There is deliberately no separate `execute --local`/`execute --airflow` verb, and no REST-based trigger/status command — `run` is the only execution primitive, in both modes.

**Execution mode (`local` vs. `orchestrator`)** is set once per environment via `etl-craft set-execution-mode`, persisted in `craft-connector.yml`. It is **not** a per-invocation CLI flag — nothing in a generated DAG or a manual command line passes `--mode`. Changing it means re-running setup, which in a real deployment goes through the same versioned pipeline as any other environment change. This is intentionally a trust boundary, not a technical one: see the Non-goals entry above.

**Crash detection.** `run` forks the actual task logic as a child process and watches it. A child that exits normally — success, or a handled failure — writes its own final log row, as always. A child that dies unannounced (OOM-kill, segfault) gets `FAILED` written on its behalf by the still-alive parent. This is identical in local and Airflow modes, since Airflow-side execution is this same subprocess form — nothing orchestrator-specific to build. Known, accepted gap: if the *entire* process tree dies at once (container OOM-killed, pod evicted, node gone), nothing survives to write `FAILED`. Airflow's own zombie-task detection covers its UI in that scenario but never touches `AUD_TASK_RUN_LOG`, which is left permanently stale. Accepted as rare enough not to solve for now.

## Run-id resolution (read this before touching anything related to run state)

`pipeline_run_id` is never passed to a task — every task resolves it itself by querying `AUD_PIPELINES_RUN_LOG`:

1. The first task of a run does a **find-or-create**: reuse an existing `IN-PROGRESS` row for this pipeline if one exists; otherwise mint a new one.
2. This must be atomic at the database level — a trigger or application-level check alone cannot close the race between two near-simultaneous kickoffs of the same pipeline. The actual guarantee comes from a **partial unique index**: `UNIQUE (PIPELINE_ID) WHERE STATUS = 'IN-PROGRESS'`. The second concurrent insert fails outright rather than racing. This is implemented in `sql/schema.sql` as `ux_pipeline_run_one_active`.
3. Every later task queries for whatever run is currently active for its pipeline and binds itself to it (one `AUD_TASK_RUN_LOG` row per task per pipeline run, found-or-created and updated in place — never a new row per retry attempt).
4. If a task already has a binding under that run and it already shows `SUCCESS`, it returns success immediately without re-running. This is what makes retries — whether triggered locally or by an orchestrator — always *continue* a failed/in-progress run rather than starting fresh, and is why a run should never end in an intermediate state.
5. A single-task invocation with nothing currently active is a dev/ad-hoc convenience path: it binds to the latest logged run and updates its dates. Not an everyday scenario — don't over-engineer around it.

## Incremental / full-refresh mechanics

- `CFG_PIPELINES.REFRESH_TYPE` (`FULL`/`INCREMENTAL`) is a **pipeline-level** value — every task in one pipeline run shares one mode. Mixing an always-full dimension load with an incremental fact load means two pipelines, not one. **Note:** both pasted schema drafts still show `REFRESH_TYPE` on `CFG_TASKS` — that was superseded by a later, explicit decision to move it to the pipeline. `sql/schema.sql` implements the pipeline-level version; `CFG_TASKS` carries a comment noting the column is intentionally absent there.
- Every SQL step's `SELECT` may contain the literal token `$$pipeline_id` in its `WHERE` clause. The engine runs its own plain-text substitution pass *before* the query reaches the driver's own bind-variable handling, so `$$` tokens and real bind params (`:param`, `%s`) never collide.
- At substitution time: `$$pipeline_id` becomes `pipeline_run_id = <this run's id>` for incremental, or `1=1` for full refresh.
- Every table written by a SQL action carries a `pipeline_run_id` column that the engine auto-stamps. A step author writes `select col1, col2 from table where $$pipeline_id` — never manually selects or inserts the id.
- **`HANDLER = PYTHON` ingestion scripts are the one exception.** The engine does not auto-inject the run id into them; the team's own script is responsible for fetching and including `pipeline_run_id` in whatever it inserts.
- Runtime substitution, rather than trusting a stored value, matters specifically for full refresh: it's what lets a downstream table that maintains history, with no new data after change-detection, still correctly carry the *current* run's id instead of getting stuck showing some run from long ago.
- `HAS_DATA` (`TARGET_COUNT > 0`) is only a meaningful signal against an **incremental** upstream — there it genuinely reflects whether anything changed, and downstream tasks can legitimately skip on it. Against a full-refresh upstream, every run re-asserts the complete current state regardless of whether values actually moved, so `HAS_DATA` isn't a real change signal there — full refresh just proceeds and re-stamps.

## Dependency resolution & polling

- **Same-pipeline task deps**, when going through a generated DAG, compile into Airflow's own native task chaining (`>>`), built from `CFG_TASK_DEPENDENCY` — series, parallel, conditional shapes, as the data dictates.
- **Any path that bypasses the DAG's own ordering** — a manual single-task run, a backfill, a re-triggered task — has no structural ordering to lean on, so the engine still has to verify same-pipeline dependencies itself: a live query against `AUD_TASK_RUN_LOG` scoped by the shared `pipeline_run_id` (no staleness comparison needed, since both tasks already share one run).
- **Cross-pipeline dependencies** (`CFG_PIPELINE_DEPENDENCY`, and `CFG_TASK_DEPENDENCY` rows where `DEPENDS_ON_PIPELINE_ID` points elsewhere) have no DAG-native equivalent in Airflow or otherwise — one DAG can't natively depend on a task in a separate DAG. These are always resolved by a self-check/poll step inserted before step 1, alongside run-id minting.
- The naive version of this check — "look at the dependency's last logged run" — breaks the moment two pipelines run on different schedules (weekly vs. daily, say): most days the dependency isn't in-progress, so the check falls straight to a stale "last logged run" from days ago and wrongly proceeds. The fix: two watermark tables, `AUD_PIPELINE_DEPENDENCY_TRACKER` and `AUD_TASK_DEPENDENCY_TRACKER` (in `sql/schema.sql`, not in either pasted schema draft — designed later), recording the run last *consumed* for each dependency edge. A candidate run only satisfies the edge if it's newer than what was already consumed for that specific edge *and* matches that edge's own `DEPENDENCY_TYPE` — a `SUCCESS` edge tracks the last `SUCCESS` run, `FAILURE` tracks the last `FAILED` run, `HAS_DATA` tracks the last run with `TARGET_COUNT > 0`. Until a new qualifying run appears, the edge keeps resolving to "not yet satisfied," with no polling wasted on a pipeline that was never going to produce one today.
- The tracker only updates *after* the gated task/pipeline completes. Cross-pipeline task-level tracking only applies to actual cross-pipeline edges — same-pipeline task deps never touch it.
- Poll only while the dependency is genuinely `IN-PROGRESS`; if it isn't, go straight to the tracker comparison above rather than polling blindly.
- Poll cadence, as specified: a hard 1-hour timeout overall. The interval strategy is duration-aware rather than fixed — based on the dependency's average run duration and current elapsed time, first poll at 70% of that average, next at 80%, +10% each poll after, capped at 30 polls total. Whichever limit is hit first (poll count or the 1-hour wall clock) ends the wait. *(Flag for review: an earlier mention of "exponential backoff of 60 minutes" alongside this was never fully reconciled with the percentage-based schedule — confirm which governs, or whether the backoff applies only when no duration history exists yet for a new pipeline pairing.)*

## Business rules

- `CFG_BUSINESS_RULES.BUSINESS_RULE_TYPE` (`INCOMPLETE`/`REJECT`) classifies the **row** a rule flags — it is not a pass/fail outcome. Rule *run* outcome (did the check itself execute successfully) is tracked separately, in `AUD_BUSINESS_RULES_RUN_LOG.STATUS`.
- Ordering is `SEQUENCE_NUMBER` (dense rank per task) directly on `CFG_BUSINESS_RULES` — same rank runs in parallel, different ranks run sequentially. There is no separate business-rule dependency table.
- Every target table is required to have a single-column primary key — an enforced framework convention, which is why `BUSINESS_RULE_KEY_COLUMN` can safely stay a single column rather than a list. This convention lives outside the Engine DB entirely (it constrains tables in the Data DB), so it cannot be checked by an Engine DB constraint — enforce it at `validate` time via schema introspection against the Data DB.
- A flagged row's classification is audit information, not a failure signal — a business-rule task that flags every row it checks is still a `SUCCESS` task run. Nothing here auto-blocks a pipeline; that would have to come from a separate, explicitly-declared task dependency on the BR task's own status.

## Handlers

Four, closed: `PYTHON` (ingestion scripts), `SQL` (engine-executed, wraps a bare `SELECT` per the closed action vocabulary), `BUSINESS_RULES` (drives `CFG_BUSINESS_RULES` rows for that task), `EMAIL_ALERT` (added later than the other three — not in either pasted schema draft, but confirmed). An `EMAIL_ALERT` task is gated the same way any other task is, via a normal `CFG_TASK_DEPENDENCY`/`CFG_PIPELINE_DEPENDENCY` row — a `FAILURE` edge fires only when what it watches fails, `ALWAYS` fires regardless. No separate conditional mechanism needed. Still open: the actual send transport (SMTP creds vs. an API like SES/SendGrid) — presumably just another named connection resolved through `craft-connector.yml` like everything else — and whether `$$`-style substitution applies inside alert bodies (e.g. `$$pipeline_id`, pulling `ERROR_MESSAGE` off the failed task's own `AUD_TASK_RUN_LOG` row).

## CLI surface

| Command | Purpose |
|---|---|
| `list` | List pipelines |
| `run --pipeline_code X [--task_code Y] [--force]` | The one execution verb (see Architecture above) |
| `configure` | Interactive setup chain |
| `configure --env` | Non-interactive setup from an env file |
| `set-execution-mode local\|orchestrator` | One-time-per-environment mode lock; persists in `craft-connector.yml` until rebuild/reconfigure |
| `graph --name <pipeline>` | Print dependency chain / lineage. Bare `graph` with no name errors. |
| `validate` | Config integrity check — including the cross-database checks (e.g. single-column PK on a `TARGET_TABLE`) that no Engine DB constraint can enforce |
| `generate-yml` | Emit YAML (replaces the earlier working name `generate-dag`) — hand-rolled, Airflow-YAML-inspired shape, consumable by Airflow, another orchestrator, or a custom script |

Retired during design: `execute --local` / `execute --airflow` (collapsed entirely into `run`), any REST-based Airflow trigger/status commands, and a proposed `reap` command for stale-run cleanup (superseded by parent/child monitoring).

Read-only query verbs conceptually agreed but not yet named or built: dependency graph, steps-in-a-pipeline, run history, table-level lineage — these are meant to replace ad hoc SQL against the Engine DB with proper `etl-craft` verbs. Writing `CFG_` rows (registering a pipeline, adding a task, adding a dependency) stays outside the CLI — pipeline creation specifically is manual, git-managed migrations/inserts; how far *other* `CFG_` writes get their own verbs later is open ("most of it is fine," not fully enumerated).

A documentation generator is also planned: the same read-layer queries as above, rendered as a static, searchable site (deployable to GitHub Pages or any endpoint) rather than returned as CLI text. Since GitHub Pages has no backend, "searchable" means a client-side search index (Fuse.js/Lunr.js-class) baked in at generation time, not a server endpoint.

## craft-connector.yml

Versioned, no secrets stored directly in it — analogous to a dbt `profiles.yml`. Structure so far:

```yaml
[Execution]
Mode: local | orchestrator       # set via `set-execution-mode` at setup time only, persists until rebuild
Orchestrator name: <optional>    # informational only, engine doesn't act on it

[Source]
Type: file | environment         # where actual secret values live
Path: <path>                     # required when Type = file (a .env-style file)

[Postgres]                       # Engine DB — required, always Postgres
Active_profile: <name>
Profiles:
  dev:  { host, port, user, auth_mode, ... }
  sit:  { ... }
  uat:  { ... }
  prod: { ... }

[<Data DB section — name not yet decided, see Open Questions>]
Active_profile: <name>
Profiles:
  dev:  { host, port, user, auth_mode, ... }
  sit:  { ... }
  uat:  { ... }
  prod: { ... }

[Cloning]
Enabled: true | false            # opt-in, off by default — nothing is copied unless explicitly turned on
Scope: cfg | aud | all           # which table groups mirror into the Data DB, merge-style
```

`auth_mode` per profile selects one of a small registry of connection-creator functions (`password`, `token`, `sso`, `key_file`) — see Architecture above.

**Cloning**, specifically: a merge-style copy of selected Engine DB tables into the Data DB, so a team can query engine config/audit from inside their own warehouse without a separate Postgres connection. Runs after each pipeline run, only when enabled. This is special-cased engine-internal machinery — it does not reopen the "one Data DB, no general cross-connection support" rule that applies to ordinary tasks.

## Database schema

Full DDL (complete first draft, needs review and sign-off) is at `sql/schema.sql`. It has been applied to a real local Postgres 16 and exercised against `sql/schema_test.sql`, which confirms — by actually triggering them, not just reading the DDL — that every load-bearing mechanism behaves as this file describes: the audit trigger stamps and preserves the right columns, the enum/conditional `CHECK` constraints reject what they should, the `DEPENDS_ON_PIPELINE_ID` default-from-sibling trigger fires, self-dependency is blocked, the partial unique indexes correctly allow a deactivated natural key to be reused while blocking a second active duplicate, and — the one that matters most — the `AUD_PIPELINES_RUN_LOG` partial unique index really does block a second concurrent `IN-PROGRESS` run for the same pipeline while leaving a second pipeline's own `IN-PROGRESS` row and any terminal-status row untouched. Re-run `sql/schema_test.sql` (instructions in its header) after any schema change that touches these mechanisms. Summary of tables:

**Config tables** — `CFG_PIPELINES`, `CFG_PIPELINE_DEPENDENCY`, `CFG_TASKS`, `CFG_TASK_DEPENDENCY`, `CFG_TASK_PARAMS`, `CFG_BUSINESS_RULES`.
**Audit tables** — `AUD_PIPELINES_RUN_LOG`, `AUD_TASK_RUN_LOG`, `AUD_BUSINESS_RULES_RUN_LOG`, `AUD_BUSINESS_RULES_RESULTS`, `AUD_TASK_OFFSET_TRACKER`, `AUD_PIPELINE_DEPENDENCY_TRACKER`, `AUD_TASK_DEPENDENCY_TRACKER`.

All primary keys use `GENERATED ALWAYS AS IDENTITY`. The pasted schema notes annotate almost every validated column as "(WILL BE ENFORCED BY THE ENGINE SETUP AS TRIGGER)" — `schema.sql` does **not** implement all of these as literal trigger functions; see its header and closing summary block for exactly which ones became real triggers (only where a `DEFAULT`/`CHECK` genuinely cannot do the job — namely `CREATED_BY`/`UPDATED_BY` capture, and `CFG_TASK_DEPENDENCY.DEPENDS_ON_PIPELINE_ID`'s default-from-sibling-column) versus which became plain `CHECK` constraints (all the enum-style value lists). This is a real, deliberate divergence from the literal pasted text — flagged prominently for sign-off, not silently decided.

## Open questions

Genuinely unresolved — don't guess silently on these, ask:

1. What should the Data DB section in `craft-connector.yml` actually be called? `"Data Db"` / `[Data Db 1]` was explicitly rejected as too Informatica-shaped; no replacement name was settled on.
2. `CFG_PIPELINES` needs a stable code/slug for the CLI's `--pipeline_code` to resolve against, distinct from the numeric `PIPELINE_ID` — the same way `CFG_TASKS.TASK_CODE` works for tasks. Neither pasted schema draft has such a column. `sql/schema.sql` adds `PIPELINE_CODE` to close this gap; confirm the name and that this is really needed (vs., say, resolving `--pipeline_code` against `PIPELINE_NAME` instead).
3. Email alert transport and `$$`-substitution-in-alert-bodies — see "Handlers" above.
4. The dependency-poll backoff schedule — see the flag under "Dependency resolution & polling" above.
5. Schema drift detection (a declared column list per target table, diffed against the live warehouse each run) is an endorsed principle from the original design discussion, but has no table or mechanism designed yet.
6. Every `[DEVIATION]`, `[ADDITION]`, and `[CHOICE]` flagged in `sql/schema.sql` — that file's closing summary block is the authoritative, current list; this file's "Database schema" section above is a pointer to it, not a duplicate.
7. No migration tooling (Alembic or otherwise) has been discussed — `schema.sql` is a single flat file for now, on the assumption schema review happens before that matters.
