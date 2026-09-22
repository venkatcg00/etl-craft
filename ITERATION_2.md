# etl-craft — iteration 2 backlog

**Written for:** whoever (person or model) picks up the next round of work on this repo.

Iteration 1 built everything `CLAUDE.md` scopes: 24 source modules, 368 tests, all passing,
schema applied and exercised against real Postgres, CI green. This file is the *other* half of
that picture — what a fresh review of the finished code found that should be better, ordered so
it can be worked through top to bottom.

## Status

| Round | Date | Scope | Outcome |
|---|---|---|---|
| 1 | 2026-09-20 | The whole of iteration 1 | E2-01…E2-40 |
| planning | 2026-09-20 | Design interview | E2-41…E2-43, phase order, settled decisions |
| 2 | 2026-09-20 | Phase 1 as committed (`06fcdb7`) | **E2-44…E2-52**, at the end of this file |
| phase 1b | 2026-09-20 | Round 2's findings | **All nine fixed**, 395 → 407 tests |
| **complete** | 2026-09-20 | Phases 2–7 + a mid-iteration addendum | **All 52 items closed**, 368 → 500 tests |
| 3 | 2026-09-20 | The completed iteration 2 (`314e0c3`) | **E2-53…E2-60**, at the end of this file. Note E2-57: two items are marked closed that were not built |
| phase 3b | 2026-09-20 | Round 3's findings | **All eight fixed**, 500 → 470 tests (net: 9 obsolete removed, 13 added) |
| warehouse change | 2026-09-21 | ClickHouse dropped, DuckDB added (`f34c2ae`) | Supported warehouses are now **PostgreSQL and DuckDB** |
| 4 | 2026-09-21 | Round 3's fixes + the DuckDB move | **E2-61…E2-64**, at the end of this file. All of round 3 verified fixed; every finding is about DuckDB |
| phase 4b | 2026-09-21 | Round 4's findings | **All four fixed**; E2-61 solved with an Engine DB advisory lock, and **E2-64 was corrected — the review had it wrong in the dangerous direction** |
| warehouse scope | 2026-09-22 | Iceberg added: Trino/Databricks/Snowflake | Warehouse is now **Postgres, or a SQL engine over Iceberg**; DuckDB stays for local dev |
| 5 | 2026-09-22 | Round 4's fixes + the Iceberg work | **E2-65…E2-69**, at the end of this file. Every finding is on the Iceberg path |
| phase 5b | 2026-09-22 | Round 5's findings | **All five fixed, plus E2-70 the review missed** — `SCD2_MERGE` was entirely broken on Trino |
| table formats | 2026-09-22 | Native (Delta / Snowflake) allowed alongside Iceberg | `TABLE_FORMAT` task parameter over `[Warehouse].Table_format`; `iceberg` stays the default |
| 6 | 2026-09-22 | Round 5's fixes + the native-format work | **E2-71…E2-73**, at the end of this file |

**Phase 1 is landed and independently re-verified** (round 2 re-ran round 1's own probes against
the current branch rather than trusting the phase notes): **E2-01 fixed**, **E2-02 fixed**,
**E2-37 fixed**, E2-41 landed, E2-14's `trigger_rule` half landed. **E2-03 and E2-04 confirmed
still open**, as expected — they are Phase 2. 368 → 395 tests, all passing.

Phase 1's own test additions mirror the round-1 reproductions closely (`test_run_task_does_not_
clobber_an_already_in_progress_row`, `test_run_pipeline_succeeds_end_to_end_with_a_failure_gated_
alert_task`, `test_check_acyclic_handles_a_chain_deeper_than_the_recursion_limit`, and eight more
around `unsatisfiable()`), which is the discipline this file asked for. Round 2's findings are
almost all in the *new* surface that work created, not in what it fixed.

## How to use this file

- Items are numbered `E2-nn` and never renumbered. Reference them in commits/PRs.
- Each item states its **confidence**: `reproduced` (a real test was written and watched to fail
  against Docker Postgres during the review), or `from code` (read, reasoned, not executed).
  Treat `from code` items as needing a confirming test *first* — write the failing test, then fix.
- **Do not** silently widen scope. Several items note a design question that needs the user's
  call; ask rather than guess, per `CLAUDE.md`'s Open questions discipline.
- Every fix keeps the existing `[DEVIATION]`/`[ADDITION]`/`[CHOICE]` flagging convention, and
  `CLAUDE.md` gets updated in the same commit — that file is the source of truth.

## Review method

Read all of `src/etl_craft/` (~7k lines), `sql/schema.sql`, `sql/schema_test.sql`, the test suite,
`pyproject.toml`, `Makefile`, `docker-compose.yml`, `.github/workflows/ci.yml`. Confirmed the
baseline first (`uv run pytest -q` → 368 passed), then wrote throwaway probe tests against the
live Docker Postgres for the findings that looked most serious. Four of those probes failed,
which is how E2-01 through E2-04 got their reproductions. The probe file was deleted afterward —
**turning each reproduction into a permanent regression test is part of fixing the item.**

The headline observation: **368 passing tests, and all four P0 bugs sat in untested scenario
space.** Every one of them is a two-component interaction (an alert task *plus* a successful
pipeline; a merge *plus* duplicate keys; `CREATE_TABLE` *plus* `validate`). The suite tests each
component thoroughly and in isolation. See E2-30.

---

## P0 — correctness bugs

### E2-01 — A `FAILURE`-gated task makes a successful pipeline report `FAILED` · reproduced

**Where:** [orchestrator.py:132-137](src/etl_craft/orchestrator.py#L132-L137), [resolver.py:132-161](src/etl_craft/resolver.py#L132-L161)

A task whose only dependency edge is `FAILURE` (the `EMAIL_ALERT` pattern `CLAUDE.md`'s Handlers
section explicitly endorses) never becomes ready when its watched task succeeds — correct. But
nothing ever marks it terminal, so it has no `AUD_TASK_RUN_LOG` row, so
`_finalize_from_task_states` counts it as `unsettled`, so the pipeline is finalized `FAILED`.

This means **the one alerting pattern the design recommends breaks the status of every pipeline
that uses it**, on every successful run. Both modes: `run_pipeline` reports `FAILED` and exits 1;
`--finalize-only` (the generated DAG's `__finalize__` task) writes `FAILED` to
`AUD_PIPELINES_RUN_LOG`. Downstream `SUCCESS`-typed `CFG_PIPELINE_DEPENDENCY` edges then never
fire, so this silently propagates.

**Reproduction:** pipeline with `PROBE_WORK` (SUCCESS) + `PROBE_ALERT` (`EMAIL_ALERT`, `FAILURE`
edge on `PROBE_WORK`) → `finalize_active_run` returned `FAILED`.

**Fix direction:** distinguish "not ready yet" from "can never become ready under this run". A
task with an edge that is permanently unsatisfiable (a `FAILURE` edge whose upstream is terminal
`SUCCESS`; a `HAS_DATA` edge whose upstream succeeded with `TARGET_COUNT = 0`) should be recorded
`SKIPPED`, which `SETTLED_STATUSES` already accepts. Best done as a new `resolver` function
(`unsatisfiable(run_state)` alongside `ready()`) so the pure layer owns the semantics and both
`_run_until_settled` and the finalize path consume it. Decide with the user whether the
`__finalize__` step should also settle such tasks in orchestrator mode, or whether Airflow's own
`all_failed` trigger rule is expected to mark them skipped (see E2-14 — currently nothing maps
`dependency_type` to a trigger rule, so Airflow can't).

### E2-02 — A duplicate invocation clobbers a still-running task's log row · reproduced

**Where:** [runner.py:168-179](src/etl_craft/runner.py#L168-L179), [runner.py:215-222](src/etl_craft/runner.py#L215-L222)

`resolver.ready()` excludes `IN-PROGRESS` tasks (deliberately — "never re-dispatch"). `run_task`
reads that exclusion as "same-pipeline dependencies not met" and calls `_bind_as_skipped`, which
**overwrites the running task's `AUD_TASK_RUN_LOG` row** with `STATUS='SKIPPED'` and a wrong
`ERROR_MESSAGE`. Three consequences:

1. The audit row lies about a task that is still executing.
2. The *original* process's crash detection is disarmed — it only writes `FAILED` when the row is
   still `IN-PROGRESS` ([runner.py:232-244](src/etl_craft/runner.py#L232-L244)).
3. When the original finishes it overwrites again, so the outcome depends on timing.

Reachable by ordinary means: an Airflow retry firing while the first attempt still runs, a human
running a task the local orchestrator already spawned, two overlapping manual invocations.

**Reproduction:** seeded an `IN-PROGRESS` row, called `run_task` → row became `SKIPPED`.

**Fix direction:** check the existing binding's status explicitly before the dependency check.
`IN-PROGRESS` should be its own outcome ("already running elsewhere, not re-dispatching"), write
nothing, and exit 0. Separately, make the skip reason name the real cause instead of always
saying "dependencies not met" — that message is wrong for both this case and E2-01's.

### E2-03 — Every engine-created table fails `validate`'s own primary-key check · reproduced

**Where:** [sql_actions.py:589-635](src/etl_craft/sql_actions.py#L589-L635), [validate.py:113-122](src/etl_craft/validate.py#L113-L122)

`CLAUDE.md`: "Every target table is required to have a single-column primary key — an enforced
framework convention", checked by `validate` via introspection. But `CREATE_TABLE` and
`SETUP_TABLE` both build the target with `CREATE TABLE ... AS SELECT`, which never creates a
primary key, and `_evolve_schema` drops-and-renames, which would destroy one anyway. So the
engine's own tables can never satisfy the engine's own convention.

**Reproduction:** ran a `CREATE_TABLE` task, then `validate_business_rule_keys` →
`'public.probe_pk_4501' must have exactly one primary key column, found []`.

**Fix direction:** needs a design decision, so **ask before building**. Options: (a) add a
`PRIMARY_KEY` parameter to `CFG_TASK_PARAMETERS` and have `CREATE_TABLE`/`SETUP_TABLE` issue the
`ALTER TABLE ... ADD PRIMARY KEY` (and `_evolve_schema` re-add it) — closes both this and E2-04;
(b) keep the convention but make it the team's responsibility on hand-built targets and have
`validate` say so clearly; (c) drop the convention. Option (a) is the only one that makes the
`CLAUDE.md` text true as written. Note `_evolve_schema`'s drop-and-rename also silently loses
indexes and grants — worth covering in the same change.

### E2-04 — SCD merges don't enforce `MERGE_KEY` uniqueness; duplicates permanently break the target · reproduced

**Where:** [sql_actions.py:719](src/etl_craft/sql_actions.py#L719) (`_scd1_merge`'s correlated `SET` subquery), [sql_actions.py:822-833](src/etl_craft/sql_actions.py#L822-L833) (`_scd2_merge`'s changed-key insert)

Nothing checks that the staged `SOURCE_SQL` yields one row per `MERGE_KEY`, and the target has no
primary key (E2-03) to catch it. With two source rows sharing a key:

- **Run 1** takes the `NOT EXISTS` insert path and writes *both* rows. The SCD1 target now holds
  two "current" rows for one key — silent corruption, reported `SUCCESS`.
- **Run 2**, once any compared value changes, hits `SET col = (SELECT s.col FROM stage s WHERE
  t.k = s.k)` and dies with `CardinalityViolation: more than one row returned by a subquery used
  as an expression`. The target is now **permanently un-mergeable** — every future run fails the
  same way, and no retry can recover it, because the duplicate rows are in the target.

**Reproduction:** source `(1,'a'),(1,'b')` → run 1 `SUCCESS`, target `[(1,'a'),(1,'b')]`; source
changed to `(1,'c'),(1,'b')` → run 2 `FAILED` with `CardinalityViolation`.

**Fix direction:** add an explicit pre-flight uniqueness check on the staging table before any
merge statement runs (`SELECT COUNT(*) FROM (SELECT key FROM stage GROUP BY key HAVING COUNT(*)>1)`),
failing with a clear `HandlerError` naming the duplicated keys — cheap, portable, and it fails the
run *before* touching the target rather than after corrupting it. Then decide with the user
whether duplicates should ever be tolerated (dedupe by some ordering) or always rejected;
rejecting is more in keeping with "the engine never writes logic it wasn't given". A real PK on
the target (E2-03a) would enforce it at the database level too.

### E2-05 — `etl-craft migrate` silently does nothing when installed from a wheel · from code

**Where:** [migrate.py:41](src/etl_craft/migrate.py#L41)

`DEFAULT_MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "sql" / "migrations"` resolves to
the repo checkout. Installed, `__file__` is `site-packages/etl_craft/migrate.py`, so `parents[2]`
is whatever sits above `site-packages` — and the wheel ships no `sql/` at all (verified: see
E2-13). `Path.glob` on a nonexistent directory yields nothing without error, so
`apply_pending_migrations` returns `[]` and the CLI prints **"migrate: already up to date"**. A
team's migrations are silently skipped and the command reports success.

Two more problems in the same module: `SELECT VERSION FROM SCHEMA_MIGRATIONS` raises a raw
`SQLAlchemyError` (not `MigrationError`, so a traceback) against a database that predates that
table, and nothing can bootstrap it. And `_split_statements` splits on `;`, which cannot survive
a `CREATE FUNCTION ... $$ ... ; ... $$` body — i.e. any migration touching the trigger functions
`schema.sql` itself defines. That limitation is documented but is very likely to be the *first*
migration anyone writes.

**Fix direction:** `--migrations-dir` on the CLI plus an `ETL_CRAFT_MIGRATIONS_DIR` env var,
defaulting to `./sql/migrations` relative to cwd (not to the package); ship the SQL in the wheel
(E2-13) and prefer a packaged path via `importlib.resources`; create `SCHEMA_MIGRATIONS` if
absent; add an advisory lock so two concurrent `migrate` runs can't double-apply; and either
handle `$$`-quoted bodies or make the file convention "one statement per file" explicit.

### E2-06 — `craft-connector.yml` can only be found in the current working directory · from code

**Where:** [cli.py:208](src/etl_craft/cli.py#L208) (`load_config()`, no argument), [config.py:47](src/etl_craft/config.py#L47)

There is no `--config` flag and no environment variable. Every command must run with cwd set to
the directory holding `craft-connector.yml`. That is a poor fit for exactly the deployment
`CLAUDE.md` targets: Airflow's `BashOperator` cwd is not something a DAG author controls
reliably, and `generate_yml` emits bare `etl-craft run --pipeline_code X ...` with no `cd`. The
same assumption is baked into `HANDLER=PYTHON` (`scripts.py` documents "via etl-craft's own
craft-connector.yml in the same directory").

**Fix direction:** `--config PATH` on every command plus `ETL_CRAFT_CONFIG`, threaded through
`load_config`/`configure`/`set_execution_mode`; have `generate_yml` include the resolved config
path in the generated `bash_command` when one was given. Also consider an upward search from cwd
(the `pyproject.toml`/`.git` pattern) so a subdirectory invocation works.

### E2-07 — "The engine owns every write" is not actually enforced · from code

**Where:** [sql_actions.py:361-378](src/etl_craft/sql_actions.py#L361-L378), [business_rules.py:154-165](src/etl_craft/business_rules.py#L154-L165)

`CLAUDE.md`'s core principle is that each SQL task supplies "a bare, **validated**, read-only
`SELECT`" and "a step cannot touch the warehouse outside its declared action". Nothing validates
it. `SOURCE_SQL` is interpolated straight into `CREATE TEMPORARY TABLE stage AS {select_sql}`, and
Postgres supports data-modifying CTEs — `WITH x AS (DELETE FROM other_table RETURNING *) SELECT *
FROM x` is a perfectly valid "SELECT" that writes. `BUSINESS_RULE_SQL` goes into an `EXISTS(...)`
with the same exposure. `sql_actions.py`'s docstring acknowledges no parser is available and
leaves it there.

This is a real gap but **not** an argument for adding a SQL parser dependency (ruled out in
Non-goals). A cheap, honest 90% check belongs in `validate`, not at runtime: require `SOURCE_SQL`
to start with `SELECT` or `WITH` after comment/whitespace stripping, and reject any occurrence of
`INSERT`/`UPDATE`/`DELETE`/`MERGE`/`TRUNCATE`/`DROP`/`ALTER`/`CREATE`/`GRANT`/`COPY` as a bare
word. Document it as a lint, not a security boundary — `CFG_` rows are git-reviewed, which is the
real control. Worth confirming with the user that a lint is the intended reading of "validated".

### E2-08 — `HAS_DATA` edges can never be satisfied downstream of `PYTHON` or `EMAIL_ALERT` tasks · from code

**Where:** [scripts.py:187](src/etl_craft/scripts.py#L187), [email_alert.py:286-288](src/etl_craft/email_alert.py#L286-L288), [resolver.py:171-176](src/etl_craft/resolver.py#L171-L176)

`HAS_DATA` is defined as `upstream.status == 'SUCCESS' and target_count > 0`. `scripts.py` reports
its ingestion count as `source_count` and never sets `target_count`; `email_alert.py` sets no
counts at all. So a `HAS_DATA` edge on an ingestion task — the most natural place to want one —
is permanently unsatisfiable, and (given E2-01) also currently poisons the pipeline's status.

**Fix direction:** decide what `TARGET_COUNT` means for an ingestion script. Simplest coherent
answer: `INGESTION_COUNT` is what the script wrote, so it should populate `target_count` (and
arguably `insert_count`) too, not just `source_count`. Confirm with the user, then add a
`validate` check rejecting a `HAS_DATA` edge whose upstream handler can't produce a count.

### E2-09 — Predictable handler failures surface as "died unexpectedly" · from code

**Where:** [handlers.py:76-80](src/etl_craft/handlers.py#L76-L80), [scripts.py:153](src/etl_craft/scripts.py#L153)

`dispatch` wraps only `ConfigError` and `SQLAlchemyError` into `HandlerError`. Anything else
escapes into the crash-detection fork, kills the child, and the parent writes the generic *"task
process died unexpectedly (exit code 1) before recording its own outcome"* — losing the real
message. The most likely instance: `SCRIPT_NAME` pointing at a file that does not exist raises
`FileNotFoundError` from `subprocess.run`, so the single most common `HANDLER=PYTHON`
misconfiguration produces an error message that says nothing about it.

**Fix direction:** in `_dispatch_and_record`, catch `BaseException` around `dispatch`, write
`FAILED` with the real exception type and message plus a traceback into `TASK_LOG`, then re-raise
or exit non-zero. The parent's generic fallback then only ever covers what it was designed for
(OOM-kill, segfault). Separately, have `scripts.py` check the script exists and is readable up
front, and add an explicit `FileNotFoundError`/`PermissionError`/`OSError` catch.

### E2-10 — The Engine DB JDBC URL's query string is silently discarded · from code

**Where:** [db.py:23-25](src/etl_craft/db.py#L23-L25), [db.py:39-46](src/etl_craft/db.py#L39-L46)

`_JDBC_POSTGRES_RE` captures host/port/database and stops at `?`; `parse_jdbc_postgres` returns
only those three, and `_password_creator` passes only those three to `psycopg.connect`. So
`jdbc:postgresql://host/db?sslmode=require` connects **without** `sslmode=require`, with no
warning. `warehouse.py`'s own translator does parse the query string and forward it
([warehouse.py:79](src/etl_craft/warehouse.py#L79)) — so the Engine DB is the weaker of the two,
and it is the one that is always Postgres and always required.

**Fix direction:** parse and forward the query parameters (or reuse `warehouse.translate_jdbc_url`
and drop the duplicate regex entirely); at minimum raise on a query string rather than ignore it.
This one is security-relevant — silently dropping `sslmode` is worse than failing.

### E2-11 — The cross-pipeline poll deadline is per-edge, not the specified one hour overall · from code

**Where:** [crosspipe.py:197](src/etl_craft/crosspipe.py#L197), [crosspipe.py:361](src/etl_craft/crosspipe.py#L361)

`CLAUDE.md` specifies "a hard 1-hour timeout overall". `deadline` is computed inside
`_wait_for_*_to_settle`, which is called once **per edge** in the `for edge in edges` loop. A task
with three cross-pipeline edges can wait three hours; `MAX_POLLS = 30` likewise resets per edge.

**Fix direction:** compute the deadline and the poll budget once in
`check_pipeline_dependencies`/`check_task_cross_pipeline_dependencies` and pass them down. Also
worth reconsidering the whole shape: a task blocking a process (and an Airflow worker slot) for an
hour is expensive — an Airflow-native sensor with `reschedule` mode does this without holding a
slot. That would be an orchestrator-specific mechanism, which Non-goals rules out for the engine
itself, but the *generated DAG* could legitimately emit one. Ask before building.

### E2-12 — The dependency tracker consumes a run the gate never actually used · from code

**Where:** [crosspipe.py:252-279](src/etl_craft/crosspipe.py#L252-L279), [crosspipe.py:411-442](src/etl_craft/crosspipe.py#L411-L442)

`consume_*_dependency_edges` re-runs `_*_dependency_satisfied` at finalize time and records
whatever qualifies *then*, not the run the gate resolved at start time. If the upstream pipeline
completes a second qualifying run while this one is executing, the watermark jumps past it, and
that run is marked consumed by a pipeline that never read its data. The whole point of the
tracker is "the run last *consumed* for each dependency edge" — this makes that claim false in
the exact case (pipelines on different cadences) the tracker was designed for.

**Fix direction:** capture the candidate run id at gate time and thread it to the consume call.
`check_*_dependencies` already computes it and throws it away (`satisfied, _ =`). This changes a
function signature and the value stored, so confirm the intended semantics with the user first.

---

## P1 — adoption blockers

`CLAUDE.md`'s first paragraph: "ships as a PyPI package (uv-compatible), and is meant to be
adoptable by any team". A team that runs `uv add etl-craft` today cannot get to a working install.

### E2-13 — The wheel ships no SQL, no type marker, and no license · verified

Built the wheel and listed it: **24 `.py` files and nothing else.** No `sql/schema.sql`, no
`sql/migrations/`, no `py.typed`, no `LICENSE` in `dist-info`. Consequences:

- **There is no way to create the Engine DB from an installed package.** `schema.sql` is the only
  authoritative full definition and it exists only in the git checkout. No `etl-craft init-db`
  verb exists either.
- `etl-craft migrate` silently no-ops (E2-05).
- Consumers get no type information despite the codebase being fully annotated.
- `[project]` has no `license`, `classifiers`, `keywords`, or `urls`, and `README.md` is the
  string `# etl-craft` — that is the PyPI landing page.

**Fix direction:** include `sql/**` as package data under `src/etl_craft/sql/` (uv_build packages
`src/<module>/` only), read it via `importlib.resources`, add `src/etl_craft/py.typed`, add an
`etl-craft init-db` (or `bootstrap`) command that applies the packaged schema to an empty Engine
DB, and fill in the packaging metadata. Add a CI step that builds the wheel, installs it into a
clean venv, and runs `etl-craft --help` + `init-db` + `migrate` against a throwaway database —
this class of bug is invisible to a test suite that always runs from the checkout.

### E2-14 — `generate-yml`'s output isn't consumable by anything · from code

**Where:** [generate_yml.py:120-160](src/etl_craft/generate_yml.py#L120-L160)

The shape is bespoke and undocumented outside the module docstring, and nothing converts it into a
real DAG. Two specific gaps make it not just unconsumed but *incorrect* if someone writes the
obvious loader:

1. **`dependency_type` is never translated into an Airflow trigger rule.** A `FAILURE` edge needs
   `trigger_rule="all_failed"`, `ALWAYS` needs `all_done`, `SUCCESS` is the default `all_success`.
   Emitting the raw `dependency_type` and leaving the mapping to the reader means the mapping will
   be got wrong, and it is the single thing that makes a generated `EMAIL_ALERT` task behave
   correctly. (This also interacts with E2-01.)
2. **`HAS_DATA` has no Airflow equivalent at all** and needs an explicit documented answer
   (closest is `all_done` plus the engine's own in-task check, which already happens).

Also: the global DAG emits only `dag_id` + `pipelines` — no `schedule`, no `catchup`, no
`default_args` — so it cannot actually run; `dag_id` is the bare `PIPELINE_CODE` with no
namespace, which will collide in a shared Airflow; `sla_hours` is emitted but Airflow wants an
`sla` timedelta in `default_args`; and `bash_command` interpolates codes with no quoting (a
`PIPELINE_CODE` containing a shell metacharacter breaks it — see E2-25).

**Fix direction:** emit `trigger_rule` directly alongside (or instead of) `dependency_type`; give
the global DAG the same dag-level fields; add a documented, *example* `dag_loader.py` in the repo
(never a dependency, never imported by the engine) that turns this YAML into `BashOperator`s, and
point at it from the README. Confirm the field-name freeze with the user before the reference
implementation repo starts depending on the shape.

### E2-15 — No user-facing documentation at all · verified

`README.md` is one line. There is no install guide, no quickstart, no `craft-connector.yml`
example file, no `CFG_` row cookbook, no `CFG_TASK_PARAMETERS` reference outside module
docstrings, no exit-code table, no operational runbook. `CLAUDE.md` is excellent but it is a
design/history document written for this repo's own contributors — not something to hand a team
adopting the engine, and it is 138 KB.

**Fix direction:** a real `README.md` (what it is, install, five-minute quickstart against
Docker Postgres, the CLI table, where to go next); `docs/` with a `craft-connector.example.yml`,
a parameter reference per `HANDLER` extracted from the module docstrings, and a "register your
first pipeline" walkthrough with the actual `INSERT` statements. Note `generate-docs` already
produces a site *from* a populated Engine DB — that is complementary, not a substitute.

### E2-16 — `configure` never tells you which secret to set, and nothing tests the connection · verified

**Where:** [configure.py:137-221](src/etl_craft/configure.py#L137-L221)

`configure` writes a profile whose secret is looked up as `ETL_CRAFT_{SECTION}_{PROFILE}_SECRET`
and never mentions that name. Grepping `configure.py` for "secret" finds only the two `[Source]`
prompts. So the flow is: run `configure`, answer every prompt, then have the next command fail
with `secret 'ETL_CRAFT_POSTGRES_DEV_SECRET' not found`. There is also no way to verify a
configuration short of running a real pipeline.

**Fix direction:** print the exact env var name (or `.env` line) each profile needs at the end of
`configure`, and offer to append it to the `[Source] Path` file. Add a `doctor` /
`check-connection` command that resolves config, resolves each secret, opens the Engine DB and (if
configured) the Data DB and `[Email]` relay, and reports each as OK/failed — the single most
useful verb for a new adopter, and it reuses code that already exists.

---

## P2 — operability and robustness

### E2-17 — Nothing has a timeout · from code

- `process.join()` on the handler fork — [runner.py:230](src/etl_craft/runner.py#L230)
- `process.wait()` on every task subprocess — [orchestrator.py:389-390](src/etl_craft/orchestrator.py#L389-L390)
- `subprocess.run` on an ingestion script — [scripts.py:153](src/etl_craft/scripts.py#L153)
- `smtplib.SMTP(host, port)` with no `timeout=` — [email_alert.py:235](src/etl_craft/email_alert.py#L235)

A hung query, a wedged ingestion script, or an unreachable-but-accepting SMTP relay hangs the task
forever with the `AUD_TASK_RUN_LOG` row stuck `IN-PROGRESS`, which (per `resolver.NOT_RETRYABLE`)
makes that task permanently un-retryable — the pipeline can never recover without manual SQL.
Note `CFG_PIPELINES.SLA_IN_HOURS` already exists, is read by `fetch_pipeline_detail`, emitted into
the YAML, and enforced nowhere (E2-23).

**Fix direction:** a `TASK_TIMEOUT_SECONDS` parameter (per task, with an `[Execution]`-level
default), enforced by `join(timeout=...)` + terminate + `FAILED`; `timeout=` on `subprocess.run`
and `smtplib.SMTP`. Ask the user whether the timeout should be per task, per pipeline, derived
from `SLA_IN_HOURS`, or all three.

### E2-18 — No logging, only `print` · from code

Every diagnostic in the codebase is `print`/`print(file=sys.stderr)`. No `logging` usage anywhere,
so no levels, no `--verbose`/`--quiet`, no log file, no timestamps, no way to correlate lines
across the parent process, the crash-detection fork, and N task subprocesses. For an ETL engine
whose failures are debugged after the fact, this is the biggest day-2 operability gap.

**Fix direction:** route everything through `logging` with a single `_setup_logging(verbosity)` in
`cli.py`; put `pipeline_code`/`task_code`/`pipeline_run_id` in the log record (a filter or an
adapter) so subprocess output is attributable; keep stdout clean for the read-only query verbs
whose output is meant to be piped (`list`, `steps`, `history`, `lineage`, `generate-yml`).

### E2-19 — Unbounded fan-out: no parallelism cap anywhere · from code

- [orchestrator.py:374-390](src/etl_craft/orchestrator.py#L374-L390): one subprocess per ready
  task, all at once. A 40-task wave spawns 40 Python processes, each building its own Engine DB
  engine, each forking a child that builds another one.
- [business_rules.py:241](src/etl_craft/business_rules.py#L241):
  `ThreadPoolExecutor(max_workers=len(wave))` — one thread and one Data DB connection per rule in
  the wave.
- No `pool_size`/`max_overflow` is ever set, and `pool_recycle` is never set at all — see E2-35.

**Fix direction:** a `Max_parallel_tasks` setting in `[Execution]` (and a per-wave cap for
business rules), with documented defaults, plus explicit pool sizing so the connection budget is a
stated number rather than an emergent one.

### E2-20 — Task output is thrown away · from code

`subprocess.Popen(cmd)` in `_run_wave` inherits the parent's stdout/stderr, so parallel tasks
interleave their output with no attribution and nothing is persisted. `scripts.py` captures an
ingestion script's stdout/stderr and then **discards both on success** — `TASK_LOG` gets only the
declared `RETURN_VALUES`. A run that succeeded but produced wrong data leaves no trace of what the
script said.

**Fix direction:** capture per-task output to a file (or into `TASK_LOG`, truncated) and name the
location in the log line. Pair with E2-18.

### E2-21 — Retry history is destroyed · from code

**Where:** [runlog.py:170-209](src/etl_craft/runlog.py#L170-L209)

`update_task_run` updates one row in place (correct per `CLAUDE.md`) but: `START_DATE` is never
refreshed, so a retried task's duration spans from the first attempt — which feeds
`_average_task_duration_seconds`, which drives the poll cadence. `COALESCE(:x, X)` on every count
means a retry that reports fewer fields silently keeps the previous attempt's values, mixing two
attempts' numbers in one row. And there is no attempt counter and no previous error, so
`AUD_TASK_RUN_LOG` cannot answer "how many times did this fail before it worked?".

**Fix direction:** add `ATTEMPT_COUNT` (incremented on each re-dispatch) and reset
`START_DATE`/counts/`ERROR_MESSAGE` at the start of each attempt rather than `COALESCE`-ing them
forward. Keep one row per task per run — that constraint is load-bearing. Requires a schema change
and therefore the first real entry in `sql/migrations/`, which conveniently also exercises E2-05.

### E2-22 — Cloning materializes whole tables in memory; `AUD_` tables grow forever · from code

**Where:** [cloning.py:265-283](src/etl_craft/cloning.py#L265-L283)

`rows = [dict(row) for row in conn.execute(select(source_table)).mappings().all()]` loads an entire
table into a Python list, then truncates the mirror and reinserts. For `AUD_TASK_RUN_LOG` after a
year of daily runs that is millions of rows, in memory, after every pipeline run. There is also no
retention or archival story for any `AUD_` table, and no `VACUUM`/partitioning guidance.

**Fix direction:** stream in batches (`yield_per` + chunked `executemany`) and consider an
id-watermark incremental mode for the append-only `AUD_` tables; separately, propose a retention
policy (a `purge --older-than` verb, or documented partitioning) and get the user's call on it.

### E2-23 — `SLA_IN_HOURS` is plumbed but never enforced · from code

Read by `fetch_pipeline_detail`, converted from `Decimal`, emitted into the YAML as `sla_hours`,
and never compared against anything. Either enforce it (warn/alert when a run exceeds it — a
natural `EMAIL_ALERT` trigger) or document it explicitly as pass-through metadata for the
orchestrator. Currently it reads like a feature.

### E2-24 — The blank-URL-plus-`creator` pattern has already caused two real bugs · from code

**Where:** [db.py:121](src/etl_craft/db.py#L121), [warehouse.py:151](src/etl_craft/warehouse.py#L151)

Both builders do `create_engine("<dialect>://", creator=...)` so no secret is ever rendered into a
logged URL — a good goal. But `engine.url` is then blank, which broke cloning's same-database
guard and ClickHouse's table-engine reflection (both documented in `cloning.py`). It will keep
biting anything that reasonably expects `engine.url.database` to be real.

**Fix direction:** pass a real `URL.create(...)` with everything *except* the password and keep
`creator` for the credential. SQLAlchemy never logs a password it wasn't given, so the original
goal is preserved while `engine.url` becomes truthful.

---

## P3 — validation, tests, CI

### E2-25 — `validate` is far too thin for what it's meant to prevent · from code

Three checks exist (graph integrity, business-rule PK, lineage declarations). Everything else is
discovered at 3 a.m. by a failing task. Candidates, all cheap and all backed by conventions the
code already relies on:

- `SQL_ACTION` is present and in `SQL_ACTIONS`, per `HANDLER='SQL'` task.
- Required parameters per action (`SOURCE_SQL` except `DROP_TABLE`; `MERGE_KEY` +
  `MERGE_COMPARE_COLUMNS` for the SCD merges; `TARGET_OBJECT` always).
- `TARGET_OBJECT` matches `schema.table` — **today a value with no dot crashes with a bare
  `ValueError: not enough values to unpack` from `target_object.split(".", 1)`**
  ([sql_actions.py:401](src/etl_craft/sql_actions.py#L401), and again at 487 and 929).
- `PIPELINE_CODE`/`TASK_CODE`/`TARGET_OBJECT`/`MERGE_KEY` are safe SQL/shell identifiers — they
  are interpolated unquoted into both SQL text and `generate-yml`'s `bash_command`.
- `HANDLER='PYTHON'` has `SCRIPT_NAME` and a `RETURN_VALUES` declaring both mandatory names;
  `HANDLER='EMAIL_ALERT'` has `EMAIL_TO` and (`EMAIL_BODY` or `EMAIL_PIPELINES`).
- `SOURCE_SQL` looks read-only (E2-07); `BUSINESS_RULE_SQL` references the `t` alias
  (`business_rules.py`'s own undocumented-elsewhere convention).
- `DROP_TABLE` has a `CREATE_TABLE` sibling for the same target, and an edge ordering it after it —
  the runtime check exists but only fires during a real run.
- A `HAS_DATA` edge's upstream can actually produce a `TARGET_COUNT` (E2-08).
- `CFG_TASK_PARAMETERS` rows whose `PARAMETER_NAME` isn't in any handler's vocabulary — silent
  typos today.
- A SQL task declaring a pipe-separated multi-value `TARGET_OBJECT` (E2-32).

### E2-26 — `sql/schema_test.sql` cannot fail CI · verified

Its own header says: *"There is no pass/fail summary line; verify by eye that failures land exactly
on the statements marked EXPECT FAIL."* CI runs it as `psql -f sql/schema_test.sql` **without**
`-v ON_ERROR_STOP=1` (unlike the `schema.sql` step immediately above it), so the step always exits
0 and nobody is looking. A regression that makes an `EXPECT FAIL` case start succeeding — the
exact class of bug this file exists to catch — passes CI silently.

**Fix direction:** make it self-asserting. Wrap each `EXPECT FAIL` case in
`DO $$ BEGIN <stmt>; RAISE EXCEPTION 'expected failure did not occur: ...'; EXCEPTION WHEN
check_violation THEN NULL; END $$;` (matching each case's documented SQLSTATE class), keep the
`EXPECT SUCCEED` cases as plain statements, and run the whole file under `ON_ERROR_STOP=1`. Then
the exit code means something and the "verify by eye" note can go.

### E2-27 — CI runs `schema_test.sql` against the same database as the test suite · verified

The `Makefile` was fixed to run it in a disposable `etl_craft_schema_test` database precisely
because its leftover `PL_A`/`PL_B` rows broke a globally-scoped query
(`cfg.fetch_all_pipeline_dependency_edges`). `.github/workflows/ci.yml` still runs it against
`etl_craft`, before `pytest`, leaving those rows behind. It passes today by luck of which global
queries exist; the local fix and CI now disagree about a known hazard. `schema_test.sql` also
hardcodes `PIPELINE_ID = 1`/`2`, so it is only correct against a genuinely empty database.

**Fix direction:** mirror the Makefile — create/apply/drop a disposable database in CI too.

### E2-28 — CI gaps · verified

- Only Python 3.11, though `requires-python = ">=3.11"` claims 3.12/3.13 support.
- No wheel-build/install smoke test (which is why E2-13 went unnoticed).
- `on: pull_request` only — nothing runs on push to `main`, so `main` can be broken by a merge
  whose base moved.
- No `mypy` (E2-29).

### E2-29 — Fully annotated code, no type checker · verified

Every module uses `from __future__ import annotations` and complete signatures, and nothing checks
them. `mypy --strict` (or `pyright`) would have caught, among others, the `split(".", 1)`
unpacking hazard in E2-25 and the `Any`-typed config values in E2-34. Add it to `make check` and
CI; expect a first pass of real findings.

### E2-30 — Scenario coverage gaps · verified

368 tests pass and all four P0 bugs live in untested space. The coverage gate (80%, actual 100%)
measures lines, not scenarios. Add these specific cases, each of which is a permanent regression
test for an item above:

| Scenario | Item |
|---|---|
| Successful pipeline containing a `FAILURE`-gated `EMAIL_ALERT` task, end to end, both modes | E2-01 |
| `run_task` invoked against a task whose row is already `IN-PROGRESS` | E2-02 |
| `validate` against a target built by `CREATE_TABLE`/`SETUP_TABLE` | E2-03 |
| SCD1 and SCD2 merges with duplicate `MERGE_KEY` rows in the source, across two runs | E2-04 |
| `migrate` with a migrations dir that doesn't exist, and against a DB with no `SCHEMA_MIGRATIONS` | E2-05 |
| `HANDLER=PYTHON` with a nonexistent `SCRIPT_NAME` (assert the message names the file) | E2-09 |
| Engine DB `jdbc_url` carrying `?sslmode=require` (assert it reaches the driver) | E2-10 |
| A task with two cross-pipeline edges, both polling (assert one shared deadline) | E2-11 |
| An upstream that completes a second qualifying run mid-execution | E2-12 |
| `TARGET_OBJECT` with no `.` (assert a clean `HandlerError`, not `ValueError`) | E2-25 |

---

## P4 — portability, consistency, cleanups

### E2-31 — Dialect-portability claims that don't hold · from code

`sql_actions.py` is careful about ANSI portability (avoids `UPDATE...FROM`, avoids `MERGE`,
explains each exception) but:

- `_hash_expression` uses Postgres-only cast syntax: `COALESCE({alias}.{c}::text, '')`
  ([sql_actions.py:357](src/etl_craft/sql_actions.py#L357)). Should be `CAST(... AS VARCHAR)`,
  which the module uses everywhere else.
- `MD5()` return shape varies — ClickHouse's returns a 16-byte `FixedString`, not hex text, so
  `HASH_KEY VARCHAR(32)` is wrong there. **Verify against the local ClickHouse before fixing**;
  the likely answer is a small per-dialect hash expression keyed off `dialect.name` (the pattern
  `cloning.py` already established) rather than assuming one spelling.
- `AUDIT_COLUMN_TYPES` uses bare `VARCHAR` with no length for `CREATED_BY`/`UPDATED_BY`, which
  several dialects reject in DDL.
- `_add_hash_key` uses `UPDATE {stage} AS s SET ...`; an alias on an `UPDATE` target is not ANSI.
- The **atomicity guarantee is Postgres-specific**: `CREATE_TABLE`, `SETUP_TABLE`, `_evolve_schema`
  and `TRUNCATE` are all DDL, which auto-commits on MySQL and Oracle. The module docstring's "a
  failure partway through rolls back everything this module did" is therefore not true on every
  supported warehouse. Worth stating plainly in the docstring rather than leaving implied.

### E2-32 — `TARGET_OBJECT` carries two incompatible contracts · from code

Lineage says `SOURCE_OBJECT`/`TARGET_OBJECT` are pipe-separated for tasks with more than one
([cfg.py:561-574](src/etl_craft/cfg.py#L561-L574)). `sql_actions` requires `TARGET_OBJECT` to be
exactly one `schema.table` and `fetch_sibling_target_writer` matches it with `=`. So a `HANDLER=SQL`
task that honours the lineage convention for two targets is broken by construction — `qualify`
produces `db.a.b|c.d` — and nothing catches it. Relatedly, `SOURCE_OBJECT` is required on every
task by `validate` but read by nothing except `lineage`, so it can drift arbitrarily from what
`SOURCE_SQL` actually reads: the lineage graph can be silently wrong.

**Fix direction:** either give lineage its own parameter names (`LINEAGE_SOURCES`/
`LINEAGE_TARGETS`) so the functional `TARGET_OBJECT` stays single-valued, or forbid multi-value
`TARGET_OBJECT` on SQL tasks in `validate`. Needs the user's call on which. Also worth deciding
whether `SOURCE_OBJECT` should be cross-checked against `SOURCE_SQL` at all, or documented as
"declared, not verified".

### E2-33 — Data DB audit timestamps are timezone-naive · from code

`schema.sql` uses `TIMESTAMPTZ` throughout, and the engine writes `datetime.now(UTC)`. But
`AUDIT_COLUMN_TYPES` declares `CREATE_DATE`/`UPDATE_DATE` as plain `TIMESTAMP`, so every
warehouse-side audit timestamp silently loses its offset. Use `TIMESTAMP WITH TIME ZONE` where the
dialect supports it, or document the naive-UTC convention explicitly.

### E2-34 — `craft-connector.yml` parsing is permissive in unhelpful ways · from code

- Unknown keys are silently kept in `extra` or ignored — a typo'd `secret_var` or `jbdc_url`
  produces a confusing downstream error instead of "unknown key".
- `[Orchestrator]` scalars are taken straight from `raw.get(...)` with no type check
  ([config.py:328-337](src/etl_craft/config.py#L328-L337)) — `Retries: "three"` flows into the
  generated YAML.
- `_load_dotenv_file` lets `FileNotFoundError`/`PermissionError` escape as raw `OSError`
  ([config.py:415](src/etl_craft/config.py#L415)); the CLI only catches `ConfigError`, so a missing
  secrets file is a traceback.
- The secrets file is re-read on every `resolve_secret` call, and nothing warns about its
  permissions.

### E2-35 — Dead and contradicted code · verified

- `db.TOKEN_POOL_RECYCLE_SECONDS` is defined and referenced nowhere; `pool_recycle` is never set
  on any engine. `CLAUDE.md` is explicit that for `token`/`sso` profiles "`pool_recycle` should
  sit comfortably under the credential's real lifetime" — so the documented behaviour doesn't
  exist. Either wire it up (it matters the moment `token`/`sso` are implemented) or note in
  `CLAUDE.md` that it's deferred with the auth modes.
- `cfg.fetch_task_handler` has no production caller — only a test that exists to cover it.
  Superseded by `fetch_task_execution_detail`; delete both.
- `runlog.TERMINAL_STATUSES` is unused (duplicate of `resolver.TERMINAL_STATUSES`).
- `_check_or_evolve_schema` returns the target's columns and no caller uses the return value.

### E2-36 — Interface inconsistencies · verified

- `graph --name` versus `--pipeline_code` everywhere else — already flagged as a doc inconsistency
  in `cli.py`; pick one (an alias keeps both working) and fix the `CLAUDE.md` table.
- `init_pipeline_run(engine, config, ...)` takes `config` only to `del config` on line 211.
- `main()`'s eleven-branch `if args.command == ...` chain wants `set_defaults(func=...)`.
- No `--version` flag.

### E2-37 — `resolver._check_acyclic` recurses · from code

Depth-first with real Python recursion ([resolver.py:214-229](src/etl_craft/resolver.py#L214-L229)) —
a pipeline with a chain deeper than ~1000 tasks raises `RecursionError` instead of a
`ResolverError`. Unlikely but trivially fixed with an explicit stack.

### E2-38 — N+1 and full-scan read patterns · from code

- `fetch_pipeline_steps` calls `fetch_task_parameters` once per task
  ([cfg.py:668-675](src/etl_craft/cfg.py#L668-L675)).
- `fetch_table_lineage` fetches **every** `SOURCE_OBJECT`/`TARGET_OBJECT` row across all pipelines
  and filters in Python ([cfg.py:843-864](src/etl_craft/cfg.py#L843-L864)) — a documented choice,
  fine at current scale, worth revisiting if `CFG_TASK_PARAMETERS` grows.
- `business_rules` runs two full `SELECT DISTINCT` passes per rule (`EXISTS` then `NOT EXISTS`)
  over the scoped target — one pass with a `CASE`/`FULL OUTER` shape would halve the warehouse
  cost, which is the expensive side.
- `runner.run_task` opens six sequential short-lived connections before dispatch.
- The email digest is N+1 per pipeline.

### E2-39 — A dead Engine DB produces a traceback, not an error message · from code

`RUN_ERRORS` covers the config/resolution exceptions but nothing catches `SQLAlchemyError` /
`OperationalError` around command dispatch in `main()`. The most common real-world failure —
Postgres unreachable mid-command — prints a raw traceback. Add a top-level catch returning exit 2
with a one-line message, matching the convention the rest of the CLI already follows.

### E2-40 — Business rules don't resume on retry · from code

`CLAUDE.md`'s "retry resumes, not restarts" holds at task level but not inside a
`BUSINESS_RULES` task: `_find_or_create_run_log` reuses the existing
`AUD_BUSINESS_RULES_RUN_LOG` row but never short-circuits on an existing `SUCCESS`, so a retry
re-runs every rule in every earlier wave. Idempotent, but wasteful and inconsistent with the
stated principle. Short-circuit on `STATUS='SUCCESS'` under the same `TASK_RUN_ID`.

---

## Deliberately *not* on this list

So the next round doesn't re-litigate settled decisions:

- **Everything in `CLAUDE.md`'s Non-goals** — no orchestrator REST calls, no XCom, no
  `dag-factory`, no multiple Data DBs, no runtime mode-spoof detection, no orchestrator
  connection stores, no bundled third-party dialects, no reaper process. E2-11 and E2-14 touch
  Airflow-shaped territory; both explicitly say to ask first and neither proposes an engine-side
  dependency.
- **A SQL parser dependency.** E2-07 and E2-25 deliberately propose lint-grade string checks.
- **The one-row-per-task-per-run constraint.** E2-21 adds columns to that row; it does not add
  rows.
- **The `$$pipeline_id` two-case rule.** Reverting the auto-append was an explicit correction
  ("this was a bad idea"); leave it alone.
- **The `[Warehouse]` section name, `PIPELINE_CODE`, SMTP transport, the percentage-based poll
  schedule, `information_schema`-based drift detection, and Alembic-lite migrations** — all
  resolved Open Questions. E2-05 fixes migration *plumbing*, not the design choice.
- **Coverage chasing.** The gate is 80% by explicit instruction. E2-30 asks for specific
  scenarios, not a percentage.

## Suggested sequencing

**Superseded 2026-09-20** by the planning session recorded in "Added during iteration-2
planning" below — scope was set to all 40 items plus E2-41/E2-42/E2-43, and several of the
design questions this list defers were answered. The ordering below still holds within each
phase; read the new section's phase list first.

1. **E2-01, E2-02** — wrong status and corrupted audit rows on ordinary, recommended usage. Both
   have reproductions ready to turn into regression tests, and both touch `resolver`/`runner`
   together, so do them as one change.
2. **E2-04** then **E2-03** — data corruption first, then the PK question it depends on. E2-03
   needs a design decision; raise it while E2-04's guard is being built.
3. **E2-13, E2-05, E2-06** — the install path. Nothing else matters to a new adopter until
   `uv add etl-craft` can reach a working Engine DB.
4. **E2-26, E2-27, E2-28, E2-29, E2-30** — make CI able to catch the next round before writing
   more features. Cheap, and E2-26 is currently a gate that doesn't gate.
5. **E2-09, E2-10, E2-08, E2-11, E2-12** — the remaining correctness items.
6. **E2-17, E2-18, E2-19, E2-20, E2-21** — operability, as one coherent "can we run this in
   production" pass.
7. **E2-14, E2-15, E2-16** — adoption polish, once the shape is stable enough to document.
8. **E2-25** and the P4 items — opportunistically, alongside whatever they touch.

---

# Added during iteration-2 planning (2026-09-20)

A planning session walked this backlog against the live code, confirmed every claim it
spot-checked, and set scope. Three corrections to the review above, and three genuinely new
items that came out of the design decisions made there. Items keep the never-renumbered
`E2-nn` scheme.

## Corrections to the review

- **E2-25 is wider than written.** `qualify()` (`sql_actions.py:265-275`) does not validate
  `schema.table` either — it just does `f"{database}.{object_ref}"` — so a dotless
  `TARGET_OBJECT` silently emits a malformed two-part name in `CREATE_TABLE`/`SETUP_TABLE`/
  `OVERWRITE_TABLE`, in addition to the bare `ValueError` at the three `split(".", 1)` sites
  (401, 487, 929).
- **E2-13's list is off by one item.** `readme` *is* declared in `pyproject.toml`; only
  `license`/`license-files`/`classifiers`/`keywords`/`urls` are missing. Everything else in
  that item holds — `find src -type f ! -name "*.py"` returns zero files, so the wheel can
  only ever ship `.py`.
- **E2-26/E2-27 apply locally too, not just in CI.** `make db-schema-test` also runs without
  `-v ON_ERROR_STOP=1`, and `make check` omits both `psql` schema steps CI runs, so the two
  disagree in a second way. Separately, `.github/dependabot.yml` is the unmodified GitHub
  template (`package-ecosystem: ""`), so it updates nothing — fold into E2-28.

## E2-41 — Conditional dependency cardinality (ALL / ANY / N)

Per explicit instruction: *"the dependency should have something like all, one, some etc to
have conditional dependency. lets say a task is dependent on 10 tasks but it can run at least
one meets the condition, it should be possible"*, and on placement: *"it should be in
cfg_tasks. run_type or run_condition etc. because these should be resolved while dag chain
generation itself"*.

**[ADDITION, post-signoff schema change]** Two nullable columns on `CFG_TASKS`:

```sql
RUN_CONDITION        VARCHAR NULL   -- ALL | ANY | N   (NULL == ALL, today's behaviour)
RUN_CONDITION_COUNT  INT     NULL   -- required when RUN_CONDITION = 'N', CHECK (>= 1)
```

**[CHOICE]** Named `RUN_CONDITION`, not `RUN_TYPE` — `CFG_PIPELINES.REFRESH_TYPE` already owns
the `*_TYPE` shape in this schema and "run type" would read as a sibling of it.

**[CHOICE]** Per-task (uniform across that task's own edges), not per-edge OR-groups. The
explicit reason given for the placement was that it must resolve *at DAG-generation time* — a
per-task mode maps 1:1 onto an Airflow trigger rule, whereas OR-groups have no Airflow
equivalent at all and would have to be gated engine-side only. The cost is that "all of A,B
plus any of C,D" cannot be expressed; flag it if that shape turns out to be needed.

Resolution table, used by both `resolver.ready()` and `generate-yml`:

| `RUN_CONDITION` | `DEPENDENCY_TYPE` | engine gate | Airflow `trigger_rule` |
|---|---|---|---|
| ALL | SUCCESS | every edge satisfied | `all_success` |
| ALL | FAILURE | every edge satisfied | `all_failed` |
| ALL | ALWAYS | every upstream terminal | `all_done` |
| ANY | SUCCESS | ≥1 edge satisfied | `one_success` |
| ANY | FAILURE | ≥1 edge satisfied | `one_failed` |
| ANY | ALWAYS | ≥1 upstream terminal | `one_done` |
| N | any | ≥ `RUN_CONDITION_COUNT` satisfied | **no equivalent** — emit `all_done`, engine gates |
| any | HAS_DATA | per `resolver` today | **no equivalent** — emit `all_success`, engine gates |

The two "no equivalent" rows are real gaps, not oversights — state them in `generate_yml.py`'s
docstring and in the generated YAML's own header rather than papering over them.

## E2-42 — Every SQL action except `DROP_TABLE`/`DELETE_ROWS` creates the target if absent

Per explicit instruction: *"apart from drop and delete, everything should create a table if the
target does not exist, using select query and adding audit columns"*.

Today only `CREATE_TABLE` and `SETUP_TABLE` create a target.
`OVERWRITE_TABLE`/`SCD1_MERGE`/`SCD2_MERGE` assume it exists and fail confusingly when it
doesn't. Each should bootstrap the target on a first run using its own `AUDIT_COLUMNS[action]`
set — which is exactly the shape `_setup_table` (`sql_actions.py:609-635`) already builds, so
this is a factor-out plus three call sites, not new machinery.

## E2-43 — `EMAIL_ALERT` becomes a pipeline-level, three-flavour completion alert

Designed in interview during the planning session. **This supersedes the task-level design
currently in `email_alert.py`.** Per explicit instruction: *"task level emails are noise"* and
*"once you exhaust retries and all of the tasks that can be run are ran and failed, then send
one email considering all"*.

- **Scope is the whole pipeline run**, not the watched task. One email per run.
- **Three flavours**, computed from every active task's own status under this
  `pipeline_run_id` — read from `AUD_TASK_RUN_LOG`, *not* `AUD_PIPELINES_RUN_LOG`, which may
  not be finalized yet at the time the alert task runs:
  - `SUCCESS` — every task SUCCESS. Green.
  - `COMPLETED_WITH_ERRORS` — every task settled, but at least one is SKIPPED or recorded a
    failed attempt. Amber. Per instruction: *"if the pipeline is marked success with failure
    then a neutral status like pipeline is COMPLETED with errors"*.
  - `FAILED` — at least one task FAILED. Red.
- **Three templates, one chosen by flavour** (*"have three templates as said in flavour answer
  and choose 1 as needed"*): `EMAIL_SUBJECT_*`/`EMAIL_BODY_*` for each of the three, each
  falling back to today's `EMAIL_SUBJECT`/`EMAIL_BODY` when not declared.
- **Optional status filter** `EMAIL_ON_STATUS` (pipe-separated). Declared → send only when the
  computed flavour is in the list. Absent → always send. Per instruction: *"email task may have
  parameters or may not have as well. if there are parameters saying which status to send, send
  only on that condition, else send on all statuses"*.
- **Condition not met → `SUCCESS`**, with `"no email sent: ..."` in `TASK_LOG`. Deliberately
  not `SKIPPED` — the task ran and correctly chose not to act, and `SUCCESS` keeps it out of
  the unsettled set so it can never re-create E2-01.

**Open, to confirm once E2-21 lands:** today's schema cannot distinguish "succeeded first
time" from "succeeded on retry", so `COMPLETED_WITH_ERRORS` has nothing reliable to key off.
E2-21's `ATTEMPT_COUNT` is what makes it computable — until then the working definition is
"at least one task SKIPPED, or a non-null `ERROR_MESSAGE` on a now-`SUCCESS` row".

## Design decisions settled (do not re-litigate)

| Item | Decision |
|---|---|
| E2-01 | `resolver.unsatisfiable()` + record `SKIPPED`, consumed by both `run_pipeline` and `--finalize-only`; **and** `generate-yml` emits `trigger_rule` as the *single* dependency vocabulary (E2-41's table), replacing `dependency_type` rather than sitting beside it (*"dont do two parameter types"*). |
| E2-03 | `PRIMARY_KEY` as a `CFG_TASK_PARAMETERS` row, **independent of `MERGE_KEY`** — per *"a merge can have both primary key and merge key"*. Issued as `ALTER TABLE … ADD PRIMARY KEY` after the CTAS, re-added by `_evolve_schema` after its drop-and-rename. |
| E2-04 | Dedupe by a **declared ordering** (`MERGE_DEDUPE_ORDER`). Duplicates with no ordering declared → clear `HandlerError` naming the duplicated keys and the parameter to add. The engine never silently drops rows it wasn't told how to order. |
| E2-08 | **[CHOICE]** `INGESTION_COUNT` populates `target_count` *and* `source_count` — what an ingestion script wrote is what a downstream `HAS_DATA` edge means. |
| E2-12 | **[CHOICE]** Capture the candidate run id at gate time and thread it to `consume_*`, so the tracker genuinely records "last consumed" as designed. |
| E2-17 | `TASK_TIMEOUT_SECONDS` task parameter → `[Execution]` global default; `SLA_IN_HOURS` separately enforced at pipeline level, which also closes E2-23. |
| E2-32 | **[CHOICE]** `TARGET_OBJECT` stays single-valued for `HANDLER='SQL'` (enforced in `validate`); pipe-separated multi-value stays legal for lineage on other handlers. Reinforced by E2-42 — an action that creates its own target cannot have two. |

## Phase order for iteration 2

0. Land this file in git (it was untracked) with the additions above.
1. **E2-01, E2-02, E2-41, E2-37**, plus `generate-yml`'s `trigger_rule` half of E2-14 — dependency
   semantics and run status. They interlock; one change.
2. **E2-03, E2-04, E2-42, E2-31, E2-33**, plus E2-25's `schema.table` crash — one coherent pass
   over `sql_actions.py`'s action bodies.
3. **E2-43** — the `EMAIL_ALERT` redesign.
4. **E2-13, E2-05, E2-06, E2-15, E2-16** — the install path.
5. **E2-26, E2-27, E2-28, E2-29, E2-30** — make CI able to catch the next round.
6. **E2-07, E2-08, E2-09, E2-10, E2-11, E2-12** — remaining correctness.
7. **E2-17..E2-24, E2-34..E2-40** and E2-25's remainder — operability and cleanups.

## Still to raise rather than guess

- E2-43's `COMPLETED_WITH_ERRORS` definition, once E2-21's `ATTEMPT_COUNT` exists.
- E2-11's shape — an hour-long in-process poll holds an Airflow worker slot; a
  `reschedule`-mode sensor in the *generated DAG* would not. Orchestrator-shaped, so ask first.
- E2-14's field-name freeze on the `generate-yml` shape, before the
  `metadata-etl-implementation` repo starts depending on it.
- E2-22's retention policy for the `AUD_` tables.

---

# Round 2 review — findings against Phase 1 (2026-09-20)

Same method as round 1: read the full Phase 1 diff (`schema-review..06fcdb7`), confirmed the
baseline (395 passing), then wrote throwaway probes against the live Docker Postgres. Six of the
nine findings below are reproduced; the probe files were deleted, so **turning each into a
regression test is part of fixing it**, exactly as in round 1.

The pattern worth naming: **E2-41's `RUN_CONDITION` was specified over "a task's dependencies",
but implemented over `same_pipeline_edges` only.** `fetch_pipeline_graph` deliberately filters
cross-pipeline edges out before `build_graph` ever sees them, so anything counting edges counts
the wrong set. E2-44 and E2-45 are the two ends of that one mistake. E2-46 is a second instance of
the same general shape — a per-edge value being used where a per-task one is needed.

## E2-44 — `RUN_CONDITION='N'` counts only same-pipeline edges, so a cross-pipeline task hard-fails · reproduced

**Where:** [resolver.py:341-345](src/etl_craft/resolver.py#L341-L345), [cfg.py:251-262](src/etl_craft/cfg.py#L251-L262)

`build_graph`'s new guard compares `RUN_CONDITION_COUNT` against `edge_count[task.task_id]`, built
from the `edges` it was handed. `fetch_pipeline_graph` hands it `same_pipeline_edges` only —
cross-pipeline edges are split off into `cross_pipeline_task_ids` and never passed. A task author
who writes "this depends on 2 things, and 2 must be satisfied" while one of those things lives in
another pipeline gets told their config could never run.

It is not a warning. `build_graph` raises, and **`run` and `graph` turn that into a raw traceback**
(E2-49), so the whole pipeline becomes unusable from the CLI on the strength of a correct config.

**Reproduction:** task `B` with one same-pipeline `SUCCESS` edge and one cross-pipeline `SUCCESS`
edge, `RUN_CONDITION='N'`, `RUN_CONDITION_COUNT=2` →
`ResolverError: task_id=… requires 2 satisfied dependencies but only has 1 — it could never run`.

**Fix direction:** needs the user's call, because it is the same question as E2-45 — **does
`RUN_CONDITION` range over all of a task's dependency edges, or only its same-pipeline ones?**
The instruction it came from ("a task is dependent on 10 tasks but it can run at least one meets
the condition") does not distinguish, and a task author has no reason to. If it ranges over all
of them, `fetch_pipeline_graph` must report the cross-pipeline edge *count* to `build_graph` even
though the edges themselves stay out of the graph, and `ready()` must combine both halves (E2-45).
If it ranges over same-pipeline only, that has to be said in `schema.sql`'s `COMMENT ON COLUMN`
and in `validate`, and the error message must stop claiming the task "could never run" when it
plainly could. Either way, at minimum the guard belongs in `validate` — a config error spanning
two tables should not be raised from the hot path of every `run`.

## E2-45 — `RUN_CONDITION='ANY'` silently means "any same-pipeline edge **and** every cross-pipeline edge" · reproduced

**Where:** [resolver.py:152-168](src/etl_craft/resolver.py#L152-L168), [runner.py:190-197](src/etl_craft/runner.py#L190-L197)

`ready()` applies the cardinality to same-pipeline edges. `run_task` then, separately and
unconditionally, calls `check_task_cross_pipeline_dependencies`, which requires **every**
cross-pipeline edge to be satisfied ([crosspipe.py:399-407](src/etl_craft/crosspipe.py#L399-L407)
returns on the first unsatisfied one). So a task declared `ANY` is gated as `ANY ∧ ALL`. Nothing
in the schema comment, the resolver docstring, or `generate_yml`'s trigger-rule table says so, and
`generate-yml` emits `one_success` — which is a third, different semantic again.

**Reproduction:** task `B`, `RUN_CONDITION='ANY'`, one same-pipeline `SUCCESS` edge (satisfied,
upstream `SUCCESS`) and one cross-pipeline `SUCCESS` edge (never run). `required_edge_count(B)`
returns `1` and that edge *is* satisfied, yet `run_task` returned
`SKIPPED — cross-pipeline task dependency on task_id=… (SUCCESS) not satisfied`.

**Fix direction:** decide with E2-44 as one question. Note the fix is not just arithmetic: making
`ANY` span both halves means the cross-pipeline check can no longer short-circuit on the first
unsatisfied edge — it has to report *which* edges are satisfied and let the resolver count them,
which changes `crosspipe.py`'s return shape from `str | None` to something per-edge.

## E2-46 — One task with mixed `DEPENDENCY_TYPE`s emits several conflicting `trigger_rule`s · reproduced

**Where:** [generate_yml.py:180-199](src/etl_craft/generate_yml.py#L180-L199)

Airflow's `trigger_rule` is a property of the **task** — one value, applied to all its upstreams.
The generated YAML puts it on each `depends_on` **edge**. That is fine only while every edge of a
task shares one `DEPENDENCY_TYPE`, and nothing requires that: `DEPENDENCY_TYPE` is a per-row value
on `CFG_TASK_DEPENDENCY`.

This matters more than the two gaps the docstring *does* flag (`N` and `HAS_DATA`), because those
are documented and safe in a stated direction, whereas this one silently hands a loader a choice
it cannot make correctly — and the round-1 argument for emitting `trigger_rule` at all was
precisely that leaving the mapping to the loader is what makes a generated DAG wrong.

**Reproduction:** task `PMX_C` with a `SUCCESS` edge to `PMX_A` and an `ALWAYS` edge to `PMX_B` →
`depends_on: [{task: PMX_A, trigger_rule: all_success}, {task: PMX_B, trigger_rule: all_done}]` —
two rules for one Airflow task.

**Fix direction:** resolve one rule per task, since that is what the target accepts. Options:
(a) reject mixed types per task in `validate` and emit one task-level `trigger_rule` (simplest,
and consistent with E2-41's own reasoning that `RUN_CONDITION` is per-task precisely so it maps
1:1 onto a trigger rule); (b) emit the weakest safe rule (`all_done`) whenever types are mixed and
let the engine gate, documenting it alongside `N`/`HAS_DATA`. Either way `trigger_rule` should move
from the edge to the task in the emitted shape — ask before changing the shape, since E2-14's
field-name freeze is still open.

## E2-47 — A "not ready yet" single-task run writes a terminal `SKIPPED` that removes the task from the run for good · reproduced

**Where:** [runner.py:190-204](src/etl_craft/runner.py#L190-L204), [resolver.py:31](src/etl_craft/resolver.py#L31)

`run_task` writes `SKIPPED` for **both** "dependencies not met yet" and "dependencies can never be
met". `_describe_unready` (new in Phase 1) carefully distinguishes the two — but only in the
*message*. Both still call `_bind_as_skipped`, and `SKIPPED` is in `NOT_RETRYABLE` and
`SETTLED_STATUSES`, so the task is permanently disqualified from that `pipeline_run_id`: `ready()`
will never offer it again, `_run_until_settled` treats it as settled, and the pipeline finalizes
**`SUCCESS` with that task never having run**.

CLAUDE.md explicitly supports the paths that trigger this — "a manual single-task run, a backfill,
a re-triggered task" — and says the engine must "verify same-pipeline dependencies itself". The
verification is currently destructive.

**Reproduction:** `B` depends on `A` (`SUCCESS`). Ran `B` first → `SKIPPED — same-pipeline
dependencies not met`. Then set `A` to `SUCCESS` and ran `B` again → still `SKIPPED`, with the now
actively false message *"same-pipeline dependencies not met … (needs 1 of 1 edge(s) satisfied)"*,
and `B`'s row still reads `SKIPPED`.

**Fix direction:** only the "can never be satisfied" branch should write a terminal row — that is
E2-01's deliberate new behaviour and it should stay. "Not yet" should write **nothing**, report a
distinct outcome, and exit 0 (the same shape E2-02 just established for `IN-PROGRESS`).
`_describe_unready` already computes the distinction, so this is a branch, not new machinery.

## E2-48 — With no active run, a task binds to and rewrites an already-finalized previous run · reproduced

**Where:** [runlog.py:103-128](src/etl_craft/runlog.py#L103-L128), [generate_yml.py:186-192](src/etl_craft/generate_yml.py#L186-L192)

`resolve_run_for_task`'s dev/ad-hoc fallback — bind to the latest logged run when nothing is
`IN-PROGRESS` — is CLAUDE.md point 5 and is correctly implemented. What changed is its blast
radius. Every generated root task now depends on `__init__` with `trigger_rule: all_done`, so when
`__init__` fails (Engine DB blip, unmet cross-pipeline gate, bad secret) **Airflow starts every
root task anyway**, and each one takes this fallback into the *previous, already-finalized* run —
updating its `END_DATE` and rewriting its `AUD_TASK_RUN_LOG` rows.

CLAUDE.md calls this path a "dev/ad-hoc convenience… not an everyday scenario — don't over-engineer
around it". It is now an everyday scenario in orchestrator mode, which is a reason to revisit it
rather than to leave it.

**Reproduction:** a pipeline whose only run was finalized `FAILED`, with a `FAILED` task row under
it. Called `run_task` with nothing `IN-PROGRESS` → the task bound to that finalized run id and
overwrote its row; the task has exactly one row and it belongs to the old run.

**Fix direction:** two independent halves. (1) Emit `all_success`, not `all_done`, for the
`__init__` edge — a task whose run was never minted has nothing correct to do. (2) Make the
fallback refuse to bind to a run that is already terminal unless something says this is an ad-hoc
invocation (a `--force`, or an explicit flag), so the production path fails loudly instead of
silently editing history. Both change documented behaviour, so confirm before building.

## E2-49 — `ResolverError` escapes `run` and `graph` as a raw traceback · reproduced

**Where:** [cli.py:74-79](src/etl_craft/cli.py#L74-L79) (`RUN_ERRORS`), [cli.py:309](src/etl_craft/cli.py#L309)

`RUN_ERRORS` lists `CfgError`, `RunLogError`, `ForceNotAllowedError`,
`OrchestratorModeRefusedError` — not `ResolverError`. `_graph_command` catches `CfgError` and then
calls `build_graph` outside the `try`. Only `generate-yml` catches it, and only `validate` handles
it properly (it reports it as a clean `[graph]` issue, which is the behaviour the others should
have).

Pre-existing, but Phase 1 made it much more reachable: `build_graph` gained four new ways to raise
(unknown mode, count with no mode, count on a mode that ignores it, count exceeding the edge
count), and `--finalize-only` now builds a graph where it previously did not — so a config problem
can now take out the synthetic *last* step of a generated DAG too.

**Reproduction:** with E2-44's config, `main(["graph", …])` and
`main(["run", "--pipeline_code", …, "--task_code", …])` both raised `ResolverError` out of `main`;
`main(["validate"])` printed `[graph] pipeline 'TEST_XPIPE_DOWN': …` and exited 1.

**Fix direction:** add `ResolverError` to `RUN_ERRORS` and wrap `_graph_command`'s `build_graph`.
Then, per E2-39, add the top-level `SQLAlchemyError` catch so `main` has no path left that
tracebacks on a non-bug.

## E2-50 — `settle_unsatisfiable_tasks` can re-create the E2-02 clobber through a different door · from code

**Where:** [orchestrator.py:128-162](src/etl_craft/orchestrator.py#L128-L162)

It reads `run_state` in one transaction, computes `unsatisfiable()`, then writes in a **second**
transaction, with nothing re-checking that the rows are still absent. `unsatisfiable()` only
returns tasks whose status is `None`, but between the read and the write a concurrent
`run --task_code` can create that row and start executing — and `find_or_create_task_run` then
returns the *live* row, which `update_task_run` overwrites with `SKIPPED`. That is precisely the
failure E2-02 just fixed, reached from the other side.

The window is real, not theoretical: `_run_until_settled` calls this **every pass** while task
subprocesses are running, and in orchestrator mode `finalize_active_run` calls it while Airflow may
still be running a task the `__finalize__` step's `all_done` rule did not wait for.

**Fix direction:** make the write conditional on the row still being absent. `find_or_create_task_run`
already returns a `TaskRunBinding` but cannot say whether it *created* the row — give it a
`created: bool`, and only update when it did. A guarded `UPDATE … WHERE STATUS = 'IN-PROGRESS' AND
END_DATE IS NULL` is not enough, since a freshly created row looks identical to a live one.

## E2-51 — `waves()` ignores `RUN_CONDITION`, so `graph` and `--force` order `ANY`/`N` tasks wrongly · from code

**Where:** [resolver.py:128-150](src/etl_craft/resolver.py#L128-L150)

`waves()` still places a task only after **every** upstream resolves — the `all(...)` that `ready()`
replaced with `required_edge_count`. Two consequences, both cosmetic-to-moderate rather than
corrupting: `etl-craft graph` shows an `ANY` task in a later wave than it can genuinely run in, so
the printed structure disagrees with what the engine does; and `run_pipeline(force=True)`, which
uses `waves()` precisely because `ready()` can't gate under `--force`, serialises further than it
needs to.

**Fix direction:** decide whether `waves()` should model cardinality at all. There is a good case
that it should not — it is the *static* view, and "earliest possible" and "guaranteed safe" are
different questions — in which case say so in its docstring and in `graph`'s output rather than
leaving the two definitions silently divergent.

## E2-52 — "One vocabulary, not two" holds for `tasks:` only · from code

**Where:** [generate_yml.py:281-295](src/etl_craft/generate_yml.py#L281-L295)

The `pipeline_dependencies` and `cross_pipeline_task_dependencies` blocks still emit
`dependency_type`, while `tasks:` and the global DAG now emit `trigger_rule`. Defensible — those
two blocks are informational and have no Airflow equivalent to map to — but the design note says
*"dont do two parameter types"* without qualification, and a reader of the generated file sees both
words with no explanation of why.

**Fix direction:** trivial either way; the point is to make it deliberate. Either keep
`dependency_type` there and say in the generated file's own header that the informational blocks
use the raw `CFG_` vocabulary on purpose, or carry `trigger_rule` through for consistency and note
that nothing consumes it.

## Suggested placement in the phase order

- **E2-44, E2-45** fold into Phase 1's tail — they are E2-41's own unfinished half, and both need
  the same single design answer. Worth raising **before** Phase 2 starts, since the answer may
  touch `crosspipe.py`'s return shape.
- **E2-47, E2-49, E2-50** join Phase 1's tail too: all three are small, all three are in code Phase
  1 just touched, and E2-47 is a silent-data-loss path.
- **E2-46, E2-48, E2-52** join Phase 4 (the install/consumability path) alongside E2-14 — they are
  all about what a generated DAG actually does when a real Airflow runs it.
- **E2-51** joins Phase 7 with the other cleanups.

## Added to "still to raise rather than guess"

- **Does `RUN_CONDITION` range over cross-pipeline edges?** (E2-44 / E2-45.) Blocks both, and the
  answer changes `crosspipe.py`'s interface, so it is worth answering before Phase 2.
- **Should `trigger_rule` move from the edge to the task in the generated shape?** (E2-46.) Part of
  E2-14's field-name freeze.
- **Should the dev/ad-hoc run-binding fallback still apply under `Mode=orchestrator`?** (E2-48.)
  CLAUDE.md says not to over-engineer it; Phase 1 made it reachable in production.


---

# Round 2 outcome — all nine fixed (2026-09-20, phase 1b)

Every finding above is closed. Three needed a design answer first, all three given explicitly:

| Item | Resolution |
|---|---|
| E2-44 / E2-45 | **`RUN_CONDITION` ranges over ALL of a task's edges**, cross-pipeline included — a task author writing "depends on 10 tasks" has no reason to care which pipeline an upstream lives in. `TaskNode` gained `cross_pipeline_edge_count`, `fetch_pipeline_graph` reports it, and `crosspipe.check_task_cross_pipeline_dependencies` returns a `CrossPipelineCheck` (satisfied count + per-edge reasons) instead of the first-failure `str \| None` that could only ever express ALL. `runner.run_task` now counts both halves against one requirement. **Bonus the new shape buys:** an `ANY` task whose condition is already met from the same-pipeline side never polls its cross-pipeline edges at all, instead of blocking a worker slot for up to an hour on an edge it does not need. |
| E2-46 | **`trigger_rule` moved from the edge to the task**, where Airflow actually wants it, and `depends_on` became a plain list of task names. Mixed `DEPENDENCY_TYPE`s resolve to the permissive `all_done` and let the engine gate — the same documented fail-safe already used for `N` and `HAS_DATA`. Preserves every currently-valid config. |
| E2-48 | **Both halves.** The `__init__` edge emits `all_success`, so a failed run-minting step no longer lets every root task start; and `resolve_run_for_task` refuses to bind to an already-**finished** run without `--force`. Deliberately `SUCCESS`/`FAILED` only, never `SKIPPED` — a `SKIPPED` run is exactly what `run_task` is designed to bind to and record `SKIPPED` under, so catching it would have broken the cross-pipeline gating flow. That interaction was caught by an existing test, not by review. |

The other six needed no design call:

- **E2-47** — only "can never be satisfied" writes a terminal `SKIPPED` now. "Not yet" writes **nothing** and exits 0, the same shape E2-02 established for `IN-PROGRESS`, so a premature manual run no longer disqualifies a task from its own pipeline run.
- **E2-49** — `ResolverError` joins `RUN_ERRORS`, and `_graph_command` builds its graph inside its own `try`. Taken together with **E2-39** (pulled forward from phase 7, since it is the same line of defence): `main()` now has a top-level `SQLAlchemyError` catch, so no command tracebacks on an unreachable Engine DB — `list` and `generate-docs` previously had no guard at all.
- **E2-50** — `TaskRunBinding` gained `created: bool`; `settle_unsatisfiable_tasks` writes one transaction per task and only when *it* created the row.
- **E2-51** — `waves()` stays the guaranteed-safe static order by deliberate choice, now stated in its own docstring; `graph` names any task whose `RUN_CONDITION` lets it start earlier, so the two definitions are no longer silently divergent.
- **E2-52** — every generated file now carries a header explaining why `tasks:` speaks `trigger_rule` and the informational blocks speak `dependency_type`.

**407 tests (up from 395), 99.8% coverage, `make check` and `make db-schema-test` clean.**

Two things worth carrying forward as method notes:

- **The E2-50 test initially passed for the wrong reason**, and coverage is what caught it: seeding the `IN-PROGRESS` row up front means `unsatisfiable()` excludes the task before the `created` guard is ever reached, so the guard line stayed uncovered while the test went green. It now drives the interleaving explicitly (monkeypatching `fetch_run_state` to create the row after returning state) and genuinely fails without the guard. A regression test for a race has to reach the race.
- **One of round 2's own premises was wrong in a small way.** E2-45's fix is not purely arithmetic as suggested: because `ready()` is also the orchestrator's wave pre-filter, and the real cross-pipeline gate runs *inside* the spawned subprocess, counting unevaluated cross-pipeline edges pessimistically there would deadlock — the task would never be spawned, so the check that settles it would never run. `ready()` therefore treats unevaluated cross-pipeline edges optimistically and takes real counts only when a caller has them.


---

# Iteration 2 complete (2026-09-20)

**E2-01 through E2-52 are closed except two, named below.** 368 → 500 tests, 97% coverage,
`make check` (black / ruff / pydocstyle / mypy / self-asserting schema test / pytest) and a
wheel build-install-smoke test both clean. Each phase's full reasoning lives in `CLAUDE.md`'s
"Where things stand" section, dated and flagged; this is the index.

| Phase | Items | What changed |
|---|---|---|
| 1 | E2-01, E2-02, E2-41, E2-37, E2-14a | Dependency semantics and run status; `RUN_CONDITION`; `trigger_rule` |
| 1b | E2-44…E2-52, E2-39 | The round-2 review's own findings against phase 1 |
| 2 | E2-03, E2-04, E2-42, E2-31, E2-33, E2-25a | SQL action correctness; `PRIMARY_KEY`; merge dedupe; portability |
| 3 | E2-43 | `EMAIL_ALERT` as a pipeline-level, three-flavour completion alert |
| 4 | E2-13, E2-05, E2-06, E2-15, E2-16 | The install path: packaging, `init-db`, `migrate`, `--config`, docs, `doctor` |
| 5 | E2-26, E2-27, E2-28, E2-29 | Gates that actually gate: self-asserting schema test, matrix CI, wheel job, mypy |
| 6 | E2-07…E2-12 | Remaining correctness |
| addendum | — | Column-level lineage (sqlglot), `DOCUMENTATION` + versioning, fuzzy search, `setup` |
| 7 | E2-17, E2-19, E2-21…E2-24, E2-34…E2-40, E2-25b | Operability and cleanups |

## Scope added during the iteration, beyond this list

Three items came from design decisions (E2-41/E2-42/E2-43, above), and a fourth block came
from a mid-iteration instruction: column-level lineage via sqlglot, a `DOCUMENTATION` task
parameter versioned by content hash, fuzzy matching in both the docs site and CLI errors, and
replacing interactive `configure` with an idempotent dbt-style `setup`. That block **reversed a
Non-goal** — "no SQL parser dependency" — on explicit instruction and on its own stated terms.
`CLAUDE.md`'s Non-goals section records the reversal rather than quietly dropping the entry.

## What the next review should know

- **Three settled decisions were reversed this iteration**, each deliberately and each recorded
  where the original decision lived: the SQL parser Non-goal, interactive `configure`, and
  `run_task`'s "unmet dependency writes SKIPPED" behaviour (E2-47 — only "can never be
  satisfied" writes a terminal row now).
- **Two bugs were found by tests that were passing for the wrong reason**, both surfaced by
  unrelated work rather than by reading: the crash-detection test's stub had a stale signature,
  so its child died of a `TypeError` and never reached the `os._exit` it claimed to exercise;
  and a settle-race test seeded its row too early, so the guard it existed to prove was never
  reached. Coverage caught the second. Worth a skim of any test whose subject is a race or a
  process boundary.
- **Everything `from code` in this file got a confirming test**, and the four reproduced P0s
  were each watched to fail against the pre-fix code before the fix was kept.
- **Deferred, not done — corrected 2026-09-20 after round 3 caught the overstatement (E2-57).**
  The completion claim above originally read "every item is closed" and the phase-7 range was
  written as `E2-17…E2-24`, which swept in two items that were never built:
  - **E2-18 (logging).** There is still no `logging` use anywhere in `src/etl_craft` — verified,
    `grep` returns nothing — no verbosity flag, no log file, and no way to correlate output
    across the parent process, the crash-detection fork and N task subprocesses. `cli.py` alone
    has 71 `print(` calls. It is a genuinely large change and deserves to be its own, but that
    is a reason to defer it, not to record it as done.
  - **E2-20 (task output thrown away).** `orchestrator._run_wave` still uses a bare
    `subprocess.Popen(cmd)` with inherited stdout/stderr, so parallel tasks interleave
    unattributed and nothing is persisted; `scripts.py` still discards a successful script's
    stdout and stderr. Largely blocked on E2-18.
- **Still open, deliberately** — raised and *not* built, for reasons recorded in `CLAUDE.md`:
  E2-11's shape (an hour-long in-process poll holds a worker slot; an Airflow `reschedule`
  sensor would not, but that is orchestrator-shaped), E2-14's field-name freeze on the
  `generate-yml` shape, and E2-22's retention policy for the `AUD_` tables.

---

# Round 3 review — findings against completed iteration 2 (2026-09-20)

Same method. Confirmed the baseline first (500 passing, `mypy` clean, `ruff`/`black`/`pydocstyle`
clean), **ran `scripts/wheel-smoke.sh` for real** (builds, installs outside the checkout,
`init-db` → refuse → `migrate` → `migrate` → `list`/`validate`/`doctor`: all pass), re-ran round 1
and round 2's own probes, then wrote new throwaway probes against the live Docker Postgres **and
the live ClickHouse container**. Probe files deleted; turning each reproduction into a regression
test is part of the fix.

**Independently re-verified as genuinely fixed**, not taken from the phase notes: E2-01, E2-02,
E2-03, E2-04, E2-13, E2-05, E2-06, E2-37. The install path works end to end from a wheel, which
was the biggest single adoption blocker. E2-17's timeouts, E2-21's `ATTEMPT_COUNT`, E2-24's real
engine URL, E2-40's business-rule resume and E2-10's query-string forwarding all read correctly.

Two themes in what follows. First, **the dialect story is now lopsided**: ClickHouse is proven for
connecting, hashing and cloning, and the SQL-action bodies have ClickHouse-specific branches — but
no test has ever run a SQL action against it, and the DDL path does not work (E2-53). Second,
**three of this round's findings are at the seams between two fixes that were each correct alone**
— E2-03 vs. SCD2 (E2-54), E2-48 vs. the mode check (E2-55), the lineage cache vs. read-only
command transactions (E2-56).

## E2-53 — Every SQL action's target-creation path fails on ClickHouse · reproduced

**Where:** [sql_actions.py:_create_target_shape](src/etl_craft/sql_actions.py), `_create_table`, `_setup_table`, `_evolve_schema`, [AUDIT_COLUMN_TYPES](src/etl_craft/sql_actions.py)

Three independent failures, each confirmed by executing the exact DDL these functions emit
against the running `clickhouse/clickhouse-server:24` container:

1. **No `ENGINE` clause.** `CREATE TABLE … AS SELECT …` →
   `Code: 42. ORDER BY or PRIMARY KEY clause is missing. Consider using extended storage
   definition syntax`. `cloning.py` already solved exactly this with `_create_clickhouse_table`
   (a literal `ENGINE = MergeTree() ORDER BY tuple()`, reached via the dialect *name*, never an
   import) — `sql_actions.py` did not reuse the lesson.
2. **`CAST(NULL AS <type>)` into a non-nullable type.** →
   `Code: 70. Cannot convert NULL to a non-nullable type`. Affects every audit column
   `_create_target_shape` and `_setup_table` emit, and `_evolve_schema`'s new-column backfill.
   ClickHouse needs `Nullable(...)` — which `_hash_expression` already learned this iteration,
   for the same reason, two functions away.
3. **`TIMESTAMP WITH TIME ZONE` is a syntax error on ClickHouse.** →
   `Code: 62. Syntax error: failed at position 80 ('WITH')`. **This one is a regression introduced
   by this iteration**: E2-33 changed `AUDIT_COLUMN_TYPES`' `CREATE_DATE`/`UPDATE_DATE` from plain
   `TIMESTAMP` to `TIMESTAMP WITH TIME ZONE`. Correct for Postgres, and it broke a dialect the
   project explicitly supports.

The root cause is a coverage shape, not carelessness: `make_config(warehouse=True)` points
`[Warehouse]` at the *same Postgres*, so every one of the many SQL-action tests exercises one
dialect. ClickHouse has fixtures and is used by `cloning` and `_hash_expression` tests, but no
test ever runs an actual `SQL_ACTION` against it.

**Fix direction:** hoist cloning's pattern into one place both modules use — a small
`create_table_as(conn, name, select_sql)` that appends the engine clause when
`conn.dialect.name == "clickhouse"` — and make `AUDIT_COLUMN_TYPES` a per-dialect lookup rather
than one dict (it already needs three ClickHouse spellings). Then add at least one end-to-end
`SQL_ACTION` test against `clickhouse_engine`, since that is the gap that let this through. Worth
confirming with the user first **how supported ClickHouse actually is** — if the answer is "proven
for cloning, best-effort for actions", say that in CLAUDE.md and stop adding per-dialect branches;
if it is "supported", it needs the same test treatment Postgres gets.

## E2-54 — `PRIMARY_KEY` and `SCD2_MERGE` are mutually exclusive · reproduced

**Where:** [sql_actions.py:_apply_primary_key](src/etl_craft/sql_actions.py), `_scd2_merge`, [validate.py:validate_business_rule_keys](src/etl_craft/validate.py)

E2-03 added `PRIMARY_KEY` so an engine-created table can satisfy the single-column-primary-key
convention `validate` enforces. But an SCD2 target holds **several rows per merge key** by
design — that is what SCD2 *is*. Declaring the natural key as `PRIMARY_KEY` therefore works for
exactly one run and then breaks permanently.

**Reproduction:** SCD2 target, `MERGE_KEY=id`, `PRIMARY_KEY=id`. Run 1 (all new) → `SUCCESS`. Run 2
with a genuine value change → `FAILED`, `duplicate key value violates unique constraint
"…_pkey"`, and the target still holds only the *old* version — the history the merge exists to
record was never written.

The bind: `validate` requires a single-column PK on every business-rule `TARGET_TABLE`, so an SCD2
target with a business rule attached cannot satisfy both rules at once. The only shape that
satisfies both is a surrogate key, which nothing generates.

**Fix direction:** needs a design decision, so **ask before building**. Either (a) SCD2 targets get
an engine-generated surrogate key column that becomes the PK (and `validate`'s convention is then
genuinely satisfiable everywhere), or (b) `PRIMARY_KEY` is rejected by `validate` on `SCD2_MERGE`
tasks and the PK convention is documented as not applying to SCD2 targets — in which case
`validate_business_rule_keys` needs to know which targets those are. Whichever way, `validate`
should catch `PRIMARY_KEY ⊆ MERGE_KEY` on an SCD2 task, because that combination is never valid.

## E2-55 — Under `Mode=orchestrator` a failed task cannot be re-run, and the error recommends a flag that mode refuses · reproduced

**Where:** [runlog.py:resolve_run_for_task](src/etl_craft/runlog.py), [runner.py:run_task](src/etl_craft/runner.py)

E2-48's guard is right: binding to an already-`SUCCESS`/`FAILED` run rewrites audit rows that have
been reported on. But it lands on top of `ForceNotAllowedError`, and the two together close the
door completely.

**Reproduction**, one pipeline whose latest run is finalized `FAILED` with a `FAILED` task row:

| invocation | result |
|---|---|
| `local`, plain | `RunLogError: … is already FAILED … or pass --force` |
| `local`, `--force` | proceeds |
| `orchestrator`, plain | `RunLogError: … or pass --force` |
| `orchestrator`, `--force` | `ForceNotAllowedError: --force is only legal under Mode=local` |

So in the mode a real deployment runs in, the "clear a failed task and re-run it" recovery — the
single most common operational action in Airflow — has no route, and the error message points at
a flag that will be refused. A workaround exists (`run --init-only` mints a fresh run, which the
task then binds to) but it changes the `pipeline_run_id`, which is not what someone retrying one
task expects, and nothing says so.

**Fix direction:** at minimum, make the message mode-aware — under `orchestrator` it should name
`--init-only`, not `--force`. Better: decide what a single-task retry against a finished run
should *mean*. Re-opening the run (setting it back to `IN-PROGRESS`) is the semantically honest
answer and has a real objection — `ux_pipeline_run_one_active` and the existing note in
`resolve_run_for_task` about not reopening terminal runs — so this is a question to raise, not to
guess.

## E2-56 — `generate-docs` writes the lineage cache through a connection that never commits · reproduced

**Where:** [cli.py:_generate_docs_command](src/etl_craft/cli.py), [docs_generator.py:118](src/etl_craft/docs_generator.py#L118), [column_lineage.py:lineage_for_tasks](src/etl_craft/column_lineage.py)

`lineage_for_tasks` calls `store_lineage`, which issues `DELETE` + `INSERT` against
`AUD_COLUMN_LINEAGE`. `_column_lineage_command` opens `engine.begin()` and commits. But
`_generate_docs_command` opens `engine.connect()`, whose implicit transaction is **rolled back on
close** — so every row the docs build parses is discarded.

**Reproduction:** one active SQL task with a parsable `SOURCE_SQL`; ran `generate_docs` through
`engine.connect()` exactly as the CLI does → `AUD_COLUMN_LINEAGE` holds **0 rows** afterwards.

Two consequences, and the second is the one that matters:

1. The cache never populates from `generate-docs`, so every docs build re-parses every task.
   Invisible, because the output is identical either way.
2. **A read-only verb now writes.** `docs_generator`'s own docstring still says it "adds no new
   query logic of its own". Against a read-only replica or a read-only DB role — a completely
   reasonable place to point a docs generator — it will now fail outright.

**Fix direction:** decide whether caching is a side effect a read command may have at all. The
cleanest answer is that it is not: give `lineage_for_tasks` a `cache: bool = True` and have
`generate-docs` pass `cache=False`, with an explicit `--refresh-lineage` (or the existing
`lineage --refresh`) as the one verb that writes. If caching in the docs build is wanted, the
command must use `engine.begin()` and say in its help that it writes.

## E2-57 — E2-18 and E2-20 are recorded as closed but were not built · verified

**Where:** this file's "Iteration 2 complete" section; [f21ef57](src/etl_craft/orchestrator.py) (phase 7)

This file states "**Every item on this list is closed: E2-01 through E2-52**", and its
"Still open, deliberately" list names only E2-11's shape, E2-14's field-name freeze and E2-22's
retention policy. Two items in phase 7's stated range are in neither list and were not done:

- **E2-18 (no logging, only `print`).** `grep -rn logging src/etl_craft` returns **nothing**. There
  is still no `logging` use anywhere, no verbosity flag, no log file, no way to correlate output
  across the parent, the crash-detection fork and N task subprocesses. `cli.py` alone has 71
  `print(` calls. The phase-7 commit message enumerates E2-17, E2-19, E2-21, E2-22, E2-24, E2-34,
  E2-35, E2-36, E2-40 and E2-25's remainder — E2-18 is not mentioned.
- **E2-20 (task output thrown away).** `orchestrator._run_wave` still does a bare
  `subprocess.Popen(cmd)` with inherited stdout/stderr, so parallel tasks still interleave
  unattributed and nothing is persisted; `scripts.py` still reads only the trailing JSON line and
  discards a successful script's stdout and stderr entirely.

Both are defensible to defer — E2-18 is a genuinely large change and E2-20 largely depends on it.
Neither is defensible to mark closed. The value of this file is that its status line can be
trusted; an inaccurate one costs more than the items themselves.

**Fix direction:** move both to an explicit "deferred, not done" list with the reason, and correct
the completion claim. Then, if logging is wanted, do it as its own change: `logging` throughout,
one `_setup_logging(verbosity)` in `cli.py`, `pipeline_code`/`task_code`/`pipeline_run_id` on every
record, and stdout kept clean for the verbs whose output is meant to be piped.

## E2-58 — `docs_generator`'s docstring and CLAUDE.md's CLI section both state the opposite of what the code does · verified

**Where:** [docs_generator.py:12-26](src/etl_craft/docs_generator.py#L12-L26), CLAUDE.md's "CLI surface" closing paragraph

The module docstring says the search is "a small, dependency-free vanilla-JS substring search over
the generated search-index.json, **not a vendored copy of Fuse.js/Lunr.js**", and calls out "no
fuzzy matching, no relevance ranking" as the accepted trade-off. Thirty lines further down the
same file, a `[DEVIATION]` says Fuse.js is vendored and used, and `generate_docs` copies
`vendor/fuse.min.js` into the output. CLAUDE.md's CLI-surface paragraph repeats the stale claim
verbatim, while CLAUDE.md's "Where things stand" correctly records the change — so the same file
says both things.

The same docstring's "this module adds no new query logic of its own beyond assembling their
results into pages" is also no longer true: it now parses SQL with sqlglot and writes to
`AUD_COLUMN_LINEAGE` (E2-56).

Small, but this repo's whole method rests on superseded text being marked rather than left to
contradict the code — the `[DEVIATION]`/`[ADDITION]`/`[CHOICE]` convention exists for exactly this.

**Fix direction:** rewrite both passages to describe what the code does, keeping the original
no-CDN reasoning (which still holds and is why Fuse is vendored rather than linked).

## E2-59 — E2-08 was fixed for `PYTHON` only; `HAS_DATA` on a `BUSINESS_RULES` or `EMAIL_ALERT` upstream is now *silently* unsatisfiable · from code

**Where:** [business_rules.py:318](src/etl_craft/business_rules.py#L318), [email_alert.py:438](src/etl_craft/email_alert.py#L438), [validate.py](src/etl_craft/validate.py)

`scripts.py` now reports `target_count` (E2-08, correctly). `business_rules.execute` still returns
`HandlerResult(insert_count=…, update_count=…)` and `email_alert.execute` returns only
`variables` — neither sets `target_count`. `HAS_DATA` is "upstream `SUCCESS` **and**
`TARGET_COUNT > 0`", so an edge on either handler can never be satisfied.

E2-08's own fix direction called for a `validate` check rejecting a `HAS_DATA` edge whose upstream
handler cannot produce a count. There is none — `grep HAS_DATA src/etl_craft/validate.py` is empty.

E2-01's `unsatisfiable()` makes this worse rather than better: the downstream task is now
**silently recorded `SKIPPED`** and the pipeline finalizes `SUCCESS`, where before it at least
showed up as stuck. A config mistake the engine can detect statically now looks like a clean run.

**Fix direction:** the `validate` check E2-08 already specified — cheap, and it turns a silent skip
into a startup error. Decide separately whether `BUSINESS_RULES` should report a count at all
(rows checked? rows flagged?); if the answer is "no meaningful count", then `HAS_DATA` on a
`BUSINESS_RULES` upstream should simply be rejected.

## E2-60 — Nothing makes the pipeline-level `EMAIL_ALERT` actually run last · from code

**Where:** [email_alert.py:run_flavour](src/etl_craft/email_alert.py), [generate_yml.py](src/etl_craft/generate_yml.py)

E2-43 redesigned `EMAIL_ALERT` into a pipeline-level completion alert — "one email per run",
"once you exhaust retries and all of the tasks that can be run are ran". Its flavour is computed
from every active task's status under this run. But how the alert task is *gated* is unchanged:
an ordinary `CFG_TASK_DEPENDENCY`, chosen by whoever writes the config.

Gate it on one task rather than on every leaf and it runs while the rest of the pipeline is still
going. `run_flavour` then sees unsettled tasks and returns `COMPLETED_WITH_ERRORS` — the amber
"something went wrong" email — for a run that goes on to finish cleanly. The `[CHOICE]` in the
docstring is honest that "nothing forces that", but the design's own premise is that this runs at
the end, and nothing in the tooling or `validate` enforces or even warns about it. A second
`EMAIL_ALERT` task in the same pipeline compounds it: `exclude_task_id` excludes only the task
computing the flavour, so each sees the other as unsettled and both send amber.

**Fix direction:** `validate` should require an active `EMAIL_ALERT` task to depend (`ALWAYS`) on
every leaf that isn't itself an alert — the same computation `generate_yml` already does for
`__finalize__`. Cheap, and it makes the design's premise true instead of hoped for. Raise with the
user whether more than one `EMAIL_ALERT` per pipeline should be allowed at all now that the alert
is pipeline-level.

## Suggested handling

- **E2-57 first**, and it costs minutes: correct the completion claim and list E2-18/E2-20 as
  deferred. Everything else in this file depends on its status being trustworthy.
- **E2-54, E2-55** next — both are silent-failure or no-recovery paths in normal operation, and
  both need a design answer before code.
- **E2-53** is a scope question before it is a bug: how supported is ClickHouse? The answer decides
  whether this is three small fixes plus a test, or a documentation change.
- **E2-56, E2-59, E2-60** are each small and self-contained.
- **E2-58** with whatever next touches `docs_generator`.

## Added to "still to raise rather than guess"

- **How supported is a non-Postgres warehouse?** (E2-53.) The code carries per-dialect branches but
  the tests exercise one dialect.
- **What should a single-task retry against a finished run do?** (E2-55.) Re-open the run, mint a
  new one, or stay refused with a better message.
- **How does an SCD2 target satisfy the single-column primary key convention?** (E2-54.)
- **Should `generate-docs` be allowed to write?** (E2-56.)


---

# Round 3 outcome — all eight fixed (2026-09-20, phase 3b)

Every finding is closed. Each was re-verified against the live Docker Postgres and the live
ClickHouse container before being acted on, rather than taken from the write-up.

| Item | Resolution |
|---|---|
| **E2-57** | **Fixed first, and it was the fair one.** The completion claim was wrong: the phase-7 range was written `E2-17…E2-24`, sweeping in E2-18 (logging) and E2-20 (task output), neither of which was built. Both are now listed as **deferred, not done**, with the reason. |
| **E2-53** | **ClickHouse is supported, per explicit decision** — fixed *and* tested. One shared `create_table_as` with the engine clause (hoisted from cloning, as the review suggested), per-dialect `AUDIT_COLUMN_TYPES`, and — the root cause — `make_config(clickhouse_warehouse=True)` plus real SQL-action tests against the container. |
| **E2-54** | **`PRIMARY_KEY` is gone; the engine generates `ROW_ID` instead.** Per explicit correction: "all primary keys are basically identity columns. merge keys are natural keys". That makes the single-column-PK convention satisfiable on *every* target including SCD2, with no exemption for `validate` to know about. |
| **E2-55** | The advice is mode-aware: under `orchestrator` it names `--init-only` and says plainly that gives a new `pipeline_run_id`, instead of pointing at a flag that mode refuses. |
| **E2-56** | `generate-docs` is genuinely read-only. Also **wider than reported** — see below. |
| **E2-58** | Both passages rewritten. The no-CDN reasoning is kept, since it is exactly why Fuse is vendored rather than linked. |
| **E2-59** | `validate` rejects a `HAS_DATA` edge whose upstream handler never reports a row count. |
| **E2-60** | `validate` requires each `EMAIL_ALERT` to depend on every non-alert leaf — the design's premise, now enforced instead of hoped for. |

## Two corrections to the review

- **E2-56 was wider than reported.** The write-up named the lineage cache; the documentation-version
  writes were discarded through the same uncommitted connection. Probed directly: after a
  `generate_docs` through `engine.connect()`, both `AUD_COLUMN_LINEAGE` and
  `AUD_TASK_DOCUMENTATION` held **0 rows**. Fixing it also exposed a design question the review
  did not reach — a page was rendering the last *recorded* documentation text, so an edit stayed
  invisible until someone ran `docs-version`. Pages now show the current parameter text with the
  recorded version beside it, or "unversioned".
- **E2-53 had a fourth and a fifth failure the review did not reach**, both found by the first test
  to run a SQL action against ClickHouse:
  4. **The staging table.** ClickHouse's temporary tables are session-scoped and its HTTP driver
     issues each statement in its own session, so the stage vanished between the `CREATE` and the
     next statement reading it. Every action builds a stage, so this blocked the whole vocabulary,
     not one action. Now an ordinary table there.
  5. **`UPDATE` does not exist.** ClickHouse has only asynchronous `ALTER TABLE … UPDATE`
     mutations, which are explicitly not row-level updates. An SCD merge built on them would
     report `SUCCESS` before the target had changed — strictly worse than failing. `SCD1_MERGE`
     and `SCD2_MERGE` are now **refused up front** on such a dialect with a clear reason, rather
     than producing a raw syntax error from deep inside a merge after the stage was built.

     **This is a real scope limit, stated rather than papered over**: on ClickHouse the
     insert-only actions (`CREATE_TABLE`, `SETUP_TABLE`, `OVERWRITE_TABLE`) work and are tested;
     the merges cannot, short of a genuinely different implementation built on `ReplacingMergeTree`
     and inserts. Worth raising before anyone plans an SCD pipeline on it.

## Also worth carrying forward

- **`qualify()` now takes the dialect.** ClickHouse names objects `database.table` — a three-part
  name is a syntax error — so the profile's database wins and the CFG_ row's schema part is
  dropped there. That keeps the environment-agnostic property but means two CFG_ rows differing
  only by schema **collide on a two-level engine**. `validate` cannot catch it without knowing
  the dialect, so it is a documented limit of pointing `[Warehouse]` at ClickHouse.
- **The E2-33 regression is instructive.** Changing `TIMESTAMP` → `TIMESTAMP WITH TIME ZONE` was
  correct for Postgres and broke a dialect the project claims to support, and it survived a full
  iteration because no test ran a SQL action against a second dialect. The fix that matters is
  the test shape, not the type string.

---

# Superseded: ClickHouse is no longer a supported warehouse (2026-09-21)

Per explicit decision after round 3 landed:

> "okay, then clickhouse goes away. DuckDB is our warehouse now. duckdb and postgresql are the
> ones we want to majorly support. because clickhouse is not really great on ansi"

**This supersedes E2-53 in full**, and with it the "Also worth carrying forward" notes directly
above. Everything E2-53 built for ClickHouse — the per-dialect `AUDIT_COLUMN_TYPES`, the shared
`create_table_as` engine clause, the `Nullable(String)` hash cast, the non-temporary staging
table, the dialect argument on `qualify()`, and the up-front merge refusal — is **deleted**, not
kept behind a flag.

The decision is the one E2-53's own write-up argued for without taking. That entry had to record
that `SCD1_MERGE`/`SCD2_MERGE` **cannot work** on ClickHouse (no row-level `UPDATE`), that two
`CFG_` rows differing only by schema **collide** on its two-level namespace, and that its
session-scoped temp tables broke the staging step every single action depends on. It closed by
saying the scope question — "how supported is ClickHouse actually?" — should be answered first.
It has been, in the other direction: half the action vocabulary did not work there and could not
be made to, so five dialect branches were being carried for a dialect nobody runs.

**DuckDB replaces it as the second supported warehouse**, and closes the real gap E2-53 identified
— that the whole action vocabulary was only ever exercised against one dialect. Unlike ClickHouse,
DuckDB runs **all** of it, merges included, so the end-to-end test is a genuine second-dialect
proof rather than a subset. It is also embedded, which is why `duckdb-engine` became a hard
dependency (a scoped, documented reversal of the Non-goal on bundling dialects — there is no
server to stand up, so shipping it is what makes `setup` reach a working warehouse in one step).

The two ClickHouse-shaped hazards that were documented as permanent limits are gone with it: the
`database.schema.table` collision (`qualify()` is back to always emitting three parts) and the
merge refusal. One genuine DuckDB branch replaces the five: it rejects adding an identity column
to an existing table, so `ROW_ID` uses a sequence default there.

**The lesson from E2-33 still stands and is the reason this is a net improvement, not a retreat**:
the fix that matters is the test shape, not the type string. There is still a second real dialect
in CI — it is just one where passing tests mean the vocabulary works, rather than one where they
mean two thirds of it does.

See `CLAUDE.md`'s dated entry under "Where things stand" for the full change list, including a
DuckDB-specific hazard found by probing: a forked child's writes are silently lost if the parent
had the warehouse file open at fork time. The engine is safe by construction; the constraint is
written into `runner.py` where a future change could break it.

---

# Round 4 review — round 3's fixes, and the DuckDB move (2026-09-21)

Same method. Baseline confirmed first: **473 passing, `mypy`/`ruff`/`black`/`pydocstyle` clean**.
Then re-ran round 3's own probes, then probed DuckDB directly — SQL shapes, process model,
URL handling — against real DuckDB files.

## Round 3 is genuinely fixed — verified, not taken from the notes

| Item | Verified how |
|---|---|
| E2-53 | Superseded by the warehouse change, and **the root cause is actually closed**: `test_sql_actions_run_end_to_end_against_real_duckdb` runs real actions against a second dialect. The fix was the test shape, which is what that entry argued for. |
| E2-54 | Re-ran the probe: SCD2 with a generated key now produces proper history — `[(1,'a','N'), (1,'b','Y')]` across two runs. The surrogate-key route is the right answer. |
| E2-55 | Re-ran the probe: the message is mode-aware now and names `etl-craft run … --init-only` instead of the `--force` that mode refuses. |
| E2-56 | `docs_generator` is read-only again, and says so; `docs-version`/`lineage --column` are the verbs that write. |
| E2-57 | **Fixed the honest way**: E2-18 and E2-20 are recorded as deferred-not-done with reasons, rather than the claim being quietly widened. |
| E2-58, E2-59, E2-60 | Docstring rewritten; `validate` has a `HAS_DATA`-upstream guard; `_alert_ordering_issues` requires each `EMAIL_ALERT` to wait on every non-alert leaf. |

**And the headline positive.** I ran every SQL shape `sql_actions.py` emits against a real DuckDB
file — staging temp table, CTAS with audit casts, `ALTER TABLE … ADD PRIMARY KEY`, `TRUNCATE`,
`MD5` hashing, the correlated `UPDATE` with a target alias, `DELETE … WHERE EXISTS`, the
drop-and-rename rebuild, `information_schema.columns`, `ROW_NUMBER() OVER (PARTITION BY …)`.
**All ten work unmodified.** The ANSI discipline this module has been held to since iteration 1
is what made a warehouse swap cost one dialect branch instead of a rewrite. That is worth saying
plainly, because the four findings below are all about the *process* model, not the SQL.

## E2-61 — DuckDB's file lock is exclusive across processes, and the execution model is subprocess-per-task · reproduced

**Where:** [orchestrator.py:_run_wave](src/etl_craft/orchestrator.py), [handlers.py:dispatch](src/etl_craft/handlers.py), [runner.py:31-61](src/etl_craft/runner.py#L31-L61)

`runner.py` documents the *fork* half of this hazard carefully — if the parent holds the warehouse
file open at fork time the child's writes are lost, and the comment warns a future change not to
open it earlier. That is correct, and it is the smaller sibling. **The larger one is unaddressed:
DuckDB refuses a second process entirely**, and the engine's core execution model is one
subprocess per ready task.

Probed against a real DuckDB file:

| Shape | Result |
|---|---|
| 4 threads, 4 pooled connections, one process (`business_rules._run_wave`) | **OK** |
| A second `Engine` in the same process (`handlers.dispatch` per task) | **OK** |
| Two task subprocesses, one wave (`orchestrator._run_wave`) | **one OK, one `IO Error: Could not set lock on file … Conflicting lock is held`** |
| A read-only verb in another process while a task holds the file | **`Could not set lock on file`** |

So same-process concurrency is fine — business-rule waves genuinely work — and cross-process
concurrency is not. What that costs:

1. **Any wave with two or more `SQL`/`BUSINESS_RULES` tasks fails all but one.** `Max_parallel_tasks`
   defaults to 8, so this is the default behaviour, not an edge case.
2. **Airflow parallelism hits the same wall** — parallel tasks in a DAG are separate processes too,
   so `Mode=orchestrator` is no safer than local.
3. **Two pipelines running at once conflict.** That is ordinary operation —
   `ux_pipeline_run_one_active` is deliberately scoped *per pipeline* precisely so pipelines can
   overlap.
4. **Read-only verbs fail during a run.** `validate`, `doctor`, `generate-docs`, `lineage --column`
   all open the Data DB, so any of them run while a task is executing errors out — including
   `doctor`, the command someone reaches for *because* something looks wrong.

Nothing in `orchestrator.py`, `limits.py` or `config.py` is DuckDB-aware, so nothing caps or
serializes this. The tests miss it for the same structural reason E2-53 identified: every DuckDB
test is single-process (its own `tmp_path` file), and the orchestrator's real subprocess tests
point `[Warehouse]` at Postgres.

**Fix direction — needs a decision, so ask first.** The options are genuinely different products:

- **Serialize Data DB access when the warehouse is embedded.** Resolve `Max_parallel_tasks` to 1
  for a `duckdb` warehouse and say so in `doctor`'s output. Honest, tiny, and gives up the
  parallelism the wave model exists for.
- **Retry on the lock.** A bounded wait-and-retry around `build_data_engine` turns the hard failure
  into queueing. Cheap, keeps the model, but serializes anyway while looking like it doesn't — and
  a task blocked on a lock still holds an Airflow worker slot.
- **Run a DuckDB pipeline in-process.** Contradicts "local runs mirror what an orchestrator does",
  which is load-bearing elsewhere, and does nothing for Airflow.
- ~~**Scope DuckDB to single-writer use** — dev, local, single-task deployments — and document it
  as such rather than as a peer of Postgres.~~ **Ruled out (2026-09-21, explicit):** *"duckdb is
  our preferred warehouse along with postgres, while postgres is the only engine"*. DuckDB is a
  first-class production warehouse, so this has to be **solved**, not documented as a limitation.

That leaves the first three, and it narrows them usefully: whatever is chosen has to keep a
DuckDB deployment correct under a real multi-task wave, not merely warn about it. Serializing is
the honest floor; retry-on-lock is the same thing with better ergonomics and worse legibility;
in-process execution is the only option that preserves parallelism, and it is the one that
conflicts with "local runs mirror what an orchestrator does" — and does nothing for Airflow,
where the processes are Airflow's, not ours. Worth noting that under `Mode=orchestrator` the
engine does not own the process model at all, so the first two are the only ones available there.

Whichever way, two things should land regardless: `doctor` should report the constraint, and
there should be one orchestrator test with a real multi-task wave against DuckDB, since that is
the test that would have caught this.

## E2-62 — The DuckDB catalog name comes from the file stem, unvalidated · reproduced

**Where:** [warehouse.py:translate_jdbc_url](src/etl_craft/warehouse.py), [sql_actions.py:qualify](src/etl_craft/sql_actions.py)

`qualify()` emits `catalog.schema.table`, and for DuckDB the catalog is `Path(path).stem`. Nothing
checks that the stem is a usable SQL identifier:

| `jdbc_url` | catalog | emitted | result |
|---|---|---|---|
| `…/warehouse.duckdb` | `warehouse` | `warehouse.public.t` | fine |
| `…/my-warehouse.duckdb` | `my-warehouse` | `my-warehouse.public.t` | **`Parser Error: syntax error at or near "-"`** |
| `…/etl.craft.duckdb` | `etl.craft` | `etl.craft.public.t` | a four-part name |
| `…/2024_wh.duckdb` | `2024_wh` | `2024_wh.public.t` | leading digit |

Confirmed by creating a table through a `my-warehouse.duckdb` profile: every SQL action fails with
a parser error that never mentions the file name. A hyphen in a filename is not an exotic choice,
and this is the sort of thing found at 3am rather than at setup.

`validate`'s identifier-safety check (E2-25b) cannot catch it — it checks `CFG_` values, and this
one comes from `craft-connector.yml`.

**Fix direction:** validate the derived catalog where it is derived, and fail in `doctor`/`setup`
with a message naming the file, not at the first action. Quoting the identifier is the other
option and is worse here: it would make the catalog case-sensitive and diverge from how the
Postgres path builds the same name.

## E2-63 — `jdbc:duckdb:` is documented as in-memory but creates a file called `memory` · reproduced

**Where:** [warehouse.py:translate_jdbc_url](src/etl_craft/warehouse.py), `_none_creator`

`translate_jdbc_url`'s own comment says "`jdbc:duckdb:` alone means an in-memory database" and "An
in-memory database's catalog is `memory`". But `path` is `""` for the bare form, so `_none_creator`
falls through to `database=parts["database"]` — the literal string `"memory"` — and builds
`duckdb:///memory`. DuckDB reads that as **a file named `memory` in the current working
directory**.

**Reproduction:** running two task subprocesses against `jdbc:duckdb:` left a **274 KB file named
`memory` in the repo root**, and the second process saw the first's data — which is how I noticed,
since a real in-memory database could not have shared it. (Deleted; the repo is clean.)

Three consequences: the documented in-memory form does not give in-memory; a stray file appears
wherever each process happened to start, so cwd differences between the orchestrator, a task
subprocess and an Airflow worker can produce several unrelated "warehouses"; and it silently
inherits E2-61's lock problem while looking like it could not.

**Fix direction:** map the bare form to `:memory:` explicitly (`duckdb:///:memory:`). Then decide
whether to support it at all — with `handlers.dispatch` building a fresh engine per task and every
task in its own process, a genuine in-memory warehouse is empty at the start of every task, which
is a worse failure than the current one because nothing errors. If it stays, `doctor` should refuse
it outside a single-process context.

## E2-64 — The DuckDB surrogate-key sequence is named from the bare table name · reproduced

**Where:** [sql_actions.py:_sequence_name](src/etl_craft/sql_actions.py), `_add_surrogate_key`, `_restore_surrogate_key`

`_sequence_name` is `f"etl_seq_{table_name}_{ROW_ID_COLUMN.lower()}"` — the **bare** table name,
with no schema — and the sequence is created unqualified. So `staging.orders` and `marts.orders`,
two perfectly ordinary targets, share one sequence name, and creating the second runs
`DROP SEQUENCE IF EXISTS` against the one the first table's column default depends on.

**Reproduction:** created `staging.orders` with a `ROW_ID` sequence default, inserted rows (ids
1,2), then created `marts.orders` exactly as `_add_surrogate_key` does →
`Dependency Error: Cannot drop entry "etl_seq_orders_row_id" because there are entries that depend
on it`. The first table survived intact (ids 1,2,3 still correct), so this fails **loudly** rather
than corrupting anything — but a valid multi-schema warehouse cannot be built on DuckDB, and the
error names an internal sequence rather than the real cause.

**Fix direction:** include the schema in the sequence name and create it in the target's schema —
`{schema}.etl_seq_{schema}_{table}_row_id`. Worth checking the same question for the
`{table_name}__etl_evolve` rebuild table in `_evolve_schema`, which is schema-qualified and so
looks fine, but shares the shape.

## Suggested handling

- **E2-61 first, and as a question, not a task.** It decides whether DuckDB is a peer of Postgres
  or a single-writer/dev warehouse, and everything else about it follows from the answer. The
  cheapest honest interim step is a `doctor` warning.
- **E2-63 then E2-62** — both small, both in `translate_jdbc_url`/`_none_creator`, both produce
  failures that point nowhere near their cause.
- **E2-64** is a one-line naming fix plus a test.

## Added to "still to raise rather than guess"

- ~~**Is DuckDB a peer of Postgres, or a single-writer warehouse?**~~ **Answered (2026-09-21):** a
  peer. *"we have replaced clickhouse with DuckDB because clickhouse does not support ansi very
  well. duckdb is our preferred warehouse along with postgres, while postgres is the only
  engine"*. So E2-61 is a bug to fix, not a limitation to document — see its narrowed options
  above. **Still open, and now the actual question: which of the three?**
- **Should a bare in-memory DuckDB warehouse be supported at all?** (E2-63.) Every task is its own
  process, so a real in-memory warehouse is empty at the start of each one.

## A note on method

Three of this round's four findings came from probing the *process model* rather than reading the
diff — the code is correct in isolation and the SQL is genuinely portable. Round 3's lesson was
"the fix that matters is the test shape, not the type string"; round 4's is the same lesson one
level up. The DuckDB tests prove the dialect. What is missing is a test that proves the
*deployment*: a real multi-task wave, in real subprocesses, against a real DuckDB file. That one
test would have caught E2-61 and E2-63 together.

---

# Round 4 outcome, and the warehouse decision (2026-09-21)

## Architecture, settled

> "engine: postgres / warehouse: postgres, (duckdb + iceberg)"

- **Engine DB: PostgreSQL.** Unchanged, and still the one hard runtime dependency.
- **Warehouse: PostgreSQL, or DuckDB + Iceberg.** Postgres is a first-class warehouse and the
  one with no caveats. The DuckDB path is intended to become DuckDB-as-compute over
  Iceberg-as-storage; the plain DuckDB *file* is what ships today.

## E2-61 — fixed, by queueing (decision: keep subprocesses)

The subprocess-per-task model stays: it is what crash detection and "local runs mirror an
orchestrator" are built on. `warehouse.data_db()` is now the single way the engine reaches the
Data DB, and on a **single-writer** warehouse it holds a Postgres advisory lock in the *Engine DB*
for the duration, so concurrent tasks queue instead of erroring. For Postgres it is exactly the
old `build_data_engine(...)`/`dispose()` pairing and costs nothing — waves stay fully parallel.

The Engine DB is the right place for the lock because CLAUDE.md makes a valid Engine DB
connection the one hard dependency of every action, so it is reachable from every process that
could contend — including Airflow workers on other machines, where the engine does not own the
process model and therefore cannot serialize by spawning less. `migrate.py` already coordinates
concurrent runs the same way.

**[CHOICE] Queueing, not retrying.** An advisory lock queues fairly; a retry loop on DuckDB's own
`IOException` would spin and can starve a waiter. The wait is bounded — by the task's own timeout
for a task, by 30s for a read-only verb — and the timeout message says what is happening rather
than surfacing "Could not set lock on file".

`doctor` now reports the constraint, and `test_run_pipeline_runs_a_parallel_wave_against_a_duckdb_warehouse`
is the test the review asked for: a real two-task wave, real subprocesses, real DuckDB file.
Verified to fail without the lock (`wave_a` dies on the file lock, pipeline reports `FAILED`).

**When Iceberg lands this largely dissolves**: with tables in object storage behind a catalog,
there is no shared DuckDB file to lock, so `is_single_writer` returns False for an Iceberg-backed
warehouse and the lock stops being taken. The mechanism is written to make that a one-line change.

## E2-62, E2-63, E2-64 — fixed

- **E2-62**: the derived DuckDB catalog name is validated where it is derived. A file whose stem
  is not a usable SQL identifier (`my-warehouse.duckdb`) is refused with a message naming the
  file, instead of every SQL action failing with a parser error that never mentions it.
  **[CHOICE]** reject rather than quote — quoting would make the catalog case-sensitive and
  diverge from how the Postgres path builds the same name.
- **E2-63**: `jdbc:duckdb:` now maps to `:memory:` and genuinely is in-memory. `doctor` then
  refuses it, because every task runs in its own process and would start against an empty
  database — nothing errors, targets simply are not there.
- **E2-64, and it was worse than reported.** The review probed on a single connection, where
  DuckDB refuses the `DROP SEQUENCE` with a dependency error — "fails loudly rather than
  corrupting anything". The engine uses **one connection per task process**, and there the DROP
  **succeeds silently**: both tables end up sharing one sequence that has just been reset. A later
  insert into a table the run never touched then dies with
  `Duplicate key "ROW_ID: 2" violates primary key constraint`, leaving it un-writable until
  someone repairs the sequence by hand. Sequences are now schema-qualified and created in the
  target's own schema. The regression test asserts the *aftermath*, not merely that both tables
  build — a test that stops at creation passes against the bug, which the first draft of it did.

## Also fixed, found while doing the above

**`setup` never wrote a `[Warehouse]` section at all.** The one command meant to take a team from
nothing to a working deployment produced a config in which every `SQL` and `BUSINESS_RULES` task
failed with "no [Warehouse] section configured". Same class of gap as E2-13: the install path
stopped short of a working state. `configure_from_env` now writes `[Warehouse]` from
`ETL_CRAFT_WAREHOUSE_JDBC_URL`/`_PROFILE`/`_USER`/`_AUTH_MODE`, merged the same way `[Postgres]`
is, and optional so an Engine-DB-only setup is unchanged.

## Added to scope: Iceberg as the warehouse storage layer

Recorded as scope, not built. What the analysis turned up, so it is not re-derived:

- **Iceberg is a table format, not a query engine.** It does not execute SQL. Tables are Parquet
  plus metadata in object storage and a catalog tracks snapshots; something still has to run
  `CREATE TABLE AS SELECT` and the SCD merges. Per the decision above that something is **DuckDB**,
  so this is DuckDB-as-compute over Iceberg-as-storage — the `[Warehouse]` profile keeps naming a
  DuckDB connection, and the Iceberg catalog is attached to it.
- **Reads are near-universal; writes are not.** "Supported everywhere" is true of Iceberg reads.
  Write support is newer and catalog-specific, and etl-craft is entirely a write engine — all
  seven actions mutate. **Verify DuckDB's Iceberg write support against a real catalog before
  committing to a design**; that probe was not completed.
- **It genuinely solves cross-process concurrency**, which is the strongest argument for it:
  optimistic concurrency with atomic catalog commits permits many concurrent writers, which a
  DuckDB file fundamentally cannot. That is a better answer than the queueing above.
- **Iceberg has no primary keys and no sequences.** The spec has no constraint concept. E2-54 made
  `ROW_ID` — a generated identity primary key — the answer for every engine-created table, and
  CLAUDE.md makes single-column PK a framework convention that `validate` enforces by
  introspection. **This needs a decision before any build**: most likely an engine-generated
  `ROW_ID` (a window function over the staged rows plus the current max, rather than a sequence)
  that is a real single-column key but is not database-enforced, with `validate`'s PK check
  reporting "not enforceable on Iceberg" rather than failing.
- **It needs infrastructure**: a catalog (REST/Glue/Nessie/Polaris) plus object storage. That
  voids the justification used two commits ago for making `duckdb-engine` a hard dependency
  ("DuckDB is embedded — there is no server to stand up"), so shipping it should be revisited
  when this lands.

---

# Warehouse architecture: SQL engines over Iceberg (2026-09-22)

> "engine: always postgres / warehouse: postgres (pg analytics), databricks with iceberg unity
> catalog, snowflake with iceberg hybrid tables, maybe be trino with iceberg or any sql tool over
> plain iceberg" ... "if the warehouse is not postgres, every table we create or operate should be
> iceberg compatible"

**This supersedes the 2026-09-21 "Postgres and DuckDB" framing and the "Iceberg is added scope,
not built" section above.** Iceberg is built. DuckDB remains supported for local development but
is no longer a headline warehouse.

## What the existing design absorbed for free

All three new dialects register under their plain vendor name and connect through
`warehouse.py`'s existing `_dbapi_connect` with **no vendor-specific connection code** — verified
via `entry_points(group="sqlalchemy.dialects")` and each dialect's own `create_connect_args`.
They are optional extras, never imported by engine code. "Any SQL tool over plain Iceberg" needs
no code at all, only a dialect on the path — which is the whole point of the `creator`-based
design, now actually exercised.

## What had to be built

- **`JDBC_PARSERS`**, a per-vendor URL registry. Databricks' parameters are semicolon-separated
  after the path; Snowflake's path is empty with the database in the query string. Neither is
  readable by the generic parser. Trino fits the generic shape, which is what the fallback is for.
- **`auth_mode='token'`**, previously `NotImplementedError`. A *minted* token is still
  unimplemented and genuinely different; a long-lived Databricks PAT presented like a password is
  not, and the engine cannot reach Databricks without it.
- **Iceberg table creation.** `create_table_as` adds the clause each dialect needs. Getting this
  wrong is **silent** — the table is created, the pipeline succeeds, and nothing else in the
  lakehouse can read it.
- **A third `ROW_ID` strategy.** Iceberg has no primary keys, identity columns or sequences, so
  it is computed (max present + row number) and every INSERT supplies it. Uniqueness is not
  database-enforced; `validate` reports that once rather than failing every business rule.

## Two things stated rather than implied

- **Snowflake hybrid tables and Iceberg tables are different features** — a row-store with
  enforced primary keys versus external Iceberg format — and a table cannot be both. Iceberg
  compatibility is binding, so Iceberg tables are the target. Snowflake table *creation* is
  **refused** rather than approximated: `CREATE ICEBERG TABLE` needs an `EXTERNAL_VOLUME` and
  `BASE_LOCATION` with no home in `CFG_` metadata yet, and silently creating an ordinary
  Snowflake table that looks fine and is not Iceberg is worse than failing.
- **The Databricks/Snowflake/Trino execution paths are unverified.** No such endpoint is
  reachable from the test suite. URL parsing, dialect resolution, token-URL construction and
  Iceberg clause selection are genuinely tested; the Iceberg execution path is proven only to
  produce well-formed SQL with correct ROW_ID arithmetic, by forcing it on against real Postgres.
  That is most of the risk but not all of it. **This is E2-53's lesson**, so it is recorded as a
  known gap rather than implied to work.

## Next, for whoever has credentials

1. `ETL_CRAFT_WAREHOUSE_<PROFILE>_SECRET=<token>` plus a Databricks `jdbc_url`, then
   `etl-craft doctor` — proves connection and auth.
2. A one-task `CREATE_TABLE` pipeline, then confirm in Unity Catalog that the table is Iceberg
   format, not Delta.
3. An `SCD1_MERGE` over two runs — the correlated UPDATE leg and ROW_ID continuity are the two
   things most likely to differ.

---

# Round 5 review — round 4's fixes, and the Iceberg warehouse (2026-09-22)

Same method. Baseline first: **510 passing, `mypy`/`ruff`/`black`/`pydocstyle` clean**. Then
re-read the diff, then probed the **live local Trino/Iceberg stack** (Trino + Iceberg REST +
MinIO, now in `docker-compose.yml`) with the SQL shapes `sql_actions.py` actually emits.

## Round 4 is fixed — and one of its findings was wrong in the dangerous direction

**E2-64 was worse than this file recorded it, and the fix caught that.** The round-4 probe drove
the `DROP SEQUENCE` on a *single* connection, where DuckDB refuses it loudly with a dependency
error — so the finding was written up as "fails loudly, nothing corrupted". The engine uses **one
connection per task process**, and there the `DROP` succeeds *silently*: `staging.orders` and
`marts.orders` end up sharing a sequence reset to 1, and a later insert into a table the run never
touched dies with a duplicate `ROW_ID`, leaving it un-writable. A review probe that does not
reproduce the engine's own connection topology can understate severity, not just miss things.
Worth carrying forward as a method note, because it is the second time the *shape* of a test, not
its subject, was the thing that mattered.

**E2-61's fix is the right one.** The subprocess model stays — it is what crash detection and
"local runs mirror an orchestrator" are built on — and Data DB access queues behind a Postgres
advisory lock held in the **Engine DB**. That is the correct place: the Engine DB is the one hard
dependency of every action, so it is reachable from every contending process including Airflow
workers on other machines. Queueing rather than retrying is also right (a retry loop on DuckDB's
`IOException` can starve a waiter), and `doctor` now reports the constraint. E2-62/E2-63 are fixed,
with `doctor` refusing an in-memory warehouse up front.

**And the Iceberg work found four real dialect defects by probing rather than assuming** — no
temporary tables, `md5()` over varbinary, `UPDATE` rejecting a table alias, no identity columns —
each fixed and covered by a real end-to-end test against real Iceberg tables in real object
storage. That is the test shape rounds 3 and 4 kept asking for.

The five findings below all sit on the Iceberg path, and three of them share one root cause:
**Trino does not roll back.**

## E2-65 — `DELETE_ROWS` with `HARD_DELETE=true` uses a table alias Trino rejects · reproduced

**Where:** [sql_actions.py:1539-1544](src/etl_craft/sql_actions.py#L1539-L1544), [`_update_target`](src/etl_craft/sql_actions.py#L877)

The alias problem was found and fixed for `UPDATE` — `_update_target` returns the bare qualified
name on Trino, and the soft-delete path uses it. The hard-delete path one branch above still emits
`DELETE FROM {qualified_target} t WHERE EXISTS (...)`. `_update_target`'s own docstring states the
assumption that made this a miss: *"Only the UPDATE statements need this"*. Trino rejects an alias
on `DELETE` the same way.

**Reproduction**, against the live stack:

```
FAIL  DELETE FROM iceberg.probe.tgt t WHERE EXISTS (...)
      TrinoUserError(SYNTAX_ERROR, "line 1:31: mismatched input 't'")
OK    DELETE FROM iceberg.probe.tgt WHERE EXISTS (...)      -- no alias
```

The end-to-end Iceberg test covers `SCD1_MERGE` only, so no test reaches this. `DELETE_ROWS` with
`HARD_DELETE=true` is simply unavailable on the warehouse the architecture now centres on.

**Fix direction:** route the hard-delete through the same helper, and rename it to say what it is
(`_mutation_target`, not `_update_target`) so the next mutating statement inherits the fix instead
of repeating the miss. Then extend the end-to-end Trino test past `SCD1_MERGE` — the remaining
five actions are all unexercised there, and this is the one that was broken.

## E2-66 — A failed action leaves a real Iceberg table behind, forever · reproduced

**Where:** [sql_actions.py:_build_stage](src/etl_craft/sql_actions.py#L590), `_drop_stage`

Trino has no temporary tables, so the stage is created as an **ordinary table** —
`etl_stage_<task_run_id>`, in the warehouse's own default schema. The reasoning given is sound as
far as it goes: the name is unique and `_drop_stage` runs on every path. But `_drop_stage` runs
*inside the transaction*, and Trino does not roll one back (E2-67), so a failure between
`_build_stage` and `_drop_stage` leaves the table committed.

**Reproduction:** an `OVERWRITE_TABLE` task against a target missing its audit columns — the check
fires after the stage is built. Task correctly reported `FAILED`; afterwards
`SHOW TABLES FROM iceberg.etltest` listed **`etl_stage_11387`**.

The name is scoped to `task_run_id`, so a retry does not reuse it: every failed attempt adds
another permanent Iceberg table, with real Parquet files and metadata in object storage, in the
schema the team's own data lives in. Nothing ever cleans them up, and `validate`/`doctor` do not
look. On Postgres and DuckDB this is invisible — a `TEMPORARY` table dies with the session.

**Fix direction:** drop the stage outside the action's transaction, in a `finally`, so it runs on
the failure path too. That alone fixes the common case. Worth pairing with a sweep — the names are
already engine-owned and carry the `task_run_id`, so `etl-craft doctor` (or a `--prune` verb) can
list and remove stages whose task run is terminal. Consider putting stages in their own schema
rather than the warehouse default, so a sweep can never touch a team's table and a leak is obvious.

## E2-67 — The atomicity caveat names MySQL and Oracle, but not the warehouse the architecture now centres on · reproduced

**Where:** [sql_actions.py:150-170](src/etl_craft/sql_actions.py#L150-L170)

The module docstring is careful and explicit: every action runs inside one Data DB transaction and
"a failure partway through rolls back everything this module did" — with a documented `[DEVIATION]`
that DDL auto-commits on **MySQL and Oracle**, so the guarantee is weaker there. Trino is not
mentioned, and on Trino nothing rolls back at all — DML included.

**Reproduction:** inside one `engine.begin()`, two `CREATE TABLE ... AS SELECT` statements followed
by a deliberate error. The error propagated; **both tables survived**.

So on the Iceberg path the stated guarantee is not merely weaker, it is absent: a merge that fails
after its `UPDATE` leg but before its `INSERT` leg leaves the target half-written, and "a retried
task always starts from the target's last genuinely-committed state" is not true. The actions are
individually idempotent — that is what actually saves this — but the docstring promises something
stronger than the engine delivers on its primary non-Postgres target, and a team reading it would
plan recovery around a rollback that will not happen.

**Fix direction:** documentation, and say it where it will be read — the module docstring, plus
CLAUDE.md's warehouse section. Worth stating the consequence rather than only the mechanism:
on Iceberg, "atomic" means "each statement commits on its own and every action re-derives its
effect on retry", which is a different promise. Related and worth a line in the same place: Iceberg
enforces no primary key, so the computed `ROW_ID` (max-present + row_number, read before the
insert) has nothing to catch a collision if two tasks ever append to one target concurrently.

## E2-68 — Cloning creates non-Iceberg tables on exactly the warehouses that need the clause · from code

**Where:** [cloning.py:203](src/etl_craft/cloning.py#L203)

CLAUDE.md states the invariant plainly: *"if the warehouse is not postgres, every table we create
or operate should be iceberg compatible"*. `sql_actions.create_table_as` upholds it carefully —
`USING ICEBERG` on Databricks, `CREATE ICEBERG TABLE` with `EXTERNAL_VOLUME`/`BASE_LOCATION` on
Snowflake, and an outright **refusal** there when the storage parameters are missing, on the stated
grounds that falling back "would look like success and silently produce something no other engine
in the lakehouse can read".

`cloning.py` still builds its mirrored `CFG_`/`AUD_` tables with
`target_metadata.create_all(data_engine)` — plain SQLAlchemy DDL, which emits a bare `CREATE TABLE`
and knows nothing about either clause. On Trino this is harmless (the catalog decides the format).
On Databricks and Snowflake — the two cloud warehouses the architecture names — the mirror is
exactly the silently-unreadable artifact the Snowflake guard exists to prevent.

Not reproduced: both need a cloud account. The code path is unambiguous.

**Fix direction:** route cloning's table creation through `create_table_as` (or a shared helper that
owns the clause), so the invariant has one implementation rather than two. That also picks up the
Snowflake refusal — though cloning has no `CFG_TASK_PARAMETERS` to read storage from, so where
`EXTERNAL_VOLUME`/`BASE_LOCATION` come from for a mirrored table is a real question to settle, not
an oversight to patch. `[Cloning]` in `craft-connector.yml` is the obvious home.

## E2-69 — "Is this warehouse Iceberg-backed?" is answered by dialect name alone · from code

**Where:** [sql_actions.py:917-919](src/etl_craft/sql_actions.py#L917-L919)

```python
def _is_iceberg_backed(dialect_name: str) -> bool:
    return dialect_name.split("+", 1)[0] not in NATIVE_STORAGE_DIALECTS
```

For Trino this is an assumption, not a fact: the table format comes from the **catalog**, and a
Trino deployment routinely has several. `jdbc:trino://host:8080/hive/analytics` is a perfectly
valid `[Warehouse]` URL that this treats as Iceberg-backed — computed `ROW_ID` instead of an
identity column, no primary key expected — while actually creating Hive tables. Everything
"succeeds"; the lakehouse invariant is silently false.

This is the same failure the Snowflake path refuses to allow, decided differently for Trino only
because the dialect name happens to be the only thing consulted. (The local stack has one catalog,
so this could not be demonstrated here — the logic is plain from reading.)

**Fix direction:** verify rather than infer, once, at connect or in `doctor` — Trino exposes the
catalog's connector through `system.metadata.catalogs`, and a cheap `SHOW CREATE TABLE` on the
first table the engine creates confirms the format. Failing in `doctor` with "catalog `hive` is not
an Iceberg catalog" is the same bargain the Snowflake guard already makes.

## Suggested handling

- **E2-65** first — a whole action is unavailable on the primary warehouse, and the fix is one call
  plus a rename that prevents the next instance.
- **E2-66** next, with the `finally` alone as the immediate fix; the sweep and the separate schema
  are the durable version.
- **E2-67** is documentation, but it is the kind that changes how a team plans recovery.
- **E2-68, E2-69** together — both are "the invariant has one careful implementation and one
  careless one".

## Added to "still to raise rather than guess"

- **Where do Snowflake `EXTERNAL_VOLUME`/`BASE_LOCATION` come from for cloned tables?** (E2-68.)
  They are `CFG_TASK_PARAMETERS` for a task's target; cloning has no task.
- **Should stage tables live in their own schema on Iceberg warehouses?** (E2-66.) It makes leaks
  obvious and a sweep safe, at the cost of one more thing to provision.

## A note on method

Round 4's lesson was "test the deployment, not just the dialect". This round it held in both
directions. The Iceberg work applied it — the end-to-end Trino test found four real defects no
unit test would have — and the gap that remains is the same shape one level in: **that test covers
`SCD1_MERGE` only**, and the one action probed outside it (`DELETE_ROWS`) was broken. Three
findings here are Postgres-shaped assumptions surviving into a warehouse that does not share them;
the fourth and fifth are an invariant with two implementations. Extending the Trino test across
the remaining six actions is the single highest-value follow-up, and it is what would have caught
E2-65 and E2-66 without a review.

---

# Round 5 outcome (2026-09-22) — all five fixed, and a sixth the review missed

Commit below. 516 tests, 96% coverage, `make check` / `make db-schema-test` / the wheel smoke
test all clean.

- **E2-65 — fixed, and the helper renamed so the next one inherits it.** `_update_target` became
  `_mutation_target` and the `HARD_DELETE` branch routes through it. The review was exactly right
  about the cause: the helper's own docstring claimed "only the UPDATE statements need this", and
  that claim is what made the omission invisible.
- **E2-66 — fixed at the dispatch point, not the seven call sites.** The stage is swept in a
  `finally` in `execute()`, so it runs on the failure path. Reproduced first: a failed
  `OVERWRITE_TABLE` left a real Iceberg table behind, and the regression test was verified to fail
  against the pre-fix code. **A note on that verification**: reverting the fix to check the test
  leaked a stage table that then made the *restored* run fail too — the bug demonstrating its own
  persistence. The test now compares against a before-snapshot rather than asserting the schema
  holds no stages at all, so it cannot fail for somebody else's leak.
- **E2-67 — documented where it will be read**, in `sql_actions.py`'s own atomicity note and in
  CLAUDE.md. Verified first: two CTAS statements plus a deliberate error inside one
  `engine.begin()` left **both** tables. The note now states the consequence rather than the
  mechanism — on Iceberg, idempotency is what makes a retry safe, not atomicity — and carries the
  reviewer's related point that Iceberg enforces no primary key, so a computed `ROW_ID` has
  nothing to catch a concurrent collision.
- **E2-68 — one implementation instead of two.** `cloning.py` builds its mirrors through the
  Iceberg-aware path now, including the Snowflake refusal. **Open question answered**:
  `EXTERNAL_VOLUME`/`BASE_LOCATION` for a mirrored table come from `[Cloning]`
  (`External_volume`/`Base_location`) — the reviewer's own suggestion, and right, because cloning
  has no task whose `CFG_TASK_PARAMETERS` could carry them.
- **E2-69 — verified, not inferred.** `warehouse.verify_iceberg_catalog` asks Trino's
  `system.metadata.catalogs` what the catalog's connector actually is, and `doctor` fails with
  "catalog 'hive' is not an Iceberg catalog". Three tests, including the real rejection case
  against a genuinely non-Iceberg catalog (`system`).

## E2-70 — `SCD2_MERGE` was entirely broken on Trino · found by taking the review's advice

The round-5 note called extending the end-to-end Trino test across the remaining actions "the
single highest-value follow-up". Doing it found a sixth defect immediately: `SCD2_MERGE` builds a
**second** scratch table (the changed-key set) and was still emitting `CREATE TEMPORARY TABLE`
directly, which Trino rejects. The E2-66 work had fixed `_build_stage` only.

So the whole action failed on the warehouse the architecture centres on, and no review caught it —
including the one that correctly diagnosed the identical problem one function away. Both scratch
tables now go through one `_create_scratch_table`, and both are swept on failure.

`test_every_sql_action_runs_on_real_trino_iceberg` walks `CREATE_TABLE`, `SETUP_TABLE`,
`OVERWRITE_TABLE`, `SCD2_MERGE`, `DROP_TABLE` and (separately) `DELETE_ROWS` with `HARD_DELETE`,
and asserts nothing leaked across all six.

## A note on method

Round 5's own lesson, confirmed by acting on it: the gap was never the dialect, it was **which
actions the deployment test covered**. Two of this round's six defects (E2-65, E2-70) were the
same mistake in two places, and both were invisible to every unit test and to two review passes.
The test that catches this class is the one that runs the *whole vocabulary* against the real
engine — which now exists.

---

# Round 6 review — round 5's fixes, and declared table formats (2026-09-22)

Baseline: **526 passing**, `mypy`/`ruff`/`black`/`pydocstyle` clean. Re-probed round 5's
reproductions against the live Trino/Iceberg stack, then read the two new commits.

## Round 5 is fixed, and the follow-up it recommended paid for itself

E2-65 through E2-69 are all closed. **E2-66 re-probed against the live stack**: the same failing
`OVERWRITE_TABLE` that leaked `etl_stage_11387` now leaks nothing — the `finally` sweep genuinely
reaches Trino, and it correctly stays best-effort so a cleanup failure can't mask the real one.
E2-69's `verify_iceberg_catalog` asks `system.metadata.catalogs` instead of inferring, and
`doctor` fails with something actionable.

**E2-70 is the one that matters, and this file should record why.** Round 5 called extending the
end-to-end Trino test across the whole vocabulary "the single highest-value follow-up"; doing it
immediately found `SCD2_MERGE` **entirely broken** on Trino — it builds a *second* scratch table
(the changed-key set) that was still emitting `CREATE TEMPORARY TABLE` directly, untouched by the
E2-66 work. Two of that round's six defects were the same mistake in two places, and both survived
two review passes.

**Why the round-5 probe missed it, since that is the reusable part:** the probe hand-wrote the SQL
shapes and ran them directly — including `CREATE TABLE AS … ROW_NUMBER()`, which works fine on
Trino. What it never did was *drive the engine* and see which keyword the code actually emitted.
Probing the shape proves the dialect accepts it; only exercising the code proves the code produces
it. Every reproduction from here should go through `run_task`, not through hand-written SQL.
(Small corroboration that the lessons in `CLAUDE.md` are live ones: this round's own probe tripped
over `:ref::regclass` — SQLAlchemy's escaped-colon pitfall that file already documents.)

The vocabulary is now genuinely covered on Trino: `CREATE_TABLE`, `SETUP_TABLE`, `OVERWRITE_TABLE`,
`SCD2_MERGE`, `DROP_TABLE` in one walk, plus `SCD1_MERGE` and `DELETE_ROWS` in their own tests.

The native-format work is well reasoned — declared rather than inferred, `iceberg` kept as the
default so no existing pipeline silently changes format, `USING DELTA` named explicitly rather
than falling through to a workspace default, and `_is_iceberg_backed` deliberately left alone
because it governs the `ROW_ID` strategy (a dialect question) and not storage. All three findings
below are about what the last two rounds of change left behind, not about that reasoning.

## E2-71 — `PRIMARY_KEY` is documented as working in three places and does nothing · reproduced

**Where:** [docs/parameters.md:30](docs/parameters.md), [sql_actions.py:55](src/etl_craft/sql_actions.py#L55), [cfg.py:721](src/etl_craft/cfg.py#L721)

E2-54 replaced the `PRIMARY_KEY` parameter with an engine-generated `ROW_ID`, for a good reason
(declaring a natural key as the PK is unusable on an SCD2 target). `sql_actions.py:1049` says so.
But the same file's **module docstring** still documents `PRIMARY_KEY` as live — *"optional, every
creating action … Applied as ALTER TABLE … ADD PRIMARY KEY once the target has been created"* —
and so does the user-facing reference:

> `PRIMARY_KEY` | no | Applied as `ADD PRIMARY KEY` when the engine creates the target, and
> re-applied after a schema evolution.

It isn't. And `KNOWN_PARAMETERS` still lists it, so `validate`'s unrecognized-parameter check —
built in E2-25b precisely to catch "a typo will be ignored" — stays silent.

**Reproduction**, a `CREATE_TABLE` task declaring `PRIMARY_KEY: id` exactly as the docs instruct:

```
outcome:               SUCCESS
target columns:        ['id', 'name', 'pipeline_run_id', 'row_id']
actual primary key:    ['row_id']          <- not 'id'
validate complaints:   []
```

So the documented path succeeds, silently ignores what was declared, and nothing warns. The
reverse gap compounds it: **`ROW_ID` is not mentioned anywhere in `docs/parameters.md`** — not as a
parameter, not in the per-action "Audit columns appended" table (which still lists only
`PIPELINE_RUN_ID` for `CREATE_TABLE`), even though it is added to every created target *and* is
the column `CFG_BUSINESS_RULES.BUSINESS_RULE_KEY_COLUMN` must now name for `validate`'s PK check to
pass. A team following the reference cannot configure a business rule correctly.

This is the same class as E2-58, found and fixed in round 3 — superseded text left contradicting
the code — so it is a recurrence rather than a new kind of gap.

**Fix direction:** delete `PRIMARY_KEY` from the module docstring, `docs/parameters.md` and
`KNOWN_PARAMETERS` (removing it from the last one is what makes `validate` catch anyone still
setting it); document `ROW_ID` in the audit-column table and say that it is what
`BUSINESS_RULE_KEY_COLUMN` should name. Worth a quick sweep for other parameters that changed
hands during iteration 2 — `SCHEMA_EVOLUTION`, `RETURN_VALUES` and `SCRIPT_NAME` all moved from
`CFG_TASKS` columns to parameters, and the same three-places-to-update shape applies.

## E2-72 — The Iceberg catalog guard is warehouse-level; the format declaration is task-level · from code

**Where:** [warehouse.py:660-686](src/etl_craft/warehouse.py#L660-L686)

`verify_iceberg_catalog(config, data_engine)` takes no task parameters — structurally it cannot —
and returns early unless `config.warehouse_table_format == "iceberg"`. But `TABLE_FORMAT` is a
*per-task* override, and the more specific setting is the one that wins at execution.

So with `[Warehouse] Table_format: native` and a single task declaring `TABLE_FORMAT: iceberg`,
`doctor` skips the catalog check entirely, and on Trino that task then creates tables in whatever
the catalog actually is — because `iceberg_clause("trino")` is empty for both formats, the format
really is the catalog's. A task that explicitly asked for Iceberg silently gets Hive tables. That
is E2-69's exact failure, reached through the override rather than the default.

The other direction is milder but also wrong: `Table_format: iceberg` with every task overriding
to `native` fails `doctor` for a catalog nothing needs.

**Fix direction:** resolve the question the way execution does. The set of formats a deployment
actually asks for is `{[Warehouse].Table_format} ∪ {every active task's TABLE_FORMAT}`, which is a
plain `CFG_TASK_PARAMETERS` read `doctor` can already do; check the catalog when `iceberg` is in
that set. If keeping `doctor` free of `CFG_` reads is preferred, `validate` is the natural home
instead — it already reads every task's parameters and already talks to the Data DB.

## E2-73 — None of the three new parameters is checked by `validate` · from code

**Where:** [validate.py](src/etl_craft/validate.py) — `TABLE_FORMAT`, `EXTERNAL_VOLUME` and `BASE_LOCATION` appear zero times

`validate_task_parameters` was extended in E2-25b to check required parameters per `SQL_ACTION`,
per-handler requirements, identifier safety and unrecognized names — on the stated reasoning that
a parameter problem should surface at `validate` rather than at 3am. The three parameters added
since are not covered:

- **`TABLE_FORMAT`'s vocabulary.** `VALID_TABLE_FORMATS` is enforced in `sql_actions.py` at
  execution. `TABLE_FORMAT: icberg` therefore passes `validate` and fails the task.
- **The Snowflake pairing.** `TABLE_FORMAT=iceberg` on Snowflake requires `EXTERNAL_VOLUME` and
  `BASE_LOCATION`, and `create_table_as` rightly refuses without them — at execution. This is a
  static, cross-parameter requirement of exactly the kind `validate` already checks for the SCD
  merges (`MERGE_KEY` + `MERGE_COMPARE_COLUMNS`).
- **The same pairing for `[Cloning]`**, which now carries the storage for mirrored tables.

Individually small; together they mean the newest and least familiar parameters are the ones with
the least pre-flight checking, on the warehouse path with the fewest people able to test it.

**Fix direction:** add them to `validate_task_parameters` alongside the existing per-action
requirements. The dialect is knowable there — `validate` already builds the Data DB engine for the
business-rule PK check — so the Snowflake pairing can be checked conditionally rather than always.

## Suggested handling

- **E2-71** first: it is documentation plus a one-line `KNOWN_PARAMETERS` deletion, and it is
  actively misleading a reader right now — including about the column business rules must name.
- **E2-73** next, as one change with E2-72 if `validate` is chosen as the home for both.
- **E2-72** needs a small decision (`doctor` or `validate`) but not a design one.

## A note on method

Round 5's recommendation was right and the follow-up proved it — but the recommendation was only
needed because that round's probe tested SQL shapes by hand instead of driving the engine. The
correction is concrete and belongs in how these reviews are run: **reproduce through `run_task`,
not through hand-written SQL.** Every reproduction in this round went that way, which is how E2-71
surfaced — the parameter looks fine in the code and only shows as inert when a real task runs and
the target's actual primary key is read back.

---

# Round 6 outcome (2026-09-22) — all three fixed, plus the cloud test harness

530 tests (2 skipped, awaiting credentials), 96% coverage, `make check` / `make db-schema-test` /
the wheel smoke test all clean.

- **E2-71 — fixed in all three places, and the one that matters is `KNOWN_PARAMETERS`.** Removing
  `PRIMARY_KEY` from the vocabulary is what makes `validate` *report* it rather than ignore it, so
  anyone still following the old docs now gets told. `ROW_ID` is documented: in the audit-column
  table, and explicitly as the column `CFG_BUSINESS_RULES.BUSINESS_RULE_KEY_COLUMN` must name —
  the gap that meant a team following the reference could not configure a business rule.
- **E2-72 — resolved in `validate`, not `doctor`, and `doctor`'s copy deleted.** The review offered
  either; `validate` already reads every task's parameters *and* already talks to the Data DB for
  the business-rule PK check, so it is the only one that can see a per-task `TABLE_FORMAT`
  override. Keeping a copy in `doctor` would have recreated the two-implementations problem E2-68
  had just been about.
  - **The fix caught itself repeating.** `verify_iceberg_catalog` still had its own
    `warehouse_table_format != "iceberg"` early return, so it short-circuited on the warehouse
    default even when a task had overridden it — E2-72 again, one layer in. The regression test
    failed and found it. The function now answers only "is this catalog Iceberg", and *when to
    ask* belongs to the caller.
- **E2-73 — the three new parameters are checked.** `TABLE_FORMAT`'s vocabulary in
  `validate_task_parameters`; the Snowflake `EXTERNAL_VOLUME`/`BASE_LOCATION` pairing and the same
  for `[Cloning]` in the new `validate_warehouse_storage`, conditioned on the real dialect.

## Databricks and Snowflake: the blocker that was ours, removed

Five rounds of "unverified, and stated rather than implied" was an honest caveat that had stopped
being useful. Neither can be stood up locally — Databricks needs a workspace, and Snowflake needs
cloud object storage for an Iceberg external volume — so the tests will always skip by default.
That part is not solvable.

What *was* ours to fix is that the tests did not exist, so verifying either one meant writing them
first. They exist now, gated on credentials exactly as the container fixtures are gated on a
container running, and they skip with the variable names in the message:

```
ETL_CRAFT_TEST_DATABRICKS_JDBC_URL / _TOKEN / _SCHEMA
ETL_CRAFT_TEST_SNOWFLAKE_JDBC_URL / _USER / _SECRET / _KEY_FILE / _SCHEMA
ETL_CRAFT_TEST_SNOWFLAKE_EXTERNAL_VOLUME / _BASE_LOCATION   (optional)
```

Each walks `CREATE_TABLE`, `OVERWRITE_TABLE` and `SCD1_MERGE` through `run_task` — per this
round's own method note, driving the engine rather than hand-written SQL. **Snowflake without an
external volume runs the `native` path instead of skipping**, so a trial account still exercises
connection, auth and the whole vocabulary; supply the volume and the same test runs Iceberg.

So the answer to "when do we actually test Databricks and Snowflake" is now: whenever credentials
are exported, locally or from CI secrets. Nothing else is in the way.

## A note on method

The round-6 correction — **reproduce through `run_task`, not hand-written SQL** — is what the
cloud tests are built on, and it is worth stating why it keeps mattering: probing a SQL shape
proves the *dialect* accepts it; only driving the engine proves the *code emits* it. Every defect
that survived a review pass this iteration (E2-65, E2-70, and E2-72's recurrence today) was
invisible to the first kind of check and obvious to the second.

---

# Round 7 — E2-74…E2-89, all fixed (2026-09-22)

All sixteen findings from `REVIEW_ROUND_7.md` are closed. Baseline going in was 533 passed /
2 skipped; out is **546 passed / 2 skipped, 96% coverage, `make check` and the wheel smoke test
both clean**. Every fix for a *reproduced* finding has a regression test that was verified to
**fail against the pre-fix code** before being kept — the review's own rule, applied to all of
E2-74, E2-75, E2-76, E2-77, E2-78, E2-79, E2-81, E2-82, E2-83, E2-86 and E2-87.

## The two that lost data

- **E2-74 — `SCD2_MERGE` now converges.** The two legs disagreed about what "already present"
  means: `changed_keys` required an `ACTIVE_FLAG = 'Y'` row, the new-row `NOT EXISTS` looked at
  every row regardless of flag, so a key holding rows but **no active row** fell through both,
  forever — SUCCESS reported, current version never written. Fixed by scoping the new-row leg to
  active rows only, which is what "new" means for SCD2: *no current version present*. Safe on the
  ordinary path because that leg runs *after* the deactivate and the changed-key insert, so a key
  handled there already has its new active row and is not matched twice. Reproduced first, exactly
  as the review described (`[(1, 'x', 'N')]` and nothing else, on the retry).
- **E2-75 — `MERGE_DEDUPE_ORDER` works on Trino, and no longer leaks.** `_dedupe_stage` was the
  third and last site still emitting `CREATE TEMPORARY TABLE` directly, so the whole E2-04 guard —
  the only thing between duplicate source rows and a permanently corrupted SCD target — was
  unreachable on the Iceberg warehouse. Routed through `_create_scratch_table`, and the
  `{stage}_dedup` name added to `_sweep_stage` via a new shared `_dedupe_table_name` so the two
  cannot drift again. `test_every_sql_action_runs_on_real_trino_iceberg` gained a genuinely
  duplicate-bearing merge, which is what the vocabulary walk never had: its sources were clean, so
  `_dedupe_stage` always returned early at `if not duplicates` and the bug was unreachable from
  the test that looked like it covered it.

## What the code assumed about its environment

The review's own closing observation — that E2-76, E2-79 and E2-83 are not about what the code
does but what it assumes about its surroundings — held up, and all three are the install path.

- **E2-76 — every read and write in `src/` now passes `encoding="utf-8"`.** `schema.sql` has 74
  non-ASCII lines, so `init-db` and `setup` died on any non-UTF-8 locale. Confirmed both ways
  under `LC_ALL=C PYTHONCOERCECLOCALE=0`: the bare `read_text()` raises, the fixed one does not.
  Held by **ruff `PLW1514`**, enabled with `preview = true` + `explicit-preview-rules = true` so
  the one rule comes in without every other preview rule in the selected categories.
- **E2-79 — `exec_driver_sql`, not `execute(text(...))`,** in both `migrate` and `init_db`.
  `_split_statements` is carefully quote-aware; `text()` then ran its own, *not* quote-aware,
  `:name` scan over the same text. A migration containing `':name'` or `'docs/#:ref'` failed
  naming a bind parameter its author never wrote. The three shipped migrations escape it only by
  luck, since SQLAlchemy's lookbehind protects a digit before a colon. A fixture migration
  carrying both spellings is now in the suite.
- **E2-83 — `setup` and `init-db` record the packaged migrations instead of running them.** New
  `migrate.mark_packaged_migrations_applied`. **The part the review did not draw, and it matters:**
  the marking is scoped to the migrations that ship *inside the package*, not to whatever
  `resolve_migrations_dir` picks. `schema.sql` is the authoritative definition of the *engine's*
  schema, so the engine's own migrations are by definition already in it — but a team's
  `./sql/migrations/` holds changes `schema.sql` knows nothing about, and recording those
  unexecuted would silently skip a team's migration, which is a worse bug than the one being
  fixed. Two tests: `setup` on a fresh database applies **nothing**, and a team's own
  non-re-runnable migration still runs.

## Correctness

- **E2-77 — every `EMAIL_ALERT` is excluded from a run's flavour, not just self.** Two alerts per
  pipeline is a supported configuration (`validate` treats them as a set; `EMAIL_ON_STATUS` exists
  precisely so one goes to ops on `FAILED` and another to stakeholders on `SUCCESS`), and they
  land in the same wave — so whichever ran first saw the other as `PENDING`, landed in the neutral
  middle, and sent a false amber email about a perfectly clean run. `TaskStatusEntry` carries
  `handler`, the same one-field extension `task_id` needed when this exclusion was first built.
- **E2-78 — `--config` reaches every spawned task.** `ConnectorConfig` now carries `config_path`,
  `_run_wave` appends it, and — caught by the test rather than by reading — it has to go **before**
  the verb, since `--config` is a top-level flag. Pre-fix the test fails with exactly the
  "config not found" the review predicted.
- **E2-82 — resolved by taking *both* shapes the review offered, because neither alone is
  enough.** `ready()` no longer lets an optimistic cross-pipeline count *alone* start an `ANY`/`N`
  task whose same-pipeline upstreams have not run; and `run_task` writes nothing (exit 0, E2-47's
  shape) rather than a terminal `SKIPPED` when `cross_reasons` is all that is short but a
  same-pipeline upstream is still pending. Option 2 alone is insufficient because
  `_run_until_settled` tracks an `attempted` set, so a task that exits 0 writing nothing is never
  re-dispatched within that `run_pipeline` call — it would convert "permanently SKIPPED" into
  "permanently unsettled". Option 1 alone is insufficient because a manual `run --task_code` never
  goes through the wave pre-filter at all.
  - **The boundary that keeps the fix from becoming its own bug:** the task is held only while an
    upstream is *not yet terminal*, **not** until it is `SETTLED`. A permanently `FAILED` upstream
    has had its say, and the cross edge may still satisfy `ANY` — which is precisely what `ANY` is
    for. Pinned by its own test.
- **E2-80/E2-81 — one pass over `handlers.dispatch`.** The Engine DB write transaction is gone
  from around both long-running handlers: `scripts.execute` takes the `Engine` and opens its one
  connection *after* `subprocess.run` returns, and `sql_actions.execute` takes the `Engine` with
  `_setup_table`/`_drop_table` opening short connections for their two reads. An open Postgres
  transaction pins the `xmin` horizon for the whole database, so eight parallel tasks holding one
  for hours stopped autovacuum reclaiming anything, anywhere. And `HANDLER=PYTHON` now takes the
  single-writer lock: new `warehouse.single_writer_lock`, split out of `open_warehouse` so a
  caller can take the queueing **without** opening a warehouse engine — which matters because an
  ingestion task legitimately runs with no `[Warehouse]` section at all, and requiring one would
  have been a regression. Verified to fail pre-fix: without the lock the script runs alongside the
  holder and leaves its marker file.
  - **Known interaction, left deliberately:** `dispatch` gives the lock the task's own timeout as
    its wait bound, so the lock and the fork watchdog expire together and the watchdog's blunter
    message usually reports first. Shortening the lock's budget to win that race would make a team
    running a short `TASK_TIMEOUT_SECONDS` fail tasks that should have queued — a worse trade than
    a blunter message. The review's own instruction was to keep the bound at the task's timeout.

## Hardening

- **E2-84** `PIPELINE_CODE` is checked by `_SAFE_IDENTIFIER` where `TASK_CODE` already was. The
  shell case is covered by CFG_ rows being git-reviewed; the `generate-docs` case
  (`f"{pipeline_code}.html"`) is a plain bug for an innocent code.
- **E2-85** `MERGE_KEY` and `MERGE_COMPARE_COLUMNS` get an identifier check per pipe-separated
  element. `MERGE_DEDUPE_ORDER` is deliberately a SQL fragment, so it gets the conservative shape
  check the review described — column name, optional `ASC`/`DESC`, optional `NULLS FIRST`/`LAST`,
  comma-separated — and the message says what was expected, since a legitimately more exotic
  fragment is rejected.
- **E2-86** `.env` values lose one *matching pair* of wrapping quotes, not every leading and
  trailing quote character. A secret ending in `"` was silently truncated and `doctor` reported it
  as found, because it was. The supported subset is now stated in `docs/configuration.md`.
- **E2-87** the lineage cache key hashes `SOURCE_SQL`, `TARGET_OBJECT` **and** the parse dialect,
  NUL-separated. Renaming a target without touching its SQL served stale lineage indefinitely.
- **E2-88** `scripts/wheel-smoke.sh` drives `etl-craft setup --env` instead of writing
  `craft-connector.yml` with a heredoc, asserts the result carries **both** `Postgres:` and
  `Warehouse:`, and that a second `setup` is idempotent — then still exercises `init-db`/`migrate`
  as standalone verbs against a fresh database. `setup` was the only verb in CLAUDE.md's CLI table
  the install-path test skipped, and it is the command that shipped without writing `[Warehouse]`
  at all. Run for real; it passes, and its output now shows E2-83 working.
- **E2-89** one wall-clock deadline for a whole batch, not a fresh clock per `wait()`. The real
  bound was `N x (timeout + 120)` — over two days at the six-hour default — against the
  `timeout + 120` the comment claimed.

## Still deferred, still not closed

**E2-18 (logging) and E2-20 (task output)** remain deferred, for the third round running.
`grep -rn logging src/etl_craft` still returns nothing. Round 3 was right that recording them as
closed would have been the dishonest option, and round 7 is right that they are now the largest
remaining operability gap — E2-80's diagnosis in particular is harder than it should be without
them.

## A note on method

Two things earned their keep again.

**A regression test must reach the thing it claims to test.** E2-75 is the sharpest example this
round: `test_every_sql_action_runs_on_real_trino_iceberg` walked the whole action vocabulary on
the real Iceberg stack and still could not see a broken `_dedupe_stage`, because its sources were
duplicate-free and the dedupe returned early at its own guard. A test that exercises the function
but not the branch is a test that looks like coverage.

**A finding's suggested fix is a starting point, not a specification.** E2-83's suggestion
("insert every present migration filename into `SCHEMA_MIGRATIONS` without executing it") is
correct for the engine's own migrations and actively harmful for a team's, and the difference is
invisible unless you ask which directory `resolve_migrations_dir` actually resolved. E2-82 was
offered as a choice between two shapes and needed both. E2-81's suggestion to reuse `data_db`
would have made `[Warehouse]` mandatory for ingestion tasks that never had it.
