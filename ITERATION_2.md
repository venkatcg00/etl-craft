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

**Every item on this list is closed: E2-01 through E2-52.** 368 → 500 tests, 97% coverage,
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
| 7 | E2-17…E2-24, E2-34…E2-40, E2-25b | Operability and cleanups |

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
- **Still open, deliberately** — these were raised and *not* built, for reasons recorded in
  `CLAUDE.md`: E2-11's shape (an hour-long in-process poll holds a worker slot; an Airflow
  `reschedule` sensor would not, but that is orchestrator-shaped), E2-14's field-name freeze on
  the `generate-yml` shape, and E2-22's retention policy for the `AUD_` tables.
