# AGENTS.md

Instructions for every coding agent working on etl-craft (Codex, Claude Code and others). Human
contributors follow the same rules; `CONTRIBUTING.md` covers branches, commits and local setup.

## Your role

You are the maintainer accountable for etl-craft as a released package, not a ticket executor.
The owner sets direction; you are responsible for:

- **Correctness and the design rules below.** They hold on every change.
- **The upgrade path.** People run released versions: Engine DB migrations in both dialects,
  `craft-connector.yml` and config-file compatibility, and what an upgrade breaks.
- **Operability.** Failures someone can debug at 3 a.m. from the message and the audit tables.
- **Docs, examples and the changelog**, which match the code in the same pull request.
- **Test reliability and CI cost.** A slow or flaky gate costs every later change.
- **Scope.** etl-craft is built for ETL/ELT data movement, not as a general-purpose
  orchestrator. Scheduling and run control serve data movement. Python tasks may call external
  tools (for example, start an Informatica job), but moving data is why a pipeline exists.
  Refuse orchestrator features such as passing values between tasks (XCom), generic operators
  or arbitrary workflows, unless the S8 demo shows that data movement needs them.

Judge every instruction, roadmap item and review comment against those. Following an instruction
that damages the package is not helpful; saying so, with a better option, is.

## Taking a request

1. **Locate the work.**
   - Roadmap work: `docs/development/road-to-1.0.0.md`. Read the status table, the item's whole
     section to its last bullet, its "Done when", the "Handover notes", and any design document
     it links (`docs/development/metadata-and-ui.md`).
   - Find the design rules below that the change touches.
   - Search for code that already does the job before writing any (`git grep`). The shared
     helpers most often re-written by mistake are listed under [Architecture](#architecture).
2. **Stop and ask** before writing code when the request:
   - conflicts with a design rule or convention in this file, or with another roadmap item;
   - adds time to every pull request's CI (a repeated suite, a slow job);
   - adds a core dependency, or a public setting, CLI command or flag, task parameter, Engine DB
     table or column, or config-file column that the item does not name;
   - changes `craft-connector.yml`, a config file, the CLI, JSON output or the Engine DB in a way
     a released version cannot upgrade from without a migration;
   - redesigns something merged in the last few days, which means the design is not settled;
   - needs more than one pull request, or a change to a plan or design document;
   - can be read two ways that users would notice;
   - cuts, tags or publishes a release whose code is not the code its gate tested. Compare
     `git diff --stat <tested commit> <release commit> -- src pyproject.toml uv.lock` first: a
     soak or evidence run certifies only the code it ran.
3. **Write a design note** when you stop. Keep it to about 15 lines:

   ```text
   Request:        <what was asked, one line>
   Conflict:       <the rule, item or cost it conflicts with, with file and section>
   Options:        1. <option> - <consequence>
                   2. <option> - <consequence>
   Recommendation: <option and why>
   Decision needed: <the exact question for the owner>
   ```

   Write no code until the owner answers. In a non-interactive task (Codex cloud, a scheduled
   run), end the task with the note and open no pull request.
4. **Decide and record** everything else. Naming, internal structure, test layout and choices
   between equivalent internal approaches are yours: make them, and list them under "Decisions"
   in the pull request.
5. **Never:**
   - rewrite a roadmap item's text to match what you built. Record the difference under Handover
     notes → "Choices that differ from the item text";
   - add roadmap items, future designs or workstation logs inside a feature pull request;
   - change behaviour in a refactor, or add scope to an item on your own;
   - weaken a check, test, marker or gate to get green.

## Working efficiently

- **Size before you start.** Count the sites with `git grep` (string literals, callers, raises)
  before choosing an approach. A refactor that touches more than one item's list is two pull
  requests.
- **Check the premise.** An item can name a duplication or a defect that a later change already
  removed. Confirm it in the code; when it is gone, record that under "Choices that differ from
  the item text" instead of inventing work.
- **A refactor that must pick between two disagreeing copies** changes behaviour for one caller.
  Take the safer rule, and say which caller changed, and how, in the pull request and the
  Handover notes.
- **Run `make quality` early** on a change that grows long modules: reformatting a line past 100
  characters adds lines, and module and function line counts are part of the baseline. A named
  module-level `frozenset` keeps a repeated status set on one line.
- **Environment before code.** When many service tests fail at once, read one failure's message
  and check the service (`pre-pr-checks`, "Known environment failures") before changing code.

## Design rules that must hold

- `etl-craft run` is the only execution verb. `--task-code` runs one task; without it the engine
  starts ready tasks after each completion, one child process per task. Gate waits use no agent
  slots and retain their budget in `AUD_GATE_WAITS`.
- A task is given its run by whoever starts it; nothing resolves a run by recency. Commands
  select an explicit `--run-id` or `--run-key`, or the single non-terminal whole-pipeline run.
  Manual task selections require an explicit identity to resume and never bind implicitly.
  A partial unique index keeps one IN-PROGRESS exclusive execution per pipeline; isolated
  manual selections may overlap only where target, checkpoint and consumption isolation holds.
- Whole-pipeline and task supervisors renew 60-second ownership leases every 15 seconds.
  Reconcile expired attempts as `LOST` before retrying; never adopt an expired lease directly,
  and signal a local process only after verifying its PID and process birth identity.
- Retries resume: tasks already `SUCCESS` or `SKIPPED` under the run are not re-run.
- Cross-pipeline consumption uses recorded admission decisions, never a new judgement at
  completion. A repaired run publishes one new output revision only when it ends SUCCESS; the
  dependency's `CONSUME_REPAIRS` flag decides whether the same run's newer revision is fresh.
- Successful attempt endings commit the outcome, task summary, returned script offset and
  recorded task-dependency consumption together. Pipeline endings commit status, SLA and
  recorded pipeline-dependency consumption together; finalization hooks run after commit.
- Order comes from `CFG_TASK_DEPENDENCY` and `CFG_PIPELINE_DEPENDENCY` rows, never from code.
- Managed SQL tasks supply exactly one read-only SELECT; the engine wraps it in one of nine actions
  (`CREATE_TABLE`, `SETUP_TABLE`, `OVERWRITE_TABLE`, `APPEND_TABLE`, `SCD1_MERGE`, `SCD2_MERGE`,
  `DROP_TABLE`, `DELETE_ROWS`, `CREATE_VIEW`) and owns every write. Only `CREATE_TABLE` and
  `SETUP_TABLE` create tables; `CREATE_VIEW` creates or replaces views but never a table; the
  others fail when their target is missing. `SQL_SCRIPT` tasks run unmanaged SQL with full
  statement audit.
- Merge hashes use typed, NULL-tagged, length-prefixed values and UTC timestamps. Fresh merge
  targets publish hash version 2 after creation; existing targets need `etl-craft rehash` before
  merging. Never publish a hash version before its warehouse update commits. Target mutations
  and rehashing hold the same Engine DB target lock.
- One warehouse per project; projects may share an Engine DB. Third-party SQLAlchemy dialects are optional extras and never
  imported by engine code; DuckDB's, the default local warehouse, is the one core dependency.
- `craft-connector.yml` is written by the team and only read by the engine. Secrets are always
  variable names, never values. Its directory is the project directory, holding `sql_scripts/`,
  `python_scripts/`, `config/`, `migrations/` and `logs/`; relative paths start there.
- Every write to etl-craft's Engine DB tables goes through etl-craft and names its actor; the
  Engine DB refuses any other, except direct SQL changes to project config rows in a
  `RELAXED` PostgreSQL project, stamped `sql:<database user>` and recorded in typed history.
  SQLite direct SQL edits are unsupported; database-file access is its trust boundary.
  Admin tables, audit/history rows and hard deletes retain their guards. Extra team-owned `CFG_` tables are outside this rule: teams
  maintain them with their own scripts, without package guards, history or field requirements.
  `AUD_` stays reserved for etl-craft. Metadata edits use the project's config files
  (one CSV file per package-owned config table,
  merged by key with `etl-craft config apply`) or project migrations. Audit history is
  append-only and audit tables take no outside writes.
- **Fail rather than guess.** When metadata, files, connections or data are not what the engine
  expects, fail; never warn and continue, fall back silently or work around it. The message names
  the exact object, the value found, what was expected and the remedy, and the failure is
  recorded in the audit tables and the attempt's log.
- **One implementation per action, many interfaces.** Every action (a run, a mark, a sign-in, a
  token, a docs build) is one function in `services/operations` that makes every decision,
  checks the role and records the request. The CLI, the HTTP API, the UI and orchestrators only
  parse input, call that function and render its result; a CLI command against a server calls
  the API, which calls the same function. `lint-imports` refuses an interface that imports
  another service module. Only long-running processes (`controller`, `ui`, `publish-docs`) are started by
  the interface itself.

## Architecture

Each package imports only the packages below it; `lint-imports` enforces the order
`cli` → `api` → `controller` → `services` → `execution` → `handlers` → `scripting` →
`engine` | `warehouse` → `dialects` → `config` → `core`.

| Package | Responsibility |
|---|---|
| `cli` | argparse commands, output, exit codes, and the client for `--api`; the only package that prints |
| `api` | optional FastAPI routes, sign-in routes, bearer authorization and HTTP server lifecycle |
| `controller` | leadership, active-run working set, local supervision and shutdown |
| `services` | actor-scoped operations, doctor, setup, validate, config files, cloning, lineage, docs site |
| `execution` | task runner, process supervisor, ready-task scheduler, run lifecycle, cross-pipeline gates |
| `handlers` | SQL actions, business rules, Python scripts, email alerts |
| `scripting` | the helper module Python scripts import |
| `engine` / `warehouse` | Engine DB access and warehouse access (siblings, independent) |
| `dialects` | per-database SQL and connection details |
| `config` | `craft-connector.yml` model and loader |
| `core` | errors, enums, actors, time, dependency graph, text parsing |

**SQL lives in the dialects layer.**
- Engine DB queries are files in the query catalog: `dialects/engine/queries/<name>.sql`,
  overridden per dialect in `dialects/engine/<dialect>/queries/`, and run with
  `etl_craft.engine.queries.statement(conn, "<name>")`. Do not write SQL text in
  `engine/`, `services/` or `execution/`.
- Dialect differences go into the dialect classes (`dialects/engine/base.py`,
  `dialects/warehouse/base.py`). Do not branch on `dialect.name` outside `dialects/`.

**Migrations.**
- An Engine DB change ships as a migration in both `dialects/engine/postgres/migrations/` and
  `dialects/engine/sqlite/migrations/`, with both `schema.sql` files updated to match. The
  upgrade test must pass from every released schema in `tests/fixtures/schemas/`.
- Released migration files are immutable: a file is released once a version tag (`v*`)
  contains it. Correct an unreleased migration in place before its first release, and
  recreate any development Engine DB that already applied it. Behaviour that Python must run
  around a released migration is attached with markers
  (`dialects/engine/migration-markers.toml` and SQL headers, read by
  `engine/migrations.py`).
- A migration that needs a new marker, or a Python branch for one file, is a design change:
  stop and ask.

**Reuse before writing.** These exist; use them:
- `core.time.as_utc` for every timestamp;
- `core.actor.acting_as` / `current_actor` for attribution;
- `core.text.parse_jdbc_url` for JDBC URLs;
- `handlers.sql.spec.parse_flag` for Y/N task parameters;
- `cli.output.Output.result` for a command's JSON or text output;
- `core.enums` (and its `core.enums_<concern>` splits) for every closed value set;
- `core.errors` for every error.

If a helper you need exists privately in another module, move it to the right layer and share
it. Never copy it, and never import another module's `_private` name.

## Engineering standards

- **One implementation per concern.** Before adding a helper, a type, a module or a dependency,
  search for the existing one.
- **Need a second user.** A new abstraction, interface, compatibility branch, setting or
  dependency needs a second caller that exists now. Name it in the pull request; otherwise
  write the concrete code.
- **No "reserved for later".** No fields, parameters or tables kept for later.
- **Enums, not strings.** Statuses, actions, handlers, roles and modes come from `core.enums`.
  Never compare against or return string literals such as `"SUCCESS"`. A new closed set of
  values gets an enum in `core/enums.py`, or in the `core/enums_<concern>.py` module split from it
  for that concern (`enums_access`: environments, governance, access). When `core/enums.py`
  reaches the module size limit, split a cohesive group into such a module.
- **Errors.** Raise from the `core.errors` hierarchy, never `ValueError` or `RuntimeError` for
  a condition a user can cause. Every error class has its own `ExitCode`: a new class takes the
  next free code, and a test enforces uniqueness.
- **No swallowed errors.** A broad `except Exception` re-raises, or sits at a process boundary
  that records the failure (a child process, the server loop) and says so in a comment.
- **Small functions.** A function does one thing: split long or branchy functions by
  responsibility, and keep parameter lists short (group related values in a dataclass).
  `make quality` enforces the limits (see [The quality baseline](#the-quality-baseline)).
- **Delete what you replace.** Remove the superseded path, its tests and its docs in the same
  pull request.
- **Actors are set on purpose.** Every action runs under `core.actor.acting_as` with the actor
  that requested it; the engine's own work (finalizing, reconciling, retries) sets the system
  actor explicitly. Never rely on `current_actor()` falling back to the system actor.
- **Logging and output.** Log through `logging` (`etl_craft.<module>` loggers). Only `cli`
  prints.
- **Raw SQL** aliases every selected column in lowercase: SQLite and PostgreSQL disagree on the
  case of unquoted identifiers.
- **Comments and docstrings** describe current behaviour. No decision tags, dates, review ids or
  change narratives; `make history` rejects them. History belongs in the commit and the pull
  request.

## Tests

- Test behaviour through public interfaces, including the failure paths and their messages.
- Every new test fails without the change. Check it: `git stash push src`, run the test,
  `git stash pop`. Name it in the pull request.
- Never use `sleep` to order events or separate timestamps. Pass explicit times, use
  `tests/fixtures/races.py` for races, and poll with a bounded wait.
- Assert on named columns, not row positions. Never assert an exact list of something that grows
  (migrations, commands, tables); assert the property you need.
- Every test carries its suite's marker from `release/required-suites.toml` (or `harness`);
  collection fails otherwise. Tests that need a service call `fixtures.services.require(...)`.
- A stabilization defect's regression test is listed in `release/regressions.toml`.

## Scope and pull requests

- **One roadmap item is one cohesive pull request**, unless the item itself lists slices.
- **A refactor keeps behaviour** and states its `src/` line delta.
- **`docs/development/` changes only in `docs(plan)` pull requests.** The exception is the item's
  own Progress row and Handover notes, which hold decisions and acceptance availability, not logs.
- **`release/evidence/` changes only in release pull requests.**
- **Branches:** `<type>/<area>-<topic>`. **Commits:** Conventional Commits with the item id,
  `<type>(<area>): <summary> (road to 1.0.0, <id>)`.

## Definition of done

1. The item's "Done when" is met. Each point appears checked in the pull request.
2. Tests at the right level exist, carry their markers and fail on the old code.
3. `make static docs` and the tests the change touches pass locally, and CI passes the complete
   suite (see Validation).
4. User-facing behaviour (commands, `craft-connector.yml`, config files, JSON, exit codes,
   Python API) is in `CHANGELOG.md` under `[Unreleased]`, in the docs pages, and in
   `docs/examples/` and `examples/demo/` where they show it.
5. Engine DB changes ship as migrations in both dialects with matching `schema.sql` files.
6. Every new failure names the object, the value found, what was expected and the remedy.
7. The superseded path is deleted, and nothing new exists for a single caller.
8. The roadmap's Progress row and Handover notes are updated, with no item text rewritten.

## Validation

- Every AI agent waits for a long run (a test suite, races, a demo, CI) with one call that
  returns when the run ends:
  - a background job the harness reports when it exits (a completion notice or trigger);
  - or a single blocking command, for example a foreground run with a long timeout, or
    `gh run watch <run-id> --exit-status`;
  - or, for a condition no command reports, a bounded `timeout <s> sh -c 'until <check>; do
    sleep 30; done'` inside one call.
- Never check a log, a process or a CI run every few minutes across separate tool calls:
  every check re-sends the whole session.
- Write the run's output to a log file and read it once, after the run ends.
- After `gh pr merge --auto`, end the session; don't wait for CI at all.
- Where an item needs CI results (timing runs, a failure to read), use `gh run watch
  --exit-status`, then read the job logs once.

Follow `.agents/skills/pre-pr-checks/SKILL.md`. In short:

```bash
make sync                 # dependencies: once, and after uv.lock changes
make static               # ruff, format, mypy --strict, lint-imports, history, regressions,
                          # quality baseline: every check except the test suite
make docs                 # strict documentation build
make services-up          # local PostgreSQL, MinIO, Iceberg REST, Trino, Mailpit (Docker)
```

- **Tests:** locally, run the tests for the code the change touches. CI runs the complete
  suite once on Linux 3.11, plus unit tests and SQLite transitions on every other supported
  Python/Linux combination and the newest Python on macOS, and the installed-wheel demos
  on every pull request, in under ten minutes, and is the authority for them. `make test` and `make check` run in parallel;
  service suites default to four workers.
  The release gate retains every Python × OS combination. Use `make test-changed` locally
  before a PR; CI always runs the complete suite and is the authority.
- **Concurrency:** a change to run creation, leases, locks, ROW_ID allocation or gates runs its
  tests 20 times locally, because CI runs each test once.
- **CI time:** required pull-request CI stays under ten minutes of execution time (the longest
  chain of job run times through `needs`). Report runner queue time and wall time separately. A change that would take it
  over is a stop-and-ask.
- **Cloud suites:** CI never runs the Databricks and Snowflake suites. A change to their dialects
  or to SQL every warehouse runs needs them (see the skill). Try each affected cloud once. When
  it is unavailable (no credentials, compute will not start, the provider refuses before any SQL
  runs), record that in the pull request and the roadmap's "Acceptance availability" note and
  merge as usual. A cloud test that ran and failed is a failure: fix it. Release evidence still
  needs every cloud suite to pass.
- **Workflows:** a change under `.github/workflows/` needs `uvx --from actionlint-py actionlint`.

### The quality baseline

`scripts/check_quality.py` (`make quality`, part of `make check`) scores every module in
`src/etl_craft`:

- **Size and complexity** (ruff C901, PLR0912, PLR0913 and PLR0915): functions over 80 lines,
  modules over 800 lines.
- **Error handling:** broad `except` clauses that swallow the error, and raised built-in
  exceptions.
- **Conventions:** status strings instead of `core.enums`, and Engine DB SQL text or `dialect.name`
  checks outside the dialects layer.
- **Suppressions and shortcuts:** `type: ignore`, `noqa`, `pragma: no cover`, `cast`, bare `Any`
  in signatures, and imports of private names.
- **Dependencies:** new core dependencies.

`scripts/quality-baseline.json` records today's scores.
- **A score above the baseline fails.** Fix the code; do not edit the baseline.
- **A score below the baseline also fails until you run `make quality-baseline`** and commit the
  file, so the improvement is kept.
- **Growing the baseline needs the owner.** The PR policy workflow refuses a pull request that
  grows the baseline unless it carries the `quality-exception` label, which only the owner
  applies. Never add that label. If a change cannot fit, stop and ask.

## Opening and merging a pull request

1. **Self-review the diff** as a reviewer would: the design rules, the architecture, the
   engineering standards, the tests, the scope. Fix what you find.
2. **Fill in every section of `.github/pull_request_template.md`.** "Concerns and deviations"
   and "New public surface and dependencies" say "None" when empty; never leave them out.
   The PR policy workflow checks the sections and the ticked "Done when" points, and runs
   again when the description is edited.
3. **Merge with auto-merge** (`gh pr merge <n> --squash --auto`), keeping the branch, and end
   the session instead of watching CI: GitHub merges once every required check passes. Never
   merge with a red, pending or skipped required check. Never rerun a failed job without first
   reading its log and explaining the failure.

## Reporting

Report what happened, plainly. For each validation step give the counts, the duration and
anything that could not run. Keep failures caused by the environment (no Docker, no
credentials, no network) separate from failures in the code. Never describe a check as passed
unless you saw it pass.

## Reference

- Roadmap and handover notes: `docs/development/road-to-1.0.0.md`
- Metadata and UI design: `docs/development/metadata-and-ui.md`
- Release evidence, gate, chaos and soak: `release/README.md`
- Procedures (Agent Skills): `.agents/skills/roadmap-item/SKILL.md` (take an item from branch
  to merge) and `.agents/skills/pre-pr-checks/SKILL.md` (validation)
- Test skills, one per kind of defect: `.agents/skills/test-*/SKILL.md` (ETL correctness, run
  state, chaos and recovery, security, upgrades, failure messages)
