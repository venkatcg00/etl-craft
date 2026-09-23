# etl-craft — iteration 3 review

**Written for:** whoever (person or model) picks up the next round of work on this repo.

**Review date:** 2026-09-23, against `iteration-2` branch, HEAD `8e0f96f` ("Make the canonical
connector format shippable") plus one uncommitted working-tree change (`ITERATION_2.md` deleted
— see item 0).

## Bottom line

**Engineering baseline is genuinely strong. Documentation of that baseline is not.** `make check`
(black, ruff, pydocstyle, mypy `src/`, the real-Postgres schema test, and the full pytest suite)
passes clean: 559 passed, 2 honestly-skipped (Databricks/Snowflake execution paths, gated on real
cloud credentials nobody has here), 94.19% coverage. The wheel builds, contains everything it
should (`sql/`, `py.typed`, all four migrations, LICENSE), and `scripts/wheel-smoke.sh` passes
end-to-end against the live Docker stack, including the new `setup` flow. CI matches the current
code. No stale ClickHouse references, no orphaned TODOs, no accidental `NotImplementedError`.

But: the last two commits landed a real pivot (a new canonical `craft-connector.yml` shape, plus
a rewritten migration-checksum system) that **`CLAUDE.md` — this project's own stated source of
truth — was never updated to reflect**, in violation of the project's own long-standing practice
("`CLAUDE.md` gets updated in the same commit — that file is the source of truth", per the old
`ITERATION_2.md`'s own review-method section). On top of that, this round's read found one
genuinely shippable-breaking bug in the new config-writing path, plus four other real correctness
bugs elsewhere that predate this pivot and were sitting in untested scenario space — the same
pattern `ITERATION_2.md` flagged in round 1 (E2-30: two-component interactions the suite tests
each half of, never together).

**Not ready to tag a release today.** `pyproject.toml` is `0.1.0`, `Development Status :: 3 -
Alpha`, no git tag exists — that's an honest, deliberate state, not an oversight. Treat this file
as the punch list for what "ready" still needs.

## How to use this file

- Items are numbered `E3-nn`, following on from `ITERATION_2.md`'s `E2-01`…`E2-89` (do not
  reuse those numbers). Reference them in commits/PRs.
- Every item below was **read and independently re-verified against the actual current code** as
  part of writing this file (exact `file:line`, and for the top four, the literal matching test
  was located and read) — not taken on faith from any prior summary, including this project's own
  `CLAUDE.md`.
- `CLAUDE.md` states it is kept in sync with the code after every change. Section "Documentation
  debt" below is the list of places that is currently untrue. Fix `CLAUDE.md` in the same commit
  as whatever code change addresses each item, per the project's own stated practice — don't let
  this list grow a third round.

## Review method

Ran `make check` for real against the live Docker stack (Postgres on 55432, Trino/Iceberg/MinIO
already up from a prior session) — clean, see above. Then four independent, parallel close reads
of the full `src/etl_craft/` tree (config/manifest layer; execution engine — `sql_actions.py`,
`business_rules.py`, `crosspipe.py`, `orchestrator.py`, `runner.py`, `resolver.py`,
`warehouse.py`, `handlers.py`; packaging/CI/docs; and everything else — `cfg.py`, `validate.py`,
`cloning.py`, `email_alert.py`, `column_lineage.py`, `documentation.py`, `docs_generator.py`,
`doctor.py`, `db.py`, `generate_yml.py`, `runlog.py`, `limits.py`), each cross-checked against the
real test files for whether a suspected scenario is actually exercised. Every finding below that
carries a severity of major or higher was then independently re-read and confirmed directly
(exact lines quoted, consuming code traced, and where a specific test was cited, that test was
opened and its assertions checked) before being kept in this file. Nothing here is a first-pass
guess.

---

## 0. Before anything else: `ITERATION_2.md` is deleted, uncommitted

`git status` on this branch shows `D ITERATION_2.md` — 2510 lines, unstaged, sitting on top of a
HEAD that still has the file. This wasn't part of commit `8e0f96f` or any other commit; it's
working-tree state as of whenever this review started, with no commit message or `CLAUDE.md` note
explaining it.

This matters because the file is still load-bearing:
- `CLAUDE.md` references it by name over a dozen times as the authoritative iteration-2 decision
  record ("read that summary anyway before touching the schema further", etc.).
- `CHANGELOG.md:4` says "Earlier development history remains available in the Git log and
  `ITERATION_2.md`" — a pointer that is currently false in this working tree.

**Action needed before anything else ships or is handed off**: either restore it
(`git restore ITERATION_2.md` — it's still in HEAD, this is non-destructive) and commit that as
its own change, or make a deliberate decision to retire it (e.g. superseded by `CHANGELOG.md` +
this file) and fix the `CLAUDE.md`/`CHANGELOG.md` cross-references that assume it still exists.
Do not let this stay an unexplained working-tree diff. Not fixed here — it's not this reviewer's
call whether the deletion was intentional.

---

## Correctness bugs, most severe first

### E3-01 — `setup` writes a different secret-variable naming convention than the one the shipped docs teach, and silently discards a hand-authored canonical manifest's own mapping · **major, confirmed**

**Where:** [`configure.py:115-170`](src/etl_craft/configure.py#L115-L170) (bootstrap input names),
[`configure.py:289-336`](src/etl_craft/configure.py#L289-L336) (`_write_canonical_manifest`),
contrast [`docs/craft-connector.example.yml:19-37`](docs/craft-connector.example.yml#L19-L37) and
[`docs/configuration.md:29-51`](docs/configuration.md#L29-L51).

Two genuinely different naming conventions exist for the same job, unreconciled:

1. The canonical manifest format itself (what `config.py` parses, what the shipped worked
   examples teach) treats `Engine.Variables`/`Warehouse.Variables` as an arbitrary mapping —
   field name → *the name of whatever env var or `.env` key holds the real value at runtime*.
   `docs/craft-connector.example.yml` and `docs/configuration.md` both teach `ENGINE_JDBC_URL`,
   `ENGINE_USER`, `ENGINE_AUTH_MODE`, `ENGINE_SECRET`, `WAREHOUSE_JDBC_URL`, etc.
2. `etl-craft setup` (`configure_from_env`) reads its own **bootstrap** input under fixed
   `ETL_CRAFT_POSTGRES_*`/`ETL_CRAFT_WAREHOUSE_*` names, then **writes those same literal names**
   into the manifest's `Variables` mapping it produces
   (`configure.py:305-308`: `"jdbc_url": "ETL_CRAFT_POSTGRES_JDBC_URL"`, etc.). Note also that
   the Engine-DB half keeps the pre-rename token `POSTGRES` in every variable name even though
   the section itself was renamed `Engine:` everywhere else in this same pivot.

The regression test that exists for this,
[`test_configure_from_env_replaces_the_selected_canonical_profile`](tests/test_unit.py#L3001-L3015),
actually **locks the mismatch in as intended behaviour** — it asserts
`raw["Engine"]["Variables"]["jdbc_url"] == "ETL_CRAFT_POSTGRES_JDBC_URL"`. So this isn't an
untested edge case; it's the codified, on-purpose output of `setup`, just never reconciled with
what the docs and worked examples separately teach a team to hand-write.

**Concrete failure scenario:** a team follows `docs/configuration.md`/the shipped example files
verbatim, hand-authoring `craft-connector.yml` with `Engine.Variables.jdbc_url: ENGINE_JDBC_URL`
and setting `ENGINE_JDBC_URL` in their environment — this works, and is a fully valid canonical
manifest on its own terms. Later, per the tool's own advertised design ("one idempotent command
... run it again to update"), someone runs `etl-craft setup --from-environment` to pick up a
config change. `_write_canonical_manifest` unconditionally overwrites `raw["Engine"]` and
`raw["Warehouse"]` wholesale with the `ETL_CRAFT_POSTGRES_*`/`ETL_CRAFT_WAREHOUSE_*` mapping,
discarding the team's `ENGINE_JDBC_URL` mapping entirely. Unless `ETL_CRAFT_POSTGRES_JDBC_URL`
also happens to be set (it likely isn't — the team was never told to set it), every subsequent
`etl-craft run` now fails to resolve its connection. The failure is loud (a clear "variable not
found" `ConfigError`, not silent corruption), but it directly breaks the one promise `setup`
makes about itself.

**Fix direction:** pick one convention and use it in both places — either make `setup` write
`ENGINE_*`/`WAREHOUSE_*` (matching the docs), or change the docs/examples to teach
`ETL_CRAFT_POSTGRES_*`/`ETL_CRAFT_WAREHOUSE_*`. Either way, `setup` re-run against an existing
manifest whose `Variables` mapping doesn't match its own hardcoded names should probably not
silently clobber it — at minimum, warn.

### E3-02 — Pipeline-level cross-pipeline dependency tracker never receives the gate-resolved run id; the pipeline half of E2-12 is dead code · **major, confirmed**

**Where:** [`orchestrator.py:209,301-310,345-351,375`](src/etl_craft/orchestrator.py#L209),
[`crosspipe.py:313-359`](src/etl_craft/crosspipe.py#L313-L359), contrast the correctly-wired task
half at [`runner.py:323`](src/etl_craft/runner.py#L323).

`crosspipe.check_pipeline_dependencies()` returns a `PipelineGateResult` whose `.consumed` field
is, by its own docstring, "what the gate actually resolved at start time, edge id → run id" —
built specifically so a later run of the same upstream can't have its watermark silently jumped
forward to a run the downstream never actually read (`crosspipe.py:318-321`, documented as the
E2-12 fix). `init_pipeline_run` and `run_pipeline` both compute this `gate` value but never read
`.consumed` off it — grep confirms nothing in `orchestrator.py` reads that field. All three call
sites of `consume_pipeline_dependency_edges(engine, pipeline_id)` (lines 209, 351, 375) pass only
two positional arguments, so `consumed` defaults to `None` and the function falls into its
pre-E2-12 "re-derive satisfaction right now" branch every time — exactly the bug E2-12 was
written to close, still present for pipelines. The task-level equivalent
(`consume_task_dependency_edges(engine, task_id, None if force else consumed_edges)` at
`runner.py:323`) correctly threads the resolved mapping through and is not affected.

**Concrete failure scenario:** pipeline A depends on pipeline B (`SUCCESS`). A's run gates against
B's already-finished run #5 and starts. Under `Mode=orchestrator` this is structurally the normal
case, not a rare race — A's execution spans two separate CLI invocations (`--init-only`, then much
later `--finalize-only`), so there's no way for an in-memory `.consumed` value to survive between
them even if it were read. While A runs, B completes another qualifying run #6. At A's finalize,
`consume_pipeline_dependency_edges` re-derives "latest qualifying run" and records #6 as consumed
— even though A's execution reflects nothing from #6. A's next scheduled run will not re-trigger
on B's run #6, silently missing it. This reproduces any time the upstream's cadence is faster than
the downstream's own runtime — not an edge case for a real deployment.

**Tested?** No. The two existing tests for `consume_pipeline_dependency_edges` both call it with
no `consumed=` argument, exercising only the always-taken re-derive path. There is no test
covering `init_pipeline_run`/`run_pipeline` + a later finalize through a different-cadence
scenario end-to-end — the task-level version has a dedicated regression test for exactly this;
the pipeline-level version does not.

### E3-03 — `fetch_recorded_versions()` keys its result by bare `TASK_CODE`, which collides across pipelines · **major, confirmed**

**Where:** [`documentation.py:146-163`](src/etl_craft/documentation.py#L146-L163), consumed at
[`docs_generator.py:108,116`](src/etl_craft/docs_generator.py#L108).

The query correctly does `DISTINCT ON (d.TASK_ID)`, but the return line collapses to
`{r.task_code: int(r.version) for r in rows}` — keyed by bare `TASK_CODE`, not `task_id` or
`(pipeline_code, task_code)`. `TASK_CODE` is only unique **per pipeline**
(`ux_tasks_code_active ON CFG_TASKS (PIPELINE_ID, TASK_CODE)`, explicitly commented in
`sql/schema.sql` as "scoped per-pipeline, not global"). `docs_generator.py` calls this once per
pipeline (`_collect_pipeline_doc_data`) purely to render each task's "docs vN" version badge.

**Concrete failure scenario:** two different pipelines each happen to have a task named, say,
`LOAD` or `VALIDATE` (an entirely ordinary naming collision across independently-authored
pipelines). Whichever pipeline's page renders last for that shared name determines what version
badge shows on **both** — one pipeline's documentation page silently displays another pipeline's
version number. Wrong data on a shipped feature, not a crash. Confirmed no test covers two
pipelines sharing a task code (`grep fetch_recorded_versions tests/` → zero hits). Minor
secondary note: this global query re-runs once per pipeline rather than once for the whole site.

**Fix direction:** key the returned dict by `task_id` (already available on the joined row) and
have callers look up by id, or by `(pipeline_id, task_code)`.

### E3-04 — `validate`'s EMAIL_ALERT-ordering check is blind to a task with zero dependency edges · **major, confirmed**

**Where:** [`validate.py:665-688`](src/etl_craft/validate.py#L665-L688), specifically
`all_tasks = {e.task_code for e in edges} | {e.depends_on_task_code for e in edges}` at line 670.

This is the E2-60 check: every `EMAIL_ALERT` task must depend on every non-alert leaf task in its
pipeline, so the completion alert can't fire before an unrelated task has finished. `all_tasks`
(and therefore `leaves`) is built purely from `CFG_TASK_DEPENDENCY` edge endpoints. A task with
**no** dependency edges at all — neither depending on anything nor depended on by anything, an
entirely ordinary standalone task such as a single ingestion step feeding nothing downstream —
never appears in any edge, so it's invisible to `all_tasks` and therefore invisible to `leaves`.
`validate` will report a pipeline as clean even when its `EMAIL_ALERT` task has no dependency on
that isolated task and can fire before it finishes.

Confirmed via the one existing test for this check,
[`test_validate_requires_an_email_alert_to_wait_on_every_leaf`](tests/test_integration.py#L2353) —
its "leaf_two" fixture is deliberately given an edge (`second depends_on first`) specifically so
it appears in `all_tasks`; a genuinely edge-free task is never exercised by any test. Note
`email_alert.run_flavour()` itself is unaffected — it reads `AUD_TASK_RUN_LOG` directly and
correctly counts the isolated task's status. The bug is specifically that `validate` fails to flag
the misconfiguration.

**Fix direction:** build `all_tasks` from the pipeline's full active task list (already available
via `cfg.fetch_pipeline_graph` or similar), not just edge endpoints.

### E3-05 — A failure while committing business-rule results leaves that rule's run-log row stuck `IN-PROGRESS` forever · **minor/moderate, confirmed**

**Where:** [`business_rules.py:183-249`](src/etl_craft/business_rules.py#L183-L249).

`_run_one_rule`'s `try/except Exception` (lines 183-201) wraps only the warehouse `SELECT` step
(fetching failing/passing keys) and correctly marks the row `FAILED` if that step throws. The
following block — `_fetch_already_active_keys`, the `INSERT INTO AUD_BUSINESS_RULES_RESULTS`, the
deactivate `UPDATE`, and `_mark_run_log(..., "SUCCESS")`, all inside one `engine.begin()` starting
around line 203 — has no exception handling of its own. If anything in that block raises (a
constraint violation, a deadlock, a transient connectivity blip), the exception propagates past
this function uncaught; the overall task still correctly fails (caught further up by
`handlers.dispatch`'s `except (ConfigError, SQLAlchemyError)`), but this specific rule's
`AUD_BUSINESS_RULES_RUN_LOG` row — created `IN-PROGRESS` in an earlier, already-committed
transaction — is never updated to `FAILED`. A retry is still safe (the whole mechanism is
idempotent), but the audit trail misrepresents what happened until then.

**Fix direction:** wrap the whole per-rule body (both the warehouse fetch and the results-commit)
in one `try`, or add a second `except` around the commit block that marks `FAILED` the same way
the first one does.

### E3-06 — `setup` silently drops `Cloning.External_volume`/`Base_location` on every write, with no bootstrap variable to set them · **minor/moderate, confirmed**

**Where:** [`configure.py:286`](src/etl_craft/configure.py#L286) and
[`configure.py:336`](src/etl_craft/configure.py#L336):
`raw["Cloning"] = {"Enabled": ..., "Scope": ...}`.

Both write paths replace the whole `Cloning` block with a fresh two-key dict on every `setup` run.
`External_volume`/`Base_location` — the fields `cloning.py` needs to mirror Engine DB tables onto
a Snowflake Iceberg warehouse (E2-68) — are never read from any `ETL_CRAFT_CLONING_*` bootstrap
variable and are silently dropped from the manifest the next time `setup` runs, even if a team
hand-added them. `grep -rn "external_volume\|External_volume" tests/` confirms this is untested.

### E3-07 — `cloning.py` interpolates config values unescaped into `CREATE ICEBERG TABLE` DDL · **minor, confirmed**

**Where:** [`cloning.py:246-249`](src/etl_craft/cloning.py#L246-L249):
`EXTERNAL_VOLUME = '{cloning.external_volume}' ... BASE_LOCATION = '{cloning.base_location}/{target_name}'`.

A `'` in either `Cloning.External_volume` or `Cloning.Base_location` breaks the statement (or, in
principle, is an injection point) since neither value is escaped or parameterized. Low severity —
these are admin-controlled `craft-connector.yml` values under the project's existing
"config is git-reviewed" trust model, same as every other piece of interpolated DDL in
`sql_actions.py` — but worth a one-line escape or a `_SAFE_IDENTIFIER`-style check given the rest
of this pivot (E2-84/E2-85) specifically hardened comparable interpolation sites elsewhere.

---

## Documentation debt — what `CLAUDE.md` currently gets wrong

None of these are code bugs. All of them are `CLAUDE.md` (and in one case `sql/schema.sql`'s own
trailer) failing to reflect what commits `578ab74`/`8e0f96f` actually shipped, in violation of the
project's stated practice of updating it in the same commit.

- **D-1 — The `craft-connector.yml` living-spec section documents the deprecated format.**
  `CLAUDE.md`'s own `craft-connector.yml` block still shows `[Execution]`/`[Source]`/`[Postgres]`/
  `[Warehouse]`/`Active_profile`/`Profiles: {dev: {...}}`. The shipped, documented format is now
  `Orchestration:`/`Secrets:`/`Engine:`/`Warehouse:`/`Cloning:`/`Dag_defaults:`/`Email:`, each with
  `Profile`/`Variables` indirection (see `docs/craft-connector.example.yml`,
  `docs/configuration.md`). The code (`config.py`) reads both — this is not a runtime break — but
  a new engineer reading only `CLAUDE.md` would write configs in a format `setup` no longer
  produces.
- **D-2 — Four new top-level docs files aren't referenced anywhere in `CLAUDE.md`.**
  `SECURITY.md`, `docs/operations.md`, `docs/release-checklist.md`, and `CHANGELOG.md` were all
  added in `8e0f96f` and are genuinely new, real content — not referenced from `CLAUDE.md`'s own
  documentation-surface description at all.
- **D-3 — `docs/operations.md` narrows the supported launch path in a way `CLAUDE.md`'s
  Architecture section doesn't reflect.** `docs/operations.md` scopes the "controlled/supported"
  path to **PostgreSQL Engine DB + PostgreSQL warehouse only**, treating DuckDB/Databricks/
  Snowflake/Trino as things a customer must separately acceptance-test before relying on.
  `CLAUDE.md`'s Architecture section still frames the warehouse story as "Postgres, or a SQL
  engine over Iceberg... settled 2026-09-22" with no equivalent scoping-down note. This is a real,
  deliberate narrowing (a good one, given the two untested cloud dialects) that should be reflected
  where `CLAUDE.md` describes warehouse support, not just in `docs/operations.md`.
- **D-4 — `sql/schema.sql`'s own "POST-SIGNOFF CHANGES" trailer has no entry for migration
  `0004`.** The `SCHEMA_MIGRATIONS` table's `CREATE TABLE` *was* correctly updated in place to the
  post-0004 shape (`SOURCE`, `CHECKSUM`, composite `(SOURCE, VERSION)` primary key) — a fresh
  install genuinely gets the current shape, so there's no functional gap. But the trailer block
  itself stops at the migration-`0003` (`ATTEMPT_COUNT`) entry, with no `[ADDITION]`/`[DEVIATION]`
  note for the stream-separation/checksum change. This violates the project's own stated
  convention in `sql/migrations/README.md`: "Each file must also be reflected directly in
  `sql/schema.sql` itself... and a note in schema.sql's own 'POST-SIGNOFF CHANGES' block."
- **D-5 — `generate-yml --global` isn't its own row in `CLAUDE.md`'s CLI table.** Minor — it's
  described in prose elsewhere in the file, but the CLI table is otherwise a complete, accurate
  list of every other command.
- **D-6 — `E2-18` (logging) and `E2-20` (task output) are still accurately described as
  deferred.** Checked directly: `grep -rn "^import logging\|^from logging\|logging\." src/etl_craft/`
  returns zero hits, and `cli.py` alone still has ~72 `print(` calls. This is the one claim in
  `CLAUDE.md`'s "Where things stand" that was checked and found **still true**, not stale — call
  it out because it remains the largest real operability gap in the codebase and nothing in this
  round's two commits touched it.

---

## Test coverage gaps worth closing (no bug confirmed, but the exact area a bug would hide in)

- `config.py:360-364`'s "mixes manifest sections with legacy sections" rejection (guards against a
  half-migrated file silently picking one format) has **no test at all** — `grep` for
  `mixes the manifest`/`has_manifest`/`has_legacy` across `tests/` returns nothing. This is exactly
  the kind of validation a format migration most needs proven.
- `migrate.py`'s legacy-ledger-adoption logic (`_legacy_adoption_plan`/`_validate_recorded_migrations`)
  has five real branches untested: a legacy filename present in both ENGINE and PROJECT streams
  (ambiguous adoption, `migrate.py:462-466`); a legacy row whose filename isn't in
  `_PRE_STREAM_ENGINE_VERSIONS` (`migrate.py:468-475`); a version with both a LEGACY row and a real
  stream row simultaneously (`migrate.py:477-481`); "SCHEMA_MIGRATIONS contains unknown migration
  source(s)" (`migrate.py:413-416`); and an applied PROJECT migration that can't be verified
  because no project directory is configured this run (`migrate.py:422-428`). This is the exact
  safety net the checksum-ledger redesign exists to provide — worth testing before trusting it on
  a real upgrade.
- No test drives a hand-authored, docs-convention (`ENGINE_JDBC_URL`-style) canonical manifest
  through `etl-craft setup` to observe the E3-01 overwrite directly — writing that test is the
  fastest way to pin the current (wrong) behaviour before fixing it.
- No test covers `init_pipeline_run`/`run_pipeline` + a later finalize spanning a different-cadence
  upstream (E3-02) — the task-level equivalent has one; the pipeline-level path does not.

---

## What's already solid — don't re-litigate

- `make check` (black, ruff, pydocstyle, mypy on all of `src/`, the disposable-database Postgres
  schema test, and pytest) is fully clean: 559 passed, 2 honestly-skipped
  (Databricks/Snowflake — real cloud credentials required, correctly gated), 94.19% coverage
  against an 80% gate.
- The wheel builds and is correct: `sql/`, `py.typed`, all four migration files plus
  `sql/migrations/README.md`, and `LICENSE` are all present in the built artifact (`unzip -l`
  confirmed). E2-13 stays fixed.
- `scripts/wheel-smoke.sh` passes end-to-end against the live Docker stack, including the new
  `setup` → `init-db`/`migrate` flow, `setup` idempotency, and `migrate` correctly reporting
  "recorded 4 packaged migration(s) as already applied."
- CI (`.github/workflows/ci.yml`) matches the current code: Python 3.11/3.12/3.13 matrix,
  `push`+`pull_request` on `main`, native Postgres `services:`, Compose-based Trino/Iceberg/MinIO
  stack, a dedicated wheel-smoke job. `dependabot.yml` is correctly configured.
  `grep -rni clickhouse` across the whole repo (code, CI, compose, dependabot) turns up nothing
  but historical/explanatory comments — the 2026-09-21 removal is clean.
- No orphaned `TODO`/`FIXME`/`XXX` anywhere in `src/`. Every `NotImplementedError` (5 total, in
  `db.py` and `warehouse.py`) is a deliberate, documented stub (token/SSO/key_file auth for
  providers nobody's specified) — none look accidentally abandoned.
- No `@pytest.mark.skip`/`xfail` anywhere in the test suite. Every runtime `pytest.skip(...)` is an
  environment-reachability guard in `conftest.py` fixtures (Postgres/Trino/Databricks/Snowflake),
  consistent with what's documented.
- `resolver.py`'s `RUN_CONDITION` arithmetic, `sql_actions.py`'s ROW_ID strategies and SCD merge
  statements (including the E2-74 SCD2 fix), and `runner.py`'s crash-detection fork were all
  traced through concrete scenarios this round and found internally consistent — no findings.
  `runner.py` in particular is the most heavily hardened path in the codebase and it shows.

---

## Suggested order of work

1. Resolve item 0 (`ITERATION_2.md`) — five minutes, unblocks having a clean starting point.
2. E3-01 (`setup` variable-naming mismatch) — pick a convention, align `configure.py` with the
   docs (or vice versa), add the missing test first so the fix is pinned.
3. E3-02 (pipeline-level tracker) — thread `gate.consumed` through the three
   `consume_pipeline_dependency_edges` call sites the same way `runner.py:323` already does for
   tasks; write the different-cadence regression test first.
4. E3-03, E3-04 (doc-version collision, validate blind spot) — both small, targeted fixes with an
   obvious regression test each.
5. E3-05, E3-06, E3-07 — smaller, can be batched together.
6. Documentation debt (D-1 through D-6) — update `CLAUDE.md` in the same commits as whatever code
   above touches those areas, rather than as a separate pass; that's what let this gap open twice
   in a row.
7. Close the test-coverage gaps list once the above is settled, before anyone relies on the
   migration-checksum ledger against a real production upgrade.
8. E2-18/E2-20 (logging, task output) remain the largest standing operability gap and aren't
   addressed by anything in this file — worth scheduling as its own piece of work, not folded in
   here.
