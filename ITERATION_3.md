# etl-craft — iteration 3 review

**Written for:** whoever (person or model) picks up the next round of work on this repo.

## Status

| Round | Date | Scope | Outcome |
|---|---|---|---|
| 1 | 2026-09-23 | Fresh shippability/bug review against HEAD `8e0f96f` | **E3-01…E3-07**, below |
| 2 | 2026-09-23 | Verification of round 1's fixes (commit `3915fd8`), plus the large Databricks/Snowflake cloud-connectivity feature landed in the same commit | **All seven round-1 items independently re-verified FIXED.** New findings **E3-08…E3-15**, below — one of them (E3-08) a real secret-leak path |

**Round 1's seven items are closed.** Commit `3915fd8` ("Verify Databricks and Snowflake against
real cloud endpoints, and fix iteration 3's findings") fixed all of E3-01 through E3-07 in the same
commit as a much larger feature addition (live Databricks/Snowflake cloud verification, a new
"preferred connection shape" for both). Round 2 independently re-read and, for the top items,
re-ran the actual regression tests against live Docker Postgres to confirm each fix holds and
introduces no new bug of its own — six of seven came back completely clean; one (E3-02) has a
narrow residual gap, recorded as E3-09. Round 2 also found one **genuine, reproduced secret-leak
path** in the new cloud-connectivity feature (E3-08) — read that one first.

## Remediation (2026-09-23, Codex)

**Round 2 is closed:** E3-08–E3-13 and E3-15 are fixed; E3-14 was investigated
and its proposed failure does not occur in the current rebuild paths. E3-16 below
records a separately reproduced Snowflake validation mismatch, now fixed.
The original review and its measurements below are retained as historical evidence.
E2-18 (logging) and E2-20 (task output capture) remain explicitly deferred.

Verification: `make check`: **619 passed, 4 skipped, 95.36% coverage**; schema checks, mypy, Black, Ruff, and pydocstyle passed. `make wheel-smoke` also passed. Cloud credentials were not loaded; Claude's
prior live checks remain the cloud verification record. The secret-persistence and
watermark regressions both failed against `3915fd8` before passing with the fixes.
The managed-storage validation test also failed before its fix.

## How to use this file

- Items are numbered `E3-nn`, continuing across every round in this file — never renumbered,
  following on from `ITERATION_2.md`'s `E2-01`…`E2-89`. Reference them in commits/PRs.
- A closed item keeps its original write-up (this project's own established practice — see
  `ITERATION_2.md`) with a **FIXED** status line added directly under its heading, stating what
  round closed it and pointing at the regression test that proves it.
- Every open item below was read and independently re-verified against the actual current code —
  not taken on faith from any commit message or from `CLAUDE.md`'s own narrative, which this file
  has twice now caught running ahead of what it actually verifies (see D-1/D-7).

## Review method (round 2)

Ran `make check` for real against the live Docker stack (Postgres, Trino/Iceberg/MinIO — all
already up): **578 passed, 4 skipped (2 Databricks + 2 Snowflake, correctly gated on missing cloud
credentials), 94.17% coverage, mypy/black/ruff/pydocstyle all clean, exit 0.** Then five
independent, parallel passes: (1) re-verifying each of E3-01…E3-07's fixes against the current
code, re-running the specific regression test for each against live Postgres; (2) the new cloud
connectivity feature end to end — `warehouse.py`, `config.py`, `configure.py`, and the
Databricks/Snowflake-specific branches in `sql_actions.py` — with a specific eye on where a secret
could leak; (3) test-suite quality, documentation consistency, and an independent secrets sweep of
the whole repo/git history; (4) a fresh bug-hunt on `business_rules.py` and `orchestrator.py`,
whose diffs in this commit were larger than their stated purpose explained, plus the schema.sql
change and its migration pairing. Every finding rated major or higher in this round (E3-08, E3-09)
was then independently re-read and confirmed directly by tracing the exact code path myself before
being kept — E3-08 by tracing `preferred_connection_url` → `configure.py`'s legacy-manifest write
path line by line; E3-09 by reading `run_pipeline`'s full control flow.

---

## Round 1 findings — E3-01 through E3-07, all FIXED

### E3-01 — `setup` writes a different secret-variable naming convention than the docs teach · **FIXED (round 2, commit `3915fd8`)**

`configure.py` now reads bootstrap input as `ENGINE_JDBC_URL`/`ENGINE_USER`/`ENGINE_AUTH_MODE`
(not `ETL_CRAFT_POSTGRES_*`) and writes those exact same names into the canonical manifest's
`Variables` mapping — the bootstrap-input name and the written pointer name are now one string,
matching what `docs/configuration.md` and both shipped worked examples teach.
`docs/craft-connector.variables.env`, `docs/warehouse-test.env.example`, and
`scripts/wheel-smoke.sh` were all updated to match. New test
`test_setup_does_not_discard_a_hand_authored_manifest_following_the_docs`
(`tests/test_unit.py:3264`) drives the exact scenario this item described — hand-authors a
docs-convention manifest, re-runs `configure_from_env`, and verifies through `load_config` that the
connection still resolves. Re-run live: passes.

<details><summary>Original write-up (round 1)</summary>

**Where:** [`configure.py:115-170`](src/etl_craft/configure.py#L115-L170) (bootstrap input names),
[`configure.py:289-336`](src/etl_craft/configure.py#L289-L336) (`_write_canonical_manifest`),
contrast [`docs/craft-connector.example.yml:19-37`](docs/craft-connector.example.yml#L19-L37) and
[`docs/configuration.md:29-51`](docs/configuration.md#L29-L51).

Two genuinely different naming conventions existed for the same job, unreconciled: the canonical
manifest format's `Variables` mapping taught `ENGINE_JDBC_URL`-style names in the docs, while
`setup` read and wrote fixed `ETL_CRAFT_POSTGRES_*`/`ETL_CRAFT_WAREHOUSE_*` names instead — and the
one regression test that existed *locked the mismatch in* as intended behaviour. A team that
hand-authored a manifest per the docs and later re-ran `etl-craft setup` (the tool's own
advertised "run it again to update" contract) had its `Variables` mapping silently overwritten
with names nothing in its environment had ever set, and the next `etl-craft run` failed to resolve
its connection.

</details>

### E3-02 — Pipeline-level cross-pipeline dependency tracker never receives the gate-resolved run id · **FIXED (round 2, commit `3915fd8`), with one residual gap — see E3-09**

`init_pipeline_run` (`orchestrator.py:334-353`) now calls
`consume_pipeline_dependency_edges(engine, pipeline_id, gate.consumed)` immediately after the gate
resolves — the one point under `Mode=orchestrator` where the gate is actually evaluated, since
`--init-only` and `--finalize-only` are separate CLI invocations with no way for an in-memory value
to survive between them. `finalize_active_run` now passes `record_consumption=False` so it no
longer re-derives and clobbers the watermark. New test
`test_init_then_finalize_consumes_the_gate_resolved_run_not_a_newer_one`
(`tests/test_integration.py:3445`) spans a real `init_pipeline_run` call, a second qualifying
upstream run appearing in between, then a separate `finalize_active_run` call, and asserts the
tracker still points at the first run. Re-run live: passes; confirmed it would have failed
pre-fix. **But** `run_pipeline` (the local-mode path) has a narrower version of the same bug left
open when it joins a run it didn't itself gate — see **E3-09** below, found in this round.

<details><summary>Original write-up (round 1)</summary>

**Where:** [`orchestrator.py:209,301-310,345-351,375`](src/etl_craft/orchestrator.py#L209),
[`crosspipe.py:313-359`](src/etl_craft/crosspipe.py#L313-L359), contrast the correctly-wired task
half at [`runner.py:323`](src/etl_craft/runner.py#L323).

`crosspipe.check_pipeline_dependencies()` returned a `PipelineGateResult` whose `.consumed` field
was, by its own docstring, "what the gate actually resolved at start time" — but nothing in
`orchestrator.py` ever read it, so every `consume_pipeline_dependency_edges` call re-derived
"whatever qualifies now" instead of recording what was actually used. Concretely: pipeline A gates
against upstream B's run #5 and starts; while A runs, B completes run #6; A's finalize re-derives
and records #6 as consumed even though A's execution reflects nothing from #6 — A's next run
silently misses B's run #6. Reproduces any time the upstream's cadence is faster than the
downstream's own runtime, not an edge case.

</details>

### E3-03 — `fetch_recorded_versions()` collides across pipelines sharing a `TASK_CODE` · **FIXED (round 2, commit `3915fd8`)**

`fetch_recorded_versions` (`documentation.py:146-176`) now returns
`dict[tuple[str, str], int]` keyed by `(pipeline_code, task_code)`; `docs_generator.py:118-122`
consumes it via `recorded_versions.get((pipeline_code, step.task_code), 0)`. Also fixed the noted
secondary inefficiency — it's now fetched once per site (`docs_generator.py:149`) rather than once
per pipeline. New test `test_collect_docs_does_not_collide_task_codes_across_pipelines`
(`tests/test_integration.py:7574`) creates two pipelines both with a task named `shared_task`,
records different version counts on each, and asserts each pipeline's page shows its own version.
Re-run live: passes.

<details><summary>Original write-up (round 1)</summary>

**Where:** [`documentation.py:146-163`](src/etl_craft/documentation.py#L146-L163), consumed at
[`docs_generator.py:108,116`](src/etl_craft/docs_generator.py#L108).

The query correctly did `DISTINCT ON (d.TASK_ID)`, but the return line collapsed to
`{r.task_code: int(r.version) for r in rows}` — keyed by bare `TASK_CODE`, which is only unique
**per pipeline**. Two different pipelines with an ordinary shared task name (`LOAD`, `VALIDATE`)
silently overwrote each other's documentation-version badge on the generated docs site.

</details>

### E3-04 — `validate`'s EMAIL_ALERT-ordering check is blind to a task with zero dependency edges · **FIXED (round 2, commit `3915fd8`) — one sub-case still untested, see E3-10**

`_alert_ordering_issues` (`validate.py:672-712`) now builds `all_tasks`/`alerts`/`leaves` from the
pipeline's full active task list (`fetch_tasks_with_parameters`), not edge endpoints — fixes both
the isolated-leaf-task blind spot this item named and a second sub-case (an `EMAIL_ALERT` task
with zero edges of its own, invisible to the old `alerts` set the same way). New test
`test_validate_requires_an_email_alert_to_wait_on_an_edge_free_leaf`
(`tests/test_integration.py:2403`) covers the isolated-leaf case and passes live. The second
sub-case is logically fixed (verified by code inspection: `alerts` now includes a zero-edge
EMAIL_ALERT task regardless, and `leaves - waits_for` correctly lists every leaf when `waits_for`
is empty) but has no dedicated regression test — see **E3-10**.

<details><summary>Original write-up (round 1)</summary>

**Where:** [`validate.py:665-688`](src/etl_craft/validate.py#L665-L688), specifically
`all_tasks = {e.task_code for e in edges} | {e.depends_on_task_code for e in edges}` at line 670.

A task with **no** dependency edges at all — an entirely ordinary standalone task such as a single
ingestion step feeding nothing downstream — never appeared in any edge, so it was invisible to
`all_tasks` and therefore invisible to `leaves`. `validate` reported a pipeline clean even when its
`EMAIL_ALERT` task had no dependency on that isolated task at all.

</details>

### E3-05 — A failure while committing business-rule results left that rule's run-log row stuck `IN-PROGRESS` · **FIXED (round 2, commit `3915fd8`)**

The existing commit block (INSERT new flags, deactivate passing keys, mark `SUCCESS`) is now inside
a `try`, with a new `except Exception` that marks the row `FAILED` in its own fresh transaction and
re-raises as `HandlerError`. The 105-line diff this fix produced in `business_rules.py` is fully
accounted for by re-indentation from wrapping the block in `try` — nothing else in the file
changed in this commit; confirmed no incidental dialect-specific edits leaked in alongside it, and
no new race was introduced in the wave-parallel (`ThreadPoolExecutor`) path, since each rule
already writes to its own row via its own connection. New test
`test_business_rules_commit_failure_marks_run_log_failed_not_stuck_in_progress`
(`tests/test_integration.py:6735`) monkeypatches `_mark_run_log` to raise specifically on the
`"SUCCESS"` call — precisely the previously-unguarded block — and asserts the row ends up `FAILED`.
Re-run live: passes; confirmed it would fail pre-fix.

<details><summary>Original write-up (round 1)</summary>

**Where:** [`business_rules.py:183-249`](src/etl_craft/business_rules.py#L183-L249).

`_run_one_rule`'s `try/except Exception` wrapped only the warehouse `SELECT` step and correctly
marked the row `FAILED` if that step threw. The results-commit block that followed had no
exception handling of its own — the overall task still correctly failed, but this specific rule's
run-log row was left `IN-PROGRESS` forever, misrepresenting what happened in the audit trail.

</details>

### E3-06 — `setup` silently dropped `Cloning.External_volume`/`Base_location` on every write · **FIXED (round 2, commit `3915fd8`)**

New `_build_cloning_block` (`configure.py:404-426`) reads `ETL_CRAFT_CLONING_EXTERNAL_VOLUME`/
`_BASE_LOCATION` as bootstrap inputs; per `configure.py:420-421`, an explicit bootstrap value wins,
otherwise the on-disk value survives (`settings.cloning_external_volume or
previous.get("External_volume")`) — not just "new variables were added," the specific
"existing value wins if bootstrap doesn't supply one" behaviour this item asked for. New test
`test_setup_preserves_hand_added_cloning_storage_parameters` (`tests/test_unit.py:3177`) hand-adds
`External_volume`/`Base_location` directly into a manifest (simulating a team with no bootstrap
variable to set them through before this fix), re-runs `configure_from_env` with neither supplied,
and asserts both survive. Re-run live: passes; confirmed it would fail pre-fix.

<details><summary>Original write-up (round 1)</summary>

**Where:** [`configure.py:286`](src/etl_craft/configure.py#L286) and
[`configure.py:336`](src/etl_craft/configure.py#L336).

Both write paths replaced the whole `Cloning` block with a fresh two-key dict on every `setup` run,
with no bootstrap variable able to set `External_volume`/`Base_location` at all — a team that
hand-added them lost them the next time `setup` ran.

</details>

### E3-07 — `cloning.py` interpolates config values unescaped into `CREATE ICEBERG TABLE` DDL · **FIXED (round 2, commit `3915fd8`)**

`_create_mirror` (`cloning.py:246-257`) now rejects a value containing `'` in either
`cloning.external_volume` or `cloning.base_location` before building the DDL string, raising a
clear `ValueError` naming the offending value; confirmed the pre-existing non-empty guard at
`cloning.py:239` means this can't instead crash on `None`. New test
`test_create_mirror_rejects_a_quote_in_snowflake_storage_values` (`tests/test_integration.py:498`)
uses a real injection-shaped payload (`"v'; DROP TABLE t; --"`) and confirms the guard fires before
any SQL reaches the (real) database connection. Re-run live: passes.

<details><summary>Original write-up (round 1)</summary>

**Where:** [`cloning.py:246-249`](src/etl_craft/cloning.py#L246-L249).

A `'` in either `Cloning.External_volume` or `Cloning.Base_location` broke the statement (or, in
principle, was an injection point) since neither value was escaped or parameterized.

</details>

---

## Round 2 findings — E3-08 through E3-15

### E3-08 — A cloud connection token can be written in cleartext into `craft-connector.yml` · **major, confirmed, security**

**FIXED (remediation).** Preferred Databricks URLs retain only public connection parameters before either manifest writer can persist them. `test_setup_strips_credentials_from_a_databricks_url_written_into_a_legacy_manifest` covers the actual setup path; additional cases exclude password and OAuth credential parameters.

**Where:** [`warehouse.py:53-71`](src/etl_craft/warehouse.py#L53-L71) (`preferred_connection_url`,
Databricks branch), [`configure.py:155-179`](src/etl_craft/configure.py#L155-L179) (resolving the
bootstrap value), [`configure.py:233,318-323`](src/etl_craft/configure.py#L318-L323)
(`_write_legacy_manifest` persisting it).

The new "preferred connection shape" lets a team configure Databricks via separate
`jdbc_url`/`catalog`/`schema`/`token` bootstrap fields instead of one packed JDBC URL.
`preferred_connection_url`'s own docstring calls its return value a "credential-free JDBC URL" —
but for Databricks it is not one: it takes the caller-supplied `fields["jdbc_url"]` **verbatim**
and appends `;ConnCatalog=...;ConnSchema=...` to it (`warehouse.py:68-71`). It never strips or
rejects `UID=`/`PWD=`/`AuthMech=` if the base URL already contains them — only the separate URL
*parser* (`_parse_databricks`, used at actual connection time) drops those parameters, and only
after this value has already been written to disk.

`configure.py:179` calls `preferred_connection_url` and stores the result as
`settings.warehouse_url`. For a **new canonical manifest**, this value is never persisted — only
variable *names* are (`_write_canonical_manifest` never writes a resolved URL) — so that path is
safe, confirmed by direct reproduction. But for an **existing legacy-shaped manifest** (an
explicitly-documented, supported upgrade path — "Existing legacy manifests remain editable without
being rewritten"), `_write_legacy_manifest` (`configure.py:318-323`) writes
`"jdbc_url": settings.warehouse_url` **literally into the warehouse profile on disk** — the exact
file this whole config-manifest pivot exists to keep secret-free and safely git-committable.

**Concrete failure scenario, reproduced directly:** Databricks' own "Connection Details" JDBC tab
presents the full string `jdbc:databricks://host:443/default;httpPath=...;AuthMech=3;UID=token;
PWD=<the personal access token>` as *the* JDBC URL to copy — not the host/http-path-only value the
docs instruct extracting. A user who pastes that full string as `WAREHOUSE_JDBC_URL` and runs
`etl-craft setup` against an **existing** (legacy-shaped) `craft-connector.yml` gets that token
written in cleartext into a file meant to be committed to version control. Reproduced end to end:
running `configure_from_env` with this input against a pre-existing legacy manifest wrote
`PWD=<token>` straight into `warehouse.jdbc_url:` on disk.

**Tested?** No. No test in the suite exercises `_write_legacy_manifest` at all
(`grep _write_legacy_manifest tests/` finds only its call sites, no direct test), and the two new
tests covering non-leakage (`test_preferred_cloud_connection_bootstrap_round_trip`,
`test_snowflake_pat_uses_driver_password_without_logging_token`) only assert the canonical-write
and in-memory-URL paths never leak — never the legacy-manifest-update path, which is the one that
actually does.

**Severity:** major, not blocker only because it needs a specific precondition — an *existing*
legacy manifest, Databricks specifically (Snowflake's preferred shape never round-trips a caller
URL this way, since it always synthesizes the URL from discrete fields — verified), and a user
pasting the transport-and-auth-inclusive JDBC string Databricks' own UI actually hands out rather
than the trimmed value the docs describe. Given that precondition is exactly what Databricks' UI
makes natural, treat this as realistic, not contrived — a long-lived personal access token
committed to a version-controlled config file is a serious exposure once it happens.

**Fix direction:** sanitize inside `preferred_connection_url` itself — strip or reject
`AuthMech`/`UID`/`PWD` (and any other credential-bearing JDBC parameter) from the caller's base
`jdbc_url` before appending `ConnCatalog`/`ConnSchema`, so the function's own "credential-free"
claim holds regardless of what a caller passes in, not only at the separate parse-time step. Add a
test that specifically drives `_write_legacy_manifest` with a credential-bearing base URL and
asserts the secret never reaches the written file.

### E3-09 — `run_pipeline` can still clobber the pipeline-level tracker when it joins a run it didn't itself gate · **minor/moderate, confirmed**

**FIXED (remediation).** Joining an existing run no longer records dependency consumption a second time. `test_run_pipeline_joining_an_already_gated_run_does_not_reconsume` covers both a run with a task and the empty-task finalization path against real Postgres.

**Where:** [`orchestrator.py:391-472`](src/etl_craft/orchestrator.py#L391-L472).

E3-02's fix threads `gate.consumed` through correctly for the `init_pipeline_run` /
`finalize_active_run` pairing (`Mode=orchestrator`'s own path — verified in E3-02 above). But
`run_pipeline` (the local-mode, no-`--task_code` path) only computes `gate_consumed` inside its own
`if existing is None:` branch (`orchestrator.py:397-405`) — i.e. only when *this call itself* mints
a new run. When `run_pipeline` instead finds an already-active run (`existing is not None` at line
397, which happens whenever a prior `init_pipeline_run`/`--init-only` call already minted and
gated one — `--init-only` is documented as "legal under both modes"), `gate_consumed` stays `None`
for the rest of the call. At finalize (`orchestrator.py:464-472`,
`_finalize_from_task_states(..., consumed_edges=gate_consumed)`), that `None` takes the default
`record_consumption=True` re-derive path and can overwrite the watermark `init_pipeline_run`
already correctly recorded moments earlier with "whatever qualifies now" — reproducing E3-02's own
bug, just via a path its regression test doesn't reach (that test only pairs `init_pipeline_run`
with `finalize_active_run`, the `Mode=orchestrator` pairing where `record_consumption=False` is
hardcoded and this specific interleaving can't happen).

The code comment at `orchestrator.py:391-394` frames "stays `None` ... for a call that finds this
run already active — a gate it didn't itself evaluate" as the documented, accepted behaviour for
callers finalizing a run they didn't gate themselves — but doesn't note that in this one case (a
prior call *in the same logical sequence* already evaluated the gate correctly), re-deriving
actively overwrites a value that was already right, which is worse than simply not recording one.

**Concrete failure scenario:** under `Mode=local`, someone runs `etl-craft run --pipeline_code X
--init-only` (mints + gates + correctly records the tracker), then later runs a bare
`etl-craft run --pipeline_code X` to execute it. The second call's own finalize re-derives and can
jump the tracker past a newer upstream run neither call actually consumed data from — silently
missing it on the next scheduled run, the same consequence E3-02 was written to prevent.

**Tested?** No. Confirmed by reading the code directly; not exercised by any existing test.

**Fix direction:** when `run_pipeline` finds an already-active run, look up whether a
tracker-consumption record already exists for it (or simply always pass `record_consumption=False`
in that branch, matching `finalize_active_run`'s own choice for the structurally identical
situation) rather than defaulting to re-derive.

### E3-10 — E3-04's second sub-case (a zero-edge `EMAIL_ALERT` task) isn't pinned by a test · **minor, test-coverage gap**

**FIXED (remediation).** `test_validate_flags_an_email_alert_with_no_edges_of_its_own` pins this case against real Postgres.

`_alert_ordering_issues`'s fix (see E3-04 above) is logically correct for both sub-cases the
original finding named, verified by code inspection. Only the isolated-leaf-task sub-case has a
dedicated regression test; an `EMAIL_ALERT` task with no dependency edges of its own — which the
old `alerts = {e.task_code for e in edges if e.handler == "EMAIL_ALERT"}` would have missed
entirely — has no test proving the fix holds for it. Low risk (the fix is a straightforward
data-source change that covers both cases identically) but cheap to close: add a pipeline fixture
with an `EMAIL_ALERT` task carrying zero `CFG_TASK_DEPENDENCY` rows and assert `validate` still
flags it against a real leaf.

### E3-11 — CLAUDE.md's own reported test counts are internally inconsistent and don't match the current repo · **minor, documentation**

**FIXED (remediation).** CLAUDE.md now leads with the full verified result for this remediation. Earlier figures are explicitly historical checkpoints, not current suite results.

`CLAUDE.md`'s "Where things stand" section cites, within the same top-of-file narrative dated
2026-09-23: "584 tests passed, 3 skipped ... 94.19% coverage" in one paragraph, then "**573
passed, 3 cloud tests skipped**, 94.39% coverage" and "**265 unit tests passed**" in another. The
actual current state, measured directly (`make check` and `pytest --collect-only`, both against
the current clean `3915fd8` tree): **578 passed, 4 skipped, 582 collected, 94.17% coverage.** None
of CLAUDE.md's cited numbers match this, and they don't fully reconcile with each other either —
the "3 skipped" figure directly contradicts the same paragraph's own description of splitting the
Snowflake test into `_native`/`_iceberg` variants (which, paired with the equivalent Databricks
split, produces exactly the 4 skips actually present, not 3). The commit message for `3915fd8`
itself states "584 tests pass locally (578 unrelated to cloud credentials, plus the Databricks and
Snowflake live suites run separately)," which implies 6 cloud tests where there are 4 — an
arithmetic mismatch present even at commit time, not something that drifted afterward.

This reads as the same "narrative added, final number never re-verified after the last fixture
change landed" pattern, not evidence of anything functionally unfinished — "265 unit tests passed"
in particular is an intermediate mid-session `test_unit.py`-only count from an earlier checkpoint
in the same day's work ("Live cloud verification continued by Codex," which — despite also being
dated 2026-09-23 — sits below, i.e. chronologically earlier than, the top entry in this file's
reverse-chronological convention), not a fourth independent full-suite total. Still worth fixing:
a reader has no way to tell which number is current without running the suite themselves, which
defeats the purpose of stating one at all.

**Fix direction:** state the test count once, at the top of the final entry for this round, and
delete or clearly label the intermediate numbers from earlier checkpoints in the same narrative as
historical rather than current.

### E3-12 — CLAUDE.md's `craft-connector.yml` living-spec section wasn't updated for the new preferred connection shape · **minor, documentation, recurrence of the same pattern this file exists to catch**

**FIXED (remediation).** The living spec includes the preferred Databricks and Snowflake variable mappings, token mode inference, and the supported legacy alternative.

`CLAUDE.md`'s "craft-connector.yml" section states in its own header that it was "rewritten
2026-09-23 to match the config-manifest pivot of commits `578ab74`..`8e0f96f`" — i.e. it was last
touched by the commit *before* `3915fd8`. It documents only the generic `jdbc_url`/`user`/
`auth_mode`/`secret`/`key_file` `Warehouse.Variables` shape and never mentions `catalog`/`account`/
`PREFERRED_CONNECTION_FIELDS`/token-only auth — the exact feature `3915fd8`'s own commit message
calls "the tested and recommended shape for both" Databricks and Snowflake. `docs/configuration.md`
and both shipped worked-example YAML files agree with each other and with the code on the new
field names (verified — no discrepancy there); only `CLAUDE.md`'s own living-spec section lagged.
This is the same class of gap E3-01's whole root cause was (a config-format change shipping without
its matching `CLAUDE.md` update), recurring at smaller scale inside the very commit that fixed it.

**Fix direction:** add the preferred-connection-shape fields to `CLAUDE.md`'s `craft-connector.yml`
section's `Warehouse:` block, alongside the existing generic shape, the same way `docs/configuration.md`
already does.

### E3-13 — The new Databricks/Snowflake dialect-specific SQL logic has no test coverage independent of live cloud credentials · **minor, test-coverage gap, stated plainly**

**FIXED (remediation).** Credential-free tests cover audit types, ALTER syntax, qualified scratch/rename names, Databricks STRING hashing, and SCD1 FIRST scalar updates after deduplication (including PRESERVE_TARGET). Snowflake rebuild tests verify matching CREATE/ALTER formats. These test emitted SQL; they do not replace live server compatibility tests.

`sql_actions.py`'s new Databricks/Snowflake branches — `_hash_expression`'s `dialect_name=
"databricks"` → `STRING` branch, `_audit_column_type`'s Snowflake-Iceberg `TIMESTAMP_NTZ(6)`
branch, `_alter_table_keyword`, `_rename_to_target`, the stage-name-splitting logic for
`NO_TEMPORARY_TABLE_DIALECTS`/`NO_DEFAULT_SCHEMA_DIALECTS`, and the `FIRST()`-based SCD1 update —
are pure functions or straightforward branches that need no live database connection to unit-test,
yet have **zero** test coverage today (`grep` across `tests/test_unit.py` confirms). The only tests
that exercise this code at all are `test_sql_actions_run_against_real_databricks_native/_iceberg`
and the Snowflake equivalents, which skip entirely without live cloud credentials — not available
in this environment, and, per CLAUDE.md's own stated caveat elsewhere, not part of any repeatable
CI gate. This is consistent with the project's repeated "found only by running it for real"
pattern (already stated as a known limit in CLAUDE.md's Architecture section) rather than a new
problem, but worth restating plainly here: a reader should not infer from "578 passed" that these
specific branches are exercised by anything that runs without a paid cloud account. The `FIRST()`
dedup mechanism was specifically traced this round and confirmed sound — `_dedupe_stage` runs
before it and guarantees ≤1 row per `MERGE_KEY` (or raises), so `FIRST()` never has more than one
candidate row to pick from and does not reintroduce E2-04/E2-75's non-determinism — but that
conclusion currently rests on manual code tracing, not a pinned unit test.

**Fix direction:** add plain unit tests for the pure-function pieces (`_hash_expression`,
`_audit_column_type`, `_alter_table_keyword`) that don't need a live connection — they can assert
on the generated SQL string directly, the same way the existing Postgres-dialect tests for these
functions already do.

### E3-14 — `_alter_table_keyword` picks its ALTER form from the task's current parameter, not the target's actual creation format · **minor, latent edge case**

**CLOSED — not reproduced (remediation).** Both callers (`_evolve_schema` and `_add_computed_surrogate_key`) create a replacement using the current format before renaming that replacement. ALTER therefore correctly uses the replacement format, even if the old target differed. `test_snowflake_rebuild_renames_the_table_it_just_created` pins native and Iceberg CREATE/ALTER consistency. No speculative metadata check was added.

**Where:** `_alter_table_keyword` (`sql_actions.py`), keyed off
`conn.info[TABLE_FORMAT_INFO_KEY]` — this task run's currently-resolved `TABLE_FORMAT` parameter —
rather than the physical target table's actual format as created. If a task's `TABLE_FORMAT`
parameter is changed (`iceberg` → `native` or vice versa) without recreating the target table, a
later schema-evolution or `ROW_ID` rebuild would select the wrong `ALTER TABLE` / `ALTER ICEBERG
TABLE` keyword against Snowflake and fail. No code path today validates a `TABLE_FORMAT` change
against an already-existing target before this point. Narrow — requires deliberately changing a
declared `TABLE_FORMAT` on a task whose target already exists — but worth a `validate`-time check
(does the target's actual format, if introspectable, match the declared `TABLE_FORMAT`?) given how
much attention adjacent format-declaration correctness already gets elsewhere in this codebase
(E2-59/E2-60-class checks).

### E3-15 — `config.py`'s token-connection mixed-shape rejections are untested; a silently-ignored stray field · **minor, test-coverage gap**

**FIXED (remediation).** Invalid token dialects, competing secrets/auth modes, and unused fields have regression tests. Token profiles now reject unused fields such as Snowflake jdbc_url instead of silently discarding them; explicit auth_mode=token remains accepted.

The new token-connection branch in `config.py` (~line 673) rejects a few invalid combinations —
"token fields require Warehouse.Name Databricks or Snowflake," "token connection must not specify
another secret/auth mode" — with no test exercising either rejection message. Separately, a
Snowflake token profile that also declares an unused `jdbc_url` variable is silently dropped rather
than flagged as a likely mistake; low-risk (extra, unused config is harmless), but inconsistent
with this codebase's general practice of flagging unrecognized/contradictory `CFG_`/config input
rather than ignoring it quietly (see `validate.py`'s `PARAMETER_NAME` vocabulary check, E2-25's
remainder, for the established precedent).

---

### E3-16 — Validation rejects supported Snowflake-managed Iceberg storage

**FIXED (remediation; newly reproduced).** Execution already defaults a missing
EXTERNAL_VOLUME to SNOWFLAKE_MANAGED, but validate still required both external
storage parameters. Validation now accepts implicit and explicit managed storage,
and requires BASE_LOCATION only for a customer external volume. Cloning's separate
storage contract is unchanged. `test_validate_snowflake_storage_matches_managed_table_creation`
covers both managed cases and customer volumes with/without a base path.

## Documentation debt — original round-2 assessment

**Remediation update:** D-1/D-7 are closed by the living-spec and measured-count updates.
The assessment below is retained as the review snapshot.

- **D-1 (round 1) — still open, and now compounded by E3-12.** `CLAUDE.md`'s `craft-connector.yml`
  living-spec section has been rewritten once since round 1 (to match the `578ab74`/`8e0f96f`
  pivot) but wasn't touched again for the preferred-connection-shape addition in `3915fd8` — see
  E3-12 for the current specific gap.
- **D-2 (round 1) — resolved.** `SECURITY.md`, `docs/operations.md`, `docs/release-checklist.md`,
  `CHANGELOG.md` are now referenced from `CLAUDE.md`'s craft-connector.yml section's closing
  "Documentation surface" paragraph.
- **D-3 (round 1) — resolved.** `CLAUDE.md`'s Architecture section now has its own "what's actually
  verified versus what's supported in principle" paragraph matching `docs/operations.md`'s scoping,
  including the live Databricks/Snowflake verification results from this round's underlying work.
- **D-4 (round 1) — resolved, self-corrected one commit later.** Traced directly: the DDL
  `sql/schema.sql`'s 23-line diff in `3915fd8` describes (`SCHEMA_MIGRATIONS` gaining `SOURCE`/
  `CHECKSUM`) was actually added correctly, paired with `sql/migrations/0004_migration_streams_
  and_checksums.sql`, one commit *earlier* in `8e0f96f` — `3915fd8`'s own 23-line diff is purely
  the missing trailer note being filled in, which `8e0f96f` had omitted. No functional gap ever
  existed; the process gap self-corrected before this review even ran. No open action.
- **D-5 (round 1) — resolved.** `generate-yml --global` now has its own CLI table row.
- **D-6 (round 1) — still accurate, unchanged.** E2-18 (logging)/E2-20 (task output) remain
  deferred; `grep -rn logging src/etl_craft/` is still empty. Still the largest standing
  operability gap; nothing in either round touched it.
- **D-7 (new, round 2) — see E3-11.** Test-count reporting is internally inconsistent within
  `CLAUDE.md`'s own current text.

---

## Test coverage gaps worth closing

**Remediation update:** All gaps listed here are closed. Mixed-format rejection
already has `test_mixed_legacy_and_manifest_sections_rejected` (the review missed it).
`test_migration_ledger_rejects_unsafe_adoption` now covers all five listed ledger errors.
E3-08–E3-15 have the regression coverage noted under their respective findings.

Carried forward from round 1, as assessed before remediation:
- `config.py`'s "mixes manifest sections with legacy sections" rejection has no test.
- `migrate.py`'s legacy-ledger-adoption logic has five untested branches (ambiguous adoption,
  unclassifiable legacy filename, simultaneous LEGACY+stream rows, unknown migration source,
  unverifiable PROJECT migration) — see round 1's original list for exact line numbers.

New this round:
- E3-08's fix needs a test driving `_write_legacy_manifest` with a credential-bearing base URL.
- E3-09's fix needs a test spanning `--init-only` then a separate bare `run_pipeline` call under
  `Mode=local`, asserting the tracker isn't clobbered.
- E3-10, E3-13, E3-15 — see each item above.

---

## What's already solid — don't re-litigate

- `make check` (black, ruff, pydocstyle, mypy, the disposable-database Postgres schema test, and
  pytest) is fully clean as of `3915fd8`: **578 passed, 4 skipped** (2 Databricks + 2 Snowflake,
  each correctly gated on missing cloud credentials, confirmed by reading the actual skip-condition
  code — a credential present-but-wrong fails loudly rather than silently skipping), **94.17%**
  coverage against an 80% gate.
- All seven round-1 findings are genuinely fixed, each with a regression test that was re-run live
  and confirmed to fail against the pre-fix code — not merely claimed fixed.
- `ITERATION_2.md` is restored and tracked; the working tree is clean; both iteration files and
  `CHANGELOG.md`'s cross-references to them are now consistent.
- The dialect-specific SQL additions for Databricks/Snowflake were traced logically (ROW_ID/format
  clause selection routes uniformly through one `create_table_as`/`_create_iceberg_table_with_storage`
  choke point for every table-creating action including the schema-evolution and ROW_ID-rebuild
  paths; the `FIRST()` dedup mechanism cannot reintroduce non-determinism, since the dedupe guard
  runs upstream of it and guarantees ≤1 candidate row) and found sound wherever traceable — the
  gap is test coverage (E3-13), not logic, as far as this round could determine without live
  credentials.
- No accidentally-committed secret anywhere in the repo or its git history: no tracked `.env` file,
  `.gitignore` correctly excludes `.env` at any depth, and every Databricks/Snowflake example value
  in the docs is an obvious placeholder — independently swept twice this round with no finding
  beyond the one runtime *mechanism* described in E3-08 (nothing has actually leaked into git; the
  bug is that running `setup` a certain way *could* write a leak into a file a team then commits).
- No `xfail`/unconditional `@pytest.mark.skip` anywhere in the suite; `business_rules.py`'s 105-line
  diff in this commit is fully explained by re-indentation for E3-05's fix, with nothing else
  changed in the file.

---

## Suggested order of work

1. **E3-08 first** — it's the only security-relevant finding in this file, has a specific and not
   contrived trigger, and the fix is small (sanitize inside `preferred_connection_url`). Add the
   missing legacy-manifest-write test alongside it.
2. E3-09 — small, same shape as E3-02's own fix; extend `run_pipeline`'s already-active-run branch
   to match `finalize_active_run`'s choice (`record_consumption=False`) rather than defaulting to
   re-derive.
3. E3-10, E3-12, E3-14, E3-15 — small, independent, can be batched together.
4. E3-11 — pure documentation, state the current test count once and remove the stale intermediate
   numbers.
5. E3-13 plus the carried-forward `migrate.py`/`config.py` test-coverage gaps — worth closing
   before anyone relies on either the migration-checksum ledger or the Databricks/Snowflake SQL
   paths in a real deployment, since neither currently has a non-cloud-credentialed regression test.
6. E2-18/E2-20 (logging, task output) remain the largest standing operability gap, untouched by
   either round of this file — still worth scheduling as its own piece of work.
