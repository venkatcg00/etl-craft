# Contributing

## Branches

- `main` is the trunk. Every change reaches it through a pull request with green CI.
- Cut each branch from `main` and keep it to one workstream or item of
  [Road to 1.0.0](docs/development/road-to-1.0.0.md). Name it `<type>/<area>-<topic>`, for example
  `feat/core-graph` or `test/e2e-demo`.
- Squash-merge. The squash commit message follows
  [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `refactor:`,
  `test:`, `docs:`, `chore:`, `build:`, `ci:`).

## The rules for every change

[AGENTS.md](AGENTS.md) holds the design rules, the architecture, the engineering standards, the
test rules, the scope of a pull request and the definition of done. People and coding agents
follow the same file. The procedures it refers to are in `.agents/skills/`: `roadmap-item` takes
an item from branch to merge, and `pre-pr-checks` is the validation.

The previous implementation is kept at the `archive/iteration-2` tag
(`git show archive/iteration-2:<path>`) for reference.

## Comments and documentation

Comments explain why the code is the way it is when that is not obvious from the code itself.
They never record history: no decision tags, dates, review item ids or "changed because" notes.
That context belongs in the pull request and the commit message. `make history` enforces this,
and CI runs it on every pull request.

Before opening a pull request, run the checks in `.agents/skills/pre-pr-checks/SKILL.md`:
`make static docs`, the tests for the code the change touches against the local services,
repeated runs for concurrency changes, `actionlint` for workflow changes, and the cloud tests
when warehouse SQL changes. CI runs the complete suite on the pull request in under ten minutes
and is its authority. Fix failures before asking for a merge.

CI partitions complete collections across jobs and isolated agent processes. Demo agents in
one job share their warehouse service; each owns its databases, Iceberg namespaces and email
addresses. The final **every test and matrix accounted for** check requires each shard, its test
outcomes and the package
matrix results. It combines coverage separately for each Python/platform pair before enforcing
the same 90% threshold. Individual shard coverage is not an acceptance result.

Download the `ci-*` artifacts to inspect collected IDs, outcomes, durations, runner resources
and cache hits. `scripts/ci-durations.json` holds measured case estimates grouped by suite and,
for coverage, by platform. An unmeasured test runs with a one-second estimate; it is never omitted.
Both CI and Docs accept a manual `cache=cold|warm` input for deliberate timing validation.
CI also accepts `serial` to measure the complete unsharded matrix. Ordinary pull requests do
not repeat either workflow for that measurement.

Prepared Engine DB fixtures copy a closed SQLite schema or clone a disconnected PostgreSQL
template. Every test still owns a separate database. Empty-database, initialization and upgrade
fixtures continue to create and migrate their schemas directly.

## Definition of done

The list is in [AGENTS.md](AGENTS.md#definition-of-done). Every section of the pull request
template is filled in; "None" is an answer, an empty section is not. Merging into `main`
publishes the documentation site as the `dev` version on GitHub Pages.

## The quality baseline

`make quality` (part of `make check`) holds `src/etl_craft` to `scripts/quality-baseline.json`:
no new function or module over the size and complexity limits, no more swallowed exceptions,
built-in raises, status strings, Engine DB SQL outside the dialects layer, suppressions or core
dependencies than the baseline records. After an improvement, run `make quality-baseline` and
commit the file.

A pull request that grows the baseline fails the PR policy workflow unless it carries the
`quality-exception` label. Only the owner applies it, for a growth they accept.

## Working with coding agents

Codex reads `AGENTS.md`; Claude Code reads `CLAUDE.md`, which imports it. Both load the skills
in `.agents/skills/`.

- **Brief by item, not by implementation.** "Take roadmap item S5.O.3" lets the agent read the
  item, its "Done when" and the design rules. Step-by-step implementation instructions bypass
  that judgement.
- **Stop-and-ask is intended.** When a request conflicts with a design rule, adds public surface
  or CI cost the item does not name, or needs more than one pull request, the agent answers with
  a short design note instead of code. Answer the decision it asks for. To overrule a rule on
  purpose, say so explicitly; the agent records the deviation in the pull request and the
  roadmap's Handover notes.
- **Agents run in your checkout.** Codex and Claude Code work in this repository on your
  machine, so they use its `.venv` and services:
  - `make sync` once, and again after `uv.lock` changes;
  - `make services-up` for the service-backed suites;
  - `.env.acceptance` for the Databricks and Snowflake suites.

  With those in place they can run every step of `.agents/skills/pre-pr-checks/SKILL.md`
  before opening a pull request.

## Tests

- Every test carries the marker of the suite it belongs to, from
  `release/required-suites.toml` (`unit`, `engine_postgres`, `warehouse_trino_iceberg`, ...), or
  `harness` for tests of the local services themselves. Collection fails on an unmarked test.
- `make services-up` starts the local services in `docker-compose.yml` (PostgreSQL with password
  and with client-certificate login, MinIO, an Iceberg REST catalog, Trino and Mailpit), and
  `make services-down` removes them. A test that needs a service skips when it is down;
  `ETL_CRAFT_REQUIRE_SERVICES=1` turns those skips into failures.
- Release evidence and the release gate are described in `release/README.md`.

## Local setup

```bash
make sync
uv run pre-commit install    # optional: run the same checks on every commit
```

## Documentation versions

`dev` documents `main`; each release line (such as `0.1`) documents its newest patch tag,
and `latest` aliases the newest release line. The versioned builder uses each tag's source,
guides and locked dependencies. All versions share `docs/gen_python_api.py`, which renders
API entry points and package navigation from the source in that release's worktree. Fixing
that renderer updates released API pages without changing released code or documenting
unreleased features as available in an older version.

`make docs-site` builds every version strictly and checks the rendered API content in each
version and the `latest` alias. Use it when changing the shared API renderer or versioned builder;
`make docs` checks only the current development documentation.

## Lifecycle regression harness

`release/regressions.toml` maps every stabilization defect in the roadmap to named tests.
`make regressions` verifies the assignments against fresh pytest collection; it runs in
`make check` and CI. Keep the mapping current when a test is renamed or a defect is reassigned.

`tests/fixtures/races.py` provides `two_at_once(fn_a, fn_b, at="module.function")`: both callers
wait once at that function before continuing. Place the boundary before a transaction that
serializes SQLite writers, so neither caller holds the write lock while waiting at the barrier.
`tests/fixtures/cli_project.py` initializes throwaway Engine DBs through the real CLI and
provides bounded process waits, file-backed output, row-state polling and signal helpers.
Linux process-tree assertions read `/proc`; their complete node ids are declared in the suite
manifest, so evidence runs on other platforms deselect them explicitly.

`ETL_CRAFT_FAULT` enables one named development failure. A matching name raises
`InjectedFaultError`; append `:kill` to exit immediately with status 137, without cleanup.
Unset the variable for normal operation. Available boundaries are `runner.after_timeout`
(before binding), `runner.after_bind`, `supervisor.after_mkdir`, `supervisor.after_popen`,
`child.after_outcome`, `pipeline.after_insert`, `runner.before_consumption`,
`pipeline.before_consumption`, `pipeline.after_consumption`, `attempt.after_status`,
`attempt.before_summary`, `attempt.after_consumption`, and `script.after_offset`. Replacement boundaries are `sql.replace.before_publish`,
`sql.replace.after_clear` and `sql.replace.after_publish`; append boundaries are
`sql.append.after_delete` and `sql.append.after_insert`. Atomic single-statement replacement
uses only `before_publish`. Ending fault points run inside the
transaction: an injected exception or hard exit rolls back status, offsets and consumption. Hard exits can leave audit rows
running and processes alive: tests must clean up their process trees and use explicit
reconciliation for expired leases. Every run reconciles before admission; `etl-craft reconcile`
requests it directly, and `mark --stale` reconciles a task before marking.


## Chaos checks

The chaos suite uses the existing admission, lease, transaction and intervention tests alongside
real CLI races, startup hard exits, late remote deliveries and a PostgreSQL outage.
Each test keeps its Engine DB marker and also carries the shared marker:

```bash
make services-up
ETL_CRAFT_REQUIRE_SERVICES=1 uv run pytest -q -m chaos
ETL_CRAFT_REQUIRE_SERVICES=1 uv run pytest -q -m "chaos and engine_sqlite"
ETL_CRAFT_REQUIRE_SERVICES=1 uv run pytest -q -m "chaos and engine_postgres"
```

Pull requests run the normal regression checks once; the CI workflow does not repeat after merge.
Pull requests test the dependencies in `uv.lock`. The Nightly workflow resolves the newest release
of every dependency instead, prints the ones newer than the lock, and runs the unit, Engine DB and
local warehouse tests and the wheel demo against them. It runs each night, on manual dispatch, and
on a pull request that changes it.
The Release gate workflow requires twenty consecutive chaos passes per Engine DB on release
branches or manual dispatch, after the evidence check passes. An iteration stops at its first
failure and fails the job; every completed iteration has an uploaded JUnit report. The PostgreSQL
outage affects only the test's temporary database, prevents new connections for five seconds,
and restores access in cleanup.
Tests advance abandoned leases explicitly rather than waiting for their normal expiry.
