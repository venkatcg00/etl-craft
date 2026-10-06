# Contributing

## Branches

- `main` is the trunk. Every change reaches it through a pull request with green CI.
- Cut each branch from `main` and keep it to one item of the
  [rewrite plan](docs/development/rewrite-plan.md) or one workstream or item of
  [Road to 1.0.0](docs/development/road-to-1.0.0.md). Name it `<type>/<area>-<topic>`, for example
  `feat/core-graph` or `test/e2e-demo`.
- Squash-merge. The squash commit message follows
  [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `refactor:`,
  `test:`, `docs:`, `chore:`, `build:`, `ci:`).

## Porting from the previous implementation

Most branches port code from the `archive/iteration-2` tag (`git show archive/iteration-2:<path>`).
Porting a slice means:

1. Move the code into its layer (see [CLAUDE.md](CLAUDE.md) for the layers).
2. Keep its behaviour. A branch that changes behaviour says so in its pull request.
3. Rewrite comments and docstrings so they describe what the code does now.
4. Port the tests that cover the slice, into `tests/unit/` or `tests/integration/<area>/`.

## Comments and documentation

Comments explain why the code is the way it is when that is not obvious from the code itself.
They never record history: no decision tags, dates, review item ids or "changed because" notes.
That context belongs in the pull request and the commit message. `make history` enforces this,
and CI runs it on every pull request.

Before opening a pull request, run the complete `make check docs` suite locally and finish
relevant service and live-cloud acceptance tests. Fix failures before creating the PR.

## Definition of done

A branch is ready to merge when:

- `make check` passes: ruff lint and format, `mypy --strict`, the layer contracts
  (`lint-imports`), the history gate, and the tests with coverage of at least 90%.
- New or ported behaviour has tests at the right level (unit, integration or end-to-end).
- The documentation page for the feature exists or is updated, and `make docs` builds it
  without warnings. Merging into `main` publishes the site as the `dev` version on
  GitHub Pages.

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
guides and locked dependencies. All versions share `docs/gen_ref_pages.py`, which renders
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
`sql.replace.after_clear` and `sql.replace.after_publish`; atomic single-statement replacement
uses only `before_publish`. Ending fault points run inside the
transaction: an injected exception or hard exit rolls back status, offsets and consumption. Hard exits can leave audit rows
running and processes alive: tests must clean up their process trees and use explicit
reconciliation for expired leases. Every run reconciles before admission; `etl-craft reconcile`
requests it directly, and `mark --stale` reconciles a task before marking.
