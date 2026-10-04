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
