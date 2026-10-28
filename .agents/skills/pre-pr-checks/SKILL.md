---
name: pre-pr-checks
description: Run etl-craft's validation before a pull request - make static docs, the tests for the code the change touches, repeated races for concurrency changes, actionlint, live-cloud acceptance (one try; unavailable compute is recorded, not blocking) when warehouse SQL changes and the owner made cloud budget available, and a self-review of the diff; the wheel demos run in CI. CI runs the complete suite in under ten minutes. Use before opening a PR or when asked to check the state of a branch.
---

# Pre-PR checks

Open the pull request only after every step that applies has finished green (`AGENTS.md`,
"Validation"). CI then runs the complete suite once, explicit Python/platform compatibility subsets and the
installed-wheel demos in under ten minutes, and is their authority: do not repeat that locally. Keep logs in a
scratch directory outside the repository:

```bash
SCRATCH="$(mktemp -d)"
```

Write each long step's output to `$SCRATCH/<step>.log` and check its exit status; `pytest | tail`
hides a failure. Run it so that one tool call returns its result: in the foreground with a long
timeout, or in the background only where your harness tells you when it ends. Never poll a log,
a process or CI: every check re-sends the whole session. Run the steps one at a time.

On a Linux workstation, start each long run in its own systemd scope so that an out-of-memory
kill of the editor does not take the run with it, and keep the machine from suspending:

```bash
systemd-run --user --scope --quiet -- \
  systemd-inhibit --what=sleep --why="etl-craft checks" <command> > "$SCRATCH/<step>.log" 2>&1
```

| Step | When | Takes |
|---|---|---|
| 0. Sweep removed names | a public name, field or setting was renamed or removed | seconds |
| 1. Static checks and touched tests | always | a few minutes |
| 1b. Repeated races | concurrency code changed | a few minutes |
| 2. Installed-wheel demos | only to reproduce a CI failure in them | about 15 min |
| 3. actionlint | `.github/workflows/` changed | seconds |
| 4. Cloud tests | Snowflake or Databricks code changed, or the SQL every warehouse runs, and the owner made cloud budget available | about 15 min for the affected tests |
| 5. Self-review | always | |

The local services must be up for the touched tests and step 2: `docker ps` lists the
`etl-craft-*` containers as healthy; otherwise run `make services-up`. Without Docker (a cloud
agent sandbox), run step 1 anyway. The service tests skip, and CI runs them on the pull request.
Say so in the pull request's Validation section.

## 0. Sweep removed names

`mypy --strict` covers `src/etl_craft` and `scripts/`. Tests, `examples/`, `docs/` and
`integrations/` are checked only by running them, and `dataclasses.replace()` keywords are never
type-checked. After renaming or removing a public name, field or setting, search every tracked
text file:

```bash
rg -n --hidden -g '!.git' -g '!.venv' -g '!site' -g '!dist' -g '!docs/development/**' \
  -g '!CHANGELOG.md' -g '!release/evidence/**' -e 'old_name|OldClass|old_setting' .
```

When a shared helper changes behaviour rather than name (it now raises, refuses or needs a scope
it did not), search `scripts/`, `tests/fixtures/` and `tests/e2e/` for callers that relied on the
old behaviour: the CI-only suites are the first to run them.

`examples/demo/` is the project the installed-wheel demos copy. The local gate never runs
`tests/acceptance/cloud/`: search it too for the behaviour you change, and run the cloud tests
that use it (step 4).

## 1. Static checks and touched tests

```bash
make static docs > "$SCRATCH/static.log" 2>&1; echo "exit $?"
```

`make static` runs everything in `make check` except the test suite:
- ruff lint and format check;
- `mypy --strict`;
- `lint-imports`;
- the history gate;
- the regression map;
- the quality baseline (`make quality`).

When the quality baseline fails, fix the code; after an improvement, run
`make quality-baseline` and commit the file (`AGENTS.md`, "The quality baseline").

`make docs` builds the site strictly.

Then run the tests for the code the change touches: the test files of the modules you changed,
the tests that import them (`git grep`), and the new tests. Include their service-backed cases,
with the services up:

```bash
uv run pytest -q -p no:cacheprovider tests/unit/test_x.py tests/integration/y/ \
  > "$SCRATCH/touched.log" 2>&1; echo "exit $?"
```

Never set `ETL_CRAFT_REQUIRE_SERVICES=1` for a run that includes the cloud suites or the wheel
demos: their skips by design would turn into failures.

`make test-changed` is the local default before a PR. It uses pytest-testmon with xdist,
first running full Engine DB and warehouse suites when SQL, migrations, schema, marker or
release TOML, examples or templates changed. CI never uses selection: its complete suite is
the authority. `make test` uses automatic unit workers and four service workers; override
`PYTEST_WORKERS` to cap both. `make check` measures coverage in parallel. Keep TMPDIR on disk
(the Make targets use `$HOME/.cache/etl-craft/tests/tmp`), because /tmp may be RAM-backed.

## 1b. Repeated races

CI runs each test once; a race shows up on CI's slower runners long before a fast workstation
sees it. When a change touches concurrent code (run creation, leases, locks, ROW_ID allocation,
gates, Engine DB outages), run its tests 20 times:

```bash
for i in $(seq 20); do
  ETL_CRAFT_REQUIRE_SERVICES=1 uv run pytest -q -p no:cacheprovider <tests> || break
done
```

CI runs every supported Python on Linux and the newest on macOS; release gates keep the full product. Read a version- or macOS-only CI failure before
rerunning it.

## 2. Installed-wheel demos

CI runs them on every pull request, so they are not a pre-PR step. Run them locally only to
reproduce a CI failure in them that the log does not explain.

```bash
uv build --out-dir "$SCRATCH/dist"      # wheel and sdist together; ./dist stays untouched
ETL_CRAFT_TEST_WHEEL="$SCRATCH/dist/etl_craft-<version>-py3-none-any.whl" \
ETL_CRAFT_REQUIRE_SERVICES=1 \
  uv run pytest -q -p no:cacheprovider tests/e2e tests/package
```

This covers all four local warehouses on both Engine DBs, pip and uv, the sdist and the
`ui` extra. Expect no skips. The demos copy `examples/demo/` from the source tree: a change
there needs only a rerun; a change under `src/` needs a rebuild. Demo copies have unique catalog namespaces; parallel workers must retain that isolation.

## 3. actionlint

```bash
uvx --from actionlint-py actionlint      # no output means clean
```

Python tests do not validate GitHub expression contexts such as `runner.temp`.

## 4. Cloud tests

Run them when `dialects/warehouse/snowflake*.py` or `databricks*.py` change, or code that shapes
the SQL every warehouse runs:
- `dialects/warehouse/base.py`;
- `handlers/sql/`;
- hashing;
- cloning;
- warehouse authentication.

Cloud compute has a usage budget: run them only when the owner has made it available, and
otherwise treat the cloud as unable to run (below). Try each affected cloud once; never retry a
cloud run that failed on compute or credentials. Skip them for refactors whose paths the four local warehouses already
run. For a pull request,
run only the cloud tests that exercise the change, on the cloud whose code changed (or both for
shared SQL). `-k` selects them. Credentials live in a gitignored env file:
`.env.acceptance` by default (`.env.acceptance.example` lists the variables). The owner's
workstation keeps them in `.env`; pass that file instead (`--env-file .env`, `ENV_FILE=.env`)
when `.env.acceptance` does not exist:

```bash
uv run python -c 'import os, sys; from pathlib import Path; import pytest
from etl_craft.core.text import parse_env_file
os.environ.update(parse_env_file(Path(".env.acceptance").read_text()), ETL_CRAFT_REQUIRE_SERVICES="1")
sys.exit(pytest.main(["-q", "-n", "6", "--dist", "loadgroup", "-p", "no:cacheprovider", "-m", sys.argv[1],
                      "tests/acceptance/cloud", "-k", sys.argv[2]]))' cloud_snowflake 'append'
```

Whole suites (release evidence, or a change across many cloud paths) run both clouds at once,
with six workers per cloud and a target of under fifteen minutes per suite:

```bash
make acceptance-cloud                    # both clouds at once; reads .env.acceptance
uv run python scripts/acceptance_cloud.py --env-file .env.acceptance cloud-snowflake   # one cloud
```

Each suite's pytest output goes to `dist/acceptance/<suite>.log`.

- **Never `source .env.acceptance`.** Its Databricks JDBC URL contains `;`. Both commands read
  the file with the project's parser.
- **Evidence files:** a whole-suite run rewrites `release/evidence/<version>/cloud-*.json`.
  Leave those files out of feature commits; only release pull requests record evidence. Ask
  before restoring them.
- **When a cloud cannot run** on the first try (no credentials, compute will not start, the
  provider refuses before any SQL runs): say so in the pull request's Validation section and the
  roadmap's "Acceptance availability" note, and merge as usual. Never skip it silently. A test
  that ran and failed is a code failure, not unavailability. Release evidence
  (`make acceptance-cloud`) still needs every cloud suite to pass.

## 5. Self-review

Read the whole diff (`git diff origin/main...`) as a reviewer who has to maintain it:

- **Design rules** (`AGENTS.md`): failures fail, actors are named, SQL lives in the dialects
  layer, and order comes from rows.
- **Reuse:**
  - nothing duplicates an existing helper, query or module;
  - no `_private` name is imported across modules;
  - no new abstraction, setting or dependency has a single caller.
- **Size:** no function grew long or branchy.
- **Enums and errors:** `core` enums replace status strings; errors come from `core.errors`.
- **Deletion:** the superseded path, its tests and its docs are gone.
- **Tests:**
  - they test behaviour;
  - each fails without the change;
  - no `sleep` for ordering, and no positional row slicing;
  - markers are present.
- **Scope:**
  - nothing outside the item;
  - no roadmap item text rewritten;
  - no plan content added;
  - no release evidence.
- **Docs:** the CHANGELOG, docs pages and examples match the new behaviour.
- **Security:** changes to `api/`, tokens, secrets, SQL generation or file paths get a security
  review of the branch.

Fix what you find, then rerun the steps it affects.

## Known environment failures

Recognize these before blaming the change:

- **Iceberg REST after a host reboot.** Every Trino and Iceberg test errors with
  `ICEBERG_CATALOG_ERROR ... Failed to create namespace`, and the `iceberg-rest` log shows
  `SQLITE_BUSY`. Recreate both containers, then rerun the failed files:
  `docker compose up -d --wait --force-recreate iceberg-rest trino`.
- **A run that hits its timeout** is usually waiting on a broken service, not a slow test. Read
  the end of its log once and check `docker ps` before running it again.
- **Cloud parallelism.** Cloud suites use `-n 6 --dist loadgroup`; keep workers at or below
  warehouse query concurrency and inspect query history for queueing. Local service suites use
  four workers on this workstation; keep TMPDIR on disk. Required PR CI keeps its shards.
- **Shells that do not split words.** In zsh, `pytest $files` passes one argument. Write the
  list to a file and use `xargs -a <file> uv run pytest ...`.
- **Stopping a background run.** `pkill -f <pattern>` also matches the shell that runs the
  command. Find the PIDs with `pgrep -af`, then `kill` them by number.
- **Evidence and dirty trees.** `scripts/run_suite.py` refuses a dirty tree, so edit nothing in
  the working tree while evidence records. Use `git worktree add` for other work meanwhile.

## Report

CI's **every test and matrix accounted for** check is required alongside the individual jobs.
It verifies complete collection/outcome artifacts and ownership against the unfiltered catalog.
It combines measured Linux, native warehouse and PostgreSQL supervision coverage at the unchanged
90% threshold. A passing partition alone is not a complete suite result.
Use the `ci-*` artifacts for shard failures; do not bypass the final check when an artifact or
matrix job is missing.

For each step, give the passed, failed and skipped counts, the coverage, the duration and the log
path. Report a failure caused by the environment (missing credentials, services down, no wheel)
as that, separately from failures in the code.
