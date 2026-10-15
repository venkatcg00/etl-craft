---
name: pre-pr-checks
description: Run etl-craft's validation before a pull request - make static docs, the tests for the code the change touches, repeated races for concurrency changes, Airflow contracts, actionlint, the installed-wheel demos for packaging changes, live-cloud acceptance when warehouse SQL changes, and a self-review of the diff. CI runs the complete suite in under ten minutes. Use before opening a PR or when asked to check the state of a branch.
---

# Pre-PR checks

Open the pull request only after every step that applies has finished green (`AGENTS.md`,
"Validation"). CI then runs the complete suite, every matrix combination and the installed-wheel
demos in under ten minutes, and is their authority: do not repeat that locally. Keep logs in a
scratch directory outside the repository:

```bash
SCRATCH="$(mktemp -d)"
```

Run each long step in the background with its output in `$SCRATCH/<step>.log`, and check its
exit status; `pytest | tail` hides a failure. Steps 1 to 3 share the local services and the
build output, so run them one after another. Step 5 uses no local service and can run alongside
them; on a machine short of memory, run everything one at a time.

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
| 2. Installed-wheel demos | packaging, `examples/demo/` or CLI installation changed | about 15 min |
| 3. Airflow contracts | `integrations/airflow/`, YAML export or `scripts/export_airflow_contracts.py` changed | a few min per version |
| 4. actionlint | `.github/workflows/` changed | seconds |
| 5. Cloud tests | Snowflake or Databricks code changed, or the SQL every warehouse runs | about 15 min for the affected tests |
| 6. Self-review | always | |

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

`examples/demo/` is the project the installed-wheel demos copy. The local gate never runs
`tests/acceptance/cloud/`: search it too for the behaviour you change, and run the cloud tests
that use it (step 5).

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

`make check` still runs the complete suite serially (about 25 minutes) when CI is unavailable.

## 1b. Repeated races

CI runs each test once; a race shows up on CI's slower runners long before a fast workstation
sees it. When a change touches concurrent code (run creation, leases, locks, ROW_ID allocation,
gates, Engine DB outages), run its tests 20 times:

```bash
for i in $(seq 20); do
  ETL_CRAFT_REQUIRE_SERVICES=1 uv run pytest -q -p no:cacheprovider <tests> || break
done
```

CI runs Python 3.12 and 3.13 on Linux and macOS. Read a version- or macOS-only CI failure before
rerunning it.

## 2. Installed-wheel demos

Run them locally only when packaging, `examples/demo/` or CLI installation changed; CI runs them
on every pull request.

```bash
uv build --out-dir "$SCRATCH/dist"      # wheel and sdist together; ./dist stays untouched
ETL_CRAFT_TEST_WHEEL="$SCRATCH/dist/etl_craft-<version>-py3-none-any.whl" \
ETL_CRAFT_REQUIRE_SERVICES=1 \
  uv run pytest -q -p no:cacheprovider tests/e2e tests/package
```

This covers all four local warehouses on both Engine DBs, pip and uv, the sdist and the
`server` extra. Expect no skips. The demos copy `examples/demo/` from the source tree: a change
there needs only a rerun; a change under `src/` needs a rebuild. Run demo sessions one at a
time, because the local Iceberg demos share fixed namespaces.

## 3. Airflow contracts

Mirror the `airflow` job in `.github/workflows/ci.yml` and take the versions from its matrix:

```bash
export ETL_CRAFT_AIRFLOW_CONTRACTS="$SCRATCH/airflow-contracts" \
  AIRFLOW_HOME="$SCRATCH/airflow-home" AIRFLOW__CORE__LOAD_EXAMPLES=false
uv run python scripts/export_airflow_contracts.py "$ETL_CRAFT_AIRFLOW_CONTRACTS"
for v in 2.11.0 3.3.2; do
  uv venv "$SCRATCH/airflow-$v" --python 3.11
  uv pip install --python "$SCRATCH/airflow-$v/bin/python" "apache-airflow==$v" pytest mypy \
    types-pyyaml types-jsonschema ./integrations/airflow
  "$SCRATCH/airflow-$v/bin/python" -m pytest -q integrations/airflow/tests
  "$SCRATCH/airflow-$v/bin/python" -m mypy --strict --follow-imports=skip \
    --ignore-missing-imports integrations/airflow/src
done
```

## 4. actionlint

```bash
uvx --from actionlint-py actionlint      # no output means clean
```

Python tests do not validate GitHub expression contexts such as `runner.temp`.

## 5. Cloud tests

Run them when `dialects/warehouse/snowflake*.py` or `databricks*.py` change, or code that shapes
the SQL every warehouse runs:
- `dialects/warehouse/base.py`;
- `handlers/sql/`;
- hashing;
- cloning;
- warehouse authentication.

Skip them for refactors whose paths the four local warehouses already run. For a pull request,
run only the cloud tests that exercise the change, on the cloud whose code changed (or both for
shared SQL). `-k` selects them. Credentials live in the gitignored `.env.acceptance`
(`.env.acceptance.example` lists the variables):

```bash
uv run python -c 'import os, sys; from pathlib import Path; import pytest
from etl_craft.core.text import parse_env_file
os.environ.update(parse_env_file(Path(".env.acceptance").read_text()), ETL_CRAFT_REQUIRE_SERVICES="1")
sys.exit(pytest.main(["-q", "-p", "no:cacheprovider", "-m", sys.argv[1],
                      "tests/acceptance/cloud", "-k", sys.argv[2]]))' cloud_snowflake 'append'
```

Whole suites (release evidence, or a change across many cloud paths) take about 85 minutes,
both clouds at once:

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
- **When a cloud cannot run** (no credentials, compute will not start): say so in the pull
  request and the roadmap's "Acceptance availability" note, and leave the pull request
  unmerged for the owner. Never skip it silently.

## 6. Self-review

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

## Report

CI's **every test and matrix accounted for** check is required alongside the individual jobs.
It verifies complete collection/outcome artifacts and combines coverage for each Python/platform
pair at the unchanged 90% threshold. A passing partition alone is not a complete suite result.
Use the `ci-*` artifacts for shard failures; do not bypass the final check when an artifact or
matrix job is missing.

For each step, give the passed, failed and skipped counts, the coverage, the duration and the log
path. Report a failure caused by the environment (missing credentials, services down, no wheel)
as that, separately from failures in the code.
