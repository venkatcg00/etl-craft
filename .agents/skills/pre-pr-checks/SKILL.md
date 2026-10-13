---
name: pre-pr-checks
description: Run etl-craft's validation before a pull request - make check docs, other Pythons and repeated races, the installed-wheel demos, Airflow contracts, actionlint, live-cloud acceptance when warehouse SQL changes, and a self-review of the diff. Use before opening a PR, when asked to check the state of a branch, or to run all the tests.
---

# Pre-PR checks

Open the pull request only after every step that applies has finished green (`AGENTS.md`,
"Validation"). Keep logs in a scratch directory outside the repository:

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
| 1. Gate | always | about 25 min with services up |
| 1b. Other Pythons, repeated races | always; races when concurrency code changed | about a minute |
| 2. Installed-wheel demos | always (CI also runs them) | about 15 min |
| 3. Airflow contracts | `integrations/airflow/`, YAML export or `scripts/export_airflow_contracts.py` changed | a few min per version |
| 4. actionlint | `.github/workflows/` changed | seconds |
| 5. Cloud tests | Snowflake or Databricks code changed, or the SQL every warehouse runs | about 15 min for the affected tests |
| 6. Self-review | always | |

The local services must be up for steps 1 and 2: `docker ps` lists the `etl-craft-*` containers
as healthy; otherwise run `make services-up`. Without Docker (a cloud agent sandbox), run step 1
anyway. The service tests skip, and CI runs them on the pull request. Say so in the pull
request's Validation section.

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

## 1. Gate

```bash
make check docs > "$SCRATCH/check.log" 2>&1; echo "exit $?"
```

`make check` runs these, in order:
- ruff lint and format check;
- `mypy --strict`;
- `lint-imports`;
- the history gate;
- the regression map;
- the tests with coverage of at least 90%.

`make docs` builds the site strictly.

Never set `ETL_CRAFT_REQUIRE_SERVICES=1` for this run. About 90 tests skip by design: the cloud
suites without credentials, and the wheel demos without a wheel. The variable turns every one of
those skips into a failure. Expect `N passed, ~90 skipped` with the services up. Any `FAILED` or
`ERROR` line is real.

## 1b. Other Pythons and repeated races

The gate runs the project's Python only; CI also runs 3.12 and 3.13, on Linux and macOS. Run the
unit tests on the other versions in scratch environments (a plain `uv run --python` would
rebuild the project `.venv`):

```bash
for v in 3.12 3.13; do
  UV_PROJECT_ENVIRONMENT="$SCRATCH/venv-$v" uv run --python "$v" --all-groups -q \
    pytest -q -p no:cacheprovider -m unit tests/unit
done
```

A race shows up on CI's slower runners long before it shows up on a fast workstation. When a
change touches concurrent code (run creation, leases, locks, ROW_ID allocation, gates), run its
tests 20 times:

```bash
for i in $(seq 20); do
  ETL_CRAFT_REQUIRE_SERVICES=1 uv run pytest -q -p no:cacheprovider <tests> || break
done
```

macOS has no local equivalent; read a macOS-only CI failure before rerunning it.

## 2. Installed-wheel demos

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

For each step, give the passed, failed and skipped counts, the coverage, the duration and the log
path. Report a failure caused by the environment (missing credentials, services down, no wheel)
as that, separately from failures in the code.
