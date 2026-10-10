# Release evidence

A version is releasable only when every suite in [required-suites.toml](required-suites.toml)
has passing evidence recorded on the commit being released.

Run `make regressions` before recording evidence: every stabilization defect must have
a named, collected test in `regressions.toml`.

## Recording evidence

```bash
make services-up                                   # the local services most suites need
python scripts/run_suite.py unit                   # or: make suite SUITE=unit
python scripts/run_suite.py package --wheel dist/etl_craft-0.1.0-py3-none-any.whl
```

Each run writes `release/evidence/<version>/<suite>.json`: the commit, whether the working
tree was clean, the exact marker expression, the full list of collected test ids, the recording
platform (`sys.platform`), the wheel's sha256 for suites that test the wheel, and each test's outcome.
The evidence format is schema 2; rerun suites to replace older evidence for a new release.
No failure text is recorded. Run suites from a clean, committed tree; evidence from a dirty
tree is rejected.

Evidence runs accept only reporting and early-stop arguments after `--` (`-q`, `-v`, `-x`,
`--maxfail=N`, `--tb=short`, and similar output options). Test selections (`-k`, `-m`, node ids,
`--deselect`, `--ignore`, last-failed), plugin and configuration overrides are refused. Unset
`PYTEST_ADDOPTS` before recording evidence. The runner and collector override configuration
`addopts` and enable strict marker/configuration checks so hidden selections cannot shrink a suite.
Use pytest directly for a selected development test, then run the complete suite for evidence.
Early-stop runs still record every collected id; missing outcomes refuse the release.

The cloud suites (`where = "local"`) need credentials and run in a local session:

```bash
cp .env.acceptance.example .env.acceptance     # gitignored; fill in the Databricks and Snowflake values
make services-up                               # Mailpit, for the demo's alerts
make acceptance-cloud                          # or: make acceptance-cloud ENV_FILE=path/to/file
```

`acceptance-cloud` reads the file with etl-craft's own parser, builds the wheel, and runs
`cloud-databricks` and `cloud-snowflake` against it: connections, every SQL action, cloning,
and the demo's warehouse work, on Databricks with Delta and UniForm and on Snowflake with its own
tables and Iceberg tables. A missing credential fails the suite instead of skipping it. The demo
creates schemas named `ec_<run>_*` in the configured catalog or database and drops them when it
ends; it touches no other schema. The evidence is written the same way as every other suite's.

## Chaos stability

The `chaos` release suite includes both Engine DB dialects. Run it with the local services up:

```bash
ETL_CRAFT_REQUIRE_SERVICES=1 python scripts/run_suite.py chaos
```

Release evidence records the complete suite once, using the same evidence rules as other suites.
The Release gate workflow separately requires twenty consecutive passes on SQLite and twenty on
PostgreSQL, after the evidence check passes. It runs on release branches or manual dispatch,
keeping repeated stress checks out of pull-request CI. Each iteration uploads its JUnit results;
any failure stops that dialect's job.

## The soak

Release 0.4's gate runs the demo for seven days under `etl-craft server`, with no cron or
Airflow, while the server is killed with SIGKILL at random times. `scripts/soak.py` prepares and
runs it against a built wheel:

```bash
python scripts/soak.py setup ~/etl-craft-soak/0.4.0 --wheel dist/etl_craft-0.4.0-py3-none-any.whl
~/etl-craft-soak/0.4.0/venv/bin/python scripts/soak.py run ~/etl-craft-soak/0.4.0 --days 7
~/etl-craft-soak/0.4.0/venv/bin/python scripts/soak.py check ~/etl-craft-soak/0.4.0
```

`setup` installs the wheel into its own environment, copies `examples/demo`, and schedules
`CLIENT_ALPHA`, `CLIENT_BETA` and `SUPPORT_DM` every 15 minutes. `run` kills the server every 20
to 180 minutes, starts it again, and appends a check to `report.jsonl` every half hour; started
again after an interruption, it continues until the same end. Run it where it outlives your
session, for example `systemd-run --user --unit etl-craft-soak -- systemd-inhibit --what=sleep
...`. The final check, in `soak-result.json`, passes when every due tick has exactly one run, no
run stays unfinished or fails unexpectedly, no task run succeeds twice, every append target holds
exactly the rows each task run inserted, and `etl-craft explain` answers for every task state the
soak reached. Mailpit (`make services-up`) receives the demo's alerts.

## Checking the gate

```bash
python scripts/release_gate.py                     # or: make release-gate
```

The gate prints one line per suite and ends with `releasable` or `not releasable`. It exits 0
only when every suite:

- has evidence for this version, recorded from a clean tree on an ancestor of HEAD;
- used exactly the marker expression in the suite manifest;
- recorded the complete, duplicate-free list of tests, matching a fresh collection on HEAD;
- has exactly one outcome per collected id and matching summary counts;
- ran at least one test, and every test passed (no failures, errors, skips or xfails);
- has only evidence, changelog or release-note changes on top of its commit;
- tested the same wheel as the other wheel suites, and the one passed with `--wheel`.

The release working tree must also be clean (evidence files are excluded). Collection executes
no tests and needs no running services, but needs the suite's dependencies. Collection errors,
empty collections, and timeouts refuse the release. No failure text is copied into evidence.

## Platform-specific tests

Tests common to Linux and macOS need no declaration. For tests that can run only on particular
platforms, list the complete node ids under that suite's `platform_only` table, keyed by
`sys.platform` (`linux`, `darwin`, or `win32`). For example:

```toml
[suites.unit.platform_only]
linux = ["tests/unit/test_platform.py::test_linux"]
darwin = ["tests/unit/test_platform.py::test_macos"]
```

A node may be listed for several platforms. The suite runner and collector deselect nodes
listed only for other platforms; unlisted tests always remain required. The gate compares the
common tests and the tests declared for the evidence's platform. It never infers exemptions
from skip reasons, paths, or the free-form platform description. Declared tests for the gate's
own platform must appear in the fresh collection; skips still refuse the release.

CI runs the gate on every `release/**` branch.
