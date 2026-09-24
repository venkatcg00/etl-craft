# Release evidence

A version is releasable only when every suite in [required-suites.toml](required-suites.toml)
has passing evidence recorded on the commit being released.

## Recording evidence

```bash
make services-up                                   # the local services most suites need
python scripts/run_suite.py unit                   # or: make suite SUITE=unit
python scripts/run_suite.py package --wheel dist/etl_craft-0.1.0-py3-none-any.whl
```

Each run writes `release/evidence/<version>/<suite>.json`: the commit, whether the working
tree was clean, the wheel's sha256 for suites that test the wheel, and each test's outcome.
No failure text is recorded. Run suites from a clean, committed tree; evidence from a dirty
tree is rejected.

The cloud suites (`where = "local"`) need credentials and run in a local session. They write
their evidence the same way.

## Checking the gate

```bash
python scripts/release_gate.py                     # or: make release-gate
```

The gate prints one line per suite and ends with `releasable` or `not releasable`. It exits 0
only when every suite:

- has evidence for this version, recorded from a clean tree on an ancestor of HEAD;
- ran at least one test, and every test passed (no failures, errors, skips or xfails);
- has only evidence, changelog or release-note changes on top of its commit;
- tested the same wheel as the other wheel suites, and the one passed with `--wheel`.

CI runs the gate on every `release/**` branch.
