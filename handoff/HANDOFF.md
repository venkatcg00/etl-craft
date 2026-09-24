# etl-craft rewrite: handoff

This is for whoever picks the rewrite up next, human or Claude Code session. It covers:

- what is finished;
- what is half done, with the exact next steps;
- what the user still has to do;
- the rules and environment details that cost time to learn.

## Start here

In a fresh session, from the root of the clone (which is on `main`):

```bash
git fetch origin claude/sharp-darwin-yftnhn
mkdir -p /tmp/handoff && git archive FETCH_HEAD handoff | tar -x -C /tmp
bash /tmp/handoff/continue.sh   # sync, dockerd, images, certs, services, harness, make check
```

`continue.sh` prints `Ready.` when the environment matches what this handoff assumes. Use
`SKIP_SERVICES=1` if Docker is not available; you then get unit checks only. The handoff files
live only on `claude/sharp-darwin-yftnhn`, and extracting them to `/tmp` keeps them out of your
working tree and out of `main`.

Then read, in this order:

1. `CLAUDE.md`
2. `CONTRIBUTING.md`
3. `docs/development/rewrite-plan.md`, which has the branch table, checkpoints and release
   checklist.

A ready-to-paste prompt for a new Claude Code session is at the end of this file.

## Where things stand

- **The rewrite trunk is `main`.** The pre-rewrite code is kept only in two tags:
  `archive/iteration-2` (the implementation being ported) and `archive/main`.
- **`main` head is `2d6ee3f`.** The CI and Docs workflows are both green on it.
- **Merged so far** (squash-merged pull requests #3–#8):

  | Plan item | Branch | Content |
  |---|---|---|
  | A1 | `chore/bootstrap` | package skeleton, tooling, CI, history gate, package verification |
  | 0 | `chore/trunk-main` | `next` renamed to `main` everywhere |
  | A2 | `chore/test-release-harness` | docker-compose services, test harness, evidence plugin, release gate |
  | A3 | `docs/site-scaffold` | MkDocs Material site with a strict build in CI |
  | — | Dependabot | action bumps to v7 (checkout, setup-uv, setup-python) |

- **What works in the package:** only `etl-craft --version`. `src/etl_craft/*` holds empty layer
  packages that have docstrings.
- **Checkpoint CP0** was verified locally:
  - `make suite SUITE=unit`, then `make release-gate`;
  - the gate reports `unit` OK, every other suite as missing evidence, and "not releasable"
    (exit 1).
  - No evidence is committed.
- **The user has allowed merging your own pull requests** once CI is green. Use squash merges.

## In flight: `docs/github-pages` (A4)

The user asked for the documentation to be hosted on GitHub Pages. The branch is pushed as
commit `3234228`. **No pull request is open yet.**

**What the branch does:**

- `mkdocs.yml` sets `site_url: https://venkatcg00.github.io/etl-craft/`.
- `.github/workflows/docs.yml`:
  - adds a `publish` job that runs only on push events, with `contents: write` and a
    `docs-publish` concurrency group;
  - triggers on tags `v[0-9]+.[0-9]+.[0-9]+` as well;
  - bumps its actions to v7.
- `scripts/publish_docs.sh REF [--push]` maps the ref to a version and runs mike:
  - `refs/heads/main` publishes `dev`, which is the default version until a `latest` exists;
  - `refs/tags/vX.Y.Z` publishes `X.Y` with the alias `latest` and sets `latest` as the default;
  - any other ref exits 2.
- **Documentation updates:**
  - `docs/contributing.md` has a new "Published versions" section;
  - README links the site;
  - CONTRIBUTING's definition of done mentions publishing;
  - the rewrite plan gains an A4 row (done), CP0 becomes A1–A4, the documentation section is
    updated, and a checklist line is added;
  - CHANGELOG has an entry.
- **Already verified:**
  - `make check`, `make docs` and `shellcheck` are clean;
  - a dry run against a local bare remote gave `dev/`, then `0.1/` with a `latest` symlink, a
    root `index.html` that redirects to the default version, and `.nojekyll`;
  - canonical URLs and the 404 page's asset paths include the version directory.

**Next steps:**

1. **Open the pull request.** Use the template in `.github/pull_request_template.md`
   (Summary / Plan item "A4 docs/github-pages" / Ported from "new" / Testing / Checklist).
2. **Wait for green CI** (CI and Docs), then squash-merge.
3. **Watch the first run of the Docs workflow's `publish` job on `main`.** It should create the
   `gh-pages` branch.
4. **Ask the user to enable Pages** (this session cannot do it): Settings → Pages → Build and
   deployment → Source "Deploy from a branch", branch `gh-pages`, folder `/ (root)`.
   Pages may enable itself when `gh-pages` first appears, so check first.
5. **Verify the site.** <https://venkatcg00.github.io/etl-craft/> should redirect to `dev/`, and
   the version selector should list `dev`.

Merge this before B1: both change `docs/development/rewrite-plan.md`.

## Next plan item: B1 `feat/core-domain`

The scope from the plan is errors, enums, logging and exit codes, in `core/errors.py`,
`core/enums.py` and `core/log.py`. The research is done and summarised below; no code is
written. Cut `feat/core-domain` from `main` after A4 merges.

### Errors: every exception class in `archive/iteration-2`

| Archived class | Raised when | Exit code in the archived CLI |
|---|---|---|
| `config.ConfigError` | `craft-connector.yml` is missing, malformed or invalid; a secret is unset | 2 |
| `db.ConnectionError_(ConfigError)` | a JDBC URL or `auth_mode` cannot become a connection | 2 |
| `dialects/warehouse_dialects.UnsupportedWarehouse(ValueError)` | no dialect for the warehouse and table-format pair | 2 |
| `SQLAlchemyError`, not wrapped | the Engine DB is unreachable | 2 |
| `cfg.CfgError` | `--pipeline_code` or `--task_code` does not resolve to an active `CFG_` row | 1 |
| `resolver.ResolverError` (`SelfDependencyError`, `CycleError`, `UnknownTaskError`) | the dependency graph is invalid | 1 |
| `runlog.RunLogError` | run-id or task-run-log resolution hits an unrecoverable state | 1 |
| `runner.ForceNotAllowedError` | `--force` under remote mode | 1 |
| `orchestrator.OrchestratorModeRefusedError` | the local wave scheduler is invoked under remote mode | 1 |
| `connections.ConnectionTestError` | a connection failed its test before the run | 1 |
| `init_db.InitDbError` | the Engine DB cannot be initialised | 1 |
| `migrate.MigrationError` | a migration fails to apply | 1 |
| `dialects/engine_dialects.LockTimeout` | a cross-process lock was not taken in time | 1 |
| `execution.HandlerError`, `sql_actions.SchemaMismatchError` | a handler is missing or fails; the task is recorded FAILED | — |

The archived CLI caught these per command (`RUN_ERRORS` in `cli.py`). `run` exits 0 when the
outcome is SUCCESS or SKIPPED, and 1 otherwise.

**Proposed design:**

- **`ExitCode` (IntEnum):** `SUCCESS = 0`, `FAILURE = 1`, `USAGE = 2`.
- **`EtlCraftError`:** the base class, with `exit_code: ClassVar[ExitCode] = FAILURE`.
- **Exit 2:**
  - `ConfigurationError`, which absorbs ConfigError, ConnectionError_, UnsupportedWarehouse and
    an unreachable Engine DB;
  - `UsageError`.
- **Exit 1:**
  - `MetadataError`, for a pipeline or task code that does not resolve;
  - `GraphError`, which B3 subclasses;
  - `RunStateError`;
  - `RunRefusedError`, covering force and mode refusals;
  - `ConnectionTestError`;
  - `EngineDbError`, with `MigrationError` and `LockTimeoutError`;
  - `HandlerError`, which F1 subclasses.

Later branches add subclasses; B2 maps `exit_code` in `cli.main`. Update
`docs/reference/exit-codes.md` to name the error families.

### Enums

These come from the archived schema's CHECK constraints
(`git show archive/iteration-2:src/etl_craft/dialects/engine_dialects/sqlite/schema.sql`) and
from module constants. Use `enum.StrEnum`; Python ≥ 3.11 is required anyway.

| Enum | Values | Archived source |
|---|---|---|
| `RunStatus` | `IN-PROGRESS` (member `IN_PROGRESS`), `SUCCESS`, `FAILED`, `SKIPPED` | pipeline, task and business-rule run logs |
| `SlaStatus` | `MET`, `BREACHED` | `AUD_PIPELINES_RUN_LOG` |
| `RefreshType` | `FULL`, `INCREMENTAL` | `CFG_PIPELINES` |
| `DependencyType` | `SUCCESS`, `FAILURE`, `ALWAYS`, `HAS_DATA` | pipeline and task dependency tables |
| `TaskType` | `INGESTION`, `ETL` | `CFG_TASKS` |
| `Handler` | `PYTHON`, `SQL`, `BUSINESS_RULES`, `EMAIL_ALERT` | `CFG_TASKS`, `handlers.py` |
| `RunCondition` | `ALL`, `ANY`, `N` | `CFG_TASKS.RUN_CONDITION` |
| `BusinessRuleType` | `INCOMPLETE`, `REJECT`, `REPORT` | business rules and their results |
| `OffsetType` | `NUMBER`, `TEXT`, `TIMESTAMP` | offset tracker |
| `SqlAction` | the seven actions in CLAUDE.md | `sql_actions.SQL_ACTIONS` |
| `EmailFlavour` | `FAILED`, `COMPLETED_WITH_ERRORS`, `SUCCESS` | `email_alert.py` |
| `Mode` | `local`, `remote` | `config.py`; the plan drops the archive's `orchestrator` alias |
| `AuthMode` | `none`, `password`, `token`, `key_file`, `oauth`, `sso`, `sts` | `warehouse_dialects/base.py` |
| `TableFormat` | `native` (default), `iceberg` | `config.py` |
| `CloningScope` | `cfg`, `aud`, `all`, `none` | `config.py` |

- **Status groups from `resolver.py` and `runlog.py`:** keep these as frozensets next to
  `RunStatus`.
  - terminal: `SUCCESS`, `FAILED`, `SKIPPED`;
  - settled: `SUCCESS`, `SKIPPED`;
  - not retryable: `SUCCESS`, `SKIPPED`, `IN-PROGRESS`;
  - finished run: `SUCCESS`, `FAILED`.
- **`ACTIVE_FLAG`** is `Y`/`N` everywhere; a small helper is probably enough.
- **Migration `SOURCE`** (`ENGINE`, `PROJECT`, `LEGACY`): the plan folds the old migrations into
  a fresh schema. Decide in C2 whether `LEGACY` survives; leave it out of B1.

### Logging

The archive had **no logging**: it printed from the CLI. `core/log.py` is new:

- a `NullHandler` on the `etl_craft` logger;
- `configure(level, fmt)`, where `fmt` is `text` or `json`, with a JSON formatter that emits
  one object per line;
- no configuration at import time.

B2 wires `--log-level` and `--log-format`. Capturing task output into `TASK_LOG` belongs to
B5/E1.

### Definition of done for B1

- Unit tests with the `unit` marker cover every error's exit code, the enum values against the
  schema lists above, and both log formats.
- `make check` stays at ≥ 90% coverage.
- The API pages generate automatically; `docs/reference/exit-codes.md` is updated.
- The plan row B1 is marked done and CHANGELOG has an entry.

## After B1

- **Order:** B2–B5 in parallel, then C1 → C2/C3 → D1 → E1 → E2 → F1–F4 → G1/G2 → H1–H3 with
  I1–I2 → J1.
- **Critical path:** B1 → B4 → C1 → C2 → D1 → E1 → E2 → G2 → H2 → J1.
- Each branch ports its archived slice with its tests (see the plan's "Where the archived
  modules go" table).

## Waiting on the user

This session cannot do these; each needs the user or repository settings:

- **Delete merged or leftover remote branches:** `archive-iteration-2`,
  `chore/test-release-harness`, `chore/trunk-main`, `docs/site-scaffold`, and
  `docs/github-pages` once merged. Branch deletes from the session get HTTP 403. The
  `archive/iteration-2` tag keeps the archived code.
- **Enable GitHub Pages** after the first publish (see A4 above).
- **Decide on MkDocs 2.0.** MkDocs is pinned `<2`, because 2.0 breaks plugins and themes.
  ProperDocs 1.6.7 was verified as a drop-in continuation with identical output. Switching is
  the user's call.
- **Optionally disable the Copilot check.** The "Code scanning AI findings" check fails with
  "400 The requested model is not supported". It is not ours and not required.
- **Supply cloud credentials for H3** (Databricks, Snowflake) through the environment settings
  or a gitignored `.env.acceptance`, never in chat.

## Working rules learned the hard way

### Git and GitHub

- **No `gh` CLI.** Use the `mcp__github__*` tools (load them with ToolSearch):
  - `create_pull_request`;
  - `pull_request_read` (`get_check_runs`, `get_status`);
  - `actions_list`, `get_job_logs`;
  - `merge_pull_request` with `merge_method: squash`.
- **Branches:** one branch per plan item, named as in the plan and cut from `main`. Pushing new
  branches works.
- **Do not retry a 403.** Tag pushes and branch deletes return 403 from the proxy.
- **Commits:**
  - author `venkatcg00 <venkatcg0@gmail.com>`;
  - Conventional Commit subject;
  - end with the attribution trailers your session's system prompt gives;
  - never put a model name in a commit, pull request or file.
- **Merge conflicts on later branches:**
  - usually in `uv.lock`, the Makefile's `.PHONY`, CHANGELOG, CLAUDE.md and the plan rows;
  - keep both sides;
  - regenerate the lockfile with `git checkout --theirs uv.lock && uv lock`, never by hand.
- **Never commit `.certs/`** (TLS private keys). It is gitignored; check `git status` before
  committing on any branch that predates the ignore rule.

### Conventions the gates enforce

- **History gate** (`make history`, `scripts/check_no_history.py`):
  - comments and docs describe current behaviour only;
  - rejected: decision tags, "per explicit instruction", review ids such as `E2-49`, and dated
    notes;
  - `CHANGELOG.md` is exempt.
- **Test markers:** every test carries a suite marker from `release/required-suites.toml`, or
  `harness`; collection fails on an unmarked test.
- **Layers:** `lint-imports` enforces the layer order.
- **Type checking:** `mypy --strict` over `src/etl_craft` and `scripts`.
- **Coverage:** `fail_under = 90`.
- **Printing:** only the `cli` package prints. Everything else logs to `etl_craft.<module>`.
- **Engine SQL:** aliases every selected column in lowercase.

### Environment

- **Docker:** `dockerd` does not start by itself. `continue.sh` starts it with
  `nohup dockerd &`.
- **Images:**
  - Docker Hub rate-limits pulls (HTTP 429) and `quay.io` is unreachable;
  - pull `mirror.gcr.io/<image>` (official images: `mirror.gcr.io/library/<image>`) and
    `docker tag` it back, as `continue.sh` does;
  - the compose file keeps the Docker Hub names.
- **Service ports** (host):

  | Service | Port(s) |
  |---|---|
  | postgres | 55432 |
  | postgres-tls, client-cert only | 55433 |
  | MinIO | 59000 / 59001 |
  | iceberg-rest | 58181 |
  | trino | 58080 |
  | mailpit | SMTP 51025, API 58025 |

- **Service test switches:** `ETL_CRAFT_TEST_<NAME>=host:port` overrides an address;
  `ETL_CRAFT_REQUIRE_SERVICES=1` turns service skips into failures.
- **Resetting services:** the compose services have no volumes, so `make services-reset` gives
  a clean slate.

## Continuation prompt

Paste this into a new Claude Code session on `venkatcg00/etl-craft`:

```text
You are continuing the etl-craft rewrite (metadata-driven ETL engine, Python + SQL).

1. From the repository root run:
     git fetch origin claude/sharp-darwin-yftnhn
     mkdir -p /tmp/handoff && git archive FETCH_HEAD handoff | tar -x -C /tmp
   Read /tmp/handoff/HANDOFF.md fully, then CLAUDE.md, CONTRIBUTING.md and
   docs/development/rewrite-plan.md.
2. Run `bash /tmp/handoff/continue.sh` and confirm it ends with "Ready.".
3. Finish A4 docs/github-pages exactly as HANDOFF.md describes:
   - open the PR with the repo template and wait for green CI;
   - squash-merge (the user has allowed merging after green CI);
   - confirm the Docs publish job on main created gh-pages;
   - tell me whether Pages needs enabling in Settings → Pages.
4. Start B1 feat/core-domain from main using the research and proposed design in
   HANDOFF.md (errors with exit codes, StrEnum domain enums, core/log.py).
   Add unit tests, keep `make check` green, update the plan row and CHANGELOG, open the PR,
   and merge after green CI.
5. Continue down the plan (B2–B5 next), one scoped branch and PR per item. Report after each
   merge.

Rules: never paste or ask for secrets in chat, never commit .certs/, don't retry proxy 403s,
use the mcp__github__ tools (no gh CLI), and keep comments free of change history.
```
