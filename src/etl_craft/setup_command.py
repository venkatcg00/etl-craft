"""`etl-craft setup` — one idempotent command that brings a deployment up to date.

[DEVIATION, 2026-09-20] Replaces interactive `configure` entirely, per explicit
instruction: "remove the ineractive setup, lets go in dbt route. one single
command with required files and it should itself up. so, everytime the command
is ran, it either set itself up, or updates the setup with newest data."

So this is the dbt shape: you keep your settings in files (or in the
environment), and one command reconciles reality with them. Run it on a fresh
machine and it writes the config and creates the schema. Run it again after
any change and it updates the config and applies whatever migrations are
pending. There is no separate first-run path to get wrong, and no prompt
sequence to sit through in CI.

What it does, in order:
  1. Reads settings from a .env-style file (`--env FILE`, default `./.env`),
     or from the process environment (`--from-environment`) — per explicit
     instruction that either source is legitimate.
  2. Writes or updates `craft-connector.yml`. New files use the canonical
     manifest with variable names; legacy files retain their existing shape.
  3. Brings the Engine DB to current: applies the packaged schema if the
     database is empty, otherwise applies pending migrations.
  4. Names the secret variables the resulting configuration expects.

[CHOICE] `init-db` and `migrate` stay as separate verbs. `setup` calls the
same code, but a DBA applying a schema by hand, or a CI job running only a
migration, should not have to rewrite craft-connector.yml to do it.

[CHOICE] Step 3 is skipped when the Engine DB is unreachable, reported rather
than raised. Writing the config is still useful on its own — that is often
exactly the step that fixes the connection — and failing the whole command
would leave nothing done.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConfigError, load_config
from etl_craft.configure import _read_raw_yaml, _required_secret_vars, configure_from_env
from etl_craft.db import build_engine
from etl_craft.init_db import InitDbError, existing_engine_tables, init_db
from etl_craft.migrate import (
    MigrationError,
    apply_pending_migrations,
    mark_packaged_migrations_applied,
)

DEFAULT_ENV_FILE = Path(".env")


@dataclass
class SetupReport:
    """What one `setup` run actually did, step by step."""

    config_path: Path
    config_action: str
    database_action: str
    applied_migrations: list[str] = field(default_factory=list)
    required_secrets: list[tuple[str, str]] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Report whether every step completed."""
        return not self.problems


def run_setup(
    *,
    config_path: Path,
    env_path: Path | str | None,
    from_environment: bool,
    migrations_dir: Path | str | None = None,
) -> SetupReport:
    """Reconcile config and schema with the supplied settings. Safe to run repeatedly."""
    existed = config_path.is_file()
    before = _read_raw_yaml(config_path) if existed else {}

    source = None if from_environment else (Path(env_path) if env_path else DEFAULT_ENV_FILE)
    if source == DEFAULT_ENV_FILE and env_path is None and not source.is_file():
        # [DEVIATION, 2026-09-24] No settings file and none asked for: read the
        # environment, where nothing needs to be set at all -- every value has
        # a default, down to a SQLite Engine DB. A file that was named
        # explicitly and is missing is still an error below.
        source = None
    if source is not None and not source.is_file():
        raise ConfigError(
            f"no settings file at {source} — create one (see "
            "docs/craft-connector.example.yml), pass --env FILE, or use "
            "--from-environment to read the settings already exported here"
        )

    configure_from_env(source, config_path)
    after = _read_raw_yaml(config_path)
    if not existed:
        config_action = f"created {config_path}"
    elif after != before:
        config_action = f"updated {config_path}"
    else:
        config_action = f"{config_path} already current"

    report = SetupReport(
        config_path=config_path,
        config_action=config_action,
        database_action="skipped",
        required_secrets=_required_secret_vars(after),
    )
    _bring_database_current(report, config_path, migrations_dir)
    return report


def _bring_database_current(
    report: SetupReport, config_path: Path, migrations_dir: Path | str | None
) -> None:
    try:
        config = load_config(config_path)
        engine = build_engine(config)
    except ConfigError as exc:
        # Reported, not raised: writing the config is useful on its own, and
        # is often the step that fixes the connection in the first place.
        report.database_action = "not reachable"
        report.problems.append(f"Engine DB not reachable yet: {exc}")
        return

    # Disposed on every path. A short-lived command that leaves pooled
    # connections open holds the database it just set up hostage — a DROP
    # DATABASE against it then fails with "other sessions using the database",
    # which is how this was found.
    try:
        try:
            tables = existing_engine_tables(engine)
        except SQLAlchemyError as exc:
            report.database_action = "not reachable"
            report.problems.append(f"Engine DB not reachable yet: {exc}")
            return

        try:
            if not tables:
                count = init_db(engine)
                report.database_action = f"schema created ({count} statements)"
                # [DEVIATION, 2026-09-22, E2-83] The comment that used to sit
                # here claimed the engine's own migrations were "recorded
                # rather than meaningfully re-run". They were not --
                # apply_pending_migrations has no record-only path, so every
                # fresh install executed all three on top of a schema that
                # already contained everything they add. Now the claim is
                # true, and it is made of the packaged migrations only: a
                # team's own migrations are not in schema.sql and still run.
                mark_packaged_migrations_applied(engine)
                report.applied_migrations = apply_pending_migrations(engine, migrations_dir)
            else:
                report.applied_migrations = apply_pending_migrations(engine, migrations_dir)
                report.database_action = (
                    f"{len(report.applied_migrations)} migration(s) applied"
                    if report.applied_migrations
                    else "already up to date"
                )
        except (InitDbError, MigrationError, SQLAlchemyError) as exc:
            report.database_action = "failed"
            report.problems.append(str(exc))
    finally:
        engine.dispose()
