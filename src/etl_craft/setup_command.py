"""`etl-craft setup` — one idempotent command that brings the Engine DB up to date.

[DEVIATION, 2026-09-24] `setup` no longer writes `craft-connector.yml`. Per
explicit instruction, "the craft connector yaml should not be something that
the engine builds. it should be provided by user." The file is the team's own,
versioned artefact -- like a dbt profiles.yml -- and a tool that rewrites it
also rewrites its comments, its ordering and its intent. So `setup` reads the
file and never touches it.

What it does: validates the configuration, then brings the Engine DB to
current -- the packaged schema for its dialect if the database is empty,
pending migrations if not. Run it again after any upgrade; there is no
separate first-run path.

[CHOICE] `init-db` and `migrate` stay as separate verbs: a DBA applying a
schema by hand, or a CI job running only a migration, uses exactly the one it
needs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from sqlalchemy.exc import SQLAlchemyError

from etl_craft.config import ConfigError, ConnectorConfig, load_config
from etl_craft.db import build_engine
from etl_craft.init_db import InitDbError, existing_engine_tables, init_db
from etl_craft.migrate import (
    MigrationError,
    apply_pending_migrations,
    mark_packaged_migrations_applied,
)


@dataclass
class SetupReport:
    """What one `setup` run actually did."""

    config_path: Path
    database_action: str = "skipped"
    applied_migrations: list[str] = field(default_factory=list)
    problems: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Report whether every step completed."""
        return not self.problems


def run_setup(*, config_path: Path, migrations_dir: Path | str | None = None) -> SetupReport:
    """Validate the user's configuration and bring the Engine DB current. Safe to repeat."""
    if not config_path.is_file():
        raise ConfigError(
            f"no craft-connector.yml at {config_path} — write one first (see "
            "docs/craft-connector.example.yml); etl-craft reads it and never writes it"
        )
    # Raised, not reported: a file that does not parse is a configuration
    # error (exit 2) like everywhere else, never "Engine DB not reachable".
    config = load_config(config_path)
    report = SetupReport(config_path=config_path)
    _bring_database_current(report, config, migrations_dir)
    return report


def _bring_database_current(
    report: SetupReport, config: ConnectorConfig, migrations_dir: Path | str | None
) -> None:
    try:
        engine = build_engine(config)
    except ConfigError as exc:
        # Reported, not raised: an unresolvable secret is often exactly what
        # the operator is in the middle of fixing, and `doctor` is the verb
        # that diagnoses a connection in detail.
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
