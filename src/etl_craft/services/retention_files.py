"""Retire log content before deleting files, with durable, idempotent cleanup."""

import os
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.faults import fault_point
from etl_craft.engine.logs import log_key, log_location, read_log_attempt, remove_log
from etl_craft.engine.queries import statement
from etl_craft.execution.attempt_records import attempt_log_path


def expire_logs(engine: Engine, config: ConnectorConfig, cutoff: datetime, now: datetime) -> int:
    """Commit removal evidence and cleanup work before touching a canonical attempt file."""
    count = 0
    while True:
        with engine.begin() as conn:
            rows = conn.execute(statement(conn, "retention_logs"), {"cutoff": cutoff}).all()
            if not rows:
                return count
            retire_log_rows(conn, config, [LogRemoval(**row._mapping) for row in rows], now)
        count += len(rows)
        fault_point("retention.after_log_commit")


@dataclass(frozen=True)
class LogRemoval:
    """Canonical identity read before retiring an attempt's log content."""

    attempt_id: int
    log_path: str | None
    attempt_number: int
    task_run_id: int
    pipeline_run_id: int
    pipeline_code: str
    task_code: str


def retire_log_rows(
    conn: Connection,
    config: ConnectorConfig,
    rows: Sequence[LogRemoval],
    now: datetime,
) -> None:
    """Queue file cleanup and removal evidence in the caller's source transaction."""
    for row in rows:
        conn.execute(statement(conn, "log_lock"), {"attempt_id": row.attempt_id})
        current = read_log_attempt(conn, row.attempt_id)
        if current is None or current.log_removed_at is not None:
            continue
        path_value = current.log_path
        expected_location = log_location(
            config,
            log_key(
                current.engine_id,
                current.project_id,
                current.pipeline_run_id,
                current.task_run_id,
                row.attempt_id,
            ),
        )
        if (
            path_value is None
            and conn.execute(
                statement(conn, "agent_chunk_last"), {"attempt_id": row.attempt_id}
            ).first()
            is not None
        ):
            path_value = expected_location
        if path_value is not None and "://" in path_value:
            if path_value != expected_location:
                raise ConfigurationError(
                    f"attempt_id={row.attempt_id}: LOG_PATH={path_value!r}; expected "
                    f"{expected_location!r}; restore Logs.Store before purge"
                )
        elif path_value is not None:
            expected = attempt_log_path(
                config,
                row.pipeline_code,
                row.pipeline_run_id,
                row.task_code,
                row.attempt_number,
            )
            path = Path(path_value)
            if path.absolute() != expected.absolute():
                raise ConfigurationError(
                    f"attempt_id={row.attempt_id}: LOG_PATH={path_value!r}; expected "
                    f"{str(expected)!r}; restore the canonical attempt path before purge"
                )
            checked_relative(config, path)
            path_value = str(path.absolute())
        if path_value is not None:
            conn.execute(
                statement(conn, "retention_file_insert"),
                {
                    "attempt_id": row.attempt_id,
                    "path": path_value,
                    "now": now,
                },
            )
        conn.execute(
            statement(conn, "retention_log_mark"),
            {
                "attempt_id": row.attempt_id,
                "now": now,
            },
        )
        conn.execute(
            statement(conn, "retention_summary_clear"),
            {
                "task_run_id": row.task_run_id,
            },
        )


def checked_relative(config: ConnectorConfig, path: Path) -> Path:
    """Require a non-linked path beneath the configured log root before recording deletion."""
    root = config.log_dir.absolute()
    if not path.absolute().is_relative_to(root) or ".." in path.parts:
        raise ConfigurationError(
            f"retention log path={str(path)!r}; expected a path under {root}; "
            "restore the configured log directory before purge"
        )
    relative = path.absolute().relative_to(root)
    if root.is_symlink():
        raise ConfigurationError(
            f"retention log directory={str(root)!r} is a symlink; expected a regular directory; "
            "restore the configured log tree before purge"
        )
    cursor = root
    for part in relative.parts:
        cursor = cursor / part
        if cursor.is_symlink():
            raise ConfigurationError(
                f"retention log path={str(cursor)!r} is a symlink; expected regular attempt "
                "files and directories; restore the canonical log tree before purge"
            )
    return relative


def remove_file(config: ConnectorConfig, path: Path) -> None:
    """Hold each ancestor directory without following links while unlinking the leaf."""
    relative = checked_relative(config, path)
    descriptors: list[int] = []
    try:
        descriptors.append(os.open(config.log_dir, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW))
        for part in relative.parts[:-1]:
            descriptors.append(
                os.open(
                    part,
                    os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                    dir_fd=descriptors[-1],
                )
            )
        os.unlink(relative.name, dir_fd=descriptors[-1])
    except FileNotFoundError:
        return
    except OSError as error:
        raise ConfigurationError(
            f"retention log path={str(path)!r}: {error.strerror}; expected a removable "
            "canonical log file; restore permissions and rerun purge"
        ) from error
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def cleanup_files(engine: Engine, config: ConnectorConfig) -> int:
    """Retry committed work even if periods have since been disabled."""
    count = 0
    while True:
        with engine.connect() as conn:
            rows = conn.execute(statement(conn, "retention_files")).all()
            engine_id = conn.execute(statement(conn, "archive_origin")).scalar_one()
        if not rows:
            return count
        for row in rows:
            if "://" in row.log_path:
                remove_log(config, row.log_path, row.project_id, row.attempt_id, engine_id)
            else:
                remove_file(config, Path(row.log_path))
            fault_point("retention.after_file_delete")
            with engine.begin() as conn:
                conn.execute(
                    statement(conn, "retention_file_delete"),
                    {
                        "attempt_id": row.attempt_id,
                    },
                )
            count += 1
