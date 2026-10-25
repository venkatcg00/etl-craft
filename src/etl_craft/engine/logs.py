"""Bounded, ordered log bytes and engine-built external log locations."""

import os
import re
import tempfile
from collections.abc import Generator, Iterator
from contextlib import nullcontext, suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

from sqlalchemy.engine import Connection, Engine

from etl_craft.config import ConnectorConfig
from etl_craft.core.enums import AttemptStatus
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.faults import fault_point
from etl_craft.core.file_bytes import open_regular, parent_directory
from etl_craft.dialects.credentials import S3Objects
from etl_craft.engine.queries import statement

LIVE_ATTEMPTS = frozenset({AttemptStatus.QUEUED, AttemptStatus.CLAIMED, AttemptStatus.RUNNING})


@dataclass(frozen=True)
class LogAttempt:
    """Exact stored-log identity and publication/retirement state."""

    attempt_id: int
    engine_id: str
    project_id: int
    pipeline_run_id: int
    task_run_id: int
    attempt_number: int
    pipeline_code: str
    task_code: str
    status: AttemptStatus
    owner_id: str | None
    log_path: str | None
    log_removed_at: datetime | None


def read_log_attempt(conn: Connection, attempt_id: int) -> LogAttempt | None:
    """Read the exact attempt and reuse the bounded deployment identity lookup."""
    row = conn.execute(statement(conn, "log_attempt"), {"attempt_id": attempt_id}).one_or_none()
    if row is None:
        return None
    return LogAttempt(
        engine_id=conn.execute(statement(conn, "archive_origin")).scalar_one(), **row._mapping
    )


def shared_log(engine: Engine | Connection, attempt_id: int, offset: int = 0) -> Iterator[bytes]:
    """Read bounded batches; relay callers release connections before sending bytes."""
    sequence = -1
    while True:
        with engine.connect() if isinstance(engine, Engine) else nullcontext(engine) as conn:
            rows = conn.execute(
                statement(conn, "agent_chunks_read"),
                {"attempt_id": attempt_id, "offset": offset, "sequence": sequence},
            ).all()
        if not rows:
            return
        for row in rows:
            yield row.content.encode()[max(0, offset - row.byte_offset) :]
            sequence = row.sequence_number


def log_key(engine_id: str, project_id: int, run_id: int, task_run_id: int, attempt_id: int) -> str:
    """Use immutable deployment and run identities, never an agent-supplied path."""
    return (
        f"engine-{engine_id}/project-{project_id}/run-{run_id}/"
        f"task-run-{task_run_id}/attempt-{attempt_id}.log"
    )


def log_location(config: ConnectorConfig, key: str) -> str:
    """Return the configured or default store location for an engine-built key."""
    return log_store(config).rstrip("/") + "/" + key


def log_store(config: ConnectorConfig) -> str:
    """Keep existing agent deployments usable with a host-owned file prefix by default."""
    return config.logs.store or (config.log_dir.absolute() / "agents").as_uri()


def log_client(config: ConnectorConfig) -> S3Objects:
    """Resolve only the configured log profile on the serving host."""
    profile = config.logs.profile
    if profile not in config.object_stores:
        raise ConfigurationError(
            f"Logs.Store={config.logs.store!r}, Logs.Profile={profile!r}: missing profile; "
            "expected a named Object_stores profile; restore that profile and retry"
        )
    assert profile is not None
    return S3Objects(config, config.object_stores[profile])


def stored_log(config: ConnectorConfig, key: str, offset: int) -> Generator[bytes, None, None]:
    """Serve a stored log in fixed blocks, closing the source on cancellation."""
    address = urlsplit(log_location(config, key))
    if address.scheme == "s3":
        yield from log_client(config).stream(
            address.netloc, unquote(address.path).lstrip("/"), offset
        )
        return
    root = Path(unquote(urlsplit(log_store(config)).path))

    with open_regular(root, key) as stream:
        stream.seek(offset)
        while data := stream.read(65536):
            yield data


def publish_log(config: ConnectorConfig, key: str, chunks: Iterator[bytes]) -> None:
    """Stage on disk and publish complete bytes; memory is independent of log length."""
    location = log_location(config, key)
    address = urlsplit(location)
    temporary = "." + Path(key).name + ".upload"
    try:
        if address.scheme == "s3":
            with tempfile.TemporaryFile() as stream:
                for data in chunks:
                    stream.write(data)
                stream.seek(0)
                log_client(config).upload(address.netloc, unquote(address.path).lstrip("/"), stream)
            return
        root = Path(unquote(urlsplit(log_store(config)).path))
        with parent_directory(root, key, create=True) as parent:
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=parent)
            descriptor = os.open(
                temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=parent
            )
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    for data in chunks:
                        stream.write(data)
                        fault_point("logs.during_upload")
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, Path(key).name, src_dir_fd=parent, dst_dir_fd=parent)
                os.fsync(parent)
            finally:
                with suppress(FileNotFoundError):
                    os.unlink(temporary, dir_fd=parent)
    except OSError as error:
        raise ConfigurationError(
            f"log {location}: upload failed ({error.strerror}); expected a writable "
            "Logs.Store and staging disk; restore permissions/free space and retry the ending "
            "report or restart the controller"
        ) from error


def remove_log(
    config: ConnectorConfig, location: str, project_id: int, attempt_id: int, engine_id: str
) -> None:
    """Remove only an engine-built key under the configured external log prefix."""
    prefix = log_store(config).rstrip("/") + "/"
    if not location.startswith(prefix):
        raise ConfigurationError(
            f"log {location!r}: outside Logs.Store={config.logs.store!r}; expected its original "
            "store; restore Logs.Store before retrying purge"
        )
    key = location[len(prefix) :]
    if (
        re.fullmatch(
            rf"engine-{re.escape(engine_id)}/project-{project_id}/run-[0-9]+/"
            rf"task-run-[0-9]+/attempt-{attempt_id}\.log",
            key,
        )
        is None
    ):
        raise ConfigurationError(
            f"log {location!r}: invalid identity; expected project_id={project_id}, "
            f"attempt_id={attempt_id} and engine-built run/task-run keys; "
            "restore the canonical location"
        )
    address = urlsplit(location)
    if address.scheme == "s3":
        log_client(config).remove(address.netloc, unquote(address.path).lstrip("/"))
        return
    root = Path(unquote(urlsplit(log_store(config)).path))
    try:
        with parent_directory(root, key) as parent:
            for name in (Path(key).name, "." + Path(key).name + ".upload"):
                with suppress(FileNotFoundError):
                    os.unlink(name, dir_fd=parent)
    except FileNotFoundError:
        return
    except OSError as error:
        raise ConfigurationError(
            f"log {location}: removal failed ({error.strerror}); expected a removable stored "
            "log; restore store permissions and rerun purge"
        ) from error
