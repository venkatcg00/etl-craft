"""The error hierarchy, and the exit status the command line returns for each error.

Every error etl-craft raises on purpose is an ``EtlCraftError``, and every error class has its
own exit status, so a scheduler or script can tell exactly what went wrong from the status
alone. ``ExitCode`` lists them all.
"""

from __future__ import annotations

from enum import IntEnum
from typing import ClassVar


class ExitCode(IntEnum):
    """The exit status of an ``etl-craft`` command."""

    SUCCESS = 0
    FAILURE = 1
    """The work ran and did not succeed: a task or pipeline failed, or a check found problems."""
    USAGE = 2
    CONFIGURATION = 3
    METADATA = 4
    GRAPH = 5
    SELF_DEPENDENCY = 6
    DEPENDENCY_CYCLE = 7
    UNKNOWN_TASK = 8
    RUN_STATE = 9
    RUN_REFUSED = 10
    CONNECTION_TEST = 11
    ENGINE_DB = 12
    MIGRATION = 13
    LOCK_TIMEOUT = 14
    HANDLER = 15
    UNEXPECTED = 16
    """An error with no class of its own, including a bug; its traceback is logged."""
    CLONING = 17


class EtlCraftError(Exception):
    """Base class of every error etl-craft raises; each subclass has its own exit status."""

    exit_code: ClassVar[ExitCode] = ExitCode.UNEXPECTED


class ConfigurationError(EtlCraftError):
    """``craft-connector.yml``, a secret or a connection target cannot be used.

    Covers a missing or invalid file, an unset secret variable, a JDBC URL or auth mode with no
    matching dialect, and an Engine DB that cannot be reached.
    """

    exit_code = ExitCode.CONFIGURATION


class UsageError(EtlCraftError):
    """The command line arguments are invalid."""

    exit_code = ExitCode.USAGE


class MetadataError(EtlCraftError):
    """A pipeline or task code does not resolve to an active ``CFG_`` row."""

    exit_code = ExitCode.METADATA


class GraphError(EtlCraftError):
    """The task or pipeline dependency graph is invalid."""

    exit_code = ExitCode.GRAPH


class RunStateError(EtlCraftError):
    """The run log is in a state the requested run cannot proceed from."""

    exit_code = ExitCode.RUN_STATE


class RunRefusedError(EtlCraftError):
    """The run is not allowed in the configured mode, for example ``--force`` in remote mode."""

    exit_code = ExitCode.RUN_REFUSED


class ConnectionTestError(EtlCraftError):
    """A connection failed its test before the run started."""

    exit_code = ExitCode.CONNECTION_TEST


class EngineDbError(EtlCraftError):
    """The Engine DB could not be initialised or changed."""

    exit_code = ExitCode.ENGINE_DB


class MigrationError(EngineDbError):
    """A migration failed to apply."""

    exit_code = ExitCode.MIGRATION


class LockTimeoutError(EngineDbError):
    """A cross-process lock was not acquired in time."""

    exit_code = ExitCode.LOCK_TIMEOUT


class HandlerError(EtlCraftError):
    """A task handler is missing or failed; the task is recorded as ``FAILED``."""

    exit_code = ExitCode.HANDLER


class CloningError(EtlCraftError):
    """Copying an Engine DB table into the warehouse failed; the message names the table."""

    exit_code = ExitCode.CLONING
