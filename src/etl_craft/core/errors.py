"""The error hierarchy and the exit codes the command line maps it to.

Every error etl-craft raises on purpose is an ``EtlCraftError``. Each class carries the exit
code the command line returns for it: ``USAGE`` (2) when the command could not start because of
its arguments or its configuration, ``FAILURE`` (1) when the work ran and did not succeed.
"""

from __future__ import annotations

from enum import IntEnum
from typing import ClassVar


class ExitCode(IntEnum):
    """The exit status of an ``etl-craft`` command."""

    SUCCESS = 0
    FAILURE = 1
    USAGE = 2


class EtlCraftError(Exception):
    """Base class of every error etl-craft raises; exits with ``FAILURE`` unless overridden."""

    exit_code: ClassVar[ExitCode] = ExitCode.FAILURE


class ConfigurationError(EtlCraftError):
    """``craft-connector.yml``, a secret or a connection target cannot be used.

    Covers a missing or invalid file, an unset secret variable, a JDBC URL or auth mode with no
    matching dialect, and an Engine DB that cannot be reached.
    """

    exit_code = ExitCode.USAGE


class UsageError(EtlCraftError):
    """The command line arguments are invalid."""

    exit_code = ExitCode.USAGE


class MetadataError(EtlCraftError):
    """A pipeline or task code does not resolve to an active ``CFG_`` row."""


class GraphError(EtlCraftError):
    """The task or pipeline dependency graph is invalid."""


class RunStateError(EtlCraftError):
    """The run log is in a state the requested run cannot proceed from."""


class RunRefusedError(EtlCraftError):
    """The run is not allowed in the configured mode, for example ``--force`` in remote mode."""


class ConnectionTestError(EtlCraftError):
    """A connection failed its test before the run started."""


class EngineDbError(EtlCraftError):
    """The Engine DB could not be initialised or changed."""


class MigrationError(EngineDbError):
    """A migration failed to apply."""


class LockTimeoutError(EngineDbError):
    """A cross-process lock was not acquired in time."""


class HandlerError(EtlCraftError):
    """A task handler is missing or failed; the task is recorded as ``FAILED``."""
