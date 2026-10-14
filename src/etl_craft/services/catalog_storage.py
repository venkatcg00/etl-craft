"""Coordinate short catalog swaps with file opens by catalog servers."""

import shutil
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from etl_craft.core.errors import ConfigurationError, LockTimeoutError, UsageError
from etl_craft.core.filelock import file_lock

MARKER = ".etl-craft-catalog"
"""Marks a generated folder that may be replaced by generate-docs."""


def prepare_site_lock(folder: Path) -> None:
    """Create the sidecar for old sites; existing sidecars need only read permission."""
    path = folder.with_name(f".{folder.name}.publish.lock")
    try:
        with site_lock(folder, shared=path.is_file()):
            pass
    except OSError as error:
        raise ConfigurationError(
            f"catalog {folder}: cannot open publication lock {path}; "
            "regenerate with generate-docs and grant the server read access to its sidecar"
        ) from error


@contextmanager
def site_lock(folder: Path, *, shared: bool = False) -> Iterator[None]:
    """Hold the stable sidecar while opening files or exchanging whole site folders."""
    path = folder.with_name(f".{folder.name}.publish.lock")
    try:
        with file_lock(path, wait_seconds=5, shared=shared):
            yield
    except LockTimeoutError as error:
        raise LockTimeoutError(
            f"catalog {folder}: publication lock {path} remained busy for 5s; "
            "expected a short folder swap; check the generator and retry the request"
        ) from error


def check_folder(folder: Path) -> None:
    """Refuse to replace unrelated files."""
    if folder.exists() and not folder.is_dir():
        raise UsageError(f"{folder} is a file; name a folder for the catalog site")
    if folder.is_dir() and any(folder.iterdir()) and not (folder / MARKER).is_file():
        raise UsageError(
            f"{folder} already holds files that generate-docs did not write; name an empty "
            "or new folder, or the folder of an earlier catalog site"
        )


def swap_site(building: Path, folder: Path) -> None:
    """Exchange folders while readers wait, then remove the unlinked old files."""
    old = folder.with_name(f".{folder.name}.old")
    if old.exists():
        shutil.rmtree(old)
    with site_lock(folder):
        if folder.exists():
            folder.rename(old)
        building.rename(folder)
    shutil.rmtree(old, ignore_errors=True)
