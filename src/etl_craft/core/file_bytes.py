"""Bounded reads of regular files without following any relative-path symlink."""

import os
import stat
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager, suppress
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from etl_craft.core.errors import ConfigurationError


@contextmanager
def parent_directory(root: Path, relative: str, *, create: bool = False) -> Iterator[int]:
    """Hold each parent descriptor so concurrent path changes cannot redirect file operations."""
    parts = PurePosixPath(relative).parts
    if not parts or PurePosixPath(relative).is_absolute() or ".." in parts:
        raise ConfigurationError(
            f"file {relative!r}: invalid relative path; expected a file below "
            f"{root}; correct its manifest"
        )
    with ExitStack() as stack:
        if create:
            root.mkdir(parents=True, exist_ok=True)
        current = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        stack.callback(os.close, current)
        for part in parts[:-1]:
            if create:
                with suppress(FileExistsError):
                    os.mkdir(part, dir_fd=current)
            current = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current)
            stack.callback(os.close, current)
        yield current


@contextmanager
def open_regular(root: Path, relative: str) -> Iterator[BinaryIO]:
    """Reject links and special files before yielding a seekable byte stream."""
    try:
        with parent_directory(root, relative) as parent:
            descriptor = os.open(
                PurePosixPath(relative).name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=parent,
            )
            with os.fdopen(descriptor, "rb") as stream:
                if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                    raise ConfigurationError(
                        f"file {root / relative}: special file; expected a regular "
                        f"file; restore the file"
                    )
                yield stream
    except OSError as error:
        raise ConfigurationError(
            f"file {root / relative}: cannot open safely ({error.strerror}); "
            f"expected readable regular files without symlinks; restore the path and permissions"
        ) from error


def read_regular(root: Path, relative: str, maximum: int) -> bytes:
    """Read at most the caller's limit from a safely opened regular file."""
    with open_regular(root, relative) as stream:
        data = stream.read(maximum + 1)
        if len(data) > maximum:
            raise ConfigurationError(
                f"file {root / relative}: exceeds {maximum} bytes; expected a "
                f"bounded regular file; reduce it or restore the artifact"
            )
        return data
