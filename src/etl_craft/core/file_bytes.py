"""Bounded reads of regular files without following any relative-path symlink."""

import os
import stat
from contextlib import ExitStack
from pathlib import Path, PurePosixPath

from etl_craft.core.errors import ConfigurationError


def read_regular(root: Path, relative: str, maximum: int) -> bytes:
    """Open each directory relative to its held descriptor; reject links and special files."""
    parts = PurePosixPath(relative).parts
    if not parts or PurePosixPath(relative).is_absolute() or ".." in parts:
        raise ConfigurationError(
            f"file {relative!r}: invalid relative path; expected a file below "
            f"{root}; correct its manifest"
        )
    try:
        with ExitStack() as stack:
            current = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            stack.callback(os.close, current)
            for part in parts[:-1]:
                current = os.open(
                    part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=current
                )
                stack.callback(os.close, current)
            descriptor = os.open(
                parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=current
            )
            stream = stack.enter_context(os.fdopen(descriptor, "rb"))
            if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                raise ConfigurationError(
                    f"file {root / relative}: special file; expected a regular "
                    f"file; restore the file"
                )
            data = stream.read(maximum + 1)
            if len(data) > maximum:
                raise ConfigurationError(
                    f"file {root / relative}: exceeds {maximum} bytes; expected a "
                    f"bounded regular file; reduce it or restore the artifact"
                )
            return data
    except OSError as error:
        raise ConfigurationError(
            f"file {root / relative}: cannot open safely ({error.strerror}); "
            f"expected readable regular files without symlinks; restore the path and permissions"
        ) from error
