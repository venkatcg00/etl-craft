"""Publish an upload cache file only after streaming size and hash verification."""

import hashlib
import os
from collections.abc import Iterator
from contextlib import suppress
from pathlib import Path
from uuid import uuid4

from etl_craft.core.errors import HandlerError
from etl_craft.core.file_bytes import parent_directory
from etl_craft.core.uploads import UploadFile, UploadInfo


def verified_upload(root: Path, upload: UploadInfo, chunks: Iterator[bytes]) -> UploadFile:
    """Keep partial or altered bytes away from scripts, for local and remote execution."""
    relative = f"{upload.upload_id}/content"
    temporary = ".receiving-" + uuid4().hex
    with parent_directory(root, relative, create=True) as parent:
        digest = hashlib.sha256()
        size = 0
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=parent,
            )
            with os.fdopen(descriptor, "wb") as stream:
                for chunk in chunks:
                    size += len(chunk)
                    if size > upload.size_bytes:
                        raise HandlerError(
                            f"upload {upload.upload_id}: received {size} bytes; expected "
                            f"{upload.size_bytes}; restore the recorded file and retry"
                        )
                    stream.write(chunk)
                    digest.update(chunk)
                stream.flush()
                os.fsync(stream.fileno())
            found = digest.hexdigest()
            if size != upload.size_bytes or found != upload.sha256:
                raise HandlerError(
                    f"upload {upload.upload_id}: size={size}, SHA-256={found}; expected "
                    f"size={upload.size_bytes}, SHA-256={upload.sha256}; restore the "
                    "original recorded upload and retry; no script received this file"
                )
            os.replace(temporary, "content", src_dir_fd=parent, dst_dir_fd=parent)
            os.fsync(parent)
        finally:
            with suppress(FileNotFoundError):
                os.unlink(temporary, dir_fd=parent)
    return UploadFile(
        upload.upload_id,
        upload.account,
        upload.file_name,
        upload.size_bytes,
        upload.sha256,
        root / relative,
        actor_kind=upload.actor_kind,
    )
