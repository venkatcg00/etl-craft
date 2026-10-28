"""Content-addressed artifact bytes, independently readable by every API copy."""

import os
from collections.abc import Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import uuid4

from etl_craft.config import ConnectorConfig
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.file_bytes import read_regular
from etl_craft.core.text import sha256_hex
from etl_craft.dialects.credentials import S3Objects
from etl_craft.engine.project_files import MAX_BYTES


@dataclass
class ArtifactStore:
    """A recorded location; credential profiles are always resolved on the serving host."""

    config: ConnectorConfig
    location: str
    profile: str | None = None

    def _client(self) -> S3Objects:
        if self.profile not in self.config.object_stores:
            raise ConfigurationError(
                f"artifact store {self.location}: profile={self.profile!r} is "
                f"missing; expected its recorded Object_stores profile; restore "
                f"that profile on the API host"
            )
        assert self.profile is not None
        return S3Objects(self.config, self.config.object_stores[self.profile])

    def read(self, digest: str) -> bytes:
        """Verify each remote object; storage corruption cannot become executable project code."""
        key = self._key(digest)
        address = urlsplit(self.location)
        if address.scheme == "file":
            data = read_regular(Path(unquote(address.path)), key, MAX_BYTES)
        else:
            data = self._client().read(
                address.netloc, (address.path.strip("/") + "/" + key).lstrip("/"), MAX_BYTES
            )
        if sha256_hex(data) != digest:
            raise ConfigurationError(
                f"artifact {self.location}/{key}: SHA-256 differs; expected "
                f"{digest}; restore the recorded object"
            )
        return data

    def write(self, digest: str, data: bytes) -> None:
        """Publish verified bytes atomically without replacing existing objects."""
        key = self._key(digest)
        if sha256_hex(data) != digest:
            raise ConfigurationError(
                f"artifact {digest}: content hash differs; expected its address; rebuild the bundle"
            )
        address = urlsplit(self.location)
        if address.scheme == "s3":
            self._client().write(
                address.netloc, (address.path.strip("/") + "/" + key).lstrip("/"), data
            )
            return
        with self._prefix() as root:
            with suppress(FileExistsError):
                os.mkdir("objects", dir_fd=root)
            directory = os.open(
                "objects", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root
            )
            temporary = ".publish-" + uuid4().hex
            try:
                descriptor = os.open(
                    temporary,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=directory,
                )
                try:
                    with os.fdopen(descriptor, "wb") as stream:
                        stream.write(data)
                        stream.flush()
                        os.fsync(stream.fileno())
                    try:
                        os.link(
                            temporary,
                            digest,
                            src_dir_fd=directory,
                            dst_dir_fd=directory,
                            follow_symlinks=False,
                        )
                    except FileExistsError:
                        self.read(digest)
                finally:
                    os.unlink(temporary, dir_fd=directory)
            finally:
                os.close(directory)

    def remove(self, digest: str) -> None:
        """Idempotently remove an unreferenced engine-built content address."""
        key = self._key(digest)
        address = urlsplit(self.location)
        if address.scheme == "s3":
            self._client().remove(address.netloc, (address.path.strip("/") + "/" + key).lstrip("/"))
            return
        with self._prefix() as root:
            try:
                directory = os.open(
                    "objects", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root
                )
            except FileNotFoundError:
                return
            try:
                with suppress(FileNotFoundError):
                    os.unlink(digest, dir_fd=directory)
            finally:
                os.close(directory)

    def probe(self) -> None:
        """Check configured prefixes without touching a published object's bytes."""
        address = urlsplit(self.location)
        if address.scheme == "s3":
            self._client().probe(address.netloc, address.path.strip("/"))
        else:
            with self._prefix() as root:
                os.listdir(root)
            data = b"etl-craft artifact prefix probe"
            self.write(sha256_hex(data), data)
            self.read(sha256_hex(data))

    @contextmanager
    def _prefix(self) -> Iterator[int]:
        """Keep filesystem operations beneath one non-linked, held directory."""
        root = Path(unquote(urlsplit(self.location).path))
        try:
            root.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                yield descriptor
            finally:
                os.close(descriptor)
        except OSError as error:
            raise ConfigurationError(
                f"artifact prefix {root}: filesystem access failed ({error.strerror}); "
                "expected readable writable directories without symlinks; "
                "correct its path and permissions"
            ) from None

    @staticmethod
    def _key(digest: str) -> str:
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ConfigurationError(
                f"artifact digest={digest!r}; expected a SHA-256; use the recorded manifest"
            )
        return "objects/" + digest
