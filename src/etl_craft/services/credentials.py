"""Private per-person session files, keyed by the canonical Engine DB identity."""

import json
import os
import stat
import tempfile
from pathlib import Path
from shlex import quote

from sqlalchemy.engine import URL, Engine

from etl_craft.core.errors import ConfigurationError
from etl_craft.core.filelock import file_lock
from etl_craft.core.text import is_secret_name


def credential_path() -> Path:
    """Use the person's XDG config directory, never the project or a shared home."""
    base = os.environ.get("XDG_CONFIG_HOME")
    if base and not Path(base).is_absolute():
        raise ConfigurationError(f"XDG_CONFIG_HOME={base!r}; expected an absolute directory path")
    return (Path(base) if base else Path.home() / ".config") / "etl-craft" / "credentials"


def database_key(engine: Engine, schema: str | None) -> str:
    """Exclude changing login credentials from the deployment's session identity."""
    url = engine.url
    query = {key: value for key, value in url.query.items() if not is_secret_name(key)}
    identity = URL.create(
        url.drivername, host=url.host, port=url.port, database=url.database, query=query
    )
    return identity.render_as_string() + "#" + (schema or "main")


def _read(path: Path) -> dict[str, str]:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except FileNotFoundError:
        return {}
    except OSError as error:
        raise ConfigurationError(
            f"credential file {path}: cannot open safely ({error.strerror}); "
            "check ownership, permissions and remove symlinks"
        ) from error
    info = os.fstat(descriptor)
    if not stat.S_ISREG(info.st_mode):
        os.close(descriptor)
        raise ConfigurationError(
            f"credential file {path}: expected a regular file; remove it and login again"
        )
    with os.fdopen(descriptor, encoding="utf-8") as stream:
        if info.st_uid != os.getuid():
            raise ConfigurationError(
                f"credential file {path}: owner={info.st_uid}; expected owner={os.getuid()}; "
                "ask the OS administrator to restore ownership, or remove the file and login again"
            )
        if stat.S_IMODE(info.st_mode) != 0o600:
            raise ConfigurationError(
                f"credential file {path}: mode={stat.S_IMODE(info.st_mode):04o}, "
                f"owner={info.st_uid}; "
                f"expected your own regular file with mode 0600; run `chmod 600 {quote(str(path))}`"
            )
        try:
            values = json.load(stream)
        except (ValueError, UnicodeError) as error:
            raise ConfigurationError(
                f"credential file {path}: invalid JSON; remove it and login again"
            ) from error
    if not isinstance(values, dict) or not all(
        isinstance(k, str) and isinstance(v, str) and 1 <= len(v) <= 256 for k, v in values.items()
    ):
        raise ConfigurationError(
            f"credential file {path}: expected Engine DB/token entries; login again"
        )
    return values


def read_session(key: str) -> str | None:
    """Read a complete old or new file across atomic replacements."""
    return _read(credential_path()).get(key)


def store_session(key: str, token: str | None) -> None:
    """Serialize read/modify/replace so signing in to another Engine DB preserves both entries."""
    path = credential_path()
    try:
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = path.parent.stat()
        if info.st_uid != os.getuid() or info.st_mode & 0o022:
            raise ConfigurationError(
                f"credential directory {path.parent}: expected your own directory "
                "without outside writes; "
                f"run `chmod go-w {quote(str(path.parent))}`"
            )
        with file_lock(path.with_suffix(".lock"), wait_seconds=10):
            values = _read(path)
            if token is None:
                values.pop(key, None)
            else:
                values[key] = token
            descriptor, temporary = tempfile.mkstemp(prefix=".credentials-", dir=path.parent)
            try:
                with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                    json.dump(values, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
            finally:
                Path(temporary).unlink(missing_ok=True)
    except OSError as error:
        raise ConfigurationError(
            f"credential file {path}: write failed ({error.strerror}); "
            "expected a writable private directory; "
            "fix its ownership and permissions, then login again"
        ) from error
