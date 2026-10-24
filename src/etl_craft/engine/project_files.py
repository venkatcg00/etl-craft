"""Hash-verified immutable project files, with bounded manifests and atomic cache publication."""

from __future__ import annotations

import ast
import base64
import importlib.util
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path, PurePosixPath

from etl_craft.core.errors import ConfigurationError, UsageError
from etl_craft.core.file_bytes import read_regular
from etl_craft.core.text import sha256_hex

FOLDERS = frozenset({"sql_scripts", "python_scripts", "seeds"})
MAX_BYTES = 64 * 1024 * 1024
MAX_FILES = 10000


def safe_path(value: str) -> PurePosixPath:
    """Accept only portable relative project files; never follow an archive path."""
    path = PurePosixPath(value)
    if (
        not value
        or len(value) > 4096
        or path.is_absolute()
        or "\\" in value
        or "\x00" in value
        or str(path) != value
        or ".." in path.parts
        or (
            value != "craft-connector.yml" and (len(path.parts) < 2 or path.parts[0] not in FOLDERS)
        )
    ):
        raise UsageError(
            f"bundle path={value!r}; expected a relative sql_scripts/, python_scripts/, "
            f"seeds/ file or craft-connector.yml without traversal; correct the manifest"
        )
    return path


def collect(folder: Path) -> dict[str, str]:
    """Read a bounded project once; refuse links, special files and missing configuration."""
    root = folder.resolve()
    files = [root / "craft-connector.yml"]
    for name in sorted(FOLDERS):
        directory = root / name
        if directory.is_symlink():
            raise UsageError(
                f"bundle folder {directory}: symlink; expected regular project files; replace "
                f"the link"
            )
        if directory.exists():
            for path in directory.rglob("*"):
                if path.is_symlink() or not (path.is_file() or path.is_dir()):
                    raise UsageError(
                        f"bundle file {path}: link or special file; expected regular files; "
                        f"replace it"
                    )
                if path.is_file():
                    files.append(path)
                if len(files) > MAX_FILES:
                    raise UsageError(
                        f"bundle {root}: more than {MAX_FILES} files; expected a bounded "
                        f"project; remove generated files"
                    )
    result: dict[str, str] = {}
    total = 0
    for path in files:
        if path.is_symlink() or not path.is_file():
            raise UsageError(
                f"bundle file {path}: missing or linked; expected a regular file; create it"
            )
        size = path.stat().st_size
        total += size
        if total > MAX_BYTES:
            raise UsageError(
                f"bundle {root}: {total} bytes; expected at most {MAX_BYTES}; remove "
                f"generated files or oversized seeds"
            )
        data = read_regular(root, path.relative_to(root).as_posix(), MAX_BYTES - total + size)
        if len(data) != size:
            raise UsageError(
                f"bundle file {path}: size changed during reading; expected stable files; "
                f"publish again from an unchanged tree"
            )
        key = path.relative_to(root).as_posix()
        safe_path(key)
        result[key] = base64.b64encode(data).decode("ascii")
    return result


def decode_files(files: Mapping[str, str]) -> tuple[dict[str, str], dict[str, bytes]]:
    """Validate all uploaded content before a store write, without trusting supplied hashes."""
    if "craft-connector.yml" not in files or not 1 <= len(files) <= MAX_FILES:
        raise UsageError(
            f"bundle contains {len(files)} files; expected craft-connector.yml and at most "
            f"{MAX_FILES} files; publish a bounded project"
        )
    manifest, content = {}, {}
    total = 0
    for name, encoded in files.items():
        safe_path(name)
        if not isinstance(encoded, str) or len(encoded) > (MAX_BYTES * 4 // 3 + 4):
            raise UsageError(
                f"bundle file {name}: invalid encoded size; expected bounded base64; publish "
                f"a smaller regular file"
            )
        try:
            data = base64.b64decode(encoded, validate=True)
        except ValueError as error:
            raise UsageError(
                f"bundle file {name}: invalid base64; expected encoded file bytes; rebuild "
                f"the upload"
            ) from error
        total += len(data)
        if total > MAX_BYTES:
            raise UsageError(
                f"bundle has {total} bytes; expected at most {MAX_BYTES}; remove generated "
                f"files or oversized seeds"
            )
        manifest[name], content[name] = sha256_hex(data), data
    return manifest, content


def verify(folder: Path, manifest: Mapping[str, str]) -> None:
    """Fail on a missing, changed, linked or unexpected cached file; never repair it silently."""
    expected = {str(safe_path(name)) for name in manifest}
    found = set()
    if folder.is_symlink():
        raise ConfigurationError(
            f"bundle cache {folder}: symlink; expected a verified directory; remove the cache "
            f"and retry"
        )
    for file in folder.rglob("*"):
        if file.is_symlink():
            raise ConfigurationError(
                f"bundle cache {file}: symlink; expected regular files; remove the cache and retry"
            )
        if file.is_file():
            found.add(file.relative_to(folder).as_posix())
        elif not file.is_dir():
            raise ConfigurationError(
                f"bundle cache {file}: special file; expected recorded regular files; "
                "remove this cache and retry"
            )
    if found != expected:
        raise ConfigurationError(
            f"bundle cache {folder}: files differ (missing={sorted(expected - found)}, "
            f"extra={sorted(found - expected)}); expected the complete manifest; remove this "
            f"cache and retry"
        )
    for name in expected:
        digest = sha256_hex(read_regular(folder, name, MAX_BYTES))
        if digest != manifest[name]:
            raise ConfigurationError(
                f"bundle cache {folder / name}: SHA-256={digest}; expected {manifest[name]}; "
                f"remove this cache and retry"
            )


def materialize(
    bundle_id: str,
    manifest: Mapping[str, str],
    read: Callable[[str], bytes],
    *,
    cache: Path | None = None,
) -> Path:
    """Publish a verified directory atomically; concurrent downloads cannot expose partial files."""
    if len(bundle_id) != 64 or any(c not in "0123456789abcdef" for c in bundle_id):
        raise UsageError(
            f"bundle id={bundle_id!r}; expected 64 lowercase hex characters; use a recorded "
            f"bundle id"
        )
    parent = cache or Path.home() / ".cache" / "etl-craft" / "bundles"
    parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if parent.is_symlink() or parent.stat().st_mode & 0o077:
        raise ConfigurationError(
            f"bundle cache {parent}: linked or accessible to other users; expected a private "
            "directory; remove links and set mode 0700"
        )
    target = parent / bundle_id
    if target.exists() or target.is_symlink():
        verify(target, manifest)
        return target
    with tempfile.TemporaryDirectory(prefix=f".{bundle_id}-", dir=parent) as scratch:
        stage = Path(scratch) / "project"
        stage.mkdir(mode=0o700)
        for name, digest in manifest.items():
            safe_path(name)
            data = read(name)
            if sha256_hex(data) != digest:
                raise ConfigurationError(
                    f"bundle {bundle_id}, file {name}: SHA-256 mismatch; expected {digest}; "
                    f"restore the recorded artifact"
                )
            file = stage / name
            file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            file.write_bytes(data)
            file.chmod(0o400)
        verify(stage, manifest)
        try:
            stage.rename(target)
        except OSError:
            if not target.is_dir():
                raise
            verify(target, manifest)
    return target


def script_imports(folder: Path) -> tuple[str, ...]:
    """Inspect imports without executing project code or loading optional packages."""
    imports = set()
    local = {
        p.stem for p in (folder / "python_scripts").glob("*") if p.is_dir() or p.suffix == ".py"
    }
    for path in (folder / "python_scripts").rglob("*.py"):
        try:
            nodes = ast.walk(ast.parse(path.read_text(encoding="utf-8"), filename=str(path)))
            for node in nodes:
                names = (
                    [a.name for a in node.names]
                    if isinstance(node, ast.Import)
                    else [node.module]
                    if isinstance(node, ast.ImportFrom) and node.level == 0 and node.module
                    else []
                )
                for value in names:
                    name = value.split(".")[0]
                    if name not in local:
                        imports.add(name)
        except (SyntaxError, UnicodeError) as error:
            raise ConfigurationError(
                f"bundle script {path}: invalid Python; expected parseable source; correct "
                f"the script"
            ) from error
    return tuple(sorted(imports))


def missing_imports(names: tuple[str, ...]) -> tuple[str, ...]:
    """Collect interpreter observations without executing a project module or its parents."""
    return tuple(name for name in names if importlib.util.find_spec(name) is None)
