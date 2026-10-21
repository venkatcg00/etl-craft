"""Files a task names in the project directory: SQL files and Python scripts.

A task names its file by a path relative to the project directory, such as
``sql_scripts/sales/orders.sql``. The path must start with the handler's folder, stay inside it
and name a file that exists; anything else is a ``MetadataError`` that names the setting, the
value and the folder, and suggests close matches.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath

from etl_craft.config.model import ConnectorConfig
from etl_craft.core.errors import MetadataError
from etl_craft.core.text import suggest


def sql_script(config: ConnectorConfig, name: str, *, setting: str = "SOURCE_SQL_FILE") -> Path:
    """Return the ``.sql`` file ``name``, a project path inside ``sql_scripts/``."""
    return project_file(config.sql_scripts_dir, name, setting=setting, suffix=".sql")


def python_script(config: ConnectorConfig, name: str, *, setting: str = "SCRIPT_FILE") -> Path:
    """Return the ``.py`` script ``name``, a project path inside ``python_scripts/``."""
    return project_file(config.python_scripts_dir, name, setting=setting, suffix=".py")


def project_file(directory: Path, name: str, *, setting: str, suffix: str) -> Path:
    """Return project path ``name`` inside ``directory``; ``MetadataError`` when unusable."""
    written = name.strip()
    where = f"{setting}={name!r}"
    folder = directory.name
    example = f"'{folder}/sales/orders{suffix}'"
    if not written:
        raise MetadataError(f"{where} is empty; name a {suffix} file such as {example}")
    relative = PurePosixPath(written)
    if relative.is_absolute() or ".." in relative.parts:
        raise MetadataError(
            f"{where} must be a path relative to the project directory {directory.parent}, "
            f"without '..', such as {example}"
        )
    if relative.parts[:1] != (folder,):
        raise MetadataError(
            f"{where} must start with {folder}/: paths are relative to the project directory, "
            f"so write '{folder}/{written}' for a file in {directory}"
        )
    if relative.suffix != suffix:
        raise MetadataError(f"{where} must name a {suffix} file")
    if not directory.is_dir():
        raise MetadataError(f"{where}: the project has no {folder}/ folder (expected {directory})")
    path = directory.parent.joinpath(*relative.parts)
    if not path.resolve().is_relative_to(directory.resolve()):
        raise MetadataError(
            f"{where}: {path} resolves outside {directory}; keep the file inside that folder"
        )
    if not path.is_file():
        known = sorted(
            p.relative_to(directory).as_posix()
            for p in directory.rglob(f"*{suffix}")
            if p.is_file()
        )
        hints = suggest(PurePosixPath(*relative.parts[1:]).as_posix(), known)
        hint = f" — did you mean: {', '.join(f'{folder}/{h}' for h in hints)}" if hints else ""
        raise MetadataError(f"{where}: no such file {path}{hint}")
    if not os.access(path, os.R_OK):
        raise MetadataError(f"{where}: {path} is not readable")
    return path
