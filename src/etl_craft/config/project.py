"""Files a task names in the project directory: SQL files and ingestion scripts.

A task names its file by a path relative to its folder, such as ``sales/orders.sql`` under
``sql_files/``. The name must stay inside that folder and the file must exist; anything else is
a ``MetadataError`` that names the setting, the value and the folder, and suggests close matches.
"""

from __future__ import annotations

import os
from pathlib import Path, PurePosixPath

from etl_craft.config.model import ConnectorConfig
from etl_craft.core.errors import MetadataError
from etl_craft.core.text import suggest


def sql_file(config: ConnectorConfig, name: str, *, setting: str = "SOURCE_SQL_FILE") -> Path:
    """Return the ``.sql`` file ``name`` under the project's ``sql_files/``."""
    return project_file(config.sql_files_dir, name, setting=setting, suffix=".sql")


def ingestion_script(config: ConnectorConfig, name: str, *, setting: str = "SCRIPT_NAME") -> Path:
    """Return the ``.py`` script ``name`` under the project's ``ingestion_scripts/``."""
    return project_file(config.ingestion_scripts_dir, name, setting=setting, suffix=".py")


def project_file(directory: Path, name: str, *, setting: str, suffix: str) -> Path:
    """Return file ``name`` inside ``directory``; ``MetadataError`` when it is not usable."""
    written = name.strip()
    where = f"{setting}={name!r}"
    if not written:
        raise MetadataError(f"{where} is empty; name a {suffix} file under {directory}")
    relative = PurePosixPath(written)
    if relative.is_absolute() or ".." in relative.parts:
        raise MetadataError(
            f"{where} must be a path relative to {directory}, without '..', such as "
            f"'sales/orders{suffix}'"
        )
    if relative.suffix != suffix:
        raise MetadataError(f"{where} must name a {suffix} file")
    if not directory.is_dir():
        raise MetadataError(
            f"{where}: the project has no {directory.name}/ folder (expected {directory})"
        )
    path = directory.joinpath(*relative.parts)
    if not path.is_file():
        known = sorted(
            p.relative_to(directory).as_posix()
            for p in directory.rglob(f"*{suffix}")
            if p.is_file()
        )
        hints = suggest(written, known)
        hint = f" — did you mean: {', '.join(hints)}" if hints else ""
        raise MetadataError(f"{where}: no such file {path}{hint}")
    if not os.access(path, os.R_OK):
        raise MetadataError(f"{where}: {path} is not readable")
    return path
