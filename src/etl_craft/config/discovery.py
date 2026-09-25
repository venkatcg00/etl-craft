"""Finding ``craft-connector.yml``."""

from __future__ import annotations

import os
from pathlib import Path

from etl_craft.config.model import CONFIG_FILENAME, PROJECT_DIRNAME
from etl_craft.core.errors import ConfigurationError

CONFIG_PATH_ENV_VAR = "ETL_CRAFT_CONFIG"


def resolve_config_path(explicit: str | Path | None = None, *, start: Path | None = None) -> Path:
    """Return the config file to read: ``--config``, then ``$ETL_CRAFT_CONFIG``, then a search.

    The search looks in ``start`` (the current directory by default) and then each parent, the
    way ``.git`` is found, because a scheduler's working directory is rarely the project's. In
    each directory it looks for ``craft-connector.yml`` (the directory is the project) and for
    ``etl-craft/craft-connector.yml`` (the directory holds the project). Finding both in one
    directory is a ``ConfigurationError``: which one is meant cannot be told. With nothing
    found, it returns ``etl-craft/craft-connector.yml`` in ``start``, so the error names where
    the file was expected.
    """
    if explicit is not None:
        return Path(explicit)
    from_env = os.environ.get(CONFIG_PATH_ENV_VAR)
    if from_env:
        return Path(from_env)
    here = (start or Path.cwd()).resolve()
    for directory in (here, *here.parents):
        found = [
            candidate
            for candidate in (
                directory / CONFIG_FILENAME,
                directory / PROJECT_DIRNAME / CONFIG_FILENAME,
            )
            if candidate.is_file()
        ]
        if len(found) > 1:
            raise ConfigurationError(
                f"found both {found[0]} and {found[1]}; keep one, or pass --config (or set "
                f"${CONFIG_PATH_ENV_VAR}) to choose"
            )
        if found:
            return found[0]
    return here / PROJECT_DIRNAME / CONFIG_FILENAME
