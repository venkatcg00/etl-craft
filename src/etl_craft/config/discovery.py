"""Finding ``craft-connector.yml``."""

from __future__ import annotations

import os
from pathlib import Path

from etl_craft.config.model import CONFIG_FILENAME

CONFIG_PATH_ENV_VAR = "ETL_CRAFT_CONFIG"


def resolve_config_path(explicit: str | Path | None = None, *, start: Path | None = None) -> Path:
    """Return the config file to read: ``--config``, then ``$ETL_CRAFT_CONFIG``, then a search.

    The search looks for ``craft-connector.yml`` in ``start`` (the current directory by
    default) and then each parent, the way ``.git`` is found, because a scheduler's working
    directory is rarely the project's. With nothing found, it returns ``craft-connector.yml``
    in ``start``, so the error names where the file was expected.
    """
    if explicit is not None:
        return Path(explicit)
    from_env = os.environ.get(CONFIG_PATH_ENV_VAR)
    if from_env:
        return Path(from_env)
    here = (start or Path.cwd()).resolve()
    for directory in (here, *here.parents):
        candidate = directory / CONFIG_FILENAME
        if candidate.is_file():
            return candidate
    return here / CONFIG_FILENAME
