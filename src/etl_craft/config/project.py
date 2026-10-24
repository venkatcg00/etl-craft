"""Files a task names in the project directory: SQL files and Python scripts.

A task names its file by a path relative to the project directory, such as
``sql_scripts/sales/orders.sql``. The path must start with the handler's folder, stay inside it
and name a file that exists; anything else is a ``MetadataError`` that names the setting, the
value and the folder, and suggests close matches.
"""

from __future__ import annotations

import json
import os
from dataclasses import replace
from pathlib import Path, PurePosixPath

from etl_craft.config.loader import parse_config, read_config
from etl_craft.config.managed_engine import managed_cloning_problem
from etl_craft.config.model import ConnectorConfig
from etl_craft.core.errors import ConfigurationError, MetadataError
from etl_craft.core.text import sha256_hex, suggest


def connection_sha256(path: Path) -> str:
    """Hash connection definitions without resolving or retaining any credential value."""
    raw = read_config(path)
    if not isinstance(raw, dict):
        raise ConfigurationError(
            f"bundle config {path}: expected a YAML mapping; correct and republish it"
        )
    orchestration = raw.get("Orchestration")
    email: object = None
    if isinstance(orchestration, dict):
        email = {
            key: value if key == "Email" else value.get("Email")
            for key, value in orchestration.items()
            if key == "Email" or isinstance(value, dict)
        }
    secrets = raw.get("Secrets")
    try:
        encoded = json.dumps(
            {
                "Warehouse": raw.get("Warehouse"),
                "Cloning": raw.get("Cloning"),
                "Email": email,
                "Profile": secrets.get("Profile") if isinstance(secrets, dict) else None,
                "Email_profile": orchestration.get("Profile")
                if isinstance(orchestration, dict)
                else None,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    except (TypeError, ValueError) as error:
        raise ConfigurationError(
            f"bundle config {path}: unsupported connection value ({error}); expected plain "
            "YAML strings, numbers, flags, lists and mappings; correct the connection sections"
        ) from None
    return sha256_hex(encoded)


def sql_script(config: ConnectorConfig, name: str, *, setting: str = "SCRIPT_FILE") -> Path:
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


def project_config(host: ConnectorConfig, path: Path) -> ConnectorConfig:
    """Pin project files without distributing Engine DB or artifact-store credentials."""
    try:
        raw = read_config(path)
    except (ConfigurationError, OSError, UnicodeError) as error:
        raise ConfigurationError(
            f"bundle config {path}: invalid or unreadable YAML ({type(error).__name__}); "
            "expected a UTF-8 project mapping; validate the source config and republish it"
        ) from None
    if not isinstance(raw, dict) or not isinstance(raw.get("Secrets"), dict):
        raise ConfigurationError(
            f"bundle config {path}: invalid Secrets section; expected a project "
            f"mapping; correct and republish it"
        )
    raw["Secrets"]["Source_type"] = host.source.type
    if host.source.path is not None:
        raw["Secrets"]["Path"] = host.source.path
    else:
        raw["Secrets"].pop("Path", None)
    # Infrastructure credentials and deployment roles belong to this host.
    for name in (
        "Engine",
        "Auth",
        "Object_stores",
        "Artifacts",
        "Deployment",
        "Docs_site",
    ):
        raw.pop(name, None)
    bootstrap = host._host_config_path or host.config_path
    try:
        pinned = parse_config(raw, bootstrap or path, agent=True)
    except ConfigurationError as error:
        raise ConfigurationError(
            f"bundle config {path}: rejected project settings ({type(error).__name__}); "
            "expected valid execution settings; validate the source config locally and "
            "republish it (resolved host values are withheld)"
        ) from None
    scoped = replace(
        pinned,
        config_path=path,
        _host_config_path=bootstrap,
        source=host.source,
        engine=host.engine,
        db_pool_size=host.db_pool_size,
        warehouse=pinned.warehouse,
        warehouse_table_format=pinned.warehouse_table_format,
        email=pinned.email,
        cloning=pinned.cloning,
        auth=host.auth,
        deployment=host.deployment,
        sizing=host.sizing,
        api_address=host.api_address,
        artifacts=host.artifacts,
        object_stores=host.object_stores,
        log_dir=host.log_dir,
        max_warehouse_queries=pinned.max_warehouse_queries,
        limits=replace(
            pinned.limits,
            max_parallel_tasks=host.limits.max_parallel_tasks,
            local_python_slots=host.limits.local_python_slots,
            local_warehouse_slots=host.limits.local_warehouse_slots,
            shutdown_grace_seconds=host.limits.shutdown_grace_seconds,
            engine_db_outage_seconds=host.limits.engine_db_outage_seconds,
        ),
    )
    if problem := managed_cloning_problem(scoped.engine, scoped.cloning):
        raise ConfigurationError(f"bundle config {path}: {problem}")
    return scoped
