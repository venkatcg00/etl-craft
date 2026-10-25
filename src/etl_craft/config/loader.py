"""Reading and validating ``craft-connector.yml``.

The file is written by the team and only read by the engine. Its sections come in this order:
``Secrets`` (where variables are read from, and the default profile), ``Orchestration`` (mode,
limits, DAG defaults and the Email relay), the optional ``Deployment`` (the declared deployment
profile, a flat section), ``Engine``, then the optional ``Warehouse`` and ``Cloning``. Every
section that varies by environment holds one block per profile (``dev``,
``prod``, or any names). The active one is the section's own ``Profile``, else
``Secrets.Profile``; a section with a single profile block needs no selection. A profile
block's settings override the section's own.

Every problem is a ``ConfigurationError`` naming the file and the setting.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

from etl_craft.config.auth import WAREHOUSES, engine_for_jdbc_url
from etl_craft.config.loader_auth import AUTH_KEYS, parse_auth
from etl_craft.config.loader_cloning import CLONING_KEYS, parse_cloning
from etl_craft.config.loader_deployment import DEPLOYMENT_KEYS, parse_deployment
from etl_craft.config.loader_docs import DOCS_SITE_KEYS, parse_docs_site
from etl_craft.config.loader_email import EMAIL_KEYS, parse_email
from etl_craft.config.loader_engine import parse_engine
from etl_craft.config.loader_helpers import (
    CONNECTION_FIELDS,
    SECRETS_KEYS,
    flag,
    mapping,
    parse_source,
    profiled_settings,
    reject_unknown,
    relative_to_config,
    shown,
    string_list,
    whole_number,
)
from etl_craft.config.loader_orchestration import (
    ORCHESTRATION_KEYS,
    parse_dependency_gates,
    parse_limits,
    parse_project,
    parse_retry_on,
)
from etl_craft.config.loader_warehouse import WAREHOUSE_SECTION_KEYS, parse_warehouse
from etl_craft.config.managed_engine import managed_cloning_problem
from etl_craft.config.model import DEFAULT_LOG_DIR, EXAMPLE_PATH, ConnectionProfile, ConnectorConfig
from etl_craft.config.object_stores import ARTIFACT_KEYS, LOG_KEYS, STORE_KEYS, with_artifacts
from etl_craft.config.resolve import Resolver
from etl_craft.config.retention import KEYS as RETENTION_KEYS
from etl_craft.config.retention import parse_retention
from etl_craft.config.targets import active_catalog
from etl_craft.core.enums import AuthMode
from etl_craft.core.errors import ConfigurationError, MetadataError

SECTIONS = (
    "Secrets",
    "Orchestration",
    "Deployment",
    "Engine",
    "Warehouse",
    "Cloning",
    "Docs_site",
    "Auth",
    "Object_stores",
    "Artifacts",
    "Logs",
    "Retention",
)


def settings_by_section() -> dict[str, frozenset[str]]:
    """Return every setting the loader accepts, by where it is written.

    ``Engine`` and ``Warehouse`` hold their section settings and the fields of a profile block;
    a warehouse's own separate fields (such as Snowflake's ``Account``) are included. Keys are
    spelled as ``craft-connector.yml`` writes them.
    """
    warehouse_fields: set[str] = set()
    for spec in WAREHOUSES:
        warehouse_fields.update(spec.preferred_fields)
        warehouse_fields.update(spec.profile_fields)
    fields = frozenset(map(shown, CONNECTION_FIELDS))
    return {
        "Secrets": SECRETS_KEYS,
        "Orchestration": ORCHESTRATION_KEYS - {"Email"},
        "Orchestration.Email": frozenset(map(shown, EMAIL_KEYS)),
        "Deployment": DEPLOYMENT_KEYS,
        "Engine": frozenset({"Name", "Listen_jdbc_url"}) | fields,
        "Warehouse": WAREHOUSE_SECTION_KEYS | fields | frozenset(map(shown, warehouse_fields)),
        "Cloning": CLONING_KEYS,
        "Docs_site": DOCS_SITE_KEYS,
        "Auth": AUTH_KEYS,
        "Object_stores": STORE_KEYS,
        "Artifacts": ARTIFACT_KEYS,
        "Logs": LOG_KEYS,
        "Retention": RETENTION_KEYS,
    }


def parse_warehouse_limit(
    raw: dict[str, object], profile: str | None, path: Path, resolver: Resolver
) -> int | None:
    """Read the deployment warehouse's concurrent-attempt ceiling."""
    if not raw.get("Warehouse"):
        return None
    return whole_number(
        profiled_settings("Warehouse", raw, profile, path, resolver),
        "Max_warehouse_queries",
        None,
        path,
        minimum=1,
    )


class _StrictLoader(yaml.SafeLoader):
    """Refuse duplicate mapping keys, with their section paths and source lines."""

    def construct_document(self, node: yaml.Node) -> object:
        self.section_paths: dict[int, str] = {}

        def index(current: yaml.Node, path: str) -> None:
            if id(current) in self.section_paths:
                return
            self.section_paths[id(current)] = path
            if isinstance(current, yaml.MappingNode):
                for key, value in current.value:
                    index(value, f"{path}.{key.value}" if path else str(key.value))
            elif isinstance(current, yaml.SequenceNode):
                for position, value in enumerate(current.value):
                    index(value, f"{path}[{position}]")

        index(node, "")
        return super().construct_document(node)

    def construct_mapping(self, node: Any, deep: bool = False) -> dict[object, object]:
        if not isinstance(node, yaml.MappingNode):
            return super().construct_mapping(node, deep=deep)
        self.flatten_mapping(node)
        seen: dict[object, int] = {}
        for key_node, _ in node.value:
            key = self.construct_object(key_node, deep=True)
            try:
                first = seen.get(key)
                if first is not None:
                    section = self.section_paths.get(id(node), "")
                    where = f"{section}.{key}" if section else str(key)
                    raise ConfigurationError(
                        f"duplicate YAML key {where!r}: line {first} and "
                        f"line {key_node.start_mark.line + 1}; keep one definition"
                    )
                seen[key] = key_node.start_mark.line + 1
            except TypeError:
                # SafeLoader supplies the diagnostic for an unhashable mapping key.
                return super().construct_mapping(node, deep=deep)
        return super().construct_mapping(node, deep=deep)


def read_config(path: str | Path) -> object:
    """Read project YAML once with duplicate-key diagnostics, without resolving credentials."""
    path = Path(path)
    if not path.is_file():
        raise ConfigurationError(
            f"craft-connector.yml not found at {path} — write one (see {EXAMPLE_PATH}); "
            "etl-craft reads it and never writes it"
        )
    try:
        raw = yaml.load(path.read_text(encoding="utf-8"), Loader=_StrictLoader) or {}
    except ConfigurationError as error:
        raise ConfigurationError(f"{path}: {error}") from error
    except yaml.YAMLError as error:
        raise ConfigurationError(f"{path} is not valid YAML: {error}") from error
    return raw


def load_config(path: str | Path, *, agent: bool = False) -> ConnectorConfig:
    """Read, parse and validate the craft-connector.yml at path."""
    return parse_config(read_config(path), Path(path), agent=agent)


def load_project(path: Path) -> str:
    """Read only project selection so API clients need no Engine or warehouse credentials."""
    raw = _check_layout(read_config(path), path)
    secrets = mapping(raw, "Secrets", path, required=True)
    environment = Resolver(values=os.environ, origin="the process environment", path=path)
    source = parse_source(secrets, path, environment)
    resolver = Resolver.for_source(source, path)
    profile = resolver.text(secrets.get("Profile"), "Secrets.Profile")
    return parse_project(profiled_settings("Orchestration", raw, profile, path, resolver), path)


def parse_config(raw: object, path: Path, *, agent: bool = False) -> ConnectorConfig:
    """Validate an already-loaded ``craft-connector.yml`` mapping; ``path`` is where it came from.

    Relative paths in it (``Secrets.Path``) resolve against ``path``'s directory.
    """
    path = path.absolute()
    raw = _check_layout(raw, path)
    secrets = mapping(raw, "Secrets", path, required=True)
    # Secrets.Source_type and Secrets.Path can only come from the process environment: the
    # source they describe is not known yet.
    environment = Resolver(values=os.environ, origin="the process environment", path=path)
    source = parse_source(secrets, path, environment)
    resolver = Resolver.for_source(source, path)
    resolver.sources.extend(environment.sources)
    global_profile = resolver.text(secrets.get("Profile"), "Secrets.Profile")

    orchestration = profiled_settings("Orchestration", raw, global_profile, path, resolver)
    limits = parse_limits(orchestration, path)
    reject_unknown(orchestration.settings, ORCHESTRATION_KEYS, "Orchestration", path)
    dependency_gates = parse_dependency_gates(orchestration, path)
    project_timezone = orchestration.text("Timezone")
    project_timezone = "UTC" if project_timezone is None else project_timezone
    from etl_craft.core.cron import timezone

    try:
        timezone(project_timezone)
    except MetadataError as error:
        raise ConfigurationError(f"{path}: Orchestration.Timezone: {error}") from error
    email = parse_email(orchestration, path, resolver)
    engine = (
        ConnectionProfile("ENGINE", "api-only", "api-only:", "", AuthMode.NONE)
        if agent
        else parse_engine(raw, global_profile, path, resolver)
    )
    warehouse, table_format = parse_warehouse(raw, global_profile, path, resolver)
    cloning = parse_cloning(raw, global_profile, path, resolver)
    docs_site = parse_docs_site(raw, global_profile, path, resolver)

    api_address = orchestration.text("Api_address")
    api_address = "127.0.0.1:8730" if api_address is None else api_address
    deployment = parse_deployment(
        raw,
        path,
        resolver,
        engine=engine_for_jdbc_url("jdbc:postgresql:" if agent else engine.jdbc_url),
        api_address=api_address,
    )
    config = ConnectorConfig(
        retention=parse_retention(raw, global_profile, path, resolver),
        deployment=deployment,
        api_address=api_address,
        timezone=project_timezone,
        project=parse_project(orchestration, path),
        db_pool_size=int(whole_number(orchestration, "Db_pool_size", 10, path, minimum=1)),
        source=source,
        engine=engine,
        auth=parse_auth(raw, global_profile, path, resolver),
        cloning=cloning,
        docs_site=docs_site,
        warehouse=warehouse,
        warehouse_table_format=table_format,
        max_warehouse_queries=parse_warehouse_limit(raw, global_profile, path, resolver),
        retries=whole_number(orchestration, "Retries", None, path),
        email_recipients=string_list(orchestration, "Email_recipients", path),
        allow_schedule=flag(orchestration, "Allow_schedule", True, path),
        retry_on=parse_retry_on(orchestration, path),
        email=email,
        limits=limits,
        config_path=path,
        dependency_gates=dependency_gates,
        settings=tuple(resolver.sources),
        log_dir=relative_to_config(orchestration.text("Log_dir") or DEFAULT_LOG_DIR, path),
    )
    config = with_artifacts(config, raw, path, resolver)
    _validate_connections(config, path)
    return config


def _validate_connections(config: ConnectorConfig, path: Path) -> None:
    """Check the selected warehouse catalog and managed Engine DB cloning policy."""
    if problem := managed_cloning_problem(config.engine, config.cloning):
        raise ConfigurationError(f"{path}: {problem}")
    if config.warehouse is not None:
        try:
            active_catalog(config)
        except ConfigurationError as error:
            raise ConfigurationError(
                f"{path}: Warehouse.{config.warehouse.name}: {error}"
            ) from error


def _check_layout(raw: object, path: Path) -> dict[str, object]:
    """Refuse a non-mapping, unknown sections and sections out of order."""
    if not isinstance(raw, dict):
        raise ConfigurationError(f"{path}: craft-connector.yml must contain a top-level mapping")
    unknown = sorted(set(raw) - set(SECTIONS))
    if unknown:
        raise ConfigurationError(
            f"{path}: unknown top-level section(s) {unknown} — expected {list(SECTIONS)}"
        )
    # A reader finds where secrets come from before anything that names one.
    present = [section for section in raw if section in SECTIONS]
    expected = [section for section in SECTIONS if section in raw]
    if present != expected:
        raise ConfigurationError(
            f"{path}: sections are in the order {present}; write them as {expected} "
            "(Secrets, Orchestration, Deployment, Engine, Warehouse, "
            "Cloning, Docs_site, then Auth; followed by Object_stores, Artifacts and Logs)"
        )
    return raw
