"""Reading and validating ``craft-connector.yml``.

The file is written by the team and only read by the engine. Its sections come in this order:
``Secrets`` (where variables are read from, and the default profile), ``Orchestration`` (mode,
limits, DAG defaults and the Email relay), ``Engine``, then the optional ``Warehouse`` and
``Cloning``. Every section that varies by environment holds one block per profile (``dev``,
``prod``, or any names). The active one is the section's own ``Profile``, else
``Secrets.Profile``; a section with a single profile block needs no selection. A profile
block's settings override the section's own.

Every problem is a ``ConfigurationError`` naming the file and the setting.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from etl_craft.config.auth import (
    AUTH_EXTRA_FIELDS,
    EMAIL_AUTH_FIELDS,
    ENGINE_NAMES,
    WAREHOUSE_NAMES,
    WAREHOUSES,
    engine_for_jdbc_url,
    warehouse_by_key,
    warehouse_spec,
)
from etl_craft.config.model import (
    DEFAULT_LOG_DIR,
    DEFAULT_SENDMAIL_PATH,
    EMAIL_TRANSPORTS,
    EXAMPLE_PATH,
    CloningConfig,
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    DagDefaults,
    EmailConfig,
    EmailProfile,
    ExecutionLimits,
    SourceConfig,
)
from etl_craft.config.resolve import Resolver
from etl_craft.config.targets import (
    active_catalog,
    parse_warehouse_url,
    preferred_connection_url,
)
from etl_craft.core.enums import AuthMode, CloningScope, Mode, TableFormat
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import is_safe_identifier

SECTIONS = ("Secrets", "Orchestration", "Engine", "Warehouse", "Cloning")
"""The top-level sections, in the order the file must present them."""

_EARLIER_LAYOUT_SECTIONS = frozenset(
    {"Execution", "Source", "Postgres", "Dag_defaults", "Email", "Orchestrator"}
)
_SOURCE_TYPES = ("environment", "file")

_ORCHESTRATION_KEYS = frozenset(
    {
        "Mode",
        "Name",
        "Log_dir",
        "Task_timeout_seconds",
        "Max_parallel_tasks",
        "Enforce_sla",
        "Global_dag",
        "Catchup",
        "Tags",
        "Retries",
        "Retry_delay_minutes",
        "Depends_on_past",
        "Email_on_failure",
        "Email_recipients",
        "Allow_schedule",
        "Email",
    }
)
_EMAIL_KEYS = frozenset(
    {"host", "port", "from_address", "auth_mode", "user", "use_tls", "secret", "scope"}
    | {"client_id", "token_url", "transport", "sendmail_path", "from_name"}
)
_SMTP_ONLY_EMAIL_KEYS = frozenset(
    _EMAIL_KEYS - {"from_address", "from_name", "transport", "sendmail_path"}
)
_CONNECTION_FIELDS = frozenset(
    {"jdbc_url", "user", "auth_mode", "secret", "schema", *AUTH_EXTRA_FIELDS}
)
# The field whose presence selects the separate-fields connection shape.
_PREFERRED_SHAPE_MARKER = {"databricks": "catalog", "snowflake": "account"}


def load_config(path: str | Path) -> ConnectorConfig:
    """Read, parse and validate the ``craft-connector.yml`` at ``path``."""
    path = Path(path)
    if not path.is_file():
        raise ConfigurationError(
            f"craft-connector.yml not found at {path} — write one (see {EXAMPLE_PATH}); "
            "etl-craft reads it and never writes it"
        )
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as error:
        raise ConfigurationError(f"{path} is not valid YAML: {error}") from error
    return parse_config(raw, path)


def parse_config(raw: Any, path: Path) -> ConnectorConfig:
    """Validate an already-loaded ``craft-connector.yml`` mapping; ``path`` is where it came from.

    Relative paths in it (``Secrets.Path``) resolve against ``path``'s directory.
    """
    _check_layout(raw, path)
    secrets = _mapping(raw, "Secrets", path, required=True)
    # Secrets.Source_type and Secrets.Path can only come from the process environment: the
    # source they describe is not known yet.
    environment = Resolver(values=os.environ, origin="the process environment", path=path)
    source = _parse_source(secrets, path, environment)
    resolver = Resolver.for_source(source, path)
    resolver.sources.extend(environment.sources)
    global_profile = resolver.text(secrets.get("Profile"), "Secrets.Profile")

    orchestration = _profiled_settings("Orchestration", raw, global_profile, path, resolver)
    _reject_unknown(orchestration.settings, _ORCHESTRATION_KEYS, "Orchestration", path)
    mode = _parse_mode(orchestration, path)
    limits = _parse_limits(orchestration, path)
    dag_defaults = _parse_dag_defaults(orchestration, path)
    orchestrator_name = orchestration.text("Name")
    log_dir = _relative_to_config(orchestration.text("Log_dir") or DEFAULT_LOG_DIR, path)
    email = _parse_email(orchestration, path, resolver)
    engine = _parse_engine(raw, global_profile, path, resolver)
    warehouse, table_format = _parse_warehouse(raw, global_profile, path, resolver)
    cloning = _parse_cloning(raw, global_profile, path, resolver)

    config = ConnectorConfig(
        mode=mode,
        source=source,
        engine=engine,
        cloning=cloning,
        warehouse=warehouse,
        warehouse_table_format=table_format,
        dag_defaults=dag_defaults,
        email=email,
        limits=limits,
        config_path=path,
        orchestrator_name=orchestrator_name,
        settings=tuple(resolver.sources),
        log_dir=log_dir,
    )
    if warehouse is not None:
        # Every warehouse profile names the database the engine connects to and writes in.
        try:
            active_catalog(config)
        except ConfigurationError as error:
            raise ConfigurationError(
                f"{path}: Warehouse.{warehouse.active_profile}: {error}"
            ) from error
    return config


def _duckdb_url_beside_config(jdbc_url: str, path: Path) -> str:
    """Return a DuckDB URL whose relative file path is taken from the config's directory."""
    file = parse_warehouse_url(jdbc_url).path
    if not file or file == ":memory:" or Path(file).is_absolute():
        return jdbc_url
    return f"jdbc:duckdb:{_relative_to_config(file, path)}"


def _relative_to_config(written: str, path: Path) -> Path:
    """Return ``written`` as an absolute path, a relative one taken from the config's directory.

    An absolute path is kept as written; symbolic links are never resolved.
    """
    resolved = Path(written).expanduser()
    if not resolved.is_absolute():
        resolved = path.absolute().parent / resolved
    return Path(os.path.normpath(resolved))


def _check_layout(raw: Any, path: Path) -> None:
    """Refuse a non-mapping, an earlier layout, unknown sections and sections out of order."""
    if not isinstance(raw, dict):
        raise ConfigurationError(f"{path}: craft-connector.yml must contain a top-level mapping")
    earlier = sorted(_EARLIER_LAYOUT_SECTIONS.intersection(raw))
    if not earlier and any(isinstance(v, dict) and "Variables" in v for v in raw.values()):
        earlier = ["Variables blocks"]
    if earlier:
        raise ConfigurationError(
            f"{path}: this is an earlier craft-connector.yml layout ({', '.join(earlier)}). "
            "The current file has Secrets, Orchestration (with the DAG defaults and Email "
            "inside it), Engine, Warehouse and Cloning, each with one block per profile — "
            f"see {EXAMPLE_PATH}"
        )
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
            "(Secrets, Orchestration, Engine, Warehouse, then Cloning)"
        )


# Sections and profiles


@dataclass(frozen=True)
class _Profiled:
    """One section's settings: its own values overlaid with the active profile's block."""

    section: str
    profile: str | None
    settings: dict[str, Any]
    resolver: Resolver
    path: Path

    def where(self, key: str) -> str:
        return f"{self.section}.{self.profile}.{key}" if self.profile else f"{self.section}.{key}"

    def value(self, key: str) -> Any:
        return self.resolver.resolve(
            self.settings.get(key), self.where(key), profile=self.profile, field_name=key
        )

    def text(self, key: str) -> str | None:
        return self.resolver.text(
            self.settings.get(key), self.where(key), profile=self.profile, field_name=key
        )


def _mapping(raw: dict[str, Any], name: str, path: Path, *, required: bool) -> dict[str, Any]:
    section = raw.get(name)
    if section is None:
        if required:
            raise ConfigurationError(f"{path}: missing the {name} section (see {EXAMPLE_PATH})")
        return {}
    if not isinstance(section, dict):
        raise ConfigurationError(f"{path}: {name} must be a mapping")
    return section


def _profiled_settings(
    name: str,
    raw: dict[str, Any],
    global_profile: str | None,
    path: Path,
    resolver: Resolver,
    *,
    nested: frozenset[str] = frozenset({"Email"}),
) -> _Profiled:
    """Select ``name``'s active profile block and overlay it on the section's own values.

    A mapping-valued key is a profile block unless it is in ``nested``, a structured setting
    that lives inside a profile, like Orchestration's Email.
    """
    section = _mapping(raw, name, path, required=name == "Orchestration")
    blocks = {
        key: value
        for key, value in section.items()
        if isinstance(value, dict) and key not in nested
    }
    base = {key: value for key, value in section.items() if key not in blocks and key != "Profile"}
    selected = resolver.text(section.get("Profile"), f"{name}.Profile") or global_profile
    if not blocks:
        return _Profiled(name, selected, base, resolver, path)
    if selected is None:
        if len(blocks) != 1:
            raise ConfigurationError(
                f"{path}: {name} has profiles {sorted(blocks)} but none is selected — set "
                f"{name}.Profile or Secrets.Profile"
            )
        selected = next(iter(blocks))
    if selected not in blocks:
        where = f"{name}.Profile" if "Profile" in section else "Secrets.Profile"
        raise ConfigurationError(
            f"{path}: {name} has no profile {selected!r} (it has {sorted(blocks)})"
            f"{resolver.hint(where)}"
        )
    return _Profiled(name, selected, {**base, **blocks[selected]}, resolver, path)


def _reject_unknown(
    settings: Mapping[str, Any], allowed: frozenset[str] | set[str], where: str, path: Path
) -> None:
    unknown = sorted(set(settings) - set(allowed))
    if unknown:
        raise ConfigurationError(f"{path}: unknown key(s) {unknown} in {where}")


# Secrets


def _parse_source(secrets: dict[str, Any], path: Path, environment: Resolver) -> SourceConfig:
    _reject_unknown(secrets, {"Source_type", "Path", "Profile"}, "Secrets", path)
    written_type = environment.text(secrets.get("Source_type"), "Secrets.Source_type")
    source_type = (written_type or "").lower()
    if source_type not in _SOURCE_TYPES:
        raise ConfigurationError(
            f"{path}: Secrets.Source_type must be one of {sorted(_SOURCE_TYPES)}, "
            f"got {secrets.get('Source_type')!r}{environment.hint('Secrets.Source_type')}"
        )
    raw_path = environment.text(secrets.get("Path"), "Secrets.Path")
    if source_type == "environment":
        if raw_path is not None:
            raise ConfigurationError(f"{path}: Secrets.Path is only used with Source_type: file")
        return SourceConfig(type=source_type)
    if raw_path is None:
        raise ConfigurationError(f"{path}: Secrets.Path is required when Source_type is file")
    source_path = Path(raw_path).expanduser()
    if not source_path.is_absolute():
        # Relative to the config file, never the working directory, so every task finds it.
        source_path = path.resolve().parent / source_path
    return SourceConfig(type=source_type, path=str(source_path.resolve()))


# Connection profiles


@dataclass(frozen=True)
class _Fields:
    """The fields of one connection profile block, resolved on demand."""

    block: dict[str, Any]
    where: str
    profile: str | None
    resolver: Resolver
    path: Path

    def value(self, key: str, *, required: bool = False) -> str | None:
        raw = self.block.get(key)
        if raw is None:
            if required:
                raise ConfigurationError(f"{self.path}: {self.where} needs {key}")
            return None
        value = self.resolver.resolve(
            raw, f"{self.where}.{key}", profile=self.profile, field_name=key
        )
        return str(value).strip() if value is not None else None

    def secret_var(self, key: str = "secret") -> str | None:
        return self.resolver.secret_name(
            self.block.get(key), f"{self.where}.{key}", profile=self.profile, field_name=key
        )

    def hint(self, key: str) -> str:
        return self.resolver.hint(f"{self.where}.{key}")


def _auth_extra(
    fields: _Fields,
    auth_mode: str,
    required: tuple[str, ...],
    *,
    user: str,
    secret_key: str = "secret",
) -> dict[str, Any]:
    """Check an auth mode's fields are present and return the profile's ``extra`` for it."""
    path, where = fields.path, fields.where
    if "user" in required and not user:
        raise ConfigurationError(f"{path}: {where} needs user for auth_mode {auth_mode}")
    extra: dict[str, Any] = {}
    secret_var = fields.secret_var(secret_key)
    if "secret" in required and not secret_var:
        raise ConfigurationError(
            f"{path}: {where} needs {secret_key} (a variable name) for auth_mode {auth_mode}"
        )
    if secret_var:
        extra["secret_var"] = secret_var
    for name in AUTH_EXTRA_FIELDS:
        value = fields.value(name)
        if value:
            extra[name] = value
        elif name in required:
            raise ConfigurationError(f"{path}: {where} needs {name} for auth_mode {auth_mode}")
    return extra


def _check_auth_mode(auth_mode: str, accepted: frozenset[str], label: str, fields: _Fields) -> None:
    if auth_mode not in accepted:
        raise ConfigurationError(
            f"{fields.path}: {fields.where}.auth_mode resolved to {auth_mode!r}; {label} "
            f"takes {sorted(accepted)}{fields.hint('auth_mode')}"
        )


def _connection_block(
    name: str, raw: dict[str, Any], global_profile: str | None, path: Path, resolver: Resolver
) -> tuple[_Profiled, dict[str, Any]] | None:
    """Return a connection section's selection and a copy of its active profile block."""
    section = _mapping(raw, name, path, required=name == "Engine")
    if not section:
        return None
    profiled = _profiled_settings(name, raw, global_profile, path, resolver)
    if profiled.profile is None or not isinstance(section.get(profiled.profile), dict):
        raise ConfigurationError(
            f"{path}: {name} needs at least one profile block (dev, prod, ...) holding its "
            f"connection settings — see {EXAMPLE_PATH}"
        )
    return profiled, dict(section[profiled.profile])


# Orchestration


def _parse_mode(orchestration: _Profiled, path: Path) -> Mode:
    mode = (orchestration.text("Mode") or "").lower()
    if mode not in {member.value for member in Mode}:
        raise ConfigurationError(
            f"{path}: Orchestration.Mode must be local or remote, got "
            f"{orchestration.settings.get('Mode')!r}"
            f"{orchestration.resolver.hint(orchestration.where('Mode'))}"
        )
    return Mode(mode)


def _whole_number(settings: _Profiled, key: str, default: int | None, path: Path) -> Any:
    value = settings.value(key)
    if value is None:
        return default
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigurationError(
            f"{path}: Orchestration.{key} must be a whole number, got {value!r}"
            f"{settings.resolver.hint(settings.where(key))}"
        )
    return value


def _flag(settings: _Profiled, key: str, default: bool | None, path: Path) -> Any:
    value = settings.value(key)
    if value is None:
        return default
    return _parse_bool(value, settings.where(key), path, default=bool(default))


def _string_list(settings: _Profiled, key: str, path: Path) -> list[str] | None:
    value = settings.value(key)
    if value is None:
        return None
    if isinstance(value, str):
        # A variable, or a value, holding a comma-separated list.
        return [item.strip() for item in value.split(",") if item.strip()]
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigurationError(f"{path}: Orchestration.{key} must be a list of strings")
    return value


def _parse_limits(settings: _Profiled, path: Path) -> ExecutionLimits:
    defaults = ExecutionLimits()
    return ExecutionLimits(
        task_timeout_seconds=_whole_number(
            settings, "Task_timeout_seconds", defaults.task_timeout_seconds, path
        ),
        max_parallel_tasks=_whole_number(
            settings, "Max_parallel_tasks", defaults.max_parallel_tasks, path
        ),
        enforce_sla=_flag(settings, "Enforce_sla", False, path),
    )


def _parse_dag_defaults(settings: _Profiled, path: Path) -> DagDefaults:
    return DagDefaults(
        global_dag=_flag(settings, "Global_dag", False, path),
        catchup=_flag(settings, "Catchup", None, path),
        tags=_string_list(settings, "Tags", path),
        retries=_whole_number(settings, "Retries", None, path),
        retry_delay_minutes=_whole_number(settings, "Retry_delay_minutes", None, path),
        depends_on_past=_flag(settings, "Depends_on_past", None, path),
        email_on_failure=_flag(settings, "Email_on_failure", None, path),
        email_recipients=_string_list(settings, "Email_recipients", path),
        allow_schedule=_flag(settings, "Allow_schedule", True, path),
    )


def _parse_email(orchestration: _Profiled, path: Path, resolver: Resolver) -> EmailConfig | None:
    block = orchestration.settings.get("Email")
    if block is None:
        return None
    where = orchestration.where("Email")
    if not isinstance(block, dict):
        raise ConfigurationError(f"{path}: {where} must be a mapping")
    _reject_unknown(block, _EMAIL_KEYS, where, path)
    fields = _Fields(block, where, orchestration.profile, resolver, path)
    transport = (fields.value("transport") or "smtp").lower()
    if transport not in EMAIL_TRANSPORTS:
        raise ConfigurationError(
            f"{path}: {where}.transport resolved to {transport!r}, which is not one of "
            f"{', '.join(EMAIL_TRANSPORTS)}{fields.hint('transport')}"
        )
    name = orchestration.profile or "default"
    if transport == "sendmail":
        smtp_only = sorted(set(block) & _SMTP_ONLY_EMAIL_KEYS)
        if smtp_only:
            raise ConfigurationError(
                f"{path}: {where} sends through sendmail, so {', '.join(smtp_only)} would be "
                "ignored; remove them, or set transport: smtp"
            )
        sendmail = EmailProfile(
            section="EMAIL",
            name=name,
            host="",
            port=0,
            from_address=fields.value("from_address", required=True) or "",
            from_name=fields.value("from_name") or "",
            transport="sendmail",
            sendmail_path=fields.value("sendmail_path") or DEFAULT_SENDMAIL_PATH,
        )
        return EmailConfig(active_profile=name, profiles={name: sendmail})
    if "sendmail_path" in block:
        raise ConfigurationError(
            f"{path}: {where}.sendmail_path applies only with transport: sendmail"
        )
    port_raw = fields.value("port", required=True)
    try:
        port = int(port_raw or "")
    except ValueError as error:
        raise ConfigurationError(
            f"{path}: {where}.port resolved to {port_raw!r}, not a number{fields.hint('port')}"
        ) from error
    auth_mode = fields.value("auth_mode") or AuthMode.NONE
    if auth_mode not in EMAIL_AUTH_FIELDS:
        raise ConfigurationError(
            f"{path}: {where}.auth_mode resolved to {auth_mode!r}, which is not one of "
            f"{sorted(EMAIL_AUTH_FIELDS)}{fields.hint('auth_mode')}"
        )
    user = fields.value("user") or ""
    extra = _auth_extra(fields, auth_mode, EMAIL_AUTH_FIELDS[auth_mode], user=user)
    profile = EmailProfile(
        section="EMAIL",
        name=name,
        host=fields.value("host", required=True) or "",
        port=port,
        from_address=fields.value("from_address", required=True) or "",
        auth_mode=auth_mode,
        user=user or None,
        use_tls=_parse_bool(fields.value("use_tls"), f"{where}.use_tls", path, default=True),
        extra=extra,
        from_name=fields.value("from_name") or "",
    )
    return EmailConfig(active_profile=name, profiles={name: profile})


# Engine


def _parse_engine(
    raw: dict[str, Any], global_profile: str | None, path: Path, resolver: Resolver
) -> ConnectionSection:
    selected = _connection_block("Engine", raw, global_profile, path, resolver)
    assert selected is not None  # the Engine section is required
    profiled, block = selected
    where = f"Engine.{profiled.profile}"
    block.pop("Name", None)  # a per-profile Name is already in profiled.settings
    _reject_unknown(block, _CONNECTION_FIELDS, where, path)
    _reject_unknown(
        {k: v for k, v in profiled.settings.items() if k == "Name" or k not in block},
        {"Name"},
        "Engine",
        path,
    )
    fields = _Fields(block, where, profiled.profile, resolver, path)
    jdbc_url = fields.value("jdbc_url", required=True) or ""
    try:
        spec = engine_for_jdbc_url(jdbc_url)
    except ConfigurationError as error:
        raise ConfigurationError(
            f"{path}: {where}.jdbc_url: {error}{fields.hint('jdbc_url')}"
        ) from error

    declared = profiled.text("Name")
    if declared and ENGINE_NAMES.get(declared.lower()) != spec.name:
        raise ConfigurationError(
            f"{path}: Engine.Name is {declared!r}, but its jdbc_url is a {spec.name} URL"
        )
    # An Engine DB with nothing to authenticate (SQLite) needs no auth_mode.
    default_auth = AuthMode.NONE if spec.auth_modes == {AuthMode.NONE} else ""
    auth_mode = fields.value("auth_mode") or default_auth
    _check_auth_mode(auth_mode, spec.auth_modes, f"a {spec.name} Engine DB", fields)
    user = (fields.value("user") or "") if auth_mode != AuthMode.NONE else ""
    extra = (
        _auth_extra(fields, auth_mode, spec.auth_fields[auth_mode], user=user)
        if auth_mode != AuthMode.NONE
        else {}
    )
    profile = ConnectionProfile(
        section="ENGINE",
        name=profiled.profile or "",
        jdbc_url=jdbc_url,
        user=user,
        auth_mode=auth_mode,
        extra=extra,
        schema=_schema(fields),
    )
    return ConnectionSection(active_profile=profile.name, profiles={profile.name: profile})


# Warehouse


def _parse_warehouse(
    raw: dict[str, Any], global_profile: str | None, path: Path, resolver: Resolver
) -> tuple[ConnectionSection | None, TableFormat]:
    selected = _connection_block("Warehouse", raw, global_profile, path, resolver)
    if selected is None:
        return None, TableFormat.NATIVE
    profiled, block = selected
    where = f"Warehouse.{profiled.profile}"
    # Name and Table_format may sit on the section or on one profile (a dev DuckDB beside a
    # prod Postgres); the profile's own value wins.
    for key in ("Name", "Table_format"):
        block.pop(key, None)
    settings = {
        k: v
        for k, v in profiled.settings.items()
        if k in {"Name", "Table_format"} or k not in block
    }
    _reject_unknown(settings, {"Name", "Table_format"}, "Warehouse", path)
    table_format = (profiled.text("Table_format") or TableFormat.NATIVE).lower()
    if table_format not in {member.value for member in TableFormat}:
        raise ConfigurationError(
            f"{path}: Warehouse.Table_format must be native or iceberg, got "
            f"{settings.get('Table_format')!r}"
        )
    declared = profiled.text("Name")
    expected_dialect = WAREHOUSE_NAMES.get(declared.lower()) if declared else None
    if declared and expected_dialect is None:
        names = sorted({spec.display_name for spec in WAREHOUSES})
        raise ConfigurationError(f"{path}: Warehouse.Name {declared!r} must be one of {names}")
    fields = _Fields(block, where, profiled.profile, resolver, path)

    marker = _PREFERRED_SHAPE_MARKER.get(expected_dialect or "")
    if marker and (marker in block or "token" in block):
        assert declared is not None and expected_dialect is not None
        profile = _preferred_shape_profile(
            fields, profiled, declared, expected_dialect, table_format
        )
    elif "token" in block:
        raise ConfigurationError(
            f"{path}: {where}.token is only for Warehouse.Name Databricks or Snowflake"
        )
    else:
        profile = _jdbc_url_profile(fields, profiled, declared, expected_dialect, table_format)
    section = ConnectionSection(active_profile=profile.name, profiles={profile.name: profile})
    return section, TableFormat(table_format)


def _preferred_shape_profile(
    fields: _Fields,
    profiled: _Profiled,
    declared: str,
    expected_dialect: str,
    table_format: str,
) -> ConnectionProfile:
    """Build a Databricks or Snowflake profile from separate fields and a credential-free URL.

    A ``token`` field means auth mode ``token``; any other mode is named in ``auth_mode``.
    """
    path, where, block = fields.path, fields.where, fields.block
    spec = warehouse_spec(expected_dialect, table_format)
    _reject_unknown(
        block, {*spec.preferred_fields, "auth_mode", "secret", *AUTH_EXTRA_FIELDS}, where, path
    )
    auth_mode = fields.value("auth_mode") or ("token" if "token" in block else "")
    _check_auth_mode(auth_mode, spec.auth_modes, f"a {declared} warehouse", fields)
    if auth_mode == AuthMode.TOKEN and "secret" in block:
        raise ConfigurationError(f"{path}: {where} names its token under `token`, not `secret`")
    parts = {
        name: fields.value(name, required=True) or ""
        for name in spec.preferred_fields
        if name != "token"
    }
    try:
        jdbc_url = preferred_connection_url(declared, parts)
    except ConfigurationError as error:
        raise ConfigurationError(f"{path}: {where}: {error}") from error
    user = parts.get("user", "")
    extra = _auth_extra(
        fields,
        auth_mode,
        spec.auth_fields[auth_mode],
        user=user,
        secret_key="token" if auth_mode == AuthMode.TOKEN else "secret",
    )
    return ConnectionProfile(
        section="WAREHOUSE",
        name=profiled.profile or "",
        jdbc_url=jdbc_url,
        user=user,
        auth_mode=auth_mode,
        extra=extra,
        schema=_schema(fields),
    )


def _jdbc_url_profile(
    fields: _Fields,
    profiled: _Profiled,
    declared: str | None,
    expected_dialect: str | None,
    table_format: str,
) -> ConnectionProfile:
    """Build a warehouse profile from a whole JDBC URL."""
    path, where, block = fields.path, fields.where, fields.block
    # DuckDB over Iceberg reads its catalog and object-storage settings from the profile.
    profile_fields: tuple[str, ...] = ()
    if expected_dialect == "duckdb" and table_format == TableFormat.ICEBERG:
        profile_fields = warehouse_by_key("duckdb_iceberg").profile_fields
    _reject_unknown(block, _CONNECTION_FIELDS | set(profile_fields), where, path)
    jdbc_url = fields.value("jdbc_url", required=True) or ""
    try:
        dialect = parse_warehouse_url(jdbc_url).dialect
        spec = warehouse_spec(dialect, table_format)
    except ConfigurationError as error:
        raise ConfigurationError(f"{path}: {where}: {error}{fields.hint('jdbc_url')}") from error
    actual = dialect.split("+", 1)[0]
    if expected_dialect and actual != expected_dialect:
        raise ConfigurationError(
            f"{path}: Warehouse.Name is {declared!r}, but its jdbc_url resolves to {actual!r}"
        )
    if actual == "duckdb":
        jdbc_url = _duckdb_url_beside_config(jdbc_url, path)
    # A DuckDB file has nothing to authenticate; over Iceberg, the catalog may still ask.
    default_auth = AuthMode.NONE if actual == "duckdb" else ""
    auth_mode = fields.value("auth_mode") or default_auth
    _check_auth_mode(auth_mode, spec.auth_modes, f"a {spec.display_name} warehouse", fields)
    user = fields.value("user") or ""
    extra = _auth_extra(fields, auth_mode, spec.auth_fields[auth_mode], user=user)
    for key in profile_fields:
        if key == "s3_secret":
            name = fields.secret_var(key)
            if name is not None:
                extra[key] = fields.resolver.values[name]
            continue
        value = fields.value(key)
        if value is not None:
            extra[key] = value
    schema = _schema(fields)
    url_schema = _url_schema(jdbc_url)
    if url_schema and url_schema.lower() != schema.lower():
        raise ConfigurationError(
            f"{path}: {where}.schema is {schema!r}, but its jdbc_url names schema "
            f"{url_schema!r}; make them the same"
        )
    return ConnectionProfile(
        section="WAREHOUSE",
        name=profiled.profile or "",
        jdbc_url=jdbc_url,
        user=user,
        auth_mode=auth_mode,
        extra=extra,
        schema=schema,
    )


def _schema(fields: _Fields) -> str:
    """Return the profile's required ``schema``: a plain identifier."""
    schema = fields.value("schema", required=True) or ""
    if not is_safe_identifier(schema):
        raise ConfigurationError(
            f"{fields.path}: {fields.where}.schema must be a plain identifier (letters, digits "
            f"and underscores), got {schema!r}{fields.hint('schema')}"
        )
    return schema


def _url_schema(jdbc_url: str) -> str | None:
    """Return the schema a Trino URL names after its catalog, if any."""
    url = parse_warehouse_url(jdbc_url)
    if url.dialect.startswith("trino") and "/" in url.database:
        return url.database.split("/", 1)[1] or None
    return None


# Cloning


def _parse_cloning(
    raw: dict[str, Any], global_profile: str | None, path: Path, resolver: Resolver
) -> CloningConfig:
    profiled = _profiled_settings(
        "Cloning", raw, global_profile, path, resolver, nested=frozenset()
    )
    if not profiled.settings:
        return CloningConfig()
    _reject_unknown(
        profiled.settings,
        {"Enabled", "Scope", "External_volume", "Base_location"},
        "Cloning",
        path,
    )
    scope = (profiled.text("Scope") or CloningScope.CFG).lower()
    scopes = sorted(member.value for member in CloningScope)
    if scope not in scopes:
        raise ConfigurationError(f"{path}: Cloning.Scope must be one of {scopes}, got {scope!r}")
    enabled = _parse_bool(profiled.value("Enabled"), profiled.where("Enabled"), path, default=False)
    return CloningConfig(
        enabled=enabled and scope != CloningScope.NONE,
        scope=CloningScope(scope),
        external_volume=profiled.text("External_volume") or "",
        base_location=profiled.text("Base_location") or "",
    )


def _parse_bool(value: Any, name: str, path: Path, *, default: bool) -> bool:
    """Accept YAML booleans and the usual environment spellings (true/false, 1/0, yes/no)."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    raise ConfigurationError(f"{path}: {name} must be true or false, got {value!r}")
