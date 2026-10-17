"""Configuration warehouse."""

from __future__ import annotations

from pathlib import Path

from etl_craft.config.auth import (
    AUTH_EXTRA_FIELDS,
    WAREHOUSE_NAMES,
    WAREHOUSES,
    warehouse_by_key,
    warehouse_spec,
)
from etl_craft.config.loader_helpers import (
    CONNECTION_FIELDS,
    Fields,
    Profiled,
    auth_extra,
    check_auth_mode,
    connection_block,
    profile_schema,
    reject_unknown,
    relative_to_config,
)
from etl_craft.config.model import ConnectionProfile
from etl_craft.config.resolve import Resolver
from etl_craft.config.targets import parse_warehouse_url, preferred_connection_url
from etl_craft.core.enums import AuthMode, TableFormat, enum_value
from etl_craft.core.errors import ConfigurationError

WAREHOUSE_SECTION_KEYS = frozenset({"Name", "Table_format"})

_PREFERRED_SHAPE_MARKER = {"databricks": "catalog", "snowflake": "account"}


def _duckdb_url_beside_config(jdbc_url: str, path: Path) -> str:
    """Return a DuckDB URL whose relative file path is taken from the config's directory."""
    file = parse_warehouse_url(jdbc_url).path
    if not file or file == ":memory:" or Path(file).is_absolute():
        return jdbc_url
    return f"jdbc:duckdb:{relative_to_config(file, path)}"


def parse_warehouse(
    raw: dict[str, object], global_profile: str | None, path: Path, resolver: Resolver
) -> tuple[ConnectionProfile | None, TableFormat]:
    """Read the optional Warehouse connection and table format."""
    selected = connection_block("Warehouse", raw, global_profile, path, resolver)
    if selected is None:
        return None, TableFormat.NATIVE
    profiled, block = selected
    where = f"Warehouse.{profiled.profile}"
    # Name and Table_format may sit on the section or on one profile (a dev DuckDB beside a
    # prod Postgres); the profile's own value wins.
    for key in ("Name", "Table_format"):
        block.pop(key, None)
    settings = {
        k: v for k, v in profiled.settings.items() if k in WAREHOUSE_SECTION_KEYS or k not in block
    }
    reject_unknown(settings, WAREHOUSE_SECTION_KEYS, "Warehouse", path)
    table_format = enum_value(
        TableFormat,
        profiled.text("Table_format") or TableFormat.NATIVE,
        ConfigurationError(
            f"{path}: Warehouse.Table_format must be native or iceberg, got "
            f"{settings.get('Table_format')!r}"
        ),
    )
    declared = profiled.text("Name")
    expected_dialect = WAREHOUSE_NAMES.get(declared.lower()) if declared else None
    if declared and expected_dialect is None:
        names = sorted({spec.display_name for spec in WAREHOUSES})
        raise ConfigurationError(f"{path}: Warehouse.Name {declared!r} must be one of {names}")
    fields = Fields(block, where, profiled.profile, resolver, path)

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
    return profile, table_format


def _preferred_shape_profile(
    fields: Fields,
    profiled: Profiled,
    declared: str,
    expected_dialect: str,
    table_format: str,
) -> ConnectionProfile:
    """Build a Databricks or Snowflake profile from separate fields and a credential-free URL.

    A ``token`` field means auth mode ``token``; any other mode is named in ``auth_mode``.
    """
    path, where, block = fields.path, fields.where, fields.block
    spec = warehouse_spec(expected_dialect, table_format)
    reject_unknown(
        block, {*spec.preferred_fields, "auth_mode", "secret", *AUTH_EXTRA_FIELDS}, where, path
    )
    auth_mode = fields.value("auth_mode") or ("token" if "token" in block else "")
    check_auth_mode(auth_mode, spec.auth_modes, f"a {declared} warehouse", fields)
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
    extra = auth_extra(
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
        schema=profile_schema(fields),
    )


def _jdbc_url_profile(
    fields: Fields,
    profiled: Profiled,
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
    reject_unknown(block, CONNECTION_FIELDS | set(profile_fields), where, path)
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
    check_auth_mode(auth_mode, spec.auth_modes, f"a {spec.display_name} warehouse", fields)
    user = fields.value("user") or ""
    extra = auth_extra(fields, auth_mode, spec.auth_fields[auth_mode], user=user)
    for key in profile_fields:
        if key == "s3_secret":
            name = fields.secret_var(key)
            if name is not None:
                extra[key] = name
            continue
        value = fields.value(key)
        if value is not None:
            extra[key] = value
    schema = profile_schema(fields)
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


def _url_schema(jdbc_url: str) -> str | None:
    """Return the schema a Trino URL names after its catalog, if any."""
    url = parse_warehouse_url(jdbc_url)
    if url.dialect.startswith("trino") and "/" in url.database:
        return url.database.split("/", 1)[1] or None
    return None
