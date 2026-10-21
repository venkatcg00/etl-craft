"""Configuration engine."""

from __future__ import annotations

from pathlib import Path

from etl_craft.config.auth import ENGINE_NAMES, engine_for_jdbc_url
from etl_craft.config.loader_helpers import (
    CONNECTION_FIELDS,
    Fields,
    auth_extra,
    check_auth_mode,
    connection_block,
    field_keys,
    profile_schema,
    reject_unknown,
)
from etl_craft.config.model import ConnectionProfile
from etl_craft.config.resolve import Resolver
from etl_craft.core.enums import AuthMode
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import mask_arguments


def parse_engine(
    raw: dict[str, object], global_profile: str | None, path: Path, resolver: Resolver
) -> ConnectionProfile:
    """Read and validate the required Engine connection profile."""
    selected = connection_block("Engine", raw, global_profile, path, resolver)
    assert selected is not None  # the Engine section is required
    profiled, block = selected
    where = f"Engine.{profiled.profile}"
    block.pop("Name", None)  # a per-profile Name is already in profiled.settings
    reject_unknown(
        {k: v for k, v in profiled.settings.items() if k == "Name" or k not in block},
        {"Name"},
        "Engine",
        path,
    )
    block = field_keys(block, where, path)
    reject_unknown(block, CONNECTION_FIELDS | {"listen_jdbc_url"}, where, path, fields=True)
    fields = Fields(block, where, profiled.profile, resolver, path)
    jdbc_url = fields.value("jdbc_url", required=True) or ""
    try:
        spec = engine_for_jdbc_url(jdbc_url)
    except ConfigurationError as error:
        raise ConfigurationError(
            f"{path}: {where}.jdbc_url: {error}{fields.hint('jdbc_url')}"
        ) from error

    listen_url = fields.value("listen_jdbc_url")
    if listen_url is not None:
        try:
            supported = (
                spec.name == "postgresql" and engine_for_jdbc_url(listen_url).name == "postgresql"
            )
        except ConfigurationError:
            supported = False
        if not supported:
            address = mask_arguments({"url": listen_url})["url"]
            raise ConfigurationError(
                f"{path}: {where}.Listen_jdbc_url={address!r}; expected PostgreSQL; "
                "remove Listen_jdbc_url for SQLite or point it at the PostgreSQL Engine DB"
            )
    declared = profiled.text("Name")
    if declared and ENGINE_NAMES.get(declared.lower()) != spec.name:
        raise ConfigurationError(
            f"{path}: Engine.Name is {declared!r}, but its Jdbc_url is a {spec.name} URL"
        )
    # An Engine DB with nothing to authenticate (SQLite) needs no auth_mode.
    default_auth = AuthMode.NONE if spec.auth_modes == {AuthMode.NONE} else ""
    auth_mode = fields.value("auth_mode") or default_auth
    check_auth_mode(auth_mode, spec.auth_modes, f"a {spec.name} Engine DB", fields)
    user = (fields.value("user") or "") if auth_mode != AuthMode.NONE else ""
    extra = (
        auth_extra(fields, auth_mode, spec.auth_fields[auth_mode], user=user)
        if auth_mode != AuthMode.NONE
        else {}
    )
    return ConnectionProfile(
        section="ENGINE",
        name=profiled.profile or "",
        jdbc_url=jdbc_url,
        user=user,
        auth_mode=auth_mode,
        extra=extra,
        schema=profile_schema(fields),
        listen_jdbc_url=listen_url,
    )
