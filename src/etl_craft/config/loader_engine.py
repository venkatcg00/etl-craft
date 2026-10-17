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
    profile_schema,
    reject_unknown,
)
from etl_craft.config.model import ConnectionProfile
from etl_craft.config.resolve import Resolver
from etl_craft.core.enums import AuthMode
from etl_craft.core.errors import ConfigurationError


def parse_engine(
    raw: dict[str, object], global_profile: str | None, path: Path, resolver: Resolver
) -> ConnectionProfile:
    """Read and validate the required Engine connection profile."""
    selected = connection_block("Engine", raw, global_profile, path, resolver)
    assert selected is not None  # the Engine section is required
    profiled, block = selected
    where = f"Engine.{profiled.profile}"
    block.pop("Name", None)  # a per-profile Name is already in profiled.settings
    reject_unknown(block, CONNECTION_FIELDS, where, path)
    reject_unknown(
        {k: v for k, v in profiled.settings.items() if k == "Name" or k not in block},
        {"Name"},
        "Engine",
        path,
    )
    fields = Fields(block, where, profiled.profile, resolver, path)
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
    )
