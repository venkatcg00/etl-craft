"""Configuration helpers."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar
from urllib.parse import parse_qsl, urlencode

from etl_craft.config.auth import AUTH_EXTRA_FIELDS
from etl_craft.config.model import EXAMPLE_PATH, SourceConfig
from etl_craft.config.resolve import Resolver
from etl_craft.core.enums import SecretsSource, enum_value
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import SECRET_KEYS, is_safe_identifier

_NumberDefault = TypeVar("_NumberDefault", int, None)
_FlagDefault = TypeVar("_FlagDefault", bool, None)

SECRETS_KEYS = frozenset({"Source_type", "Path", "Profile"})


CONNECTION_FIELDS = frozenset(
    {"jdbc_url", "user", "auth_mode", "secret", "schema", *AUTH_EXTRA_FIELDS}
)


def connection_url(written: str, path: Path, where: str) -> str:
    """Refuse URL credentials and make certificate URL paths project-relative."""
    base, separator, query = written.partition("?")
    if not separator:
        return written
    pairs = parse_qsl(query, keep_blank_values=True)
    for key, _ in pairs:
        if key.lower() in SECRET_KEYS:
            raise ConfigurationError(
                f"{path}: {where} contains credential query key {key!r}; "
                "put the secret in a variable and name it in `secret`"
            )
    resolved = [
        (key, str(relative_to_config(value, path)))
        if key.lower() in {"sslrootcert", "sslcert", "sslkey", "private_key_file"}
        else (key, value)
        for key, value in pairs
    ]
    return f"{base}?{urlencode(resolved)}" if resolved != pairs else written


def relative_to_config(written: str, path: Path) -> Path:
    """Return ``written`` as an absolute path, a relative one taken from the config's directory.

    An absolute path is kept as written; symbolic links are never resolved.
    """
    resolved = Path(written).expanduser()
    if not resolved.is_absolute():
        resolved = path.absolute().parent / resolved
    return Path(os.path.normpath(resolved))


@dataclass(frozen=True)
class Profiled:
    """One section's settings: its own values overlaid with the active profile's block."""

    section: str
    profile: str | None
    settings: dict[str, object]
    resolver: Resolver
    path: Path

    def where(self, key: str) -> str:
        """Return the setting's section and selected profile path."""
        return f"{self.section}.{self.profile}.{key}" if self.profile else f"{self.section}.{key}"

    def value(self, key: str) -> object:
        """Resolve a raw setting while recording its source."""
        return self.resolver.resolve(
            self.settings.get(key), self.where(key), profile=self.profile, field_name=key
        )

    def text(self, key: str) -> str | None:
        """Resolve a setting that must contain non-empty text."""
        return self.resolver.text(
            self.settings.get(key), self.where(key), profile=self.profile, field_name=key
        )


def mapping(raw: dict[str, object], name: str, path: Path, *, required: bool) -> dict[str, object]:
    """Return a section mapping, refusing missing required sections and other shapes."""
    section = raw.get(name)
    if section is None:
        if required:
            raise ConfigurationError(f"{path}: missing the {name} section (see {EXAMPLE_PATH})")
        return {}
    if not isinstance(section, dict):
        raise ConfigurationError(f"{path}: {name} must be a mapping")
    return section


def profiled_settings(
    name: str,
    raw: dict[str, object],
    global_profile: str | None,
    path: Path,
    resolver: Resolver,
    *,
    nested: frozenset[str] = frozenset({"Email"}),
) -> Profiled:
    """Select ``name``'s active profile block and overlay it on the section's own values.

    A mapping-valued key is a profile block unless it is in ``nested``, a structured setting
    that lives inside a profile, like Orchestration's Email.
    """
    section = mapping(raw, name, path, required=False)
    blocks = {
        key: value
        for key, value in section.items()
        if isinstance(value, dict) and key not in nested
    }
    base = {key: value for key, value in section.items() if key not in blocks and key != "Profile"}
    selected = resolver.text(section.get("Profile"), f"{name}.Profile") or global_profile
    if not blocks:
        return Profiled(name, selected, base, resolver, path)
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
    return Profiled(name, selected, {**base, **blocks[selected]}, resolver, path)


def shown(key: str) -> str:
    """Return a connection field's key as ``craft-connector.yml`` spells it: ``Jdbc_url``."""
    return key[:1].upper() + key[1:]


def field_keys(block: Mapping[str, object], where: str, path: Path) -> dict[str, object]:
    """Read a block's ``Capitalized_snake`` keys as field names, refusing any other spelling."""
    wrong = sorted(key for key in block if not key[:1].isupper())
    if wrong:
        raise ConfigurationError(
            f"{path}: unknown key(s) {wrong} in {where}; keys are written Capitalized_snake, "
            f"such as {shown(wrong[0])}"
        )
    return {key[:1].lower() + key[1:]: value for key, value in block.items()}


def reject_unknown(
    settings: Mapping[str, object],
    allowed: frozenset[str] | set[str],
    where: str,
    path: Path,
    *,
    fields: bool = False,
) -> None:
    """Refuse settings outside the accepted keys; ``fields`` shows a block's field keys."""
    unknown = sorted((shown(key) if fields else key) for key in set(settings) - set(allowed))
    if unknown:
        raise ConfigurationError(f"{path}: unknown key(s) {unknown} in {where}")


def parse_source(secrets: dict[str, object], path: Path, environment: Resolver) -> SourceConfig:
    """Read the secrets source using only process environment values."""
    reject_unknown(secrets, SECRETS_KEYS, "Secrets", path)
    written_type = environment.text(secrets.get("Source_type"), "Secrets.Source_type")
    source_type = enum_value(
        SecretsSource,
        written_type or "",
        ConfigurationError(
            f"{path}: Secrets.Source_type must be one of {', '.join(SecretsSource)}, "
            f"got {secrets.get('Source_type')!r}{environment.hint('Secrets.Source_type')}"
        ),
    )
    raw_path = environment.text(secrets.get("Path"), "Secrets.Path")
    if source_type == SecretsSource.ENVIRONMENT:
        if raw_path is not None:
            raise ConfigurationError(f"{path}: Secrets.Path is only used with Source_type: FILE")
        return SourceConfig(type=source_type)
    if raw_path is None:
        raise ConfigurationError(f"{path}: Secrets.Path is required when Source_type is FILE")
    return SourceConfig(type=source_type, path=str(relative_to_config(raw_path, path)))


@dataclass(frozen=True)
class Fields:
    """The fields of one connection profile block, resolved on demand."""

    block: dict[str, object]
    where: str
    profile: str | None
    resolver: Resolver
    path: Path

    def value(self, key: str, *, required: bool = False) -> str | None:
        """Resolve a connection field and validate credential-free URLs."""
        raw = self.block.get(key)
        if raw is None:
            if required:
                raise ConfigurationError(f"{self.path}: {self.where} needs {shown(key)}")
            return None
        value = self.resolver.resolve(
            raw, f"{self.where}.{shown(key)}", profile=self.profile, field_name=key
        )
        text = str(value).strip() if value is not None else None
        if text is not None and key in {"jdbc_url", "listen_jdbc_url"}:
            return connection_url(text, self.path, f"{self.where}.{shown(key)}")
        return text

    def secret_var(self, key: str = "secret") -> str | None:
        """Return the name of a required, populated secret variable when present."""
        return self.resolver.secret_name(
            self.block.get(key), f"{self.where}.{shown(key)}", profile=self.profile, field_name=key
        )

    def hint(self, key: str) -> str:
        """Explain an unresolved variable used as literal text."""
        return self.resolver.hint(f"{self.where}.{shown(key)}")


def auth_extra(
    fields: Fields,
    auth_mode: str,
    required: tuple[str, ...],
    *,
    user: str,
    secret_key: str = "secret",
) -> dict[str, str]:
    """Check an auth mode's fields are present and return the profile's ``extra`` for it."""
    path, where = fields.path, fields.where
    if "user" in required and not user:
        raise ConfigurationError(f"{path}: {where} needs User for Auth_mode {auth_mode}")
    extra: dict[str, str] = {}
    secret_var = fields.secret_var(secret_key)
    if "secret" in required and not secret_var:
        raise ConfigurationError(
            f"{path}: {where} needs {shown(secret_key)} (a variable name) for Auth_mode {auth_mode}"
        )
    if secret_var:
        extra["secret_var"] = secret_var
    for name in AUTH_EXTRA_FIELDS:
        value = fields.value(name)
        if value:
            extra[name] = (
                str(relative_to_config(value, path))
                if name in {"key_file", "cert_file", "private_key_file"}
                else value
            )
        elif name in required:
            raise ConfigurationError(
                f"{path}: {where} needs {shown(name)} for Auth_mode {auth_mode}"
            )
    return extra


def check_auth_mode(auth_mode: str, accepted: frozenset[str], label: str, fields: Fields) -> None:
    """Refuse authentication modes the selected connection does not support."""
    if auth_mode not in accepted:
        raise ConfigurationError(
            f"{fields.path}: {fields.where}.Auth_mode resolved to {auth_mode!r}; {label} "
            f"takes {', '.join(sorted(accepted))}{fields.hint('auth_mode')}"
        )


def connection_block(
    name: str, raw: dict[str, object], global_profile: str | None, path: Path, resolver: Resolver
) -> tuple[Profiled, dict[str, object]] | None:
    """Return a connection section's selection and a copy of its active profile block."""
    section = mapping(raw, name, path, required=name == "Engine")
    if not section:
        return None
    profiled = profiled_settings(name, raw, global_profile, path, resolver)
    if profiled.profile is None or not isinstance(section.get(profiled.profile), dict):
        raise ConfigurationError(
            f"{path}: {name} needs at least one profile block (dev, prod, ...) holding its "
            f"connection settings — see {EXAMPLE_PATH}"
        )
    block = section[profiled.profile]
    assert isinstance(block, dict)
    return profiled, dict(block)


def bounded_number(
    value: object,
    where: str,
    path: Path,
    *,
    minimum: int = 0,
    maximum: int | None = None,
    hint: str = "",
) -> int:
    """Accept whole numbers or ASCII decimal strings inside the setting's bounds."""
    written = value
    if isinstance(value, str) and re.fullmatch(r"[0-9]+", value.strip()):
        value = int(value.strip())
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or (maximum is not None and value > maximum)
    ):
        bounds = f"{minimum} to {maximum}" if maximum is not None else f"at least {minimum}"
        raise ConfigurationError(
            f"{path}: {where} must be a whole number {bounds}, got {written!r}{hint}"
        )
    return value


def whole_number(
    settings: Profiled,
    key: str,
    default: _NumberDefault,
    path: Path,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int | _NumberDefault:
    """Read a bounded integer, preserving the default when absent."""
    value = settings.value(key)
    if value is None:
        return default
    return bounded_number(
        value,
        settings.where(key),
        path,
        minimum=minimum,
        maximum=maximum,
        hint=settings.resolver.hint(settings.where(key)),
    )


def flag(settings: Profiled, key: str, default: _FlagDefault, path: Path) -> bool | _FlagDefault:
    """Read a boolean, preserving the default when absent."""
    value = settings.value(key)
    if value is None:
        return default
    return parse_bool(value, settings.where(key), path, default=bool(default))


def string_list(settings: Profiled, key: str, path: Path) -> list[str] | None:
    """Read a YAML string list or comma-separated text."""
    value = settings.value(key)
    if value is None:
        return None
    if isinstance(value, str):
        # A variable, or a value, holding a comma-separated list.
        return [item.strip() for item in value.split(",") if item.strip()]
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigurationError(f"{path}: Orchestration.{key} must be a list of strings")
    return value


def parse_bool(value: object, name: str, path: Path, *, default: bool) -> bool:
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


def profile_schema(fields: Fields) -> str:
    """Return the profile's required ``schema``: a plain identifier."""
    schema = fields.value("schema", required=True) or ""
    if not is_safe_identifier(schema):
        raise ConfigurationError(
            f"{fields.path}: {fields.where}.Schema must be a plain identifier (letters, digits "
            f"and underscores), got {schema!r}{fields.hint('schema')}"
        )
    return schema
