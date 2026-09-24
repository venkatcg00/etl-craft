"""Resolving settings: every value is a variable name or a value.

A setting whose text is a valid variable name, and that the secrets source defines, takes that
variable's value; anything else is used exactly as written. So ``Profile: dev`` is the profile
``dev`` unless a variable named ``dev`` is set, and ``Profile: ETL_CRAFT_PROFILE`` is whatever
that variable holds. For a profile's own fields, a profile-specific variable wins over the plain
name: ``ENGINE_PROD_SECRET`` over ``ENGINE_SECRET`` for the ``prod`` profile.

Secrets are the exception. A secret field must name a variable, and that variable must be set:
the file is meant to be committed, and falling back to the text would send a mistyped
variable name to the server as a password.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from etl_craft.config.auth import SECRET_AUTH_MODES
from etl_craft.config.model import (
    ConnectionProfile,
    ConnectorConfig,
    EmailProfile,
    SettingSource,
    SourceConfig,
)
from etl_craft.core.errors import ConfigurationError
from etl_craft.core.text import is_env_name, parse_env_file


def read_secrets_file(path: str | None) -> dict[str, str]:
    """Read the ``.env`` file ``Secrets.Path`` names; ``ConfigurationError`` if it is unreadable."""
    if not path:
        raise ConfigurationError("Secrets.Path is required when Source_type is file")
    try:
        contents = Path(path).read_text(encoding="utf-8")
    except OSError as error:
        raise ConfigurationError(f"could not read secrets file {path!r}: {error}") from error
    return parse_env_file(contents)


def source_values(source: SourceConfig) -> Mapping[str, str]:
    """Return the variables the secrets source defines."""
    if source.type == "file":
        return read_secrets_file(source.path)
    return os.environ


def profile_variable_name(var_name: str, profile: str, field_name: str) -> str | None:
    """Insert the profile before a field's suffix: ``ENGINE_SECRET`` becomes ``ENGINE_DEV_SECRET``.

    Returns ``None`` when ``var_name`` does not end with the field's name, and ``var_name``
    itself when it already carries the profile. ``from_address`` also matches ``_FROM``.
    """
    suffixes = [field_name.upper()]
    if field_name == "from_address":
        suffixes.append("FROM")
    upper_name = var_name.upper()
    upper_profile = profile.upper()
    for suffix in suffixes:
        plain_suffix = f"_{suffix}"
        if upper_name.endswith(f"_{upper_profile}{plain_suffix}"):
            return var_name
        if upper_name.endswith(plain_suffix):
            cut = len(var_name) - len(plain_suffix)
            return f"{var_name[:cut]}_{upper_profile}{var_name[cut:]}"
    return None


@dataclass
class Resolver:
    """Resolves settings against one set of variables and records where each value came from.

    ``origin`` names the source in error messages; ``path`` is the config file.
    """

    values: Mapping[str, str]
    origin: str
    path: Path
    sources: list[SettingSource] = field(default_factory=list)

    @classmethod
    def for_source(cls, source: SourceConfig, path: Path) -> Resolver:
        """Build the resolver for everything after ``Secrets``."""
        if source.type == "file":
            origin = f"{source.path} (Secrets.Source_type: file)"
        else:
            origin = "the process environment (Secrets.Source_type: environment)"
        return cls(values=source_values(source), origin=origin, path=path)

    def selected_name(self, var_name: str, profile: str | None, field_name: str | None) -> str:
        """Return the profile-specific variable when it is set, else ``var_name``."""
        if profile and field_name:
            specific = profile_variable_name(var_name, profile, field_name)
            if specific and specific in self.values:
                return specific
        return var_name

    def resolve(
        self, raw: Any, where: str, *, profile: str | None = None, field_name: str | None = None
    ) -> Any:
        """Return ``raw`` resolved: a set variable's value, else ``raw`` itself.

        Lists resolve item by item; values YAML already typed (numbers, booleans) are returned
        as they are.
        """
        if isinstance(raw, list):
            return [
                self.resolve(item, f"{where}[{index}]", profile=profile, field_name=field_name)
                for index, item in enumerate(raw)
            ]
        if not isinstance(raw, str):
            return raw
        written = raw.strip()
        if is_env_name(written):
            name = self.selected_name(written, profile, field_name)
            if name in self.values:
                self.sources.append(SettingSource(where, written, name))
                return self.values[name]
        self.sources.append(SettingSource(where, written))
        return written

    def text(
        self, raw: Any, where: str, *, profile: str | None = None, field_name: str | None = None
    ) -> str | None:
        """Resolve a setting that must be a non-empty string; ``None`` when it is absent."""
        if raw is None:
            return None
        value = self.resolve(raw, where, profile=profile, field_name=field_name)
        if not isinstance(value, str) or not value.strip():
            raise ConfigurationError(f"{self.path}: {where} must be a non-empty string")
        return value.strip()

    def secret_name(
        self, raw: Any, where: str, *, profile: str | None, field_name: str
    ) -> str | None:
        """Return the variable a secret field names, or ``None`` when the field is absent.

        Raises ``ConfigurationError`` when the field is not a variable name, or names one that
        is not set. Only the selected profile is parsed, so another environment's secrets need
        not be present.
        """
        if raw is None:
            return None
        if not isinstance(raw, str) or not is_env_name(raw.strip()):
            raise ConfigurationError(
                f"{self.path}: {where} must be the name of a variable holding the secret — "
                "a secret is never written into craft-connector.yml"
            )
        written = raw.strip()
        name = self.selected_name(written, profile, field_name)
        if name not in self.values:
            specific = profile_variable_name(written, profile, field_name) if profile else None
            names = f"{name!r}"
            if specific and specific != name:
                names = f"{specific!r} or {name!r}"
            raise ConfigurationError(
                f"{self.path}: {where} names the secret variable {names}, which is not set "
                f"in {self.origin}"
            )
        self.sources.append(SettingSource(where, written, name))
        return name

    def hint(self, where: str) -> str:
        """Explain a bad value that was used as written because no variable by its name is set."""
        for source in reversed(self.sources):
            if source.where == where:
                if source.looks_like_a_missing_variable:
                    return (
                        f" — no variable named {source.written} is set in {self.origin}, so "
                        "the text was used as written"
                    )
                return ""
        return ""


def resolve_secret(config: ConnectorConfig, profile: ConnectionProfile | EmailProfile) -> str:
    """Read ``profile``'s secret from the secrets source, as it is now.

    Raises ``ConfigurationError`` if the variable is no longer set.
    """
    var_name = profile.secret_var
    value = source_values(config.source).get(var_name)
    if value is None:
        raise ConfigurationError(
            f"secret {var_name!r} not found (Secrets.Source_type: {config.source.type})"
        )
    return value


def profile_needs_secret(profile: ConnectionProfile | EmailProfile) -> bool:
    """Whether connecting with ``profile`` reads a secret."""
    return profile.auth_mode in SECRET_AUTH_MODES or bool(profile.extra.get("secret_var"))


def profile_secret(config: ConnectorConfig, profile: ConnectionProfile | EmailProfile) -> str:
    """Return ``profile``'s secret, or ``""`` when its auth mode presents none.

    ``none``, ``sts`` and ``sso`` without a client secret have nothing to present; a key file's
    passphrase is read when the profile names one.
    """
    if not profile_needs_secret(profile):
        return ""
    return resolve_secret(config, profile)
