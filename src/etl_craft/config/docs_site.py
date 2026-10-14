"""Validation of organization-managed catalog publication URLs."""

import ipaddress
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit, urlunsplit

from etl_craft.core.errors import ConfigurationError


def external_url(value: str) -> str:
    """Return an absolute HTTP(S) base URL, with a canonical trailing-slash prefix."""
    remedy = "set Docs_site.External_url to an HTTP(S) URL without credentials, query or fragment"
    try:
        parsed = urlsplit(value)
        port = parsed.port
        valid = (
            parsed.scheme in ("http", "https")
            and bool(parsed.hostname)
            and parsed.username is None
            and parsed.password is None
            and not parsed.query
            and not parsed.fragment
            and (port is None or port > 0)
            and not any(character.isspace() or ord(character) < 32 for character in value)
        )
    except ValueError:
        raise ConfigurationError(
            f"Docs_site.External_url has an invalid authority; {remedy}"
        ) from None
    if not valid:
        raise ConfigurationError(
            f"Docs_site.External_url has an invalid scheme or suffix; {remedy}"
        )
    return urlunsplit((parsed.scheme, parsed.netloc, _prefix(parsed.path), "", ""))


def _prefix(path: str) -> str:
    try:
        decoded = unquote(path, errors="strict")
    except UnicodeDecodeError as error:
        raise ConfigurationError(
            f"Docs_site.External_url path {path!r} is not UTF-8; use a valid URL path"
        ) from error
    parts = decoded.strip("/").split("/") if decoded.strip("/") else []
    if any(part.startswith(".") or not part for part in parts) or any(
        character in "\\%" or ord(character) < 32 or ord(character) == 127 for character in decoded
    ):
        raise ConfigurationError(
            f"Docs_site.External_url path {path!r} is not a public catalog prefix; "
            "use plain path segments without hidden, parent or repeated-encoding components"
        )
    return quote("/" + "/".join(parts) + ("/" if parts else ""), safe="/-._~")


def cidr_ranges(value: object, where: str, path: Path) -> tuple[str, ...]:
    """Read a list of CIDR ranges, as a YAML list or a comma-separated string."""
    if value is None or value == "":
        return ()
    items = value if isinstance(value, list) else str(value).split(",")
    ranges = []
    for item in items:
        text = str(item).strip()
        try:
            ranges.append(str(ipaddress.ip_network(text, strict=False)))
        except ValueError as error:
            raise ConfigurationError(
                f"{path}: {where} must list CIDR ranges such as 203.0.113.0/24; {text!r} is not "
                f"one ({error})"
            ) from error
    return tuple(ranges)
