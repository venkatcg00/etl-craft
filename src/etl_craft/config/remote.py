"""Validate the address of an API reached by CLI and agent clients."""

from urllib.parse import urlsplit

from etl_craft.core.errors import UsageError

LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
"""Hosts a server may be reached at over plain HTTP: this machine only."""


def server_url(value: str) -> str:
    """Return ``scheme://host[:port]``; refuse anything but HTTPS except to this machine."""
    try:
        url = urlsplit(value)
        port = url.port
    except ValueError as error:
        raise UsageError(f"--api {value!r} is malformed; expected https://<host>[:port]") from error
    if (
        url.scheme not in {"https", "http"}
        or not url.hostname
        or url.username
        or url.password
        or url.path not in {"", "/"}
        or url.query
        or url.fragment
    ):
        raise UsageError(f"--api {value!r}; expected https://<host>[:port] with nothing after it")
    if url.scheme == "http" and url.hostname not in LOCAL_HOSTS:
        raise UsageError(
            f"--api {value!r}: plain HTTP is refused except to localhost; use https://"
        )
    host = f"[{url.hostname}]" if ":" in url.hostname else url.hostname
    return f"{url.scheme}://{host}" + (f":{port}" if port else "")
