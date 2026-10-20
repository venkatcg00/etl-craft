"""``publish-docs``: serve the catalog locally, through ngrok or an organization proxy.

The site ``generate-docs`` writes is served from its folder by a small web server on
``127.0.0.1``. Each request reads the folder as it is then, so a site written again while it is
served (``generate-docs`` swaps a new one in whole) is served from then on.

Published through ngrok, the site is reachable only by its link: there is no login. Every
response tells search engines and caches to keep out (``X-Robots-Tag: noindex``, a
``robots.txt`` that disallows everything, ``Referrer-Policy: no-referrer``), no page may be
framed, and folders are never listed. ``Docs_site.Domain`` keeps the link the same from one
publish to the next.

``Docs_site.Allowed_ips`` limits visitors. Organization hosting uses the configured public
base URL and trusts forwarding headers only from ``Trusted_proxies``. Ngrok uses its managed
loopback hop. Trusted chains are read from the right; missing or malformed visitor evidence is
refused when IP restrictions apply. Direct, untrusted peers are checked by their socket address.

The URL is recorded in the Engine DB. When a later publish gets another one, links already
shared would break, so it fails naming both, unless told to accept the new one.
"""

from __future__ import annotations

import importlib
import logging
import threading
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from io import BytesIO
from ipaddress import IPv4Network, IPv6Network, ip_address, ip_network
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import unquote, urlsplit

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig, DocsSiteConfig
from etl_craft.config.resolve import source_values
from etl_craft.core.errors import ConfigurationError, LockTimeoutError, UsageError
from etl_craft.core.http import forwarded_visitor
from etl_craft.engine.repository.docs_site import fetch_publication, record_publication
from etl_craft.services.catalog.storage import MARKER, prepare_site_lock, site_lock

logger = logging.getLogger(__name__)

ROBOTS = "User-agent: *\nDisallow: /\n"
"""Asks every crawler to keep out of the whole site."""

HEADERS = {
    "X-Robots-Tag": "noindex, nofollow, noarchive",
    "Referrer-Policy": "no-referrer",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Cache-Control": "no-cache",
}
"""Sent with every response."""

EXTRA = "publish"
"""The optional extra that installs the ngrok SDK."""


class _Server(ThreadingHTTPServer):
    """The web server, with who may read the site and whether requests come through ngrok."""

    allowed: tuple[IPv4Network | IPv6Network, ...] = ()
    trusted_proxies: tuple[IPv4Network | IPv6Network, ...] = ()
    path_prefix: str = ""


class _Handler(SimpleHTTPRequestHandler):
    """Serves the site's files, never a folder listing or a hidden file, to allowed visitors."""

    server: _Server

    def _visitor_allowed(self) -> bool:
        allowed = self.server.allowed
        if not allowed:
            return True
        try:
            visitor = ip_address(
                forwarded_visitor(
                    self.client_address[0],
                    self.headers.get_all("X-Forwarded-For", []),
                    self.server.trusted_proxies,
                    required=True,
                ).address
            )
        except (ValueError, UsageError):
            return False
        return any(visitor in network for network in allowed)

    def _safe_path(self, raw: str) -> Path | None:
        """Decode a request path once and keep it inside the site, with no hidden segments."""
        try:
            decoded = unquote(raw.split("?", 1)[0].split("#", 1)[0], errors="strict")
        except UnicodeDecodeError:
            return None
        prefix = self.server.path_prefix
        if decoded == prefix:
            decoded = "/"
        elif decoded.startswith(prefix + "/"):
            decoded = decoded[len(prefix) :]
        else:
            return None
        parts = decoded.replace("\\", "/").split("/")
        if any(part.startswith(".") for part in parts if part) or "\x00" in decoded:
            return None
        root = Path(self.directory).resolve()
        try:
            path = root.joinpath(*(part for part in parts if part)).resolve()
            path.relative_to(root)
            if path.is_dir():
                for index in ("index.html", "index.htm"):
                    candidate = path / index
                    if candidate.is_file():
                        candidate.resolve().relative_to(root)
                        break
        except (ValueError, OSError, RuntimeError):
            return None
        return path

    def translate_path(self, path: str) -> str:
        safe = self._safe_path(path)
        if safe is None:
            raise FileNotFoundError("request path is outside the site's public files")
        return str(safe)

    def send_head(self) -> BinaryIO | None:
        try:
            with site_lock(Path(self.directory), shared=True):
                return self._open_response()
        except LockTimeoutError as error:
            logger.error("%s", error)
            self.send_error(HTTPStatus.SERVICE_UNAVAILABLE, "catalog publication busy; retry")
            return None

    def _open_response(self) -> BinaryIO | None:
        if not self._visitor_allowed():
            self.send_error(HTTPStatus.FORBIDDEN)
            return None
        if self._safe_path(self.path) is None:
            self.send_error(HTTPStatus.NOT_FOUND)
            return None
        if unquote(self.path.split("?", 1)[0]) == self.server.path_prefix + "/robots.txt":
            body = ROBOTS.encode()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return BytesIO(body)
        return super().send_head()

    def end_headers(self) -> None:
        for name, value in HEADERS.items():
            self.send_header(name, value)
        super().end_headers()

    def list_directory(self, path: str | Any) -> None:
        self.send_error(HTTPStatus.NOT_FOUND)

    def log_message(self, format: str, *args: Any) -> None:
        logger.debug("%s %s", self.address_string(), format % args)


@dataclass(frozen=True)
class Published:
    """Where the site is served, and the local address the web server listens on."""

    url: str
    local_address: str


def serve(
    folder: Path,
    host: str = "127.0.0.1",
    port: int = 0,
    *,
    allowed: tuple[str, ...] = (),
    behind_tunnel: bool = False,
    site: DocsSiteConfig | None = None,
) -> ThreadingHTTPServer:
    """Return a web server for the site in ``folder``, not yet serving.

    ``allowed`` are the CIDR ranges visitors may come from, every address when empty.
    ``site`` supplies the organization URL prefix and trusted proxy networks.
    ``ConfigurationError`` when ``folder`` holds no site ``generate-docs`` wrote.
    """
    folder = folder.absolute()
    if not folder.name:
        raise ConfigurationError(
            f"catalog folder {folder} is the filesystem root; choose a generated catalog directory"
        )
    prepare_site_lock(folder)
    with site_lock(folder, shared=True):
        if not (folder / MARKER).is_file():
            raise ConfigurationError(
                f"there is no catalog site in {folder}: run `etl-craft generate-docs` first"
            )
    server = _Server((host, port), partial(_Handler, directory=str(folder)))
    try:
        server.allowed = tuple(ip_network(cidr, strict=False) for cidr in allowed)
        proxies = (
            ("127.0.0.1/32", "::1/128") if behind_tunnel else site.trusted_proxies if site else ()
        )
        server.trusted_proxies = tuple(ip_network(cidr, strict=False) for cidr in proxies)
    except ValueError as error:
        server.server_close()
        raise ConfigurationError(
            "catalog Allowed_ips or Trusted_proxies contains an invalid network; "
            "use IP addresses or CIDR ranges"
        ) from error
    path = urlsplit(site.external_url).path if site and site.external_url else ""
    server.path_prefix = unquote(path).rstrip("/")
    return server


@contextmanager
def published(
    engine: Engine,
    config: ConnectorConfig,
    folder: Path,
    *,
    local_only: bool = False,
    host: str = "127.0.0.1",
    port: int = 0,
    accept_new_url: bool = False,
) -> Iterator[Published]:
    """Serve the site for the ``with`` body, locally or through ngrok, and yield where.

    Organization and ngrok URLs use the same recorded URL-change acceptance rule.
    """
    site = config.docs_site
    organization = site.external_url if not local_only else None
    tunnel = not local_only and organization is None
    server = serve(
        folder,
        "127.0.0.1" if tunnel else host,
        port,
        allowed=site.allowed_ips,
        behind_tunnel=tunnel,
        site=site if organization else None,
    )
    local = f"{server.server_address[0]!s}:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, name="catalog-site", daemon=True)
    thread.start()
    close: Callable[[], None] | None = None
    try:
        if local_only:
            url = f"http://{local}/"
        elif organization:
            url = organization
            _check_url(engine, url, accept_new_url=accept_new_url)
        else:
            url, close = open_tunnel(local, config)
            _check_url(engine, url, accept_new_url=accept_new_url)
        logger.info("serving %s at %s", folder, url)
        yield Published(url, local)
    finally:
        try:
            if close is not None:
                close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


def open_tunnel(address: str, config: ConnectorConfig) -> tuple[str, Callable[[], None]]:
    """Open an ngrok tunnel to ``address``; return its URL and how to close it."""
    ngrok = _ngrok()
    site = config.docs_site
    token = authtoken(config)
    options: dict[str, Any] = {"authtoken": token}
    if site.domain:
        options["domain"] = site.domain
    try:
        listener = ngrok.forward(address, **options)
    except Exception as error:
        # ngrok's messages can quote the authtoken back; it never reaches a log or terminal.
        message = str(error).replace(token, "[the authtoken]")
        raise ConfigurationError(f"ngrok could not open the tunnel: {message}") from None
    url = str(listener.url())
    return url, lambda: ngrok.disconnect(url)


def authtoken(config: ConnectorConfig) -> str:
    """Return the ngrok authtoken from the variable ``Docs_site.Authtoken`` names."""
    name = config.docs_site.authtoken_var
    if name is None:
        raise ConfigurationError(
            "publishing through ngrok needs Docs_site.Authtoken: the name of the variable "
            "holding your ngrok authtoken (or publish on this machine with `local_only`)"
        )
    value = source_values(config.source).get(name)
    if not value:
        raise ConfigurationError(
            f"Docs_site.Authtoken names the variable {name!r}, which is not set in the "
            f"{config.source.type} secrets source"
        )
    return value


def _ngrok() -> Any:
    try:
        return importlib.import_module("ngrok")
    except ImportError as error:
        raise ConfigurationError(
            f"publishing through ngrok needs the ngrok SDK: pip install 'etl-craft[{EXTRA}]'"
        ) from error


def _check_url(engine: Engine, url: str, *, accept_new_url: bool) -> None:
    with engine.begin() as conn:
        before = fetch_publication(conn)
        if before is not None and before.url != url and not accept_new_url:
            raise ConfigurationError(
                f"the catalog was published at {before.url}, and publishing now gives {url}; links "
                "already shared would break. Keep Docs_site.External_url or Domain stable, or "
                "publish with `accept_new_url` to use the new one from now on"
            )
        record_publication(conn, url)
