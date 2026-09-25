"""``publish-docs``: serve the catalog site, on this machine or at a link through ngrok.

The site ``generate-docs`` writes is served from its folder by a small web server on
``127.0.0.1``. Each request reads the folder as it is then, so a site written again while it is
served (``generate-docs`` swaps a new one in whole) is served from then on.

Published through ngrok, the site is reachable only by its link: there is no login. Every
response tells search engines and caches to keep out (``X-Robots-Tag: noindex``, a
``robots.txt`` that disallows everything, ``Referrer-Policy: no-referrer``), no page may be
framed, and folders are never listed. ``Docs_site.Domain`` keeps the link the same from one
publish to the next.

``Docs_site.Allowed_ips`` is enforced by this server, whatever the ngrok plan: a visitor from
any other address gets 403. Through ngrok the visitor's address is the last entry of
``X-Forwarded-For``, which ngrok adds; only ngrok reaches the server on ``127.0.0.1``, and a
request without that header is refused. Served locally, it is the connecting address.

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
from ipaddress import IPv4Network, IPv6Network, ip_address, ip_network
from pathlib import Path
from typing import Any

from sqlalchemy.engine import Engine

from etl_craft.config import ConnectorConfig
from etl_craft.config.resolve import source_values
from etl_craft.core.errors import ConfigurationError
from etl_craft.engine.repository.docs_site import fetch_publication, record_publication
from etl_craft.services.catalog_site import MARKER

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
    behind_tunnel: bool = False


class _Handler(SimpleHTTPRequestHandler):
    """Serves the site's files, never a folder listing or a hidden file, to allowed visitors."""

    server: _Server

    def _visitor_allowed(self) -> bool:
        allowed = self.server.allowed
        if not allowed:
            return True
        if self.server.behind_tunnel:
            forwarded = [p.strip() for p in self.headers.get("X-Forwarded-For", "").split(",")]
            address = forwarded[-1] if forwarded and forwarded[-1] else ""
        else:
            address = str(self.client_address[0])
        try:
            visitor = ip_address(address)
        except ValueError:
            return False
        return any(visitor in network for network in allowed)

    def do_HEAD(self) -> None:
        if not self._visitor_allowed():
            self.send_error(HTTPStatus.FORBIDDEN)
            return
        super().do_HEAD()

    def end_headers(self) -> None:
        for name, value in HEADERS.items():
            self.send_header(name, value)
        super().end_headers()

    def do_GET(self) -> None:
        if not self._visitor_allowed():
            self.send_error(HTTPStatus.FORBIDDEN)
            return
        if self.path.split("?", 1)[0] == "/robots.txt":
            body = ROBOTS.encode()
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if any(part.startswith(".") for part in self.path.split("?", 1)[0].split("/") if part):
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        super().do_GET()

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
) -> ThreadingHTTPServer:
    """Return a web server for the site in ``folder``, not yet serving.

    ``allowed`` are the CIDR ranges visitors may come from, every address when empty.
    ``ConfigurationError`` when ``folder`` holds no site ``generate-docs`` wrote.
    """
    if not (folder / MARKER).is_file():
        raise ConfigurationError(
            f"there is no catalog site in {folder}: run `etl-craft generate-docs` first"
        )
    server = _Server((host, port), partial(_Handler, directory=str(folder)))
    server.allowed = tuple(ip_network(cidr, strict=False) for cidr in allowed)
    server.behind_tunnel = behind_tunnel
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

    Through ngrok the URL is checked against the one recorded before and recorded.
    """
    server = serve(
        folder,
        host if local_only else "127.0.0.1",
        port,
        allowed=config.docs_site.allowed_ips,
        behind_tunnel=not local_only,
    )
    local = f"{server.server_address[0]!s}:{server.server_port}"
    thread = threading.Thread(target=server.serve_forever, name="catalog-site", daemon=True)
    thread.start()
    close: Callable[[], None] | None = None
    try:
        if local_only:
            url = f"http://{local}/"
        else:
            url, close = open_tunnel(local, config)
            _check_url(engine, url, accept_new_url=accept_new_url)
        logger.info("serving %s at %s", folder, url)
        yield Published(url, local)
    finally:
        if close is not None:
            close()
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
            "holding your ngrok authtoken (or publish on this machine with --local-only)"
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
                f"the catalog was published at {before.url}, and ngrok now gives {url}; links "
                "already shared would break. Set Docs_site.Domain to keep the first link, or "
                "publish with --accept-new-url to use the new one from now on"
            )
        record_publication(conn, url)
