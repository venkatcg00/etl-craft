"""Send a command to an etl-craft controller's API, as the signed-in person or a service token."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from http.client import HTTPResponse
from typing import TypeVar
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from etl_craft.core.errors import EtlCraftError, ServerError, UsageError
from etl_craft.services import credentials
from etl_craft.services.operations import from_json
from etl_craft.services.operations.sessions import Session

T = TypeVar("T")

LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})
"""Hosts a server may be reached at over plain HTTP: this machine only."""


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(
        self, req: Request, fp: object, code: int, msg: str, headers: object, newurl: str
    ) -> None:
        return None


def server_url(value: str) -> str:
    """Return ``scheme://host[:port]``; refuse anything but HTTPS except to this machine."""
    try:
        url = urlsplit(value)
        port = url.port
    except ValueError as error:
        raise UsageError(
            f"--server {value!r} is malformed; expected https://<host>[:port]"
        ) from error
    if (
        url.scheme not in {"https", "http"}
        or not url.hostname
        or url.username
        or url.password
        or url.path not in {"", "/"}
        or url.query
        or url.fragment
    ):
        raise UsageError(
            f"--server {value!r}; expected https://<host>[:port] with nothing after it"
        )
    if url.scheme == "http" and url.hostname not in LOCAL_HOSTS:
        raise UsageError(
            f"--server {value!r}: plain HTTP is refused except to localhost; use https://"
        )
    host = f"[{url.hostname}]" if ":" in url.hostname else url.hostname
    return f"{url.scheme}://{host}" + (f":{port}" if port else "")


def _error(status: int, body: bytes, url: str) -> EtlCraftError:
    """Rebuild the server's error class from its exit code, so local and remote exits match."""
    try:
        document = json.loads(body)
    except ValueError:
        document = None
    if isinstance(document, dict) and isinstance(document.get("message"), str):
        code = document.get("exit_code")
        classes = {int(cls.exit_code): cls for cls in _subclasses(EtlCraftError)}
        cls = classes.get(code, ServerError) if isinstance(code, int) else ServerError
        return cls(document["message"])
    if isinstance(document, dict) and "detail" in document:
        return UsageError(f"{url} refused the request (HTTP {status}): {document['detail']}")
    return ServerError(f"{url} answered HTTP {status} without an etl-craft error; check the URL")


def _subclasses(cls: type[EtlCraftError]) -> Iterator[type[EtlCraftError]]:
    for sub in cls.__subclasses__():
        yield sub
        yield from _subclasses(sub)


class ServerClient:
    """One server, and the credential that proves who calls it."""

    def __init__(self, url: str) -> None:
        """Use ``ETL_CRAFT_TOKEN`` for a service account, else this person's stored session."""
        self.url = server_url(url)
        self._service_token = os.environ.get("ETL_CRAFT_TOKEN")

    def call(
        self,
        method: str,
        path: str,
        result: type[T],
        *,
        query: Mapping[str, object] | None = None,
        body: Mapping[str, object] | None = None,
        signed_in: bool = True,
    ) -> T:
        """Send one request and rebuild its JSON document as ``result``."""
        with self._open(method, path, query, body, signed_in=signed_in) as response:
            payload = response.read(16 * 1024 * 1024 + 1)
        if len(payload) > 16 * 1024 * 1024:
            raise ServerError(f"{self.url}{path}: response exceeds 16 MiB")
        try:
            document = json.loads(payload)
        except ValueError as error:
            raise ServerError(f"{self.url}{path}: response is not JSON; check the URL") from error
        return from_json(result, document)

    def send(self, method: str, path: str, body: Mapping[str, object]) -> None:
        """Send one request that answers with no content."""
        with self._open(method, path, None, body, signed_in=False):
            pass

    def stream(self, path: str, query: Mapping[str, object]) -> Iterator[bytes]:
        """Yield a text response in chunks, such as an attempt's log."""
        with self._open("GET", path, query, None, signed_in=True) as response:
            while chunk := response.read(65536):
                yield chunk

    def _open(
        self,
        method: str,
        path: str,
        query: Mapping[str, object] | None,
        body: Mapping[str, object] | None,
        *,
        signed_in: bool,
    ) -> HTTPResponse:
        token = self._token() if signed_in else None
        try:
            return self._send(method, path, query, body, token)
        except HTTPError as error:
            if error.code == 401 and signed_in and self._service_token is None:
                return self._send(method, path, query, body, self._token(renew=True))
            raise _error(error.code, error.read(65536), self.url) from error

    def _send(
        self,
        method: str,
        path: str,
        query: Mapping[str, object] | None,
        body: Mapping[str, object] | None,
        token: str | None,
    ) -> HTTPResponse:
        present = {k: v for k, v in (query or {}).items() if v is not None}
        url = self.url + path + (f"?{urlencode(present)}" if present else "")
        headers = {"Accept": "application/json"}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps({k: v for k, v in body.items() if v is not None}).encode()
        request = Request(url, data=data, headers=headers, method=method)
        try:
            response: HTTPResponse = build_opener(_NoRedirects()).open(request, timeout=60)
        except HTTPError:
            raise  # an answer from the server; _open turns it into the server's own error
        except (URLError, OSError) as error:
            raise ServerError(
                f"{self.url}: cannot reach the server ({error}); check the URL and the network"
            ) from error
        return response

    def _token(self, *, renew: bool = False) -> str:
        if self._service_token is not None:
            return self._service_token
        stored = credentials.read_server(self.url)
        if stored is None:
            raise UsageError(
                f"not signed in to {self.url}; run `etl-craft login --server {self.url}`"
            )
        expires = datetime.fromisoformat(stored["access_expires_at"])
        if not renew and expires > datetime.now(UTC):
            return stored["access"]
        session = self.call(
            "POST",
            "/api/v1/login/refresh",
            Session,
            body={"token": stored["refresh"]},
            signed_in=False,
        )
        self.store(session, refresh=stored["refresh"])
        return session.access_token

    def store(self, session: Session, *, refresh: str | None = None) -> None:
        """Keep this server's access and refresh tokens in the person's credential file."""
        refresh_token = session.refresh_token or refresh
        if refresh_token is None or session.metadata.expires_at is None:
            raise ServerError(f"{self.url}: sign-in answered without a refresh token or expiry")
        credentials.store_session(
            self.url,
            {
                "access": session.access_token,
                "refresh": refresh_token,
                "access_expires_at": session.metadata.expires_at.isoformat(),
            },
        )

    def forget(self) -> str | None:
        """Remove this server's stored session; return its refresh token to revoke."""
        stored = credentials.read_server(self.url)
        credentials.store_session(self.url, None)
        return None if stored is None else stored["refresh"]


def api_path(*parts: str) -> str:
    """Join an API path, escaping codes so a slash in one cannot reach another route."""
    return "/api/v1/" + "/".join(quote(part, safe="") for part in parts)


def selector_query(args: argparse.Namespace) -> dict[str, object]:
    """Return the run selector a command was given, as query or body fields."""
    return {"run_id": args.run_id, "run_key": args.run_key}
