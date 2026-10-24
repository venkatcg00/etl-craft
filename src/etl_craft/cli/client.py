"""Send an action to an etl-craft deployment's API, as the signed-in person or a service token."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterator, Mapping
from datetime import UTC, datetime
from http.client import HTTPResponse
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request

from etl_craft.config.remote import server_url as server_url
from etl_craft.core.api_errors import response_error
from etl_craft.core.enums import ActionName, HttpMethod
from etl_craft.core.errors import ServerError, UsageError
from etl_craft.core.http_client import open_request
from etl_craft.services import credentials
from etl_craft.services.operations.actions import ACTIONS, action_path
from etl_craft.services.operations.serialization import decode, encode
from etl_craft.services.operations.sessions import Session, TokenInput


class ApiClient:
    """One API endpoint, and the credential that proves who calls it."""

    def __init__(self, url: str, project: str = "default") -> None:
        """Use ``ETL_CRAFT_TOKEN`` for a service account, else this person's stored session."""
        self.url = server_url(url)
        self.project = project
        self._service_token = os.environ.get("ETL_CRAFT_TOKEN")

    def invoke(self, name: ActionName, request: object) -> object:
        """Send one action's request to its route and rebuild the result the action returns."""
        action = ACTIONS[name]
        if action.method is None or action.path is None:
            raise UsageError(f"`{name}` runs on the controller's host only: {action.remedy}")
        fields = encode(request)
        assert isinstance(fields, dict)
        route = action_path(action)
        path = route.format(
            project=quote(self.project, safe=""),
            **{
                key: quote(str(fields.pop(key)), safe="")
                for key in _path_fields(route)
                if key != "project"
            },
        )
        query, body = (fields, None) if action.method == HttpMethod.GET else (None, fields)
        signed_in = action.role is not None
        if action.streams:
            return self._stream(path, query or {})
        with self._open(action.method, path, query, body, signed_in=signed_in) as response:
            payload = response.read(16 * 1024 * 1024 + 1)
        if action.result is None:
            return None
        if len(payload) > 16 * 1024 * 1024:
            raise ServerError(f"{self.url}{path}: response exceeds 16 MiB")
        try:
            document = json.loads(payload)
        except ValueError as error:
            raise ServerError(f"{self.url}{path}: response is not JSON; check the URL") from error
        return decode(action.result, document)

    def _stream(self, path: str, query: Mapping[str, object]) -> Iterator[bytes]:
        """Yield a text response in chunks, such as an attempt's log."""
        with self._open(HttpMethod.GET, path, query, None, signed_in=True) as response:
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
            raise response_error(error.code, error.read(65536), self.url) from error

    def _send(
        self,
        method: str,
        path: str,
        query: Mapping[str, object] | None,
        body: Mapping[str, object] | None,
        token: str | None,
    ) -> HTTPResponse:
        present = {k: _query_value(v) for k, v in (query or {}).items() if v is not None}
        url = self.url + path + (f"?{urlencode(present, doseq=True)}" if present else "")
        headers = {"Accept": "application/json"}
        if token is not None:
            headers["Authorization"] = f"Bearer {token}"
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps({k: v for k, v in body.items() if v is not None}).encode()
        request = Request(url, data=data, headers=headers, method=method)
        try:
            response: HTTPResponse = open_request(request, timeout=60)
        except HTTPError:
            raise  # an answer from the server; _open turns it into the server's own error
        except (URLError, OSError) as error:
            raise ServerError(
                f"{self.url}: cannot reach the API ({error}); check the URL and the network"
            ) from error
        return response

    def _token(self, *, renew: bool = False) -> str:
        if self._service_token is not None:
            return self._service_token
        stored = credentials.read_server(self.url)
        if stored is None:
            raise UsageError(f"not signed in to {self.url}; run `etl-craft login --api {self.url}`")
        expires = datetime.fromisoformat(stored["access_expires_at"])
        if not renew and expires > datetime.now(UTC):
            return stored["access"]
        session = self.invoke(ActionName.REFRESH, TokenInput(token=stored["refresh"]))
        assert isinstance(session, Session)
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


def _path_fields(path: str) -> list[str]:
    """Return the request fields a route carries in its path, such as ``run_id``."""
    return re.findall(r"{([a-z_]+)}", path)


def _query_value(value: object) -> object:
    """Write booleans as the API reads them; other values are already JSON text or numbers."""
    return str(value).lower() if isinstance(value, bool) else value
