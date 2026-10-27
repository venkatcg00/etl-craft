"""OIDC device authorization using the issuer's authenticated TLS token endpoint."""

import base64
import json
import math
import secrets
import time
from dataclasses import dataclass
from threading import Lock
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from etl_craft.config.model import AuthConfig
from etl_craft.core.errors import ConfigurationError

CLOCK_LEEWAY_SECONDS = 120
DISCOVERY_SECONDS = 600
_DISCOVERY: dict[str, tuple[float, dict[str, object]]] = {}
_DISCOVERY_LOCK = Lock()


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(
        self, req: Request, fp: object, code: int, msg: str, headers: object, newurl: str
    ) -> None:
        return None


def _endpoint(value: object) -> str:
    if isinstance(value, str):
        try:
            parsed = urlsplit(value)
        except ValueError as error:
            raise ConfigurationError(
                "OIDC endpoint is malformed; "
                "expected a credential-free HTTPS URL; check issuer metadata"
            ) from error
        if (
            parsed.scheme == "https"
            and parsed.hostname
            and not parsed.username
            and not parsed.password
            and not parsed.fragment
        ):
            return value
    raise ConfigurationError(
        "OIDC endpoint: expected an absolute HTTPS URL; check the issuer metadata"
    )


def _request(url: str, data: dict[str, str] | None = None) -> dict[str, object]:
    url = _endpoint(url)
    location = url.partition("?")[0]
    request = Request(
        url,
        data=None if data is None else urlencode(data).encode(),
        headers={"Accept": "application/json"},
    )
    try:
        try:
            response = build_opener(_NoRedirects()).open(request, timeout=15)
        except HTTPError as error:
            if data is None or error.code != 400:
                raise ConfigurationError(
                    f"OIDC endpoint {location}: HTTP {error.code}; check provider configuration"
                ) from error
            response = error
        with response:
            body = response.read(1_048_577)
        if len(body) > 1_048_576:
            raise ConfigurationError(
                f"OIDC endpoint {location}: response exceeds 1 MiB; check the provider"
            )
        value = json.loads(body)
    except (URLError, OSError, ValueError) as error:
        raise ConfigurationError(
            f"OIDC endpoint {location}: request failed; check connectivity and TLS"
        ) from error
    if not isinstance(value, dict):
        raise ConfigurationError(
            f"OIDC endpoint {location}: expected a JSON object; check the provider"
        )
    return value


def _text(data: dict[str, object], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value or not value.isprintable() or len(value) > 65536:
        raise ConfigurationError(
            f"OIDC response {key}: expected non-empty text; check the provider"
        )
    return value


def _seconds(data: dict[str, object], key: str, default: int = 0) -> int:
    value = data.get(key, default)
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 86400:
        raise ConfigurationError(f"OIDC response {key}: expected seconds from 1 to 86400")
    return value


@dataclass(frozen=True)
class DeviceGrant:
    """What the person needs to approve the sign-in, and what polling it needs back."""

    user_code: str
    verification_uri: str
    device_code: str
    nonce: str
    interval: int
    expires_in: int


def _client(auth: AuthConfig) -> tuple[str, str, dict[str, object]]:
    if not auth.oidc_issuer or not auth.oidc_client_id:
        raise ConfigurationError(
            "SSO requires Auth.Oidc_issuer and Auth.Oidc_client_id; configure both"
        )
    metadata = _discovery(auth.oidc_issuer)
    return auth.oidc_issuer, auth.oidc_client_id, metadata


def _discovery(issuer: str) -> dict[str, object]:
    with _DISCOVERY_LOCK:
        cached = _DISCOVERY.get(issuer)
        if cached is not None and time.monotonic() < cached[0]:
            return cached[1]
    metadata = _request(issuer.rstrip("/") + "/.well-known/openid-configuration")
    if metadata.get("issuer") != issuer:
        raise ConfigurationError(
            "OIDC discovery issuer differs from Auth.Oidc_issuer; correct the setting"
        )
    for key in ("token_endpoint", "device_authorization_endpoint"):
        _endpoint(_text(metadata, key))
    with _DISCOVERY_LOCK:
        _DISCOVERY[issuer] = (time.monotonic() + DISCOVERY_SECONDS, metadata)
    return metadata


def start_device(auth: AuthConfig) -> DeviceGrant:
    """Ask the issuer for a user code with a public registered client."""
    _, client, metadata = _client(auth)
    nonce = secrets.token_urlsafe(32)
    device = _request(
        _text(metadata, "device_authorization_endpoint"),
        {"client_id": client, "scope": "openid", "nonce": nonce},
    )
    return DeviceGrant(
        user_code=_text(device, "user_code"),
        verification_uri=_endpoint(_text(device, "verification_uri")),
        device_code=_text(device, "device_code"),
        nonce=nonce,
        interval=_seconds(device, "interval", 5),
        expires_in=_seconds(device, "expires_in"),
    )


@dataclass(frozen=True)
class IdentityClaims:
    """The subject and group names from the validated issuer response."""

    subject: str
    groups: tuple[str, ...] = ()


def poll_device(auth: AuthConfig, device_code: str, nonce: str) -> IdentityClaims | int:
    """Poll once: the validated subject, or the seconds to wait before polling again."""
    issuer, client, metadata = _client(auth)
    response = _request(
        _text(metadata, "token_endpoint"),
        {
            "client_id": client,
            "device_code": device_code,
            "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        },
    )
    error = response.get("error")
    if error == "authorization_pending":
        return 0
    if error == "slow_down":
        return 5
    if error is not None:
        raise ConfigurationError("OIDC sign-in refused or expired; run `etl-craft login` again")
    claims = _claims(_text(response, "id_token"), issuer, client, nonce)
    groups = claims.get(auth.oidc_groups_claim) if auth.oidc_groups_claim else []
    if not isinstance(groups, list) or any(not isinstance(g, str) or not g.strip() for g in groups):
        raise ConfigurationError(
            f"OIDC claim {auth.oidc_groups_claim!r}={groups!r}; expected a list of group names; "
            "correct the provider claim mapping"
        )
    return IdentityClaims(_text(claims, "sub"), tuple(groups))


def _claims(token: str, issuer: str, client: str, nonce: str) -> dict[str, object]:
    # OIDC Core 3.1.3.7 permits TLS instead of signature verification only for ID tokens
    # obtained by our own direct token-endpoint call; never accept client-supplied ID tokens.
    try:
        header, payload, signature = token.split(".")
        head = json.loads(base64.urlsafe_b64decode(header + "=" * (-len(header) % 4)))
        claims = json.loads(base64.urlsafe_b64decode(payload + "=" * (-len(payload) % 4)))
    except (ValueError, UnicodeError) as error:
        raise ConfigurationError("OIDC ID token is malformed; check the provider") from error
    if not isinstance(head, dict) or not isinstance(claims, dict):
        raise ConfigurationError(
            "OIDC ID token: expected JSON header and claims objects; check the provider"
        )
    if head.get("alg") != "RS256" or not signature:
        raise ConfigurationError(
            "OIDC ID token: expected a signed RS256 token; check client registration"
        )
    for key, expected in (("iss", issuer), ("nonce", nonce), ("azp", client)):
        found = claims.get(key, client if key == "azp" else None)
        _check_claim(key, found, expected)
    if claims.get("aud") not in (client, [client]):
        _check_claim("aud", claims.get("aud"), client)
    now = time.time()
    for key, future in (("exp", True), ("iat", False), ("nbf", False)):
        value = claims.get(key, claims.get("iat") if key == "nbf" else None)
        if not _valid_time(value, now, future=future):
            expected = "after" if future else "at or before"
            raise ConfigurationError(
                f"OIDC ID token {key}={value!r}; "
                f"expected a finite timestamp {expected} {now}; login again"
            )
    _text(claims, "sub")
    return claims


def _check_claim(key: str, found: object, expected: str) -> None:
    if found != expected:
        detail = "value differs" if key == "nonce" else f"found {found!r}, expected {expected!r}"
        raise ConfigurationError(
            f"OIDC ID token {key}: {detail}; check client registration and login again"
        )


def _valid_time(value: object, now: float, *, future: bool) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isfinite(value)
        and (
            value > now - CLOCK_LEEWAY_SECONDS
            if future
            else 0 < value <= now + CLOCK_LEEWAY_SECONDS
        )
    )
