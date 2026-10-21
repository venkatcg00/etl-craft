"""Shared listening sockets for API and standard-library health servers."""

import socket
from collections.abc import Sequence
from dataclasses import dataclass
from ipaddress import IPv4Network, IPv6Network, ip_address

from etl_craft.core.errors import RunRefusedError, UsageError


def bind_api_address(host: str, port: int) -> socket.socket:
    """Bind the listening socket before anything starts, refusing an address already in use."""
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        sock.bind((host, port))
    except OSError as error:
        sock.close()
        raise RunRefusedError(
            f"Orchestration.Api_address {host}:{port} cannot be used: {error.strerror}; stop the "
            "process listening there, or set Api_address to a free port (port 0 picks one)"
        ) from error
    return sock


@dataclass(frozen=True)
class Visitor:
    """An address resolved through explicitly trusted proxy hops."""

    address: str
    trusted_proxy: bool


def forwarded_visitor(
    peer: str,
    headers: Sequence[str],
    trusted: Sequence[IPv4Network | IPv6Network],
    *,
    required: bool = False,
) -> Visitor:
    """Walk forwarded addresses backwards only while each preceding hop is trusted."""
    if not trusted:
        return Visitor(peer, False)
    try:
        visitor = ip_address(peer)
        proxied = any(visitor in network for network in trusted)
        if proxied:
            if not headers and not required:
                return Visitor(str(visitor), True)
            if len(headers) != 1:
                raise UsageError(
                    f"X-Forwarded-For has {len(headers)} headers; expected one from the trusted "
                    "proxy; configure it to overwrite the header"
                )
            forwarded = [ip_address(part.strip()) for part in headers[0].split(",")]
            for visitor in reversed(forwarded):
                if not any(visitor in network for network in trusted):
                    break
        return Visitor(str(visitor), proxied)
    except ValueError as error:
        raise UsageError(
            f"X-Forwarded-For contains an invalid address: {error}; expected IP addresses; "
            "configure the trusted proxy to overwrite the header"
        ) from error
