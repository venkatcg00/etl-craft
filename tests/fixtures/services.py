"""The local services docker-compose.yml starts, and how tests reach them.

Each service listens on 127.0.0.1 at the port below. ``ETL_CRAFT_TEST_<NAME>=host:port``
points a test run at another address (for example ``ETL_CRAFT_TEST_POSTGRES=db:5432``).

A test that needs a service skips when the service is down, unless
``ETL_CRAFT_REQUIRE_SERVICES=1`` is set: then it fails, so a run that is meant to exercise the
services (CI, release evidence) cannot pass by skipping.
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CERTS_DIR = REPO_ROOT / ".certs"

DEFAULT_PORTS = {
    "postgres": 55432,
    "postgres_tls": 55433,
    "minio": 59000,
    "iceberg_rest": 58181,
    "trino": 58080,
    "mailpit_smtp": 51025,
    "mailpit_api": 58025,
}

# Credentials of the local services; they exist only inside the test containers.
POSTGRES_USER = "etl_craft"
POSTGRES_PASSWORD = "etl_craft"
POSTGRES_DB = "etl_craft"
MINIO_BUCKET = "warehouse"


@dataclass(frozen=True)
class Service:
    """Where one local service listens."""

    name: str
    host: str
    port: int

    @property
    def address(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def http_url(self) -> str:
        return f"http://{self.address}"

    def reachable(self, timeout: float = 1.0) -> bool:
        """Report whether something accepts TCP connections at the service's address."""
        try:
            with socket.create_connection((self.host, self.port), timeout=timeout):
                return True
        except OSError:
            return False


def service(name: str) -> Service:
    """Return the address of a service, honouring its ``ETL_CRAFT_TEST_<NAME>`` override."""
    if name not in DEFAULT_PORTS:
        raise KeyError(f"unknown service {name!r}; known: {', '.join(DEFAULT_PORTS)}")
    override = os.environ.get(f"ETL_CRAFT_TEST_{name.upper()}")
    if override:
        host, _, port = override.rpartition(":")
        return Service(name, host or "127.0.0.1", int(port))
    return Service(name, "127.0.0.1", DEFAULT_PORTS[name])


def require(name: str) -> Service:
    """Return a service, or skip (or fail, when services are required) if nothing listens there."""
    found = service(name)
    if not found.reachable():
        message = f"{name} is not running at {found.address}; start it with `make services-up`"
        if os.environ.get("ETL_CRAFT_REQUIRE_SERVICES") == "1":
            pytest.fail(message, pytrace=False)
        pytest.skip(message)
    return found
