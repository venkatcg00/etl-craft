import socket

import pytest

from fixtures.services import DEFAULT_PORTS, Service, require, service

pytestmark = pytest.mark.unit


def test_each_service_defaults_to_its_offset_local_port():
    assert service("postgres") == Service("postgres", "127.0.0.1", 55432)
    assert service("mailpit_api").http_url == "http://127.0.0.1:58025"


def test_an_environment_override_moves_a_service(monkeypatch):
    monkeypatch.setenv("ETL_CRAFT_TEST_TRINO", "trino.internal:8080")
    assert service("trino") == Service("trino", "trino.internal", 8080)
    monkeypatch.setenv("ETL_CRAFT_TEST_TRINO", ":9999")
    assert service("trino").address == "127.0.0.1:9999"


def test_an_unknown_service_is_rejected_with_the_known_names():
    with pytest.raises(KeyError, match="postgres_tls"):
        service("oracle")


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def test_reachable_tells_a_listening_port_from_a_closed_one():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        assert Service("probe", "127.0.0.1", port).reachable()
    assert not Service("probe", "127.0.0.1", free_port()).reachable(timeout=0.2)


def test_require_skips_with_the_way_to_start_the_services(monkeypatch):
    monkeypatch.setitem(DEFAULT_PORTS, "postgres", free_port())
    with pytest.raises(pytest.skip.Exception, match="make services-up"):
        require("postgres")


def test_require_fails_instead_when_services_are_required(monkeypatch):
    monkeypatch.setitem(DEFAULT_PORTS, "postgres", free_port())
    monkeypatch.setenv("ETL_CRAFT_REQUIRE_SERVICES", "1")
    with pytest.raises(pytest.fail.Exception, match="postgres is not running"):
        require("postgres")
