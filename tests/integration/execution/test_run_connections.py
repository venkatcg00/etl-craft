"""The connection tests a pipeline run makes before it starts."""

import socket
from dataclasses import replace

import pytest

from etl_craft.config import (
    CloningConfig,
    ConnectionProfile,
    ConnectionSection,
    ConnectorConfig,
    EmailConfig,
    EmailProfile,
    SourceConfig,
)
from etl_craft.core.enums import Mode
from etl_craft.core.errors import ConnectionTestError
from etl_craft.execution import connections
from etl_craft.execution.connections import (
    check_run_connections,
    probe_email_relay,
    probe_warehouse,
)
from fixtures.services import POSTGRES_DB, POSTGRES_PASSWORD, POSTGRES_USER, require

SECRET_VAR = "ETL_CRAFT_TEST_WAREHOUSE_SECRET"


def config(warehouse_url=None, relay=None, **fields):
    engine = ConnectionProfile("ENGINE", "dev", "jdbc:sqlite:e.db", "", "none")
    warehouse = None
    if warehouse_url is not None:
        profile = ConnectionProfile("WAREHOUSE", "dev", warehouse_url, "", "none", fields)
        warehouse = ConnectionSection("dev", {"dev": profile})
    email = None
    if relay is not None:
        host, port = relay
        email = EmailConfig("dev", {"dev": EmailProfile("EMAIL", "dev", host, port, "e@x.io")})
    return ConnectorConfig(
        mode=Mode.LOCAL,
        source=SourceConfig(type="environment"),
        engine=ConnectionSection("dev", {"dev": engine}),
        warehouse=warehouse,
        email=email,
    )


def closed_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
def probes(monkeypatch):
    """Replace both probes with fakes that fail, and record which ran."""
    ran = []

    def warehouse(config, engine_db=None):
        ran.append("warehouse")
        return "refused"

    def relay(config):
        ran.append("relay")
        return "timed out"

    monkeypatch.setattr(connections, "probe_warehouse", warehouse)
    monkeypatch.setattr(connections, "probe_email_relay", relay)
    return ran


@pytest.mark.unit
@pytest.mark.parametrize(
    ("handlers", "kwargs", "tested"),
    [
        ({"PYTHON"}, {}, []),
        ({"SQL"}, {}, ["warehouse"]),
        ({"BUSINESS_RULES", "EMAIL_ALERT"}, {}, ["warehouse", "relay"]),
        ({"PYTHON"}, {"sends_sla_email": True}, ["relay"]),
    ],
)
def test_only_the_connections_a_run_uses_are_tested(probes, handlers, kwargs, tested):
    both = config("jdbc:postgresql://127.0.0.1:1/x", ("127.0.0.1", 1))
    if tested:
        with pytest.raises(ConnectionTestError) as error:
            check_run_connections(None, both, "P", handlers, **kwargs)
        assert str(error.value).startswith("P: a connection test failed, so no run was started")
    else:
        check_run_connections(None, both, "P", handlers, **kwargs)
    assert probes == tested


@pytest.mark.unit
def test_cloning_needs_the_warehouse_and_a_duckdb_file_is_not_tested(probes, tmp_path):
    cloning = replace(config("jdbc:postgresql://127.0.0.1:1/x"), cloning=CloningConfig(True))
    with pytest.raises(ConnectionTestError, match="warehouse: refused"):
        check_run_connections(None, cloning, "P", set())
    duckdb = config(f"jdbc:duckdb:{tmp_path / 'w.duckdb'}")
    check_run_connections(None, duckdb, "P", {"SQL"})
    # Nothing configured, nothing to test.
    check_run_connections(None, config(), "P", {"SQL", "EMAIL_ALERT"})
    assert probes == ["warehouse"]


@pytest.mark.unit
def test_the_probes_report_what_failed(monkeypatch):
    port = closed_port()
    assert probe_email_relay(config(relay=("127.0.0.1", port))).startswith(f"127.0.0.1:{port}: ")
    assert probe_email_relay(config()) is None
    assert "not available for a Postgres warehouse" in probe_warehouse(
        config(f"jdbc:postgresql://127.0.0.1:{port}/x")
    )
    monkeypatch.setenv(SECRET_VAR, "unused")
    unreachable = replace(
        config(),
        warehouse=ConnectionSection(
            "dev",
            {
                "dev": ConnectionProfile(
                    "WAREHOUSE",
                    "dev",
                    f"jdbc:postgresql://127.0.0.1:{port}/x",
                    "etl",
                    "password",
                    {"secret_var": SECRET_VAR},
                )
            },
        ),
    )
    assert "connection" in probe_warehouse(unreachable).lower()


@pytest.mark.connections
def test_the_probes_reach_live_services(monkeypatch):
    mailpit = require("mailpit_smtp")
    assert probe_email_relay(config(relay=(mailpit.host, mailpit.port))) is None
    pg = require("postgres")
    monkeypatch.setenv(SECRET_VAR, POSTGRES_PASSWORD)
    warehouse = ConnectionProfile(
        "WAREHOUSE",
        "dev",
        f"jdbc:postgresql://{pg.address}/{POSTGRES_DB}",
        POSTGRES_USER,
        "password",
        {"secret_var": SECRET_VAR},
    )
    live = replace(config(), warehouse=ConnectionSection("dev", {"dev": warehouse}))
    assert probe_warehouse(live) is None
