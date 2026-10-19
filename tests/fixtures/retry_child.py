"""Interrupt one live warehouse connection before letting the SQL handler continue."""

import socket
from pathlib import Path

from sqlalchemy import text

from etl_craft.execution import child
from etl_craft.handlers.sql.session import Session

run_statement = Session.run
flag = Path(child.parse_args().config).with_suffix(".interrupted")


def interrupted(session, sql, params=None, *, step):
    if not flag.exists():
        flag.touch()
        driver = session.conn.connection.driver_connection
        name = session.conn.dialect.name
        if name == "postgresql":
            session.conn.execute(text("SELECT pg_terminate_backend(pg_backend_pid())"))
        elif name == "trino":
            # A closed local listener makes the real HTTP client observe a connection drop.
            with socket.socket() as refused:
                refused.bind(("127.0.0.1", 0))
                port = refused.getsockname()[1]
                request = driver._create_request()
                request._host = "127.0.0.1"
                request._port = port
                request._max_attempts = 1
                from trino.client import TrinoQuery

                TrinoQuery(request, "SELECT 1").execute()
        else:
            driver.close()
    return run_statement(session, sql, params, step=step)


Session.run = interrupted
child.run_as_process()
