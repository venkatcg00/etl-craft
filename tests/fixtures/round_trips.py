"""The same SQLAlchemy counters in a supervisor and its fresh task interpreter."""

import json
import os
import threading
from collections import Counter
from contextlib import contextmanager
from contextvars import ContextVar
from functools import cache
from pathlib import Path

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import Pool

_ACTIVE = ContextVar("query_counter", default=None)


def cloud_statement(sql):
    counter = _ACTIVE.get()
    if counter is not None and counter.driver_statements:
        from etl_craft.handlers.sql_script import statement_text

        counter.record("warehouse", "statements", statement_text(sql)[:80])


@cache
def catalog_queries():
    from etl_craft.dialects.engine import all_dialects

    root = Path(__file__).parents[2] / "src" / "etl_craft" / "dialects" / "engine"
    names = {query.stem for query in root.rglob("queries/*.sql")}
    result = {}
    for dialect in all_dialects():
        for name in names:
            if (root / "queries" / f"{name}.sql").is_file() or (
                dialect.directory / "queries" / f"{name}.sql"
            ).is_file():
                result[" ".join(dialect.query(name).split())] = name
    return result


def database(url):
    """The database a URL reaches, ignoring driver and connection options."""
    name = url.database
    if url.get_backend_name() == "sqlite" and name:
        name = os.path.realpath(name)
    return [url.get_backend_name(), url.host, url.port, name]


class RoundTrips:
    def __init__(
        self,
        path=None,
        engine_database=None,
        *,
        engine=None,
        include_threads=False,
        driver_statements=False,
    ):
        self.path = path
        self.engine_database = engine_database
        self.engine = engine
        self.include_threads = include_threads
        self.driver_statements = driver_statements
        self.thread_id = threading.get_ident()
        self.counts = Counter()
        self.queries = Counter()
        self.catalog = catalog_queries()
        self.listeners = [
            (Pool, "connect", self.connected),
            (Engine, "engine_connect", self.checked_out),
            (Engine, "before_cursor_execute", self.statement),
            (Engine, "begin", self.begin),
        ]

    def record(self, kind, measure, query=None):
        self.counts[f"{kind}_{measure}"] += 1
        if query:
            self.queries[query] += 1
        if self.path:
            payload = (
                json.dumps({"pid": os.getpid(), "kind": kind, "measure": measure, "query": query})
                + "\n"
            )
            descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            try:
                os.write(descriptor, payload.encode())
            finally:
                os.close(descriptor)

    def kind(self, conn):
        return "engine" if database(conn.engine.url) == self.engine_database else "warehouse"

    def connected(self, connection, record):
        if not self.include_threads and threading.get_ident() != self.thread_id:
            return
        record.info["budget_new_connection"] = True

    def accepts(self, conn):
        if not self.include_threads and threading.get_ident() != self.thread_id:
            return False
        return self.engine is None or self.kind(conn) != "engine" or conn.engine is self.engine

    def checked_out(self, conn):
        if not self.accepts(conn):
            return
        if conn.info.pop("budget_new_connection", False):
            self.record(self.kind(conn), "connections")

    def statement(self, conn, cursor, sql, parameters, context, many):
        if not self.accepts(conn):
            return
        if sql.lstrip().upper().startswith("BEGIN"):
            return
        if self.driver_statements and self.kind(conn) == "warehouse":
            return
        compiled = context.compiled
        source = str(compiled.statement) if compiled is not None else sql
        flat = " ".join(source.split())
        self.record(self.kind(conn), "statements", self.catalog.get(flat, "other: " + flat[:80]))

    def begin(self, conn):
        if not self.accepts(conn):
            return
        self.record(self.kind(conn), "transactions")

    def install(self):
        self.token = _ACTIVE.set(self)
        for target, name, callback in self.listeners:
            event.listen(target, name, callback)
        return self

    def remove(self):
        _ACTIVE.reset(self.token)
        for target, name, callback in self.listeners:
            event.remove(target, name, callback)

    def result(self):
        names = (
            "warehouse_statements",
            "warehouse_connections",
            "engine_statements",
            "engine_transactions",
            "engine_connections",
        )
        return {name: self.counts[name] for name in names}


@contextmanager
def count_round_trips(engine_db, *, path=None, include_threads=False, driver_statements=False):
    """Count statements, transactions and new connections, split by the Engine DB ``engine_db``."""
    counter = RoundTrips(
        path,
        database(engine_db.url),
        engine=engine_db,
        include_threads=include_threads,
        driver_statements=driver_statements,
    ).install()
    try:
        yield counter
    finally:
        counter.remove()


def install_child_counter():
    path = os.environ.get("ETL_CRAFT_TEST_ROUND_TRIPS")
    if path:
        RoundTrips(
            path, json.loads(os.environ["ETL_CRAFT_TEST_ROUND_TRIPS_ENGINE"]), include_threads=True
        ).install()
