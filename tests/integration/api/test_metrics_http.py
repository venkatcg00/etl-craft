"""Both listeners delegate unsigned exposition and copy the operation's response."""

import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from urllib.request import urlopen

from fastapi.testclient import TestClient

from etl_craft.api import app
from etl_craft.controller import health
from etl_craft.core.actor import SYSTEM_ACTOR
from etl_craft.services.operations import OperationContext
from etl_craft.services.operations.metrics import CONTENT_TYPE, Exposition


def test_both_listeners_copy_the_same_unsigned_service_document(engine_db, monkeypatch):
    ctx = OperationContext(
        engine_db.engine, replace(engine_db.config, api_address="127.0.0.1:0"), SYSTEM_ACTOR
    )
    document = Exposition(
        "# HELP etl_craft_example A sample.\n# TYPE etl_craft_example gauge\netl_craft_example 1\n"
    )
    seen = []

    def operation(selected):
        seen.append(selected.engine)
        return document

    monkeypatch.setattr(app, "scrape", operation)
    monkeypatch.setattr(health, "scrape", operation)
    started, stop = threading.Event(), threading.Event()
    ports = []
    bind = health.bind_api_address

    def address(host, port):
        listener = bind(host, port)
        ports.append(listener.getsockname()[1])
        return listener

    def supervise(*args):
        started.set()
        assert stop.wait(timeout=10)

    monkeypatch.setattr(health, "bind_api_address", address)
    monkeypatch.setattr(health, "serve", supervise)
    with ThreadPoolExecutor(1) as pool:
        future = pool.submit(health.serve_health, ctx, stop)
        try:
            assert started.wait(timeout=10)
            with urlopen(f"http://127.0.0.1:{ports[0]}/metrics", timeout=5) as response:
                native = response.read().decode()
                assert response.headers["Content-Type"] == CONTENT_TYPE
            client = TestClient(app.create_app(ctx))
            response = client.get("/metrics", headers={"Authorization": "Bearer invalid"})
            assert response.status_code == 200
            assert response.headers["content-type"] == CONTENT_TYPE
            assert response.text == native == document.body
            assert client.get("/api/v1/health").status_code == 401
            assert "access-control-allow-origin" not in response.headers
            assert len(seen) == 2
        finally:
            stop.set()
            future.result(timeout=10)
