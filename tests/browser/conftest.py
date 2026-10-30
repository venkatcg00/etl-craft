"""A served etl-craft UI over HTTPS, with an admin and a viewer account, for browser tests."""

import socket
import subprocess
import threading
from dataclasses import replace

import pytest
import uvicorn

from etl_craft.api.app import create_app
from etl_craft.core.actor import SYSTEM_ACTOR, Actor, ActorKind, acting_as
from etl_craft.core.enums import ActionName, ApiRole
from etl_craft.engine.schema import init_db
from etl_craft.services.operations import OperationContext
from fixtures.actions import act
from fixtures.engine_db import sqlite_engine_db

PASSWORD = "correct horse battery"


@pytest.fixture
def site(tmp_path, request):
    db = sqlite_engine_db(tmp_path)
    with acting_as(SYSTEM_ACTOR):
        init_db(db.engine)
    config = db.config
    if getattr(request, "param", None) == "sso":
        config = replace(
            config,
            auth=replace(
                config.auth, oidc_issuer="https://issuer.example", oidc_client_id="console"
            ),
        )
    ctx = OperationContext(db.engine, config, Actor("owner", ActorKind.USER), role=ApiRole.ADMIN)
    act(ctx, ActionName.ACCOUNT_ADD, name="owner", role=ApiRole.ADMIN, password=PASSWORD)
    act(ctx, ActionName.ACCOUNT_ADD, name="alice", role=ApiRole.VIEWER, password=PASSWORD)
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-keyout",
            str(key),
            "-out",
            str(cert),
        ],
        check=True,
        capture_output=True,
    )
    ready = threading.Event()

    class Server(uvicorn.Server):
        async def startup(self, sockets=None):
            await super().startup(sockets)
            ready.set()

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    server = Server(
        uvicorn.Config(
            create_app(ctx), ssl_keyfile=str(key), ssl_certfile=str(cert), log_level="error"
        )
    )
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    thread.start()
    assert ready.wait(10)
    try:
        yield ctx, f"https://localhost:{port}"
    finally:
        server.should_exit = True
        thread.join(10)
        assert not thread.is_alive()
        listener.close()
        db.engine.dispose()
