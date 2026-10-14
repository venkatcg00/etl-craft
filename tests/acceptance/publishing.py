"""Explicit manual catalog acceptance; excluded from ordinary test discovery and PR CI.

Run with the publish extra and NGROK_AUTHTOKEN. The proxy cases require Docker on Linux.
Only a synthetic site is exposed; no project metadata or credentials are published.
"""

import importlib.util
import os
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest

from etl_craft.config import DocsSiteConfig
from etl_craft.services.catalog_site import MARKER
from etl_craft.services.docs_publish import published

pytestmark = pytest.mark.harness
BODY = "ETL Craft synthetic publication acceptance"


@pytest.fixture
def synthetic_site(tmp_path):
    folder = tmp_path / "catalog"
    folder.mkdir()
    (folder / MARKER).touch()
    (folder / "index.html").write_text(f'<html>{BODY}<link href="catalog.css"></html>')
    (folder / "catalog.css").write_text("body { color: black; }")
    return folder


def page(url):
    """Poll endpoint readiness with a deadline, including ngrok's propagation delay."""
    deadline = time.monotonic() + 60
    while True:
        try:
            request = urllib.request.Request(url, headers={"ngrok-skip-browser-warning": "1"})
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, response.read().decode()
        except urllib.error.URLError:
            if time.monotonic() >= deadline:
                raise
            threading.Event().wait(0.1)


def test_real_ngrok_tunnel_serves_and_closes(engine_db, synthetic_site):
    assert importlib.util.find_spec("ngrok"), "install etl-craft[publish] for tunnel acceptance"
    assert os.environ.get("NGROK_AUTHTOKEN"), "set NGROK_AUTHTOKEN for tunnel acceptance"
    config = replace(engine_db.config, docs_site=DocsSiteConfig(authtoken_var="NGROK_AUTHTOKEN"))
    with published(engine_db.engine, config, synthetic_site) as serving:
        assert page(serving.url + "/")[1].find(BODY) >= 0
        assert "color: black" in page(serving.url + "/catalog.css")[1]
        local = serving.local_address
    with pytest.raises(urllib.error.URLError):
        urllib.request.urlopen("http://" + local, timeout=2)


@pytest.mark.parametrize("prefix", ["", "etl-craft/"])
def test_documented_nginx_configuration(engine_db, synthetic_site, tmp_path, prefix):
    with socket.socket() as port_probe:
        port_probe.bind(("127.0.0.1", 0))
        port = port_probe.getsockname()[1]
    base = f"http://127.0.0.1:{port}/{prefix}"
    config = replace(
        engine_db.config,
        docs_site=DocsSiteConfig(
            external_url=base,
            trusted_proxies=("127.0.0.1/32",),
            allowed_ips=("127.0.0.1/32",),
        ),
    )
    template = Path(__file__).resolve().parents[2] / "deploy/catalog/nginx.conf"
    name = "etl-craft-catalog-acceptance-" + uuid4().hex[:12]
    with published(engine_db.engine, config, synthetic_site) as serving:
        rendered = tmp_path / "nginx.conf"
        rendered.write_text(
            template.read_text()
            .replace("127.0.0.1:8081", f"127.0.0.1:{port}")
            .replace("127.0.0.1:8080", serving.local_address)
        )
        try:
            subprocess.run(
                [
                    "docker",
                    "run",
                    "--detach",
                    "--name",
                    name,
                    "--network",
                    "host",
                    "--mount",
                    f"type=bind,source={rendered},target=/etc/nginx/nginx.conf,readonly",
                    "nginx:stable-alpine",
                    "nginx",
                    "-g",
                    "daemon off;",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            assert BODY in page(base)[1]
            assert "color: black" in page(base + "catalog.css")[1]
            # The example overwrites a forged visitor header with the actual peer address.
            request = urllib.request.Request(base, headers={"X-Forwarded-For": "203.0.113.9"})
            with urllib.request.urlopen(request, timeout=5) as response:
                assert response.status == 200
        finally:
            subprocess.run(["docker", "rm", "--force", name], check=False, capture_output=True)
