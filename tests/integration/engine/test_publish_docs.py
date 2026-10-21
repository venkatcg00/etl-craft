"""``publish-docs``: the site served locally, and through a stand-in for the ngrok SDK."""

import logging
import types
import urllib.error
import urllib.request
from dataclasses import replace
from pathlib import Path

import pytest
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.cli.commands import publish_docs
from etl_craft.config import DocsSiteConfig
from etl_craft.core.errors import ConfigurationError, ExitCode
from etl_craft.services.catalog.collect import build_catalog
from etl_craft.services.catalog.site import write_site
from etl_craft.services.docs_publish import published
from fixtures.auth import service_token

TOKEN = "2abcDEFghiJKLmnoPQRstuVWXyz_0123456789ABCDEFghi"


@pytest.fixture(autouse=True)
def restore_logger():
    logger = logging.getLogger("etl_craft")
    handlers, level = list(logger.handlers), logger.level
    yield
    logger.handlers[:] = handlers
    logger.setLevel(level)


@pytest.fixture
def site(engine_db, tmp_path):
    return write_site(
        build_catalog(engine_db.engine, engine_db.config), engine_db.config, tmp_path / "site"
    ).folder


def publish(*args, **options):
    """Publish and stop at once."""
    with published(*args, **options):
        pass


def fetch(url, **headers):
    try:
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, dict(response.headers), response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), ""


def test_the_site_is_served_privately(engine_db, site, monkeypatch):
    with published(engine_db.engine, engine_db.config, site, local_only=True) as served:
        status, headers, body = fetch(served.url + "index.html")
        assert status == 200 and "etl-craft catalog" in body
        assert headers["X-Robots-Tag"] == "noindex, nofollow, noarchive"
        assert headers["Referrer-Policy"] == "no-referrer"
        assert headers["X-Frame-Options"] == "DENY"
        assert fetch(served.url + "robots.txt")[2] == "User-agent: *\nDisallow: /\n"
        # No folder listings, no hidden files.
        assert fetch(served.url + "tasks/")[0] == 404
        assert fetch(served.url + ".etl-craft-catalog")[0] == 404
        # A site written again while served is served from then on.
        write_site(build_catalog(engine_db.engine, engine_db.config), engine_db.config, site)
        assert fetch(served.url + "index.html")[0] == 200
    with pytest.raises(urllib.error.URLError):
        urllib.request.urlopen(served.url, timeout=2)
    monkeypatch.chdir(site)
    with published(engine_db.engine, engine_db.config, Path("."), local_only=True) as served:
        assert fetch(served.url)[0] == 200


def test_a_folder_without_a_site_is_refused(engine_db, tmp_path):
    with pytest.raises(ConfigurationError, match="run `etl-craft generate-docs` first"):
        publish(engine_db.engine, engine_db.config, tmp_path, local_only=True)
    with pytest.raises(ConfigurationError):
        publish(engine_db.engine, engine_db.config, Path("/"), local_only=True)


@pytest.fixture
def ngrok(monkeypatch):
    """A stand-in for the ngrok SDK: records what it was asked and gives out ``urls`` in turn."""
    fake = types.SimpleNamespace(calls=[], closed=[], urls=["https://etl-docs.ngrok.app"])

    def forward(address, **options):
        fake.calls.append((address, options))
        if fake.urls[0] == "fail":
            raise ValueError(f"failed to connect session: bad authtoken {options['authtoken']}")
        return types.SimpleNamespace(url=lambda: fake.urls[0])

    fake.forward = forward
    fake.disconnect = fake.closed.append
    monkeypatch.setitem(__import__("sys").modules, "ngrok", fake)
    monkeypatch.setenv("ETL_CRAFT_TEST_NGROK", TOKEN)
    return fake


def publishing(engine_db, **site):
    settings = {"authtoken_var": "ETL_CRAFT_TEST_NGROK", **site}
    return replace(engine_db.config, docs_site=DocsSiteConfig(**settings))


def recorded(engine_db):
    with engine_db.engine.connect() as conn:
        return [
            row[0]
            for row in conn.execute(
                text("SELECT PUBLISHED_URL FROM AUD_DOCS_PUBLICATION ORDER BY DOCS_PUBLICATION_ID")
            )
        ]


def test_publishing_through_ngrok_keeps_its_link(engine_db, site, ngrok):
    config = publishing(engine_db, domain="etl-docs.ngrok.app", allowed_ips=("203.0.113.0/24",))
    with published(engine_db.engine, config, site) as served:
        assert served.url == "https://etl-docs.ngrok.app"
        assert served.local_address.startswith("127.0.0.1:")
    ((address, options),) = ngrok.calls
    assert address == served.local_address
    assert options == {"authtoken": TOKEN, "domain": "etl-docs.ngrok.app"}
    assert ngrok.closed == ["https://etl-docs.ngrok.app"]
    publish(engine_db.engine, config, site)
    assert recorded(engine_db) == ["https://etl-docs.ngrok.app"]

    # Another URL would break the links already shared: refused, naming both, and closed.
    ngrok.urls[0] = "https://other.ngrok-free.app"
    with pytest.raises(ConfigurationError) as refused:
        publish(engine_db.engine, config, site)
    assert "published at https://etl-docs.ngrok.app" in str(refused.value)
    assert "now gives https://other.ngrok-free.app" in str(refused.value)
    assert ngrok.closed[-1] == "https://other.ngrok-free.app"
    publish(engine_db.engine, config, site, accept_new_url=True)
    assert recorded(engine_db) == ["https://etl-docs.ngrok.app", "https://other.ngrok-free.app"]


def test_ngrok_problems_say_what_to_do_and_never_show_the_token(
    engine_db, site, ngrok, monkeypatch
):
    ngrok.urls[0] = "fail"
    with pytest.raises(ConfigurationError) as failed:
        publish(engine_db.engine, publishing(engine_db), site)
    assert "ngrok could not open the tunnel" in str(failed.value)
    assert TOKEN not in str(failed.value) and failed.value.__cause__ is None
    with pytest.raises(ConfigurationError, match=r"needs Docs_site\.Authtoken"):
        publish(engine_db.engine, engine_db.config, site)
    monkeypatch.delenv("ETL_CRAFT_TEST_NGROK")
    with pytest.raises(ConfigurationError, match="ETL_CRAFT_TEST_NGROK', which is not set"):
        publish(engine_db.engine, publishing(engine_db), site)
    monkeypatch.setitem(__import__("sys").modules, "ngrok", None)
    with pytest.raises(ConfigurationError, match=r"pip install 'etl-craft\[publish\]'"):
        publish(engine_db.engine, publishing(engine_db), site)


def test_the_command(engine_db, site, tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("ETL_CRAFT_TOKEN", service_token(engine_db.engine))
    root = engine_db.config.config_path.parent if engine_db.config.config_path else tmp_path
    profile = engine_db.config.engine
    lines = [
        "Secrets:",
        "  Source_type: ENVIRONMENT",
        "Orchestration:",
        "Engine:",
        "  dev:",
    ]
    lines += [
        f"    Jdbc_url: {profile.jdbc_url}",
        f"    Schema: {'public' if 'postgresql' in profile.jdbc_url else 'main'}",
    ]
    if profile.auth_mode != "NONE":
        lines += [
            f"    User: {profile.user}",
            f"    Auth_mode: {profile.auth_mode}",
            f"    Secret: {profile.secret_var}",
        ]
    lines += ["Docs_site:", f"  Output: {site}"]
    (root / "craft-connector.yml").write_text("\n".join(lines) + "\n", "utf-8")
    monkeypatch.chdir(root)
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    monkeypatch.setattr(publish_docs, "wait_until_stopped", lambda: None)
    assert main(["publish-docs", "--local-only"]) == ExitCode.SUCCESS
    out = capsys.readouterr().out.splitlines()
    assert any(
        line.startswith(f"publish-docs: serving {site} at http://127.0.0.1:") for line in out
    )
    assert any("does not enforce pipeline grants" in line for line in out)
    assert out[-1] == "publish-docs: stopped"


def test_only_allowed_visitors_are_served(engine_db, site, ngrok):
    # Through ngrok: the visitor is the address ngrok puts last in X-Forwarded-For.
    config = publishing(engine_db, allowed_ips=("203.0.113.0/24",))
    with published(engine_db.engine, config, site) as served:
        local = f"http://{served.local_address}/index.html"
        assert fetch(local, **{"X-Forwarded-For": "203.0.113.9"})[0] == 200
        assert fetch(local, **{"X-Forwarded-For": "198.51.100.4"})[0] == 403
        # A visitor cannot claim an allowed address: ngrok appends the real one last.
        assert fetch(local, **{"X-Forwarded-For": "203.0.113.9, 198.51.100.4"})[0] == 403
        assert fetch(local)[0] == 403
    # Served locally: the connecting address.
    here = replace(config, docs_site=replace(config.docs_site, allowed_ips=("127.0.0.0/8",)))
    with published(engine_db.engine, here, site, local_only=True) as served:
        assert fetch(served.url + "index.html")[0] == 200
    with published(engine_db.engine, config, site, local_only=True) as served:
        assert fetch(served.url + "index.html")[0] == 403
        assert fetch(served.url + "robots.txt")[0] == 403


@pytest.mark.parametrize("method", ["GET", "HEAD"])
@pytest.mark.parametrize(
    "path", ["/.x", "/%2ex", "/a/../.x", "/%2e%2e/", "/%2eetl-craft-catalog", "/assets/%2e%2e/.x"]
)
def test_hidden_and_parent_paths_are_refused(engine_db, site, method, path):
    (site / ".x").write_text("private contents")
    with published(engine_db.engine, engine_db.config, site, local_only=True) as served:
        request = urllib.request.Request(served.url.rstrip("/") + path, method=method)
        with pytest.raises(urllib.error.HTTPError) as caught:
            urllib.request.urlopen(request, timeout=5)
        assert caught.value.code == 404


def test_links_outside_the_site_are_refused_for_get_and_head(engine_db, site, tmp_path):
    private = tmp_path / "private.txt"
    private.write_text("private contents")
    (site / "link.txt").symlink_to(private)
    with published(engine_db.engine, engine_db.config, site, local_only=True) as served:
        for method in ("GET", "HEAD"):
            request = urllib.request.Request(served.url + "link.txt", method=method)
            with pytest.raises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(request, timeout=5)
            assert caught.value.code == 404


def test_directory_index_links_outside_the_site_are_refused(engine_db, site, tmp_path):
    private = tmp_path / "private.html"
    private.write_text("private contents")
    folder = site / "public"
    folder.mkdir()
    (folder / "index.html").symlink_to(private)
    with published(engine_db.engine, engine_db.config, site, local_only=True) as served:
        for method in ("GET", "HEAD"):
            request = urllib.request.Request(served.url + "public/", method=method)
            with pytest.raises(urllib.error.HTTPError) as caught:
                urllib.request.urlopen(request, timeout=5)
            assert caught.value.code == 404


@pytest.fixture
def proxy():
    import http.client
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    class Proxy(BaseHTTPRequestHandler):
        def do_GET(self):
            connection = http.client.HTTPConnection(self.server.upstream, timeout=10)
            try:
                forwarded = self.headers.get("X-Forwarded-For", "")
                visitor = self.server.visitor
                connection.request(
                    self.command,
                    self.path,
                    headers={
                        "X-Forwarded-For": f"{forwarded}, {visitor}" if forwarded else visitor,
                    },
                )
                response = connection.getresponse()
                body = response.read()
                self.send_response(response.status)
                for name, value in response.getheaders():
                    if name.lower() not in ("connection", "transfer-encoding"):
                        self.send_header(name, value)
                self.end_headers()
                self.wfile.write(body)
            finally:
                connection.close()

        def log_message(self, *_args):
            pass

        def do_HEAD(self):
            self.do_GET()

    server = ThreadingHTTPServer(("127.0.0.1", 0), Proxy)
    server.visitor = "203.0.113.9"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize("prefix", ["", "etl/catalog", "etl%20catalog"])
def test_organization_url_through_proxy_keeps_links_and_records_the_public_url(
    engine_db, site, proxy, prefix, monkeypatch
):
    import sys
    from html.parser import HTMLParser
    from urllib.parse import urljoin

    from fixtures.metadata import add_pipeline, add_task

    with engine_db.engine.begin() as conn:
        pipeline = add_pipeline(conn, "DOCS")
        add_task(
            conn,
            pipeline,
            "load",
            SQL_ACTION="CREATE_TABLE",
            TARGET_OBJECT="mart.sample",
            SELECT_QUERY="SELECT 1 AS id",
        )
    write_site(build_catalog(engine_db.engine, engine_db.config), engine_db.config, site)

    # Organization hosting never imports the optional tunnel SDK or resolves its token.
    monkeypatch.setitem(sys.modules, "ngrok", None)
    monkeypatch.delenv("ETL_CRAFT_TEST_NGROK", raising=False)
    base = f"http://127.0.0.1:{proxy.server_port}/" + (prefix + "/" if prefix else "")
    config = publishing(engine_db, external_url=base, trusted_proxies=("127.0.0.1/32",))
    links = []

    class Links(HTMLParser):
        def handle_starttag(self, _tag, attrs):
            links.extend(value for name, value in attrs if name in ("href", "src") and value)

    with published(engine_db.engine, config, site) as served:
        proxy.upstream = served.local_address
        assert served.url == base and served.local_address.startswith("127.0.0.1:")
        for page in ("", "pipelines/DOCS.html", "tasks/DOCS.load.html", "tables/mart.sample.html"):
            page_url = urljoin(base, page)
            status, _headers, body = fetch(page_url)
            assert status == 200 and "Schema and lineage gaps" in body
            links.clear()
            Links().feed(body)
            assert links
            for link in links:
                assert fetch(urljoin(page_url, link))[0] == 200, link
        assert fetch(base + "robots.txt")[0] == 200
        with urllib.request.urlopen(
            urllib.request.Request(base + "robots.txt", method="HEAD")
        ) as head:
            assert head.status == 200 and head.read() == b""
        assert fetch(base + ".etl-craft-catalog")[0] == 404
        assert fetch(base + "%2e%2e/private")[0] == 404
        if prefix:
            assert fetch(f"http://{served.local_address}/index.html")[0] == 404
            assert fetch(base.rstrip("/"))[0] == 200  # redirected beneath the prefix
    assert recorded(engine_db) == [base]
    changed = replace(config, docs_site=replace(config.docs_site, external_url=base + "new/"))
    with pytest.raises(ConfigurationError, match="already shared would break"):
        publish(engine_db.engine, changed, site)
    publish(engine_db.engine, changed, site, accept_new_url=True)
    assert recorded(engine_db) == [base, base + "new/"]


def test_proxy_ip_rules_ignore_untrusted_headers_and_preserve_the_last_untrusted_hop(
    engine_db, site, proxy
):
    base = f"http://127.0.0.1:{proxy.server_port}/docs/"
    config = publishing(
        engine_db,
        external_url=base,
        trusted_proxies=("127.0.0.1/32",),
        allowed_ips=("203.0.113.0/24",),
    )
    with published(engine_db.engine, config, site) as served:
        proxy.upstream = served.local_address
        assert fetch(base)[0] == 200
        proxy.visitor = "198.51.100.4"
        assert fetch(base, **{"X-Forwarded-For": "203.0.113.9"})[0] == 403
        proxy.visitor = "203.0.113.9"
        assert fetch(base, **{"X-Forwarded-For": "not-an-address"})[0] == 403
        assert fetch(f"http://{served.local_address}/docs/")[0] == 403
    direct = replace(config, docs_site=replace(config.docs_site, trusted_proxies=("192.0.2.1/32",)))
    with published(engine_db.engine, direct, site) as served:
        proxy.upstream = served.local_address
        assert fetch(base, **{"X-Forwarded-For": "203.0.113.9"})[0] == 403


def test_regeneration_cannot_expose_the_directory_exchange_gap(engine_db, site, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from queue import Queue

    from etl_craft.core import filelock
    from fixtures.races import paused_after

    observed = Queue()
    original = filelock._try_lock

    def trying(*args, **kwargs):
        if kwargs.get("shared"):
            observed.put("reader waiting")
        return original(*args, **kwargs)

    def read(url):
        result = fetch(url)
        observed.put("reader finished")
        return result

    with published(engine_db.engine, engine_db.config, site, local_only=True) as served:
        monkeypatch.setattr(filelock, "_try_lock", trying)
        with (
            ThreadPoolExecutor(max_workers=1) as pool,
            paused_after(
                lambda: write_site(
                    build_catalog(engine_db.engine, engine_db.config), engine_db.config, site
                ),
                owner=Path,
                name="rename",
                when=lambda source, *_a, **_k: source == site,
            ) as resume,
        ):
            request = pool.submit(read, served.url)
            observed.get(timeout=5)
            resume.set()
            status, _headers, body = request.result(timeout=10)
            assert status == 200 and "etl-craft catalog" in body


def test_tunnel_cleanup_failure_still_stops_the_server(engine_db, site, ngrok):
    address = None

    def failed_close(_url):
        raise ConfigurationError("tunnel cleanup failed")

    ngrok.disconnect = failed_close
    with (
        pytest.raises(ConfigurationError, match="tunnel cleanup failed"),
        published(engine_db.engine, publishing(engine_db), site) as served,
    ):
        address = f"http://{served.local_address}/"
    with pytest.raises(urllib.error.URLError):
        urllib.request.urlopen(address, timeout=2)


def test_read_only_catalog_needs_no_writable_publication_sidecar(engine_db, site):
    lock = site.with_name(f".{site.name}.publish.lock")
    assert lock.is_file()
    lock.chmod(0o444)
    try:
        with published(engine_db.engine, engine_db.config, site, local_only=True) as served:
            assert fetch(served.url)[0] == 200
    finally:
        lock.chmod(0o644)


def test_busy_publication_is_reported_as_unavailable(engine_db, site, monkeypatch, caplog):
    import types

    from etl_craft.core import filelock

    with published(engine_db.engine, engine_db.config, site, local_only=True) as served:
        clock = iter((0, 10))
        monkeypatch.setattr(filelock, "time", types.SimpleNamespace(monotonic=lambda: next(clock)))
        monkeypatch.setattr(filelock, "_try_lock", lambda *_args, **_kwargs: False)
        assert fetch(served.url)[0] == 503
    assert "publication lock" in caplog.text and "retry the request" in caplog.text
