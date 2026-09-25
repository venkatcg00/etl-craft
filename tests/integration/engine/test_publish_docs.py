"""``publish-docs``: the site served locally, and through a stand-in for the ngrok SDK."""

import logging
import types
import urllib.error
import urllib.request
from dataclasses import replace

import pytest
from sqlalchemy import text

from etl_craft.cli import main
from etl_craft.cli.commands import publish_docs
from etl_craft.config import DocsSiteConfig
from etl_craft.core.errors import ConfigurationError, ExitCode
from etl_craft.services.catalog import build_catalog
from etl_craft.services.catalog_site import write_site
from etl_craft.services.docs_publish import published

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


def fetch(url):
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.status, dict(response.headers), response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, dict(error.headers), ""


def test_the_site_is_served_privately(engine_db, site):
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


def test_a_folder_without_a_site_is_refused(engine_db, tmp_path):
    with pytest.raises(ConfigurationError, match="run `etl-craft generate-docs` first"):
        publish(engine_db.engine, engine_db.config, tmp_path, local_only=True)


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
    assert options == {
        "authtoken": TOKEN,
        "domain": "etl-docs.ngrok.app",
        "ip_restriction_allow_cidrs": ["203.0.113.0/24"],
    }
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
    root = engine_db.config.config_path.parent if engine_db.config.config_path else tmp_path
    profile = engine_db.config.engine.active
    lines = [
        "Secrets:",
        "  Source_type: environment",
        "Orchestration:",
        "  Mode: local",
        "Engine:",
        "  dev:",
    ]
    lines += [
        f"    jdbc_url: {profile.jdbc_url}",
        f"    schema: {'public' if 'postgresql' in profile.jdbc_url else 'main'}",
    ]
    if profile.auth_mode != "none":
        lines += [
            f"    user: {profile.user}",
            f"    auth_mode: {profile.auth_mode}",
            f"    secret: {profile.secret_var}",
        ]
    lines += ["Docs_site:", f"  Output: {site}"]
    (root / "craft-connector.yml").write_text("\n".join(lines) + "\n", "utf-8")
    monkeypatch.chdir(root)
    monkeypatch.delenv("ETL_CRAFT_CONFIG", raising=False)
    monkeypatch.setattr(publish_docs, "wait_until_stopped", lambda: None)
    assert main(["publish-docs", "--local-only"]) == ExitCode.SUCCESS
    out = capsys.readouterr().out.splitlines()
    assert out[0].startswith(f"publish-docs: serving {site} at http://127.0.0.1:")
    assert out[-1] == "publish-docs: stopped"
