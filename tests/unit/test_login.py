"""Credential storage, password verification and device-flow trust boundaries."""

import base64
import json
import time

import pytest

from etl_craft.config import parse_config
from etl_craft.config.model import AuthConfig
from etl_craft.core.errors import ConfigurationError, UsageError
from etl_craft.services import credentials, oidc, passwords

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def private_home(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "personal"))


def test_passwords_are_salted_and_verified_without_trusting_costs():
    first = passwords.hash_password("correct horse battery")
    assert first != passwords.hash_password("correct horse battery")
    assert passwords.verify_password("correct horse battery", first)
    assert not passwords.verify_password("wrong password", first)
    for value in (
        None,
        "scrypt$999999999$8$5$00$00",
        "scrypt$16384$8$5$xx$xx",
        "scrypt$16384$8$5$00$00",
    ):
        assert not passwords.verify_password("correct horse battery", value)
    assert not passwords.verify_password("x" * 1025, first)
    with pytest.raises(UsageError, match="12 to 1024"):
        passwords.hash_password("short")


def test_credentials_are_private_and_separate_per_database():
    assert credentials.read_session("first") is None
    credentials.store_session("first", "token-a")
    credentials.store_session("second", "token-b")
    assert credentials.read_session("first") == "token-a"
    assert credentials.read_session("second") == "token-b"
    path = credentials.credential_path()
    assert path.stat().st_mode & 0o777 == 0o600
    credentials.store_session("first", None)
    assert credentials.read_session("first") is None
    assert credentials.read_session("second") == "token-b"


@pytest.mark.parametrize("mode", [0o644, 0o640, 0o666])
def test_readable_credentials_are_refused_with_the_exact_remedy(mode):
    credentials.store_session("db", "secret")
    path = credentials.credential_path()
    path.chmod(mode)
    with pytest.raises(ConfigurationError, match=f"chmod 600 {path}"):
        credentials.read_session("db")
    with pytest.raises(ConfigurationError, match="0600"):
        credentials.store_session("other", "secret")


def test_symlinks_malformed_files_and_shared_directories_are_refused(tmp_path):
    credentials.store_session("db", "secret")
    path = credentials.credential_path()
    path.write_text('{"db": 42}')
    with pytest.raises(ConfigurationError, match="Engine DB/token"):
        credentials.read_session("db")
    path.write_text("not json")
    with pytest.raises(ConfigurationError, match="invalid JSON"):
        credentials.read_session("db")
    path.unlink()
    path.symlink_to(tmp_path / "target")
    with pytest.raises(ConfigurationError, match="symlinks"):
        credentials.read_session("db")
    path.unlink()
    path.parent.chmod(0o777)
    with pytest.raises(ConfigurationError, match="chmod go-w"):
        credentials.store_session("db", "secret")


def test_credential_writers_preserve_other_database_entries():
    from fixtures.races import two_at_once

    two_at_once(
        lambda: credentials.store_session("first", "token-a"),
        lambda: credentials.store_session("second", "token-b"),
        at="etl_craft.services.credentials.file_lock",
    )
    assert credentials.read_session("first") == "token-a"
    assert credentials.read_session("second") == "token-b"


def test_xdg_path_requires_an_absolute_directory(monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", "relative")
    with pytest.raises(ConfigurationError, match="absolute"):
        credentials.credential_path()
    monkeypatch.delenv("XDG_CONFIG_HOME")
    assert credentials.credential_path().parts[-3:] == (".config", "etl-craft", "credentials")


def test_auth_settings_validate_lifetime_and_issuer(tmp_path):
    raw = {
        "Secrets": {"Source_type": "environment"},
        "Orchestration": {"Mode": "local"},
        "Engine": {"dev": {"jdbc_url": "jdbc:sqlite:e.db", "schema": "main"}},
    }
    assert parse_config(raw, tmp_path / "c.yml").auth.session_hours == 12
    raw["Auth"] = {
        "Session_hours": 2,
        "Oidc_issuer": "https://issuer.example",
        "Oidc_client_id": "etl",
        "Airflow_connection_id": "craft",
    }
    assert parse_config(raw, tmp_path / "c.yml").auth == AuthConfig(
        2, "https://issuer.example", "etl", "craft"
    )
    for broken in (
        {"Session_hours": True},
        {"Session_hours": 0},
        {"Oidc_client_id": "etl"},
        {"Oidc_issuer": "http://issuer", "Oidc_client_id": "etl"},
        {"Oidc_issuer": "https://[malformed", "Oidc_client_id": "etl"},
    ):
        raw["Auth"] = broken
        with pytest.raises(ConfigurationError, match="Auth"):
            parse_config(raw, tmp_path / "c.yml")


def jwt(**changes):
    claims = {
        "iss": "https://issuer",
        "aud": "etl",
        "sub": "person-id",
        "nonce": "nonce",
        "iat": time.time() - 1,
        "exp": time.time() + 300,
        **changes,
    }

    def encode(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")

    return encode({"alg": "RS256"}) + "." + encode(claims) + ".signature"


@pytest.mark.parametrize(
    "claims",
    [
        {"iss": "https://other"},
        {"aud": "other"},
        {"aud": ["etl", "other"]},
        {"nonce": "wrong"},
        {"azp": "other"},
        {"exp": 1},
        {"exp": True},
        {"iat": time.time() + 1000},
        {"sub": ""},
    ],
)
def test_device_token_requires_issuer_client_nonce_and_fresh_lifetime(claims):
    with pytest.raises(ConfigurationError):
        oidc._subject(jwt(**claims), "https://issuer", "etl", "nonce")


def test_device_polling_obeys_pending_and_slow_down(monkeypatch):
    replies = iter([{"error": "authorization_pending"}, {"error": "slow_down"}, {"id_token": "id"}])
    intervals = []
    monkeypatch.setattr(oidc.interrupts, "sleep", intervals.append)
    monkeypatch.setattr(oidc, "_request", lambda *a: next(replies))
    assert oidc._poll("https://issuer/token", {}, 60, 2) == {"id_token": "id"}
    assert intervals == [2, 2, 7]


@pytest.mark.parametrize(
    "reply", [{"error": "access_denied"}, {"error": "expired_token"}, {"error": "invalid_client"}]
)
def test_device_polling_stops_on_refusal(monkeypatch, reply):
    monkeypatch.setattr(oidc.interrupts, "sleep", lambda _: None)
    monkeypatch.setattr(oidc, "_request", lambda *a: reply)
    with pytest.raises(ConfigurationError, match="refused or expired"):
        oidc._poll("https://issuer/token", {}, 60, 1)


def test_device_polling_never_exceeds_the_code_lifetime():
    with pytest.raises(ConfigurationError, match="expired"):
        oidc._poll("https://issuer/token", {}, 1, 5)


def test_oidc_never_accepts_insecure_or_credentialed_endpoints():
    for url in (
        "https://[malformed",
        "http://issuer/token",
        "https://user:secret@issuer/token",
        "https://issuer/#token",
        "oops",
    ):
        with pytest.raises(ConfigurationError, match="HTTPS"):
            oidc._request(url)


def test_failed_credential_replacement_keeps_the_previous_session(monkeypatch):
    import errno

    credentials.store_session("db", "existing")

    def read_only(*args):
        raise OSError(errno.EROFS, "Read-only file system")

    monkeypatch.setattr(credentials.os, "replace", read_only)
    with pytest.raises(ConfigurationError, match=r"write failed.*Read-only file system"):
        credentials.store_session("db", "replacement")
    assert credentials.read_session("db") == "existing"
    assert not list(credentials.credential_path().parent.glob(".credentials-*"))


def test_credentials_refuse_non_regular_files_and_wrong_owners(monkeypatch):
    credentials.store_session("db", "existing")
    owner = credentials.os.getuid()
    monkeypatch.setattr(credentials.os, "getuid", lambda: owner + 1)
    with pytest.raises(ConfigurationError, match="restore ownership"):
        credentials.read_session("db")
    monkeypatch.setattr(credentials.os, "getuid", lambda: owner)
    path = credentials.credential_path()
    path.unlink()
    path.mkdir()
    with pytest.raises(ConfigurationError, match="expected a regular file"):
        credentials.read_session("db")
