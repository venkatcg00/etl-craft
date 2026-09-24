"""The OAuth client-credentials exchange and the AWS RDS IAM token."""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import ClassVar
from urllib.parse import parse_qs

import pytest

from etl_craft.core.errors import ConfigurationError
from etl_craft.dialects import credentials

pytestmark = pytest.mark.unit


class _TokenEndpoint(BaseHTTPRequestHandler):
    status = 200
    body: object = {"access_token": "t0k3n", "token_type": "Bearer"}
    requests: ClassVar[list[dict]] = []

    def do_POST(self):
        length = int(self.headers["Content-Length"])
        form = parse_qs(self.rfile.read(length).decode())
        type(self).requests.append({key: value[0] for key, value in form.items()})
        body = type(self).body
        payload = body.encode() if isinstance(body, str) else json.dumps(body).encode()
        self.send_response(type(self).status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *args):
        return None


@pytest.fixture
def token_endpoint():
    server = HTTPServer(("127.0.0.1", 0), _TokenEndpoint)
    _TokenEndpoint.status = 200
    _TokenEndpoint.body = {"access_token": "t0k3n", "token_type": "Bearer"}
    _TokenEndpoint.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/token", _TokenEndpoint
    server.shutdown()
    server.server_close()


def test_the_grant_is_posted_and_the_access_token_returned(token_endpoint):
    url, endpoint = token_endpoint
    assert credentials.client_credentials_token(url, "cid", "csecret", "sql") == "t0k3n"
    assert endpoint.requests == [
        {
            "grant_type": "client_credentials",
            "client_id": "cid",
            "client_secret": "csecret",
            "scope": "sql",
        }
    ]
    credentials.client_credentials_token(url, "cid", "csecret")
    assert "scope" not in endpoint.requests[1]


def test_a_refusal_is_reported_without_the_secret(token_endpoint):
    url, endpoint = token_endpoint
    endpoint.status = 401
    endpoint.body = {"error": "invalid_client", "error_description": "bad secret"}
    with pytest.raises(
        ConfigurationError, match=r"HTTP 401 \(invalid_client: bad secret\)"
    ) as info:
        credentials.client_credentials_token(url, "cid", "hunter2")
    assert "hunter2" not in str(info.value)


@pytest.mark.parametrize("body", [["not", "an", "object"], {"unrelated": 1}, "<html>busy</html>"])
def test_a_refusal_without_an_rfc_error_body(token_endpoint, body):
    url, endpoint = token_endpoint
    endpoint.status = 400
    endpoint.body = body
    with pytest.raises(ConfigurationError, match=r"HTTP 400 $"):
        credentials.client_credentials_token(url, "cid", "s")


@pytest.mark.parametrize("body", [{"token_type": "Bearer"}, {"access_token": ""}, ["x"]])
def test_the_answer_needs_an_access_token(token_endpoint, body):
    url, endpoint = token_endpoint
    endpoint.body = body
    with pytest.raises(ConfigurationError, match="had no access_token"):
        credentials.client_credentials_token(url, "cid", "s")


def test_an_unreachable_endpoint():
    with pytest.raises(ConfigurationError, match="failed"):
        credentials.client_credentials_token("http://127.0.0.1:9/token", "cid", "s")


@pytest.fixture
def fake_aws_credentials(monkeypatch):
    # generate_db_auth_token signs locally, so fake credentials make a real, well-formed token.
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAEXAMPLE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    for name in ("AWS_SESSION_TOKEN", "AWS_PROFILE", "AWS_CONFIG_FILE"):
        monkeypatch.delenv(name, raising=False)


def test_an_rds_token_is_signed_for_the_host_and_user(fake_aws_credentials):
    token = credentials.aws_rds_auth_token("db.example.com", 5432, "etl", "us-east-1")
    assert token.startswith("db.example.com:5432/?")
    assert "DBUser=etl" in token
    assert "X-Amz-Signature=" in token


def test_an_rds_token_signs_as_the_assumed_role(fake_aws_credentials, monkeypatch):
    import boto3

    assumed = []
    real_client = boto3.session.Session.client

    class FakeSts:
        def assume_role(self, RoleArn, RoleSessionName):  # noqa: N803 - boto3's names
            assumed.append((RoleArn, RoleSessionName))
            return {
                "Credentials": {
                    "AccessKeyId": "ASIAROLE",
                    "SecretAccessKey": "role-secret",
                    "SessionToken": "role-session",
                }
            }

    def client(self, name, *args, **kwargs):
        return FakeSts() if name == "sts" else real_client(self, name, *args, **kwargs)

    monkeypatch.setattr(boto3.session.Session, "client", client)
    token = credentials.aws_rds_auth_token(
        "db.example.com", 5432, "etl", "us-east-1", "arn:aws:iam::1:role/etl"
    )
    assert assumed == [("arn:aws:iam::1:role/etl", "etl-craft")]
    assert "X-Amz-Security-Token=role-session" in token
    assert "ASIAROLE" in token


def test_a_signing_failure_is_a_configuration_error(fake_aws_credentials, monkeypatch):
    import boto3

    def broken(self, *args, **kwargs):
        raise RuntimeError("no region endpoint")

    monkeypatch.setattr(boto3.session.Session, "client", broken)
    with pytest.raises(ConfigurationError, match="could not obtain an AWS IAM database token"):
        credentials.aws_rds_auth_token("db", 5432, "etl", "us-east-1")


def test_sts_without_boto3_names_the_extra(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def no_boto3(name, *args, **kwargs):
        if name == "boto3":
            raise ImportError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_boto3)
    with pytest.raises(ConfigurationError, match=r"etl-craft\[aws\]"):
        credentials.aws_rds_auth_token("db", 5432, "etl", "us-east-1")
