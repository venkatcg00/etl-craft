"""The OIDC device grant against an actual local TLS issuer."""

import base64
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs

import pytest

from fixtures.tls import localhost_tls


@pytest.fixture
def issuer(tmp_path, monkeypatch):
    from etl_craft.config.model import AuthConfig

    context, certificate = localhost_tls(tmp_path)
    state = {"nonce": None, "requests": [], "redirect": False, "wrong_issuer": False}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if state["redirect"]:
                self.send_response(302)
                self.send_header("Location", "http://localhost/insecure")
                self.end_headers()
                return
            self.reply(
                {
                    "issuer": "https://other" if state["wrong_issuer"] else address,
                    "device_authorization_endpoint": address + "/device",
                    "token_endpoint": address + "/token",
                }
            )

        def do_POST(self):
            data = parse_qs(self.rfile.read(int(self.headers["Content-Length"])).decode())
            state["requests"].append((self.path, data))
            if self.path == "/device":
                state["nonce"] = data["nonce"][0]
                self.reply(
                    {
                        "device_code": "private-device-code",
                        "user_code": "ABCD-EFGH",
                        "verification_uri": address + "/verify",
                        "expires_in": 30,
                        "interval": 1,
                    }
                )
            else:

                def encode(obj):
                    return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")

                claims = {
                    "iss": address,
                    "aud": "etl",
                    "sub": "known-subject",
                    "nonce": state["nonce"],
                    "iat": time.time() - 1,
                    "exp": time.time() + 60,
                }
                self.reply(
                    {"id_token": encode({"alg": "RS256"}) + "." + encode(claims) + ".signature"}
                )

        def reply(self, value):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(value).encode())

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("localhost", 0), Handler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    address = f"https://localhost:{server.server_port}"
    monkeypatch.setenv("SSL_CERT_FILE", str(certificate))
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield AuthConfig(12, address, "etl"), state
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()
