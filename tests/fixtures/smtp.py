"""A local SMTP relay with a test certificate, recipient refusals and captured messages."""

import shutil
import socket
import ssl
import subprocess
import threading
from types import SimpleNamespace

import pytest


@pytest.fixture
def tls_relay(tmp_path):
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("the local TLS relay needs openssl")
    certificate, key = tmp_path / "relay.crt", tmp_path / "relay.key"
    configuration = tmp_path / "relay.cnf"
    configuration.write_text(
        "[req]\nprompt = no\ndistinguished_name = dn\nx509_extensions = extensions\n"
        "[dn]\nCN = localhost\n[extensions]\nsubjectAltName = DNS:localhost\n"
        "basicConstraints = critical,CA:TRUE\nextendedKeyUsage = serverAuth\n",
        encoding="utf-8",
    )
    subprocess.run(
        [
            openssl,
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-sha256",
            "-config",
            str(configuration),
            "-keyout",
            str(key),
            "-out",
            str(certificate),
        ],
        check=True,
        capture_output=True,
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate, key)
    servers = []

    def start(mode="starttls", refused=()):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(0.2)
        stop = threading.Event()
        relay = SimpleNamespace(
            host="localhost",
            port=listener.getsockname()[1],
            ca_file=certificate,
            messages=[],
            commands=[],
            errors=[],
        )

        def serve(client):
            client.settimeout(5)
            if mode == "ssl":
                client = context.wrap_socket(client, server_side=True)
            stream = client.makefile("rb")
            try:
                client.sendall(b"220 localhost test relay\r\n")
                while command := stream.readline():
                    verb = command.split(b" ", 1)[0].strip().upper()
                    relay.commands.append(verb)
                    if verb in {b"EHLO", b"HELO"}:
                        client.sendall(b"250-localhost\r\n250-STARTTLS\r\n250 AUTH PLAIN\r\n")
                    elif verb == b"STARTTLS":
                        client.sendall(b"220 ready for TLS\r\n")
                        stream.close()
                        client = context.wrap_socket(client, server_side=True)
                        stream = client.makefile("rb")
                    elif verb == b"AUTH":
                        client.sendall(b"235 authenticated\r\n")
                    elif verb == b"RCPT" and any(
                        address.encode() in command for address in refused
                    ):
                        client.sendall(b"550 no such recipient\r\n")
                    elif verb == b"DATA":
                        client.sendall(b"354 send message\r\n")
                        lines = []
                        while line := stream.readline():
                            if line == b".\r\n":
                                break
                            lines.append(line[1:] if line.startswith(b"..") else line)
                        relay.messages.append(b"".join(lines))
                        client.sendall(b"250 accepted\r\n")
                    elif verb == b"QUIT":
                        client.sendall(b"221 closing\r\n")
                        break
                    else:
                        client.sendall(b"250 OK\r\n")
            finally:
                stream.close()
                client.close()

        def accept():
            while not stop.is_set():
                try:
                    client, _ = listener.accept()
                except TimeoutError:
                    continue
                except OSError:
                    if stop.is_set():
                        return
                    raise
                try:
                    serve(client)
                except (ssl.SSLError, ConnectionError):
                    client.close()
                except Exception as error:
                    relay.errors.append(error)
                    client.close()

        thread = threading.Thread(target=accept, daemon=True)
        thread.start()
        servers.append((stop, listener, thread, relay))
        return relay

    yield start
    for stop, listener, thread, relay in servers:
        stop.set()
        listener.close()
        thread.join(timeout=6)
        assert not thread.is_alive(), "the SMTP fixture did not stop"
        assert not relay.errors, relay.errors
