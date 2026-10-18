"""A temporary trusted localhost certificate for SMTP and OIDC protocol tests."""

import shutil
import ssl
import subprocess

import pytest


def localhost_tls(tmp_path):
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
    return context, certificate
