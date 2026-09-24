#!/usr/bin/env bash
# Generate throwaway certificates for the local test services: a CA, a server certificate for
# postgres-tls, and a client certificate for the etl_craft database user.
#
# Usage: scripts/make_test_certs.sh [DIR]    (default .certs/; FORCE=1 regenerates)
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
out="${1:-$root/.certs}"

if [[ -f "$out/client.crt" && "${FORCE:-0}" != "1" ]]; then
    echo "test certificates already in $out"
    exit 0
fi

mkdir -p "$out"
cd "$out"
openssl_quiet() { openssl "$@" 2>/dev/null; }

openssl_quiet req -x509 -new -nodes -newkey rsa:2048 -days 3650 \
    -subj "/CN=etl-craft test CA" -keyout ca.key -out ca.crt

openssl_quiet req -new -nodes -newkey rsa:2048 -subj "/CN=localhost" \
    -keyout server.key -out server.csr
printf 'subjectAltName = DNS:localhost, DNS:postgres-tls, IP:127.0.0.1\nextendedKeyUsage = serverAuth\n' \
    > server.ext
openssl_quiet x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 \
    -extfile server.ext -out server.crt

openssl_quiet req -new -nodes -newkey rsa:2048 -subj "/CN=etl_craft" \
    -keyout client.key -out client.csr
printf 'extendedKeyUsage = clientAuth\n' > client.ext
openssl_quiet x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 3650 \
    -extfile client.ext -out client.crt

rm -f ./*.csr ./*.ext ./*.srl
chmod 0600 ./*.key
chmod 0644 ./*.crt
echo "test certificates written to $out"
