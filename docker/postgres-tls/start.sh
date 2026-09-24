#!/bin/bash
# Install the test certificates with the ownership PostgreSQL requires, then run the image's
# own entrypoint with TLS on and certificate-only authentication.
set -euo pipefail

tls=/var/lib/postgresql/tls
install -d -o postgres -g postgres -m 0700 "$tls"
install -o postgres -g postgres -m 0600 /certs/server.key "$tls/server.key"
install -o postgres -g postgres -m 0644 /certs/server.crt /certs/ca.crt "$tls/"

exec docker-entrypoint.sh postgres \
    -c ssl=on \
    -c ssl_cert_file="$tls/server.crt" \
    -c ssl_key_file="$tls/server.key" \
    -c ssl_ca_file="$tls/ca.crt" \
    -c hba_file=/etc/postgresql/pg_hba.conf
