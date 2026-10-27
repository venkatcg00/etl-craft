#!/bin/sh
# Run beside a configured craft-connector.yml before any active administrator exists.
# Supply BOOTSTRAP_ADMIN_PASSWORD from the deployment secret manager; never enable shell tracing.
set -eu
: "${BOOTSTRAP_ADMIN_PASSWORD:?Set the first admin password through the secret manager}"
printf '%s\n' "$BOOTSTRAP_ADMIN_PASSWORD" | etl-craft setup --admin-name owner --password-stdin
