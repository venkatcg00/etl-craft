#!/usr/bin/env bash
# Bring a fresh Claude Code cloud container to the working state of the etl-craft rewrite.
#
# Usage (run it from inside the clone; the script itself may live anywhere):
#   bash /tmp/handoff/continue.sh                   # sync, docker, services, make check
#   SKIP_SERVICES=1 bash /tmp/handoff/continue.sh   # no docker: unit checks only
#
# It is safe to run more than once.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(git rev-parse --show-toplevel)"

step() { printf '\n==> %s\n' "$*"; }

step "Fetch main and the in-flight branches"
git fetch origin main
git fetch origin docs/github-pages || echo "docs/github-pages is gone (merged or deleted)"

step "Install every dependency group into .venv"
make sync

if [[ "${SKIP_SERVICES:-0}" != "1" ]]; then
    step "Start dockerd if it is not running"
    if ! docker info >/dev/null 2>&1; then
        nohup dockerd >/tmp/dockerd.log 2>&1 &
        for _ in $(seq 1 60); do
            docker info >/dev/null 2>&1 && break
            sleep 1
        done
        docker info >/dev/null 2>&1 || { echo "dockerd did not start; see /tmp/dockerd.log" >&2; exit 1; }
    fi

    step "Pull the compose images (Docker Hub, falling back to mirror.gcr.io)"
    # Docker Hub rate-limits this sandbox (HTTP 429) and quay.io is unreachable.
    # mirror.gcr.io serves Docker Hub images; official images live under library/.
    docker compose config --images | sort -u | while read -r image; do
        if docker image inspect "$image" >/dev/null 2>&1; then
            echo "present: $image"
            continue
        fi
        if docker pull -q "$image"; then
            continue
        fi
        mirror="$image"
        [[ "$image" != */* ]] && mirror="library/$image"
        docker pull -q "mirror.gcr.io/$mirror"
        docker tag "mirror.gcr.io/$mirror" "$image"
    done

    step "Generate the test certificates and start the services"
    make certs services-up

    step "Check every local service"
    make test-harness
fi

step "Run the full local gate"
make check

step "Current state"
git status --short --branch
git log --oneline -3 origin/main
echo
echo "Ready. Read $here/HANDOFF.md for what to do next."
