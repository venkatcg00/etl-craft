#!/usr/bin/env bash
# Publish the documentation site to the gh-pages branch with mike.
#
# Usage: scripts/publish_docs.sh REF [--push]
#
#   refs/heads/main   publishes version "dev". It is the default version until a release is
#                     published.
#   refs/tags/vX.Y.Z  publishes version "X.Y" with the alias "latest", and makes "latest" the
#                     default version.
#
# Without --push the commit stays on the local gh-pages branch; `uv run mike serve` previews it.
set -euo pipefail

if [[ $# -lt 1 || $# -gt 2 ]]; then
    echo "usage: scripts/publish_docs.sh REF [--push]" >&2
    exit 2
fi
ref="$1"
push=()
if [[ "${2:-}" == "--push" ]]; then
    push=(--push)
elif [[ -n "${2:-}" ]]; then
    echo "unknown option: $2" >&2
    exit 2
fi

mike=(uv run mike)

case "$ref" in
    refs/heads/main)
        "${mike[@]}" deploy ${push[@]+"${push[@]}"} --update-aliases dev
        if ! "${mike[@]}" list latest >/dev/null 2>&1; then
            "${mike[@]}" set-default ${push[@]+"${push[@]}"} dev
        fi
        ;;
    refs/tags/v*)
        release="${ref#refs/tags/v}"
        if [[ ! "$release" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
            echo "not a release tag: $ref" >&2
            exit 2
        fi
        version="${release%.*}"
        "${mike[@]}" deploy ${push[@]+"${push[@]}"} --update-aliases "$version" latest
        "${mike[@]}" set-default ${push[@]+"${push[@]}"} latest
        ;;
    *)
        echo "no documentation version is published for $ref" >&2
        exit 2
        ;;
esac
