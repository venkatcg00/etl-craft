#!/bin/sh
# Run as a deployment administrator; environment fields are not config-file settings.
set -eu
etl-craft project set --project sales --environment-type DEV --environment-name perf --governance RELAXED --reason 'Development testing'
etl-craft project list --reason 'Review environments'
# Production always uses STRICT, including after a development project is promoted.
etl-craft project set --project sales --environment-type PROD --governance STRICT --reason 'Production promotion'
