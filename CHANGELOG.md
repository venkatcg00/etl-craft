# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versions follow
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Package skeleton for the rewrite: layered packages, `etl-craft --version`, tooling, CI and
  package verification with pip and uv.
- Local test services in `docker-compose.yml`: PostgreSQL with password and with
  client-certificate login, MinIO, an Iceberg REST catalog, Trino and Mailpit, with tests that
  prove each one works.
- Release evidence: every test belongs to a suite in `release/required-suites.toml`;
  `scripts/run_suite.py` records a suite's results, and `scripts/release_gate.py` reports whether
  the current commit is releasable.
- Documentation site built with MkDocs Material: overview, section skeleton, exit codes, and a
  Python API reference generated from the code. `make docs` builds it with `--strict`.
- The documentation site is deployed to GitHub Pages by the Docs workflow on every push to
  `main`, with a version selector: `dev` from `main`, and `X.Y` from each release line's
  newest tag, the newest aliased `latest`. `make docs-site` builds the same site locally.
