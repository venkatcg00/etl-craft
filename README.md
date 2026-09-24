# etl-craft

A metadata-driven ETL orchestration engine. Pipelines, tasks and their dependencies are rows in
an Engine DB; etl-craft reads that metadata and runs it against one warehouse. It works with or
without an external orchestrator.

> **Status: pre-release rewrite.** `main` is being rebuilt in a layered, documented
> structure, ahead of the first release (0.1.0). Only `etl-craft --version` works so
> far. The previous implementation remains available at the `archive/iteration-2` tag.
> The rewrite plan and its progress are in
> [docs/development/rewrite-plan.md](docs/development/rewrite-plan.md).

## Install from source

Requires Python 3.11 or newer and [uv](https://docs.astral.sh/uv/).

```bash
uv build                                        # dist/etl_craft-<version>-py3-none-any.whl
pip install dist/etl_craft-*.whl                # or: uv pip install dist/etl_craft-*.whl
etl-craft --version
```

## Development

```bash
make sync     # create .venv with every dependency group
make check    # lint, format check, mypy --strict, layer contracts, history gate, tests
make verify-package   # build, then install with pip and uv into clean environments
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the branch workflow and conventions.

## License

Apache License 2.0. See [LICENSE](LICENSE).
