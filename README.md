# etl-craft

A metadata-driven ETL orchestration engine. Pipelines, tasks and their dependencies are rows in
an Engine DB; etl-craft reads that metadata and runs it against one warehouse. It works with or
without an external orchestrator.

Documentation: <https://venkatcg00.github.io/etl-craft/>

> **0.1.0** is the first release. The previous implementation remains available at the
> `archive/iteration-2` tag; the [changelog](CHANGELOG.md) lists what 0.1.0 holds.

## Try it

The [Quick Start](docs/quick-start.md) runs the Support Insights demo (`examples/demo`): five
pipelines on a SQLite Engine DB and a DuckDB warehouse, in about ten minutes. It sends its alerts
to a local Mailpit, which the Quick Start starts with one `docker run`.

```bash
git clone https://github.com/venkatcg00/etl-craft.git && cd etl-craft
python -m venv .venv && . .venv/bin/activate
pip install .                                   # extras: [trino], [databricks], [snowflake], [aws], [publish]
cd examples/demo
python prepare.py warehouse && etl-craft setup && python prepare.py metadata
etl-craft run --pipeline_code CLIENT_ALPHA
```

Requires Python 3.11 or newer. `uv build` builds the wheel and sdist into `dist/`.

## Development

```bash
make sync     # create .venv with every dependency group
make check    # lint, format check, mypy --strict, layer contracts, history gate, tests
make verify-package   # build, then install with pip and uv into clean environments
make docs     # build the documentation site into site/
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for the branch workflow and conventions.

## License

Apache License 2.0. See [LICENSE](LICENSE).
