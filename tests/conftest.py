"""Suite-wide pytest configuration: evidence, the suite-marker rule, Engine DB fixtures."""

pytest_plugins = [
    "pytester",
    "plugins.evidence",
    "plugins.ci_shards",
    "plugins.suites",
    "plugins.demo_diagnostics",
    "fixtures.engine_db",
    "fixtures.retention",
    "fixtures.cli_project",
    "fixtures.cluster",
    "fixtures.pool_contract",
    "fixtures.metadata_project",
    "fixtures.sql_warehouse",
    "fixtures.smtp",
    "fixtures.oidc",
]
