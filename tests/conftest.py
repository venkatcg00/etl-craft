"""Suite-wide pytest configuration: evidence, the suite-marker rule, Engine DB fixtures."""

pytest_plugins = [
    "pytester",
    "plugins.evidence",
    "plugins.suites",
    "fixtures.engine_db",
    "fixtures.cli_project",
    "fixtures.metadata_project",
    "fixtures.sql_warehouse",
    "fixtures.smtp",
]
