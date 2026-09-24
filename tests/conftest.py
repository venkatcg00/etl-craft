"""Suite-wide pytest configuration: evidence recording and the suite-marker rule."""

pytest_plugins = ["pytester", "plugins.evidence", "plugins.suites"]
