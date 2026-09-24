import tomllib
from pathlib import Path

import pytest

from plugins.suites import suite_markers

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]


def registered_markers() -> set[str]:
    with (REPO_ROOT / "pyproject.toml").open("rb") as handle:
        lines = tomllib.load(handle)["tool"]["pytest"]["ini_options"]["markers"]
    return {line.split(":", 1)[0].strip() for line in lines}


def test_every_suite_marker_is_registered_with_pytest():
    assert registered_markers() == set(suite_markers())


def test_collection_stops_on_a_test_without_a_suite_marker(pytester):
    pytester.makepyfile(
        test_mix="""
        import pytest

        @pytest.mark.unit
        def test_marked():
            pass

        def test_forgotten():
            pass
        """
    )
    result = pytester.runpytest("-p", "plugins.suites")
    assert result.ret == pytest.ExitCode.USAGE_ERROR
    result.stderr.fnmatch_lines(["*unmarked: test_mix.py::test_forgotten*"])


def test_marked_tests_run_normally(pytester):
    pytester.makepyfile(
        test_marked="""
        import pytest

        pytestmark = pytest.mark.harness

        def test_one():
            pass
        """
    )
    result = pytester.runpytest("-p", "plugins.suites")
    result.assert_outcomes(passed=1)
