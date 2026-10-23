"""A stalled cloud statement fails with its identity and restores the enclosing timer."""

import signal

import pytest

from acceptance.cloud.conftest import statement_deadline

pytestmark = pytest.mark.unit


def test_cloud_statement_deadline_names_the_query_without_its_values():
    previous = signal.getsignal(signal.SIGALRM)
    with (
        pytest.raises(pytest.fail.Exception) as error,
        statement_deadline("SNOWFLAKE", "SELECT 'private-value' AS password"),
    ):
        signal.raise_signal(signal.SIGALRM)
    message = str(error.value)
    assert "SNOWFLAKE" in message and "statement exceeded 180s" in message
    assert "SELECT [REDACTED] AS password" in message
    assert "private-value" not in message
    assert "inspect its query queue" in message
    assert signal.getsignal(signal.SIGALRM) == previous
