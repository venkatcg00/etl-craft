"""Retry budgets, validation and nonretryable failures."""

from dataclasses import replace

import pytest

from etl_craft.config import DagDefaults
from etl_craft.core.errors import (
    ConfigurationError,
    HandlerError,
    MetadataError,
    SqlGuardError,
    UsageError,
)
from etl_craft.execution.retries import RetryPolicy, task_retry_policy
from unit.test_execution_limits import CONFIG

pytestmark = pytest.mark.unit


def test_defaults_overrides_and_capped_backoff():
    assert task_retry_policy({}, CONFIG) == RetryPolicy(0, 60, 2.0)
    config = replace(CONFIG, dag_defaults=DagDefaults(retries=3))
    assert task_retry_policy({}, config).retries == 3
    assert task_retry_policy({"RETRIES": "0", "RETRY_DELAY_SECONDS": "0"}, config) == RetryPolicy(
        0, 0, 2.0
    )
    policy = RetryPolicy(10000, 60, 2.0)
    assert [policy.delay(n) for n in (1, 2, 3, 7, 10000)] == [60, 120, 240, 3600, 3600]
    assert RetryPolicy(2, 0, 2).delay(10000) == 0


@pytest.mark.parametrize(
    "params",
    [
        {"RETRIES": "-1"},
        {"RETRIES": "1.5"},
        {"RETRY_DELAY_SECONDS": "no"},
        {"RETRY_DELAY_SECONDS": "-1"},
        {"RETRY_BACKOFF": "0.5"},
        {"RETRY_BACKOFF": "nan"},
        {"RETRY_BACKOFF": "inf"},
        {"RETRY_BACKOFF": "no"},
    ],
)
def test_invalid_parameters_fail_before_admission(params):
    with pytest.raises(MetadataError, match="CFG_TASK_PARAMETERS"):
        task_retry_policy(params, CONFIG)


def test_failure_classification():
    assert HandlerError.retryable
    assert not any(
        error.retryable for error in (ConfigurationError, MetadataError, UsageError, SqlGuardError)
    )
