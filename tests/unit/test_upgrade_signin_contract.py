"""The upgrade-only password proof stays independent of later account migrations."""

from importlib.resources import files

import pytest
import sqlglot
from sqlglot import exp

pytestmark = pytest.mark.unit


def test_upgrade_signin_reads_only_the_original_account_contract():
    query = (
        files("etl_craft.dialects.engine")
        .joinpath("queries/account_upgrade_signin.sql")
        .read_text()
    )
    selected = sqlglot.parse_one(query)
    assert {column.name for column in selected.find_all(exp.Column)} == {
        "ACCOUNT_NAME",
        "PASSWORD_HASH",
        "ROLE",
        "ACTIVE_FLAG",
    }
    assert not list(selected.find_all(exp.Star))
