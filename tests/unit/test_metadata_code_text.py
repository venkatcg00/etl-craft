"""Metadata codes cannot become shell fragments or generated control steps."""

import pytest

from etl_craft.core import text

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("code", ["P", "load_2", "a" * 128])
def test_metadata_codes_accept_letters_digits_and_underscores(code):
    assert text.is_metadata_code(code)


@pytest.mark.parametrize(
    "code",
    [
        "",
        "2load",
        "__init__",
        "__finalize__",
        "load-task",
        "load $(touch x); echo",
        "x.y",
        "a" * 129,
        "P\n",
        "P\x00hidden",
        "éclair",
    ],
)
def test_metadata_codes_refuse_control_names_and_unsafe_text(code):
    assert not text.is_metadata_code(code)
