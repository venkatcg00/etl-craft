"""Every command's and option's help is one sentence: a capital first, a full stop last."""

import argparse
import re

import pytest

from etl_craft.cli import build_parser

pytestmark = pytest.mark.unit


def helps(parser, where="etl-craft"):
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            for choice in action._choices_actions:
                yield f"{where} {choice.dest}", choice.help
            for name, sub in action.choices.items():
                yield from helps(sub, f"{where} {name}")
        elif action.help not in (None, argparse.SUPPRESS) and action.dest != "help":
            yield f"{where} {'/'.join(action.option_strings) or action.dest}", action.help


def test_every_help_line_is_one_sentence():
    wrong = [
        (where, text)
        for where, text in helps(build_parser())
        if not re.fullmatch(r"[A-Z$`(][^\n]*[.)]", text.strip())
        or re.search(r"\. [A-Z]", text.rstrip("."))
    ]
    assert wrong == []
