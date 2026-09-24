import io

import pytest

from etl_craft.cli.output import Output

pytestmark = pytest.mark.unit


def test_results_go_to_stdout_and_errors_to_stderr(capsys):
    out = Output()
    out.line("result")
    out.line()
    out.error("something broke")
    captured = capsys.readouterr()
    assert captured.out == "result\n\n"
    assert captured.err == "error: something broke\n"


def test_rows_are_tab_separated_with_none_as_an_empty_field():
    stdout = io.StringIO()
    Output(stdout=stdout).rows([("P1", "Daily load", "FULL"), ("P2", None, 3)])
    assert stdout.getvalue() == "P1\tDaily load\tFULL\nP2\t\t3\n"


def test_an_empty_result_is_one_parenthesised_line():
    stdout = io.StringIO()
    Output(stdout=stdout).empty("no active pipelines")
    assert stdout.getvalue() == "(no active pipelines)\n"


def test_given_streams_are_used_instead_of_the_process_streams(capsys):
    stdout, stderr = io.StringIO(), io.StringIO()
    out = Output(stdout, stderr)
    out.line("a")
    out.error("b")
    assert (stdout.getvalue(), stderr.getvalue()) == ("a\n", "error: b\n")
    assert capsys.readouterr() == ("", "")
