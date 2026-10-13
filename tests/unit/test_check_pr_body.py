import pytest

from fixtures.scripts import load

pytestmark = pytest.mark.unit

pr_body = load("check_pr_body")

TEMPLATE = pr_body.TEMPLATE.read_text(encoding="utf-8")
FILLED = {
    "Summary": "Adds the thing.",
    "Roadmap item": "S5.R Consolidate before the cluster\n\n- [x] baseline shrinks",
    "Decisions": "Kept the module layout.",
    "Concerns and deviations": "None",
    "New public surface and dependencies": "None",
    "Validation": "make check: 3,600 passed, 96 skipped, 94% coverage, 26 min.",
    "Handover": "None",
}


def _body(**overrides):
    sections = {**FILLED, **overrides}
    return "\n\n".join(
        f"## {heading}\n\n{text}" for heading, text in sections.items() if text is not None
    )


def test_the_template_defines_the_sections_this_check_expects():
    assert list(pr_body.sections(TEMPLATE)) == list(FILLED)


def test_a_filled_in_description_passes():
    assert pr_body.problems(_body(), TEMPLATE) == []


def test_a_missing_section_is_reported():
    assert pr_body.problems(_body(Decisions=None), TEMPLATE) == ['missing section "## Decisions"']


def test_a_section_left_with_only_the_template_comment_is_empty():
    body = _body(Handover='<!-- What the next item needs to know, or "None". -->')
    assert pr_body.problems(body, TEMPLATE) == [
        'section "## Handover" is empty; write "None" if nothing applies'
    ]


def test_every_done_when_point_must_be_ticked():
    roadmap = "S5.R\n\n- [x] no behaviour change\n- [ ] baseline shrinks"
    assert pr_body.problems(_body(**{"Roadmap item": roadmap}), TEMPLATE) == [
        '"Done when" point not met: baseline shrinks'
    ]


def test_main_reads_the_description_from_the_environment(monkeypatch, capsys):
    monkeypatch.setenv("PR_BODY", _body(Validation=""))
    assert pr_body.main([]) == 1
    assert 'section "## Validation" is empty' in capsys.readouterr().out
    monkeypatch.setenv("PR_BODY", _body())
    assert pr_body.main([]) == 0


def test_a_template_without_the_checklist_section_is_a_usage_error(tmp_path, monkeypatch):
    template = tmp_path / "template.md"
    template.write_text("## Summary\n", encoding="utf-8")
    monkeypatch.setenv("PR_BODY", "## Summary\n\ntext")
    assert pr_body.main(["--template", str(template)]) == 2
