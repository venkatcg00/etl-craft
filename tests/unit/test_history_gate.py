import pytest

from fixtures.scripts import load

pytestmark = pytest.mark.unit

gate = load("check_no_history")

# Built by concatenation so this file does not trip the gate it tests.
TAG = "# [" + "DEVIATION] moved from the old module"
ITEM = "# fixes E" + "2-18"
PROVENANCE = "# per explicit " + "instruction from review"
DATED = "# Added (" + "2026-09-24) for the release"


@pytest.mark.parametrize(
    ("line", "kind"),
    [
        (TAG, "decision tag"),
        (ITEM, "review item id"),
        (PROVENANCE, "decision provenance"),
        (DATED, "dated note"),
    ],
)
def test_history_commentary_is_reported(tmp_path, line, kind):
    source = tmp_path / "module.py"
    source.write_text(f"x = 1\n{line}\n", encoding="utf-8")
    assert gate.check([source]) == [f"{source}:2: {kind}: {line}"]
    assert gate.main([str(source)]) == 1


def test_behaviour_comments_and_allowed_lines_pass(tmp_path, capsys):
    source = tmp_path / "module.py"
    source.write_text(
        "# Retries resume from the last failed task.\n"
        f"{TAG}  # history-gate: allow\n"
        "offset = '2023-01-01 00:00:00|timestamp'\n",
        encoding="utf-8",
    )
    assert gate.main([str(source)]) == 0
    assert capsys.readouterr().out == ""


def test_changelog_binary_and_unknown_files_are_skipped(tmp_path):
    changelog = tmp_path / "CHANGELOG.md"
    changelog.write_text(DATED + "\n", encoding="utf-8")
    binary = tmp_path / "data.py"
    binary.write_bytes(b"\xff\xfe" + TAG.encode("utf-8"))
    other = tmp_path / "image.png"
    other.write_text(TAG, encoding="utf-8")
    missing = tmp_path / "gone.py"
    assert gate.check([changelog, binary, other, missing]) == []


def test_with_no_paths_every_tracked_file_is_checked(monkeypatch, tmp_path):
    clean = tmp_path / "clean.md"
    clean.write_text("Describes behaviour.\n", encoding="utf-8")
    monkeypatch.setattr(gate, "tracked_files", lambda: [clean])
    assert gate.main([]) == 0
