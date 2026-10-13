import json
import subprocess
import textwrap

import pytest

from fixtures.scripts import load

pytestmark = pytest.mark.unit

quality = load("check_quality")

PYPROJECT = """
[project]
name = "probe"
dependencies = ["SQLAlchemy>=2", "pyyaml>=6"]

[tool.ruff.lint.mccabe]
max-complexity = 15

[tool.ruff.lint.pylint]
max-args = 6
max-branches = 12
max-statements = 50
"""


def _project(tmp_path, files):
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    (root / "pyproject.toml").write_text(PYPROJECT, encoding="utf-8")
    for name, source in files.items():
        _write(root, name, source)
    return root


def _write(root, name, source):
    path = root / "src" / "etl_craft" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(source), encoding="utf-8")


def _scores(root, metric):
    measured = quality.measure(root)
    return {path.split("etl_craft/", 1)[1]: score for path, score in measured[metric].items()}


def _long_function(name, lines):
    """A function of ``lines`` lines holding one statement, so only its length is over a limit."""
    items = "".join("        1,\n" for _ in range(lines - 3))
    return f"def {name}():\n    return [\n{items}    ]\n"


def test_a_long_function_scores_its_lines_over_the_limit(tmp_path):
    root = _project(tmp_path, {"engine/long.py": _long_function("long", 90)})
    score = _scores(root, "function-lines")["engine/long.py"]
    assert score.value == 10
    assert score.details == ["long:1 90 lines (limit 80)"]


def test_complexity_branches_and_arguments_come_from_ruff(tmp_path):
    branches = "".join(f"    if a == {i}:\n        return {i}\n" for i in range(16))
    wide = "def wide(a, b, c, d, e, f, g, h):\n    return a\n"
    source = f"def branchy(a):\n{branches}    return -1\n\n\n{wide}"
    root = _project(tmp_path, {"engine/rules.py": source})
    assert _scores(root, "complexity")["engine/rules.py"].value == 2
    assert _scores(root, "branches")["engine/rules.py"].value == 4
    arguments = _scores(root, "arguments")["engine/rules.py"]
    assert arguments.value == 2
    assert arguments.details == ["wide:37 8 (limit 6)"]


def test_only_a_broad_except_that_swallows_the_error_counts(tmp_path):
    source = """
    def swallow():
        try:
            pass
        except Exception:
            return None

    def bare():
        try:
            pass
        except:
            pass

    def reraise():
        try:
            pass
        except Exception as error:
            raise RuntimeError("context") from error

    def specific():
        try:
            pass
        except KeyError:
            return None
    """
    root = _project(tmp_path, {"execution/errors.py": source})
    assert _scores(root, "broad-except")["execution/errors.py"].value == 2


def test_raising_a_builtin_exception_counts_but_not_not_implemented(tmp_path):
    source = """
    class MetadataError(Exception):
        pass

    def check(value):
        if value is None:
            raise ValueError("value is missing")
        if value == 1:
            raise MetadataError("bad value")
        raise NotImplementedError
    """
    root = _project(tmp_path, {"services/check.py": source})
    score = _scores(root, "builtin-raise")["services/check.py"]
    assert score.value == 1
    assert score.details == ["check:7 ValueError"]


def test_status_strings_count_outside_the_enums_module(tmp_path):
    used = '''
    def done(status):
        """Return whether the run is SUCCESS."""
        label = f"{status}SUCCESS"
        return status in ("SUCCESS", "FAILED")
    '''
    root = _project(tmp_path, {"engine/runs.py": used, "core/enums.py": 'SUCCESS = "SUCCESS"\n'})
    scores = _scores(root, "enum-literal")
    assert scores["engine/runs.py"].value == 2
    assert "core/enums.py" not in scores


def test_literal_sql_counts_only_in_the_engine_side_packages(tmp_path):
    engine = """
    from sqlalchemy import text

    def read(conn, settings):
        conn.execute(text("SELECT 1 AS one"))
        conn.exec_driver_sql("LISTEN etl_craft_events")
        conn.execute(text(f"SELECT * FROM {settings}"))
        return settings.text("Timezone")
    """
    dialect = 'from sqlalchemy import text\nQUERY = text("SELECT 1 AS one")\n'
    handler = 'from sqlalchemy import text\nQUERY = text("INSERT INTO t VALUES (1)")\n'
    root = _project(
        tmp_path,
        {
            "engine/reads.py": engine,
            "dialects/engine/reads.py": dialect,
            "handlers/sql/write.py": handler,
        },
    )
    assert {path: score.value for path, score in _scores(root, "inline-sql").items()} == {
        "engine/reads.py": 3
    }


def test_dialect_name_checks_count_outside_the_dialects_layer(tmp_path):
    check = 'def table(conn):\n    return "t" if conn.dialect.name == "postgresql" else "T"\n'
    root = _project(tmp_path, {"engine/names.py": check, "dialects/engine/names.py": check})
    assert set(_scores(root, "dialect-branch")) == {"engine/names.py"}


def test_suppressions_casts_any_and_private_imports_count(tmp_path):
    source = """
    from typing import Any, cast

    from etl_craft.execution.runner import _preflight, run_task

    def loose(value: Any) -> Any:  # noqa: ANN401
        return cast(int, value)  # type: ignore[no-any-return]

    def untested():  # pragma: no cover
        return _preflight, run_task
    """
    root = _project(tmp_path, {"services/loose.py": source})
    counts = {
        metric: _scores(root, metric)["services/loose.py"].value
        for metric in ("noqa", "type-ignore", "no-cover", "cast", "any-signature", "private-import")
    }
    assert counts == {
        "noqa": 1,
        "type-ignore": 1,
        "no-cover": 1,
        "cast": 1,
        "any-signature": 1,
        "private-import": 1,
    }


def test_the_baseline_holds_scores_and_keeps_improvements(tmp_path, capsys):
    root = _project(tmp_path, {"engine/long.py": _long_function("long", 90)})
    assert quality.main(["--update", "--root", str(root)]) == 0
    document = json.loads((root / "scripts/quality-baseline.json").read_text(encoding="utf-8"))
    assert document["metrics"] == {"function-lines": {"src/etl_craft/engine/long.py": 10}}
    assert document["dependencies"] == ["pyyaml", "sqlalchemy"]
    assert quality.main(["--root", str(root)]) == 0

    _write(root, "engine/long.py", _long_function("long", 95))
    capsys.readouterr()
    assert quality.main(["--root", str(root)]) == 1
    out = capsys.readouterr().out
    assert "function-lines: src/etl_craft/engine/long.py scores 15, baseline 10" in out
    assert "long:1 95 lines (limit 80)" in out
    assert "quality-exception" in out

    _write(root, "engine/long.py", _long_function("long", 40))
    assert quality.main(["--root", str(root)]) == 1
    assert "make quality-baseline" in capsys.readouterr().out


def test_a_new_core_dependency_is_refused(tmp_path, capsys):
    root = _project(tmp_path, {"core/ok.py": "VALUE = 1\n"})
    assert quality.main(["--update", "--root", str(root)]) == 0
    pyproject = root / "pyproject.toml"
    pyproject.write_text(
        pyproject.read_text(encoding="utf-8").replace('"pyyaml>=6"', '"pyyaml>=6", "watchdog>=6"'),
        encoding="utf-8",
    )
    capsys.readouterr()
    assert quality.main(["--root", str(root)]) == 1
    assert "new core dependency watchdog" in capsys.readouterr().out


def _git(root, *args):
    identity = ["-c", "user.name=t", "-c", "user.email=t@t"]
    subprocess.run(["git", *identity, *args], cwd=root, check=True, capture_output=True)


def test_compare_to_refuses_a_baseline_that_grew_unless_allowed(tmp_path, capsys):
    root = _project(tmp_path, {"engine/long.py": _long_function("long", 90)})
    _git(root, "init", "--quiet")
    _git(root, "commit", "--quiet", "--allow-empty", "-m", "before the baseline")
    assert quality.main(["--compare-to", "HEAD", "--root", str(root)]) == 0
    assert "has no baseline" in capsys.readouterr().out

    quality.main(["--update", "--root", str(root)])
    _git(root, "add", "scripts/quality-baseline.json")
    _git(root, "commit", "--quiet", "-m", "baseline")
    assert quality.main(["--compare-to", "HEAD", "--root", str(root)]) == 0

    _write(root, "engine/long.py", _long_function("long", 99))
    quality.main(["--update", "--root", str(root)])
    capsys.readouterr()
    assert quality.main(["--compare-to", "HEAD", "--root", str(root)]) == 1
    assert "function-lines: src/etl_craft/engine/long.py scores 19, baseline 10" in (
        capsys.readouterr().out
    )
    assert quality.main(["--compare-to", "HEAD", "--allow-growth", "--root", str(root)]) == 0

    _write(root, "engine/long.py", _long_function("long", 20))
    quality.main(["--update", "--root", str(root)])
    assert quality.main(["--compare-to", "HEAD", "--root", str(root)]) == 0
