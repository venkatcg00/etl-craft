import os
import signal
import subprocess
import sys
import threading
import time

import pytest

from etl_craft.execution.supervisor import ChildResult, ChildSpec, run_child

pytestmark = pytest.mark.unit


def python(code: str, **kwargs) -> ChildSpec:
    return ChildSpec(argv=(sys.executable, "-c", code), **kwargs)


def _alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:  # pragma: no cover - a reused pid owned by someone else
        return True
    return True


def _wait_until_gone(pid: int, seconds: float = 5.0) -> bool:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if not _alive(pid):
            return True
        time.sleep(0.05)
    return False


def test_a_successful_child():
    result = run_child(python("print('hello')"))
    assert result.succeeded
    assert (result.returncode, result.timed_out) == (0, False)
    assert result.output_tail == "hello\n"
    assert result.describe() == "exited with code 0"
    assert result.elapsed_seconds >= 0


def test_a_failing_child_keeps_its_exit_code_and_stderr():
    result = run_child(python("import sys; print('out'); sys.exit('boom')"))
    assert not result.succeeded
    assert result.returncode == 1
    assert result.describe() == "exited with code 1"
    assert "out\n" in result.output_tail
    assert "boom" in result.output_tail


def test_a_child_killed_by_a_signal_reports_the_signal():
    result = run_child(python("import os, signal; os.kill(os.getpid(), signal.SIGKILL)"))
    assert result.returncode == -signal.SIGKILL
    assert not result.succeeded
    assert result.describe() == "was killed by signal SIGKILL"


def test_an_unknown_signal_number_is_described_by_number():
    result = ChildResult(
        spec=python(""), returncode=-999, timed_out=False, elapsed_seconds=0, output_tail=""
    )
    assert result.describe() == "was killed by signal 999"


def test_a_child_past_its_time_limit_is_stopped():
    started = time.monotonic()
    result = run_child(
        python("import time; print('started', flush=True); time.sleep(60)", timeout_seconds=0.5),
        kill_grace_seconds=5,
    )
    assert time.monotonic() - started < 30
    assert result.timed_out
    assert not result.succeeded
    assert result.describe() == "timed out after 0.5s and was killed"
    assert result.output_tail == "started\n"


def test_a_child_that_ignores_sigterm_is_killed_after_the_grace_period():
    code = (
        "import signal, time\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "print('ready', flush=True)\n"
        "time.sleep(60)\n"
    )
    result = run_child(python(code, timeout_seconds=1), kill_grace_seconds=0.5)
    assert result.timed_out
    assert result.returncode == -signal.SIGKILL


def test_a_timeout_stops_everything_the_child_started(tmp_path):
    pid_file = tmp_path / "grandchild.pid"
    code = (
        "import subprocess, sys, time\n"
        "g = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])\n"
        f"open({str(pid_file)!r}, 'w').write(str(g.pid))\n"
        "time.sleep(60)\n"
    )
    result = run_child(python(code, timeout_seconds=2), kill_grace_seconds=2)
    assert result.timed_out
    assert _wait_until_gone(int(pid_file.read_text()))


def test_no_time_limit_when_zero():
    assert run_child(python("print(1)", timeout_seconds=0)).succeeded


def test_output_is_appended_to_the_log_file_and_the_tail_covers_this_run(tmp_path):
    log = tmp_path / "logs" / "task.log"
    log.parent.mkdir()
    log.write_text("previous attempt\n", encoding="utf-8")
    result = run_child(python("print('this attempt')", log_path=log))
    assert result.output_tail == "this attempt\n"
    assert log.read_text(encoding="utf-8") == "previous attempt\nthis attempt\n"


def test_the_log_directory_is_created(tmp_path):
    log = tmp_path / "a" / "b" / "task.log"
    run_child(python("print('x')", log_path=log))
    assert log.read_text(encoding="utf-8") == "x\n"


def test_the_tail_keeps_only_the_last_bytes_and_replaces_invalid_utf8():
    code = "import sys; sys.stdout.buffer.write(b'a' * 1000 + b'\\xff' + b'end')"
    result = run_child(python(code), tail_bytes=5)
    assert result.output_tail == "a\ufffdend"


def test_the_child_gets_the_given_environment_and_directory(tmp_path):
    env = {**os.environ, "ETL_CRAFT_SUPERVISOR_TEST": "yes"}
    code = "import os; print(os.environ['ETL_CRAFT_SUPERVISOR_TEST'], os.getcwd())"
    result = run_child(python(code, env=env, cwd=tmp_path))
    value, cwd = result.output_tail.split()
    assert value == "yes"
    assert os.path.samefile(cwd, tmp_path)


def test_the_child_reads_no_input():
    result = run_child(python("import sys; print(repr(sys.stdin.read()))"))
    assert result.output_tail == "''\n"


def test_the_child_leads_its_own_session():
    result = run_child(python("import os; print(os.getsid(0) == os.getpid())"))
    assert result.output_tail == "True\n"


def test_an_interrupted_wait_stops_the_child_and_propagates(monkeypatch, tmp_path):
    pid_file = tmp_path / "child.pid"
    real_wait = subprocess.Popen.wait
    calls = []

    def interrupted_wait(self, timeout=None):
        if not calls:
            calls.append(self.pid)
            while not pid_file.exists():
                time.sleep(0.01)
            raise KeyboardInterrupt
        return real_wait(self, timeout=timeout)

    monkeypatch.setattr(subprocess.Popen, "wait", interrupted_wait)
    code = f"import os, time; open({str(pid_file)!r}, 'w').write('x'); time.sleep(60)"
    with pytest.raises(KeyboardInterrupt):
        run_child(python(code), kill_grace_seconds=5)
    assert _wait_until_gone(calls[0])


def test_a_cancelled_child_is_stopped():
    cancel = threading.Event()
    timer = threading.Timer(0.3, cancel.set)
    timer.start()
    started = time.monotonic()
    result = run_child(
        python("import time; time.sleep(30)", timeout_seconds=20),
        kill_grace_seconds=1,
        cancel=cancel,
    )
    assert result.cancelled and not result.timed_out
    assert result.describe() == "was stopped because the run was interrupted"
    assert time.monotonic() - started < 10


def test_a_cancellable_child_still_times_out_and_finishes():
    cancel = threading.Event()
    slow = run_child(
        python("import time; time.sleep(30)", timeout_seconds=0.5),
        kill_grace_seconds=1,
        cancel=cancel,
    )
    assert slow.timed_out and not slow.cancelled
    quick = run_child(python("print('done')"), cancel=cancel)
    assert quick.succeeded and quick.output_tail.strip() == "done"
