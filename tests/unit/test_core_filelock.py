import subprocess
import sys
import threading
import time

import pytest

from etl_craft.core.errors import LockTimeoutError
from etl_craft.core.filelock import file_lock

pytestmark = pytest.mark.unit


def test_the_lock_creates_its_file(tmp_path):
    path = tmp_path / "engine.db.migrate.lock"
    with file_lock(path):
        assert path.exists()


def test_a_held_lock_times_out_another_holder(tmp_path):
    path = tmp_path / "x.lock"
    with (
        file_lock(path),
        pytest.raises(LockTimeoutError, match=r"timed out after 0.2s"),
        file_lock(path, wait_seconds=0.2),
    ):
        pass  # pragma: no cover - the lock is never acquired


def test_the_lock_is_released_after_the_block(tmp_path):
    path = tmp_path / "x.lock"
    with file_lock(path):
        pass
    with file_lock(path, wait_seconds=0.2):
        pass


def test_the_lock_is_released_when_the_block_raises(tmp_path):
    path = tmp_path / "x.lock"
    with pytest.raises(RuntimeError), file_lock(path):
        raise RuntimeError
    with file_lock(path, wait_seconds=0.2):
        pass


def test_zero_wait_waits_until_the_holder_releases(tmp_path):
    path = tmp_path / "x.lock"
    held = threading.Event()
    release = threading.Event()

    def holder():
        with file_lock(path):
            held.set()
            release.wait()

    thread = threading.Thread(target=holder)
    thread.start()
    held.wait()
    threading.Timer(0.3, release.set).start()
    started = time.monotonic()
    with file_lock(path, wait_seconds=0):
        waited = time.monotonic() - started
    thread.join()
    assert waited >= 0.2


def test_the_lock_excludes_other_processes_and_dies_with_its_holder(tmp_path):
    path = tmp_path / "x.lock"
    code = (
        "import sys, time\n"
        "from etl_craft.core.filelock import file_lock\n"
        f"with file_lock({str(path)!r}):\n"
        "    print('held', flush=True)\n"
        "    time.sleep(60)\n"
    )
    holder = subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline() == "held\n"
        with pytest.raises(LockTimeoutError), file_lock(path, wait_seconds=0.2):
            pass  # pragma: no cover - the lock is never acquired
    finally:
        holder.kill()
        holder.wait()
        if holder.stdout is not None:
            holder.stdout.close()
    with file_lock(path, wait_seconds=2):
        pass
