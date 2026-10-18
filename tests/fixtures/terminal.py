"""Drive the installed CLI's actual terminal prompts without bypassing authentication."""

import errno
import os
import pty
import select
import subprocess
import time


def terminal_run(argv, *, cwd, env, answers, timeout=60):
    master, slave = pty.openpty()
    process = subprocess.Popen(argv, cwd=cwd, env=env, stdin=slave, stdout=slave, stderr=slave)
    os.close(slave)
    output = b""
    pending = iter(answers)
    prompt = next(pending, None)
    seen = 0
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            if select.select([master], [], [], 0.1)[0]:
                try:
                    data = os.read(master, 65536)
                except OSError as error:
                    if error.errno != errno.EIO:
                        raise
                    break
                if not data:
                    break
                output += data
                if prompt and prompt[0].encode() in output[seen:]:
                    os.write(master, (prompt[1] + "\n").encode())
                    seen = len(output)
                    prompt = next(pending, None)
            elif process.poll() is not None:
                break
        else:
            raise AssertionError(
                f"interactive command timed out: {output.decode(errors='replace')}"
            )
        code = process.wait(timeout=5)
        assert prompt is None, output.decode(errors="replace")
        return subprocess.CompletedProcess(
            argv, code, output.decode(errors="replace").replace("\r\n", "\n"), ""
        )
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        os.close(master)


def setup_answers():
    return [
        ("admin account name: ", "demo-admin"),
        ("Password: ", "demo password for tests"),
        ("Confirm password: ", "demo password for tests"),
    ]


def login_answers():
    return [("Account name: ", "demo-admin"), ("Password: ", "demo password for tests")]
