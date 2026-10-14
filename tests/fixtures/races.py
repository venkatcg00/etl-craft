"""Run two callers together at a named function boundary."""

import importlib
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from functools import wraps

import pytest


def two_at_once(fn_a, fn_b, *, at, timeout=10):
    """Patch ``module.function`` so each caller waits once, then return both results in order."""
    module_name, name = at.rsplit(".", 1)
    module = importlib.import_module(module_name)
    original = getattr(module, name)
    barrier = threading.Barrier(2, timeout=timeout)
    local = threading.local()

    @wraps(original)
    def together(*args, **kwargs):
        if not getattr(local, "arrived", False):
            local.arrived = True
            barrier.wait()
        return original(*args, **kwargs)

    with pytest.MonkeyPatch.context() as patch, ThreadPoolExecutor(max_workers=2) as pool:
        patch.setattr(module, name, together)
        futures = [pool.submit(fn) for fn in (fn_a, fn_b)]
        try:
            # Future waits include scheduling time before a caller reaches the barrier.
            return tuple(future.result(timeout=max(1, timeout * 2)) for future in futures)
        finally:
            barrier.abort()


@contextmanager
def paused_after(fn, *, owner, name, when):
    """Run a change and pause after a selected method call until the observer releases it."""
    reached, resume = threading.Event(), threading.Event()
    original = getattr(owner, name)

    @wraps(original)
    def paused(*args, **kwargs):
        result = original(*args, **kwargs)
        if when(*args, **kwargs):
            reached.set()
            assert resume.wait(timeout=10), f"observer did not release {name}"
        return result

    with pytest.MonkeyPatch.context() as patch, ThreadPoolExecutor(max_workers=1) as pool:
        patch.setattr(owner, name, paused)
        future = pool.submit(fn)
        try:
            assert reached.wait(timeout=10), f"change did not reach {name}"
            yield resume
        finally:
            resume.set()
            future.result(timeout=20)
